/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.master.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.RootTable.ZROOT_TABLET;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.metadata.TabletMutatorBase;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Handles upgrading from 2.0 to 2.1
 */
public class Upgrader9to10 implements Upgrader {

  private static final Logger log = LoggerFactory.getLogger(Upgrader9to10.class);

  public static final String ZROOT_TABLET_LOCATION = ZROOT_TABLET + "/location";
  public static final String ZROOT_TABLET_FUTURE_LOCATION = ZROOT_TABLET + "/future_location";
  public static final String ZROOT_TABLET_LAST_LOCATION = ZROOT_TABLET + "/lastlocation";
  public static final String ZROOT_TABLET_WALOGS = ZROOT_TABLET + "/walogs";
  public static final String ZROOT_TABLET_CURRENT_LOGS = ZROOT_TABLET + "/current_logs";
  public static final String ZROOT_TABLET_PATH = ZROOT_TABLET + "/dir";

  @Override
  public void upgradeZookeeper(ServerContext ctx) {
    upgradeRootTabletMetadata(ctx);
  }

  @Override
  public void upgradeMetadata(ServerContext ctx) {

  }

  private void upgradeRootTabletMetadata(ServerContext ctx) {
    String rootMetaSer = getFromZK(ctx, ZROOT_TABLET);

    if (rootMetaSer.isEmpty()) {
      String dir = getFromZK(ctx, ZROOT_TABLET_PATH);
      List<LogEntry> logs = getRootLogEntries(ctx);

      TServerInstance last = getLocation(ctx, ZROOT_TABLET_LAST_LOCATION);
      TServerInstance future = getLocation(ctx, ZROOT_TABLET_FUTURE_LOCATION);
      TServerInstance current = getLocation(ctx, ZROOT_TABLET_LOCATION);

      UpgradeMutator tabletMutator = new UpgradeMutator(ctx);

      tabletMutator.putPrevEndRow(RootTable.EXTENT.getPrevEndRow());

      tabletMutator.putDir(dir);

      if (last != null)
        tabletMutator.putLocation(last, LocationType.LAST);

      if (future != null)
        tabletMutator.putLocation(future, LocationType.FUTURE);

      if (current != null)
        tabletMutator.putLocation(current, LocationType.CURRENT);

      logs.forEach(tabletMutator::putWal);

      tabletMutator.mutate();
    }

    // this operation must be idempotent, so deleting after updating is very important

    delete(ctx, ZROOT_TABLET_CURRENT_LOGS);
    delete(ctx, ZROOT_TABLET_FUTURE_LOCATION);
    delete(ctx, ZROOT_TABLET_LAST_LOCATION);
    delete(ctx, ZROOT_TABLET_LOCATION);
    delete(ctx, ZROOT_TABLET_WALOGS);
    delete(ctx, ZROOT_TABLET_PATH);
  }

  private static class UpgradeMutator extends TabletMutatorBase {

    private ServerContext context;

    UpgradeMutator(ServerContext context) {
      super(context, RootTable.EXTENT);
      this.context = context;
    }

    @Override
    public void mutate() {
      Mutation mutation = getMutation();

      try {
        context.getZooReaderWriter().mutate(context.getZooKeeperRoot() + RootTable.ZROOT_TABLET,
            new byte[0], ZooUtil.PUBLIC, currVal -> {

              // Earlier, it was checked that root tablet metadata did not exists. However the
              // earlier check does handle race conditions. Race conditions are unexpected. This is
              // a sanity check when making the update in ZK using compare and set. If this fails
              // and its not a bug, then its likely some concurrency issue. For example two masters
              // concurrently running upgrade could cause this to fail.
              Preconditions.checkState(currVal.length == 0,
                  "Expected root tablet metadata to be empty!");

              RootTabletMetadata rtm = new RootTabletMetadata();

              rtm.update(mutation);

              String json = rtm.toJson();

              log.info("Upgrading root tablet metadata, writing following to ZK : \n {}", json);

              return json.getBytes(UTF_8);
            });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }

  }

  protected TServerInstance getLocation(ServerContext ctx, String relpath) {
    String str = getFromZK(ctx, relpath);
    if (str == null) {
      return null;
    }

    String[] parts = str.split("[|]", 2);
    HostAndPort address = HostAndPort.fromString(parts[0]);
    if (parts.length > 1 && parts[1] != null && parts[1].length() > 0) {
      return new TServerInstance(address, parts[1]);
    } else {
      // a 1.2 location specification: DO NOT WANT
      return null;
    }
  }

  static List<LogEntry> getRootLogEntries(ServerContext context) {

    try {
      ArrayList<LogEntry> result = new ArrayList<>();

      IZooReaderWriter zoo = context.getZooReaderWriter();
      String root = context.getZooKeeperRoot() + ZROOT_TABLET_WALOGS;
      // there's a little race between getting the children and fetching
      // the data. The log can be removed in between.
      outer: while (true) {
        result.clear();
        for (String child : zoo.getChildren(root)) {
          try {
            LogEntry e = LogEntry.fromBytes(zoo.getData(root + "/" + child, null));
            // upgrade from !0;!0<< -> +r<<
            e = new LogEntry(RootTable.EXTENT, 0, e.server, e.filename);
            result.add(e);
          } catch (KeeperException.NoNodeException ex) {
            // TODO I think this is a bug, probably meant to continue to while loop... was probably
            // a bug in the original code.
            continue outer;
          }
        }
        break;
      }

      return result;
    } catch (KeeperException | InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getFromZK(ServerContext ctx, String relpath) {
    try {
      byte[] data = ctx.getZooReaderWriter().getData(ctx.getZooKeeperRoot() + relpath, null);
      if (data == null)
        return null;

      return new String(data, StandardCharsets.UTF_8);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void delete(ServerContext ctx, String relpath) {
    try {
      ctx.getZooReaderWriter().recursiveDelete(ctx.getZooKeeperRoot() + relpath,
          NodeMissingPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.manager.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.manager.thrift.ManagerState;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.core.util.AddressUtil;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class TabletStateChangeIterator extends SkippingIterator {

  private static final String SERVERS_OPTION = "servers";
  private static final String TABLES_OPTION = "tables";
  private static final String MERGES_OPTION = "merges";
  private static final String DEBUG_OPTION = "debug";
  private static final String MIGRATIONS_OPTION = "migrations";
  private static final String MANAGER_STATE_OPTION = "managerState";
  private static final String SHUTTING_DOWN_OPTION = "shuttingDown";
  private static final Logger log = LoggerFactory.getLogger(TabletStateChangeIterator.class);

  private Set<TServerInstance> current;
  private Set<TableId> onlineTables;
  private Map<TableId,MergeInfo> merges;
  private boolean debug = false;
  private Set<KeyExtent> migrations;
  private ManagerState managerState = ManagerState.NORMAL;

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    super.init(source, options, env);
    current = parseServers(options.get(SERVERS_OPTION));
    onlineTables = parseTableIDs(options.get(TABLES_OPTION));
    merges = parseMerges(options.get(MERGES_OPTION));
    debug = options.containsKey(DEBUG_OPTION);
    migrations = parseMigrations(options.get(MIGRATIONS_OPTION));
    try {
      managerState = ManagerState.valueOf(options.get(MANAGER_STATE_OPTION));
    } catch (Exception ex) {
      if (options.get(MANAGER_STATE_OPTION) != null) {
        log.error("Unable to decode managerState {}", options.get(MANAGER_STATE_OPTION));
      }
    }
    Set<TServerInstance> shuttingDown = parseServers(options.get(SHUTTING_DOWN_OPTION));
    if (current != null && shuttingDown != null) {
      current.removeAll(shuttingDown);
    }
  }

  private Set<KeyExtent> parseMigrations(String migrations) {
    if (migrations == null) {
      return Collections.emptySet();
    }
    try {
      Set<KeyExtent> result = new HashSet<>();
      DataInputBuffer buffer = new DataInputBuffer();
      byte[] data = Base64.getDecoder().decode(migrations);
      buffer.reset(data, data.length);
      while (buffer.available() > 0) {
        result.add(KeyExtent.readFrom(buffer));
      }
      return result;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private Set<TableId> parseTableIDs(String tableIDs) {
    if (tableIDs == null) {
      return null;
    }
    Set<TableId> result = new HashSet<>();
    for (String tableID : tableIDs.split(",")) {
      result.add(TableId.of(tableID));
    }
    return result;
  }

  private Set<TServerInstance> parseServers(String servers) {
    if (servers == null) {
      return null;
    }
    // parse "host:port[INSTANCE]"
    Set<TServerInstance> result = new HashSet<>();
    if (!servers.isEmpty()) {
      for (String part : servers.split(",")) {
        String[] parts = part.split("\\[", 2);
        String hostport = parts[0];
        String instance = parts[1];
        if (instance != null && instance.endsWith("]")) {
          instance = instance.substring(0, instance.length() - 1);
        }
        result.add(new TServerInstance(AddressUtil.parseAddress(hostport, false), instance));
      }
    }
    return result;
  }

  private Map<TableId,MergeInfo> parseMerges(String merges) {
    if (merges == null) {
      return null;
    }
    try {
      Map<TableId,MergeInfo> result = new HashMap<>();
      DataInputBuffer buffer = new DataInputBuffer();
      byte[] data = Base64.getDecoder().decode(merges);
      buffer.reset(data, data.length);
      while (buffer.available() > 0) {
        MergeInfo mergeInfo = new MergeInfo();
        mergeInfo.readFields(buffer);
        result.put(mergeInfo.extent.tableId(), mergeInfo);
      }
      return result;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  protected void consume() throws IOException {
    while (getSource().hasTop()) {
      Key k = getSource().getTopKey();
      Value v = getSource().getTopValue();

      if (onlineTables == null || current == null || managerState != ManagerState.NORMAL) {
        return;
      }

      TabletLocationState tls;
      try {
        tls = MetaDataTableScanner.createTabletLocationState(k, v);
        if (tls == null) {
          return;
        }
      } catch (BadLocationStateException e) {
        // maybe the manager can do something with a tablet with bad/inconsistent state
        return;
      }
      // we always want data about merges
      MergeInfo merge = merges.get(tls.extent.tableId());
      if (merge != null) {
        // could make this smarter by only returning if the tablet is involved in the merge
        return;
      }
      // always return the information for migrating tablets
      if (migrations.contains(tls.extent)) {
        return;
      }

      // is the table supposed to be online or offline?
      boolean shouldBeOnline = onlineTables.contains(tls.extent.tableId());

      if (debug) {
        log.debug("{} is {} and should be {} line", tls.extent, tls.getState(current),
            (shouldBeOnline ? "on" : "off"));
      }
      switch (tls.getState(current)) {
        case ASSIGNED:
          // we always want data about assigned tablets
          return;
        case HOSTED:
          if (!shouldBeOnline) {
            return;
          }
          break;
        case ASSIGNED_TO_DEAD_SERVER:
          return;
        case SUSPENDED:
        case UNASSIGNED:
          if (shouldBeOnline) {
            return;
          }
          break;
        default:
          throw new AssertionError(
              "Inconceivable! The tablet is an unrecognized state: " + tls.getState(current));
      }
      // table is in the expected state so don't bother returning any information about it
      getSource().next();
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  public static void setCurrentServers(IteratorSetting cfg, Set<TServerInstance> goodServers) {
    if (goodServers != null) {
      List<String> servers = new ArrayList<>();
      for (TServerInstance server : goodServers) {
        servers.add(server.getHostPortSession());
      }
      cfg.addOption(SERVERS_OPTION, Joiner.on(",").join(servers));
    }
  }

  public static void setOnlineTables(IteratorSetting cfg, Set<TableId> onlineTables) {
    if (onlineTables != null) {
      cfg.addOption(TABLES_OPTION, Joiner.on(",").join(onlineTables));
    }
  }

  public static void setMerges(IteratorSetting cfg, Collection<MergeInfo> merges) {
    DataOutputBuffer buffer = new DataOutputBuffer();
    try {
      for (MergeInfo info : merges) {
        KeyExtent extent = info.getExtent();
        if (extent != null && !info.getState().equals(MergeState.NONE)) {
          info.write(buffer);
        }
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    String encoded =
        Base64.getEncoder().encodeToString(Arrays.copyOf(buffer.getData(), buffer.getLength()));
    cfg.addOption(MERGES_OPTION, encoded);
  }

  public static void setMigrations(IteratorSetting cfg, Collection<KeyExtent> migrations) {
    DataOutputBuffer buffer = new DataOutputBuffer();
    try {
      for (KeyExtent extent : migrations) {
        extent.writeTo(buffer);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    String encoded =
        Base64.getEncoder().encodeToString(Arrays.copyOf(buffer.getData(), buffer.getLength()));
    cfg.addOption(MIGRATIONS_OPTION, encoded);
  }

  public static void setManagerState(IteratorSetting cfg, ManagerState state) {
    cfg.addOption(MANAGER_STATE_OPTION, state.toString());
  }

  public static void setShuttingDown(IteratorSetting cfg, Set<TServerInstance> servers) {
    if (servers != null) {
      cfg.addOption(SHUTTING_DOWN_OPTION, Joiner.on(",").join(servers));
    }
  }

}

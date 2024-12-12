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
package org.apache.accumulo.server.util.checkCommand;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.FindOfflineTablets;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;

public class RootMetadataCheckRunner implements MetadataCheckRunner {
  private static final Admin.CheckCommand.Check check = Admin.CheckCommand.Check.ROOT_METADATA;

  @Override
  public String tableName() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TableId tableId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<ColumnFQ> requiredColFQs() {
    return Set.of(MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN,
        MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN,
        MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN,
        MetadataSchema.TabletsSection.ServerColumnFamily.LOCK_COLUMN);
  }

  @Override
  public Set<Text> requiredColFams() {
    return Set.of(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
  }

  @Override
  public String scanning() {
    return "root tablet metadata in ZooKeeper";
  }

  @Override
  public Admin.CheckCommand.CheckStatus runCheck(ServerContext context, ServerUtilOpts opts,
      boolean fixFiles) throws TableNotFoundException, InterruptedException, KeeperException {
    Admin.CheckCommand.CheckStatus status = Admin.CheckCommand.CheckStatus.OK;
    printRunning();

    log.trace("********** Looking for offline tablets **********");
    if (FindOfflineTablets.findOffline(context, AccumuloTable.ROOT.tableName(), false, true,
        log::trace, log::warn) != 0) {
      status = Admin.CheckCommand.CheckStatus.FAILED;
    } else {
      log.trace("All good... No offline tablets found");
    }

    log.trace("********** Looking for missing columns **********");
    status = checkRequiredColumns(context, status);

    log.trace("********** Looking for invalid columns **********");
    final String path = context.getZooKeeperRoot() + RootTable.ZROOT_TABLET;
    final String json = new String(context.getZooReader().getData(path), UTF_8);
    final var rtm = new RootTabletMetadata(json);
    status = checkColumns(context, rtm.getKeyValues().iterator(), status);

    printCompleted(status);
    return status;
  }

  @Override
  public Admin.CheckCommand.CheckStatus checkRequiredColumns(ServerContext context,
      Admin.CheckCommand.CheckStatus status)
      throws TableNotFoundException, InterruptedException, KeeperException {
    final String path = context.getZooKeeperRoot() + RootTable.ZROOT_TABLET;
    final String json = new String(context.getZooReader().getData(path), UTF_8);
    final var rtm = new RootTabletMetadata(json);
    final Set<Text> rowsSeen = new HashSet<>();
    final Set<ColumnFQ> requiredColFQs = new HashSet<>(requiredColFQs());
    final Set<Text> requiredColFams = new HashSet<>(requiredColFams());

    log.trace("Scanning the {} for missing required columns...\n", scanning());
    rtm.getKeyValues().forEach(e -> {
      var key = e.getKey();
      rowsSeen.add(key.getRow());
      boolean removed =
          requiredColFQs.remove(new ColumnFQ(key.getColumnFamily(), key.getColumnQualifier()));
      if (!removed) {
        requiredColFams.remove(key.getColumnFamily());
      }
    });

    if (rowsSeen.size() != 1) {
      status = Admin.CheckCommand.CheckStatus.FAILED;
      log.warn("Did not see one tablet for the root table!");
    } else {
      if (!requiredColFQs.isEmpty() || !requiredColFams.isEmpty()) {
        log.warn("Tablet {} is missing required columns: col FQs: {}, col fams: {} in the {}\n",
            rowsSeen.stream().findFirst().orElseThrow(), requiredColFQs, requiredColFams,
            scanning());
        status = Admin.CheckCommand.CheckStatus.FAILED;
      } else {
        log.trace("...The {} contains all required columns for the root tablet\n", scanning());
      }
    }

    return status;
  }

  @Override
  public Admin.CheckCommand.Check getCheck() {
    return check;
  }
}

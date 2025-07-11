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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.util.Admin;

public class SystemConfigCheckRunner implements CheckRunner {
  private static final Admin.CheckCommand.Check check = Admin.CheckCommand.Check.SYSTEM_CONFIG;

  @Override
  public Admin.CheckCommand.CheckStatus runCheck(ServerContext context, ServerUtilOpts opts,
      boolean fixFiles) throws Exception {
    Admin.CheckCommand.CheckStatus status = Admin.CheckCommand.CheckStatus.OK;
    printRunning();

    log.trace("********** Checking validity of some ZooKeeper nodes **********");
    status = checkZkNodes(context, status);

    printCompleted(status);
    return status;
  }

  private static Admin.CheckCommand.CheckStatus checkZkNodes(ServerContext context,
      Admin.CheckCommand.CheckStatus status) throws Exception {
    status = checkZKLocks(context, status);
    status = checkZKTableNodes(context, status);
    status = checkZKWALsMetadata(context, status);

    return status;
  }

  private static Admin.CheckCommand.CheckStatus checkZKLocks(ServerContext context,
      Admin.CheckCommand.CheckStatus status) throws Exception {
    final ServerId.Type[] serverTypes = ServerId.Type.values();

    log.trace("Checking ZooKeeper locks for Accumulo server processes...");

    // check that essential server processes have a ZK lock failing otherwise
    // check that nonessential server processes have a ZK lock only if they are running. If they are
    // not running, alerts the user that the process is not running which may or may not be expected
    for (ServerId.Type serverType : serverTypes) {
      log.trace("Looking for {} lock(s)...", serverType);
      var servers = context.instanceOperations().getServers(serverType);

      switch (serverType) {
        case MANAGER:
          // essential process
        case GARBAGE_COLLECTOR:
          // essential process
          if (servers.size() != 1) {
            log.warn("Expected 1 server to be found for {} but found {}", serverType,
                servers.size());
            status = Admin.CheckCommand.CheckStatus.FAILED;
          } else {
            // no exception and 1 server found
            log.trace("Verified ZooKeeper lock for {}", servers);
          }
          break;
        case MONITOR:
          // nonessential process
          if (servers.isEmpty()) {
            log.debug("No {} appears to be running. This may or may not be expected", serverType);
          } else if (servers.size() > 1) {
            log.warn("More than 1 {} was found running. This is not expected", serverType);
            status = Admin.CheckCommand.CheckStatus.FAILED;
          } else {
            // no exception and 1 server found
            log.trace("Verified ZooKeeper lock for {}", servers);
          }
          break;
        case TABLET_SERVER:
          // essential process(es)
        case COMPACTOR:
          // essential process(es)
          if (servers.isEmpty()) {
            log.warn("No {} appear to be running. This is not expected.", serverType);
            status = Admin.CheckCommand.CheckStatus.FAILED;
          } else {
            // no exception and >= 1 server found
            log.trace("Verified ZooKeeper lock(s) for {}", servers);
          }
          break;
        case SCAN_SERVER:
          // nonessential process(es)
          if (servers.isEmpty()) {
            log.debug("No {} appear to be running. This may or may not be expected.", serverType);
          } else {
            // no exception and >= 1 server found
            log.trace("Verified ZooKeeper lock(s) for {}", servers);
          }
          break;
        default:
          throw new IllegalStateException("Unhandled case: " + serverType);
      }
    }

    return status;
  }

  private static Admin.CheckCommand.CheckStatus checkZKTableNodes(ServerContext context,
      Admin.CheckCommand.CheckStatus status) throws Exception {
    log.trace("Checking ZooKeeper table nodes...");

    final var zrw = context.getZooSession().asReaderWriter();
    final var tableNameToId = context.tableOperations().tableIdMap();
    final Map<String,String> systemTableNameToId = new HashMap<>();
    for (var accumuloTable : SystemTables.values()) {
      systemTableNameToId.put(accumuloTable.tableName(), accumuloTable.tableId().canonical());
    }

    // ensure all system tables exist
    if (!tableNameToId.values().containsAll(systemTableNameToId.values())) {
      log.warn(
          "Missing essential Accumulo table. One or more of {} are missing from the tables found {}",
          systemTableNameToId, tableNameToId);
      status = Admin.CheckCommand.CheckStatus.FAILED;
    }
    for (var nameToId : tableNameToId.entrySet()) {
      var tablePath = Constants.ZTABLES + "/" + nameToId.getValue();
      // expect the table path to exist and some data to exist
      if (!zrw.exists(tablePath) || zrw.getChildren(tablePath).isEmpty()) {
        log.warn("Failed to find table ({}) info at expected path {}", nameToId, tablePath);
        status = Admin.CheckCommand.CheckStatus.FAILED;
      }
    }

    return status;
  }

  private static Admin.CheckCommand.CheckStatus checkZKWALsMetadata(ServerContext context,
      Admin.CheckCommand.CheckStatus status) throws Exception {
    final var zs = context.getZooSession();
    final var zrw = zs.asReaderWriter();
    final var rootWalsDir = WalStateManager.ZWALS;
    final Set<TServerInstance> tserverInstances = TabletMetadata.getLiveTServers(context);
    final Set<TServerInstance> seenTServerInstancesAtWals = new HashSet<>();

    log.trace("Checking that WAL metadata in ZooKeeper is valid...");

    // each child node of the root wals dir should be a TServerInstance.toString()
    var tserverInstancesAtWals = zrw.getChildren(rootWalsDir);
    for (var tserverInstanceAtWals : tserverInstancesAtWals) {
      final TServerInstance tsi = new TServerInstance(tserverInstanceAtWals);
      seenTServerInstancesAtWals.add(tsi);
      final var tserverPath = rootWalsDir + "/" + tserverInstanceAtWals;
      // each child node of the tserver should be WAL metadata
      final var wals = zrw.getChildren(tserverPath);
      if (wals.isEmpty()) {
        log.debug("No WAL metadata found for tserver {}", tsi);
      }
      for (var wal : wals) {
        // should be able to parse the WAL metadata
        final var fullWalPath = tserverPath + "/" + wal;
        log.trace("Attempting to parse WAL metadata at {}", fullWalPath);
        var parseRes = WalStateManager.parse(zrw.getData(fullWalPath));
        log.trace("Successfully parsed WAL metadata at {} result {}", fullWalPath, parseRes);
        log.trace("Checking if the WAL path {} found in the metadata exists in HDFS...",
            parseRes.getSecond());
        if (!context.getVolumeManager().exists(parseRes.getSecond())) {
          log.warn("WAL metadata for tserver {} references a WAL that does not exist",
              tserverInstanceAtWals);
          status = Admin.CheckCommand.CheckStatus.FAILED;
        }
      }
    }
    if (!tserverInstances.equals(seenTServerInstancesAtWals)) {
      log.warn(
          "Expected WAL metadata in ZooKeeper for all tservers. tservers={} tservers seen storing WAL metadata={}",
          tserverInstances, seenTServerInstancesAtWals);
      status = Admin.CheckCommand.CheckStatus.FAILED;
    }

    return status;
  }

  @Override
  public Admin.CheckCommand.Check getCheck() {
    return check;
  }
}

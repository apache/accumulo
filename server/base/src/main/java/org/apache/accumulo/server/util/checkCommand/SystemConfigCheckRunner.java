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
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.util.Admin;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;

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
            log.trace("Verified ZooKeeper lock(s) for {} servers", servers.size());
          }
          break;
        case SCAN_SERVER:
          // nonessential process(es)
          if (servers.isEmpty()) {
            log.debug("No {} appear to be running. This may or may not be expected.", serverType);
          } else {
            // no exception and >= 1 server found
            log.trace("Verified ZooKeeper lock(s) for {} servers", servers.size());
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
    final var zrw = context.getZooSession().asReaderWriter();

    log.trace("Checking that WAL metadata in ZooKeeper is valid...");

    var walsBefore = gatherWalsFromZK(context, zrw);

    // gather any wals present in ZooKeeper but missing in DFS
    Map<TServerInstance,Set<Pair<WalStateManager.WalState,Path>>> missingWals = new HashMap<>();
    for (var instanceAndWals : walsBefore.entrySet()) {
      for (var wal : instanceAndWals.getValue()) {
        if (!context.getVolumeManager().exists(wal.getSecond())) {
          missingWals.computeIfAbsent(instanceAndWals.getKey(), k -> new HashSet<>()).add(wal);
        }
      }
    }

    var walsAfter = gatherWalsFromZK(context, zrw);

    for (var instanceAndMissingWals : missingWals.entrySet()) {
      // if the TServer is alive before AND after the DFS check AND any missing WAL is still in
      // use after the DFS check
      var actualMissing = Sets.intersection(instanceAndMissingWals.getValue(),
                walsAfter.getOrDefault(instanceAndMissingWals.getKey(), Set.of()));
      if(!actualMissing.isEmpty()){
        log.warn("WAL metadata for tserver {} references a WAL that does not exist : {}",
                instanceAndMissingWals.getKey(), actualMissing);
        status = Admin.CheckCommand.CheckStatus.FAILED;
      }
    }

    return status;
  }

  private static Map<TServerInstance,Set<Pair<WalStateManager.WalState,Path>>>
      gatherWalsFromZK(ServerContext context, ZooReaderWriter zrw) throws Exception {
    final var rootWalsDir = WalStateManager.ZWALS;
    Map<TServerInstance,Set<Pair<WalStateManager.WalState,Path>>> wals = new HashMap<>();
    var tserverInstances = TabletMetadata.getLiveTServers(context);
    for (var tsi : tserverInstances) {
      wals.put(tsi, new HashSet<>());
      // each child node of the root wals dir is a TServerInstance
      final var tserverPath = rootWalsDir + "/" + tsi.toString();

      // each child node of the tserver should be WAL metadata
      final var walsPaths = zrw.getChildren(tserverPath);
      if (walsPaths.isEmpty()) {
        log.warn("No WAL metadata found for tserver {}. If it is expected that mutations have "
            + "occurred on the tserver, this is a problem. Otherwise, this is normal", tsi);
      }

      for (var walPath : walsPaths) {
        // should be able to parse the WAL metadata
        final var fullWalPath = tserverPath + "/" + walPath;
        log.trace("Attempting to parse WAL metadata at {}", fullWalPath);
        var data = zrw.getData(fullWalPath);
        if(data == null){
          continue;
        }
        var parseRes = WalStateManager.parse(data);
        log.trace("Successfully parsed WAL metadata at {} result {}", fullWalPath, parseRes);
        if (parseRes.getFirst() == WalStateManager.WalState.OPEN
            || parseRes.getFirst() == WalStateManager.WalState.CLOSED) {
          wals.get(tsi).add(parseRes);
        }
      }
    }
    return wals;
  }

  @Override
  public Admin.CheckCommand.Check getCheck() {
    return check;
  }
}

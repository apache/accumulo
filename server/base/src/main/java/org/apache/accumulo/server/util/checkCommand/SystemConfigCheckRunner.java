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
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.util.Admin;

public class SystemConfigCheckRunner implements CheckRunner {
  private static final Admin.CheckCommand.Check check = Admin.CheckCommand.Check.SYSTEM_CONFIG;

  public enum ServerProcess {
    MANAGER, GC, TSERVER, COMPACTION_COORDINATOR, COMPACTOR, MONITOR, SCAN_SERVER
  }

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
    final ServerProcess[] serverProcesses = ServerProcess.values();
    final String zkRoot = context.getZooKeeperRoot();
    final var zs = context.getZooSession();
    final var zrw = zs.asReaderWriter();
    final var compactors = context.instanceOperations().getCompactors();
    final var sservers = context.instanceOperations().getScanServers();

    log.trace("Checking ZooKeeper locks for Accumulo server processes...");

    // check that essential server processes have a ZK lock failing otherwise
    // check that nonessential server processes have a ZK lock only if they are running. If they are
    // not running, alerts the user that the process is not running which may or may not be expected
    for (ServerProcess proc : serverProcesses) {
      log.trace("Looking for {} lock(s)...", proc);
      switch (proc) {
        case MANAGER:
          // essential process
          status = checkLock(zkRoot + Constants.ZMANAGER_LOCK, proc, true, zs, status);
          break;
        case GC:
          // essential process
          status = checkLock(zkRoot + Constants.ZGC_LOCK, proc, true, zs, status);
          break;
        case TSERVER:
          // essential process(es)
          final var tservers = TabletMetadata.getLiveTServers(context);
          if (tservers.isEmpty()) {
            log.warn("Did not find any running tablet servers!");
            status = Admin.CheckCommand.CheckStatus.FAILED;
          }
          break;
        case COMPACTION_COORDINATOR:
          // nonessential process
          status = checkLock(zkRoot + Constants.ZCOORDINATOR_LOCK, proc, false, zs, status);
          break;
        case COMPACTOR:
          // nonessential process(es)
          if (compactors.isEmpty()) {
            log.debug("No compactors appear to be running... This may or may not be expected");
          }
          for (String compactor : compactors) {
            // for each running compactor, ensure a zk lock exists for it
            boolean checkedLock = false;
            String compactorQueuesPath = zkRoot + Constants.ZCOMPACTORS;
            var compactorQueues = zrw.getChildren(compactorQueuesPath);
            // find the queue the compactor is in
            for (var queue : compactorQueues) {
              String compactorQueuePath = compactorQueuesPath + "/" + queue;
              String lockPath = compactorQueuePath + "/" + compactor;
              if (zrw.exists(lockPath)) {
                status = checkLock(lockPath, proc, true, zs, status);
                checkedLock = true;
                break;
              }
            }
            if (!checkedLock) {
              log.warn("Did not find a ZooKeeper lock for the compactor {}!", compactor);
              status = Admin.CheckCommand.CheckStatus.FAILED;
            }
          }
          break;
        case MONITOR:
          // nonessential process
          status = checkLock(zkRoot + Constants.ZMONITOR_LOCK, proc, false, zs, status);
          break;
        case SCAN_SERVER:
          // nonessential process(es)
          if (sservers.isEmpty()) {
            log.debug("No scan servers appear to be running... This may or may not be expected");
          }
          for (String sserver : sservers) {
            status =
                checkLock(zkRoot + Constants.ZSSERVERS + "/" + sserver, proc, true, zs, status);
          }
          break;
        default:
          throw new IllegalStateException("Unhandled case: " + proc);
      }
    }

    return status;
  }

  private static Admin.CheckCommand.CheckStatus checkLock(String path, ServerProcess proc,
      boolean requiredProc, ZooSession zs, Admin.CheckCommand.CheckStatus status) throws Exception {
    log.trace("Checking ZooKeeper lock at path {}", path);

    ServiceLock.ServiceLockPath slp = ServiceLock.path(path);
    var opData = ServiceLock.getLockData(zs, slp);
    if (requiredProc && opData.isEmpty()) {
      log.warn("No ZooKeeper lock found for {} at {}! The process may not be running.", proc, path);
      status = Admin.CheckCommand.CheckStatus.FAILED;
    } else if (!requiredProc && opData.isEmpty()) {
      log.debug("No ZooKeeper lock found for {} at {}. The process may not be running. "
          + "This may or may not be expected.", proc, path);
    }
    return status;
  }

  private static Admin.CheckCommand.CheckStatus checkZKTableNodes(ServerContext context,
      Admin.CheckCommand.CheckStatus status) throws Exception {
    log.trace("Checking ZooKeeper table nodes...");

    final var zrw = context.getZooSession().asReaderWriter();
    final var zkRoot = context.getZooKeeperRoot();
    final var tableNameToId = context.tableOperations().tableIdMap();
    final Map<String,String> systemTableNameToId = new HashMap<>();
    for (var accumuloTable : AccumuloTable.values()) {
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
      var tablePath = zkRoot + Constants.ZTABLES + "/" + nameToId.getValue();
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
    final String zkRoot = context.getZooKeeperRoot();
    final var zs = context.getZooSession();
    final var zrw = zs.asReaderWriter();
    final var rootWalsDir = zkRoot + WalStateManager.ZWALS;
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

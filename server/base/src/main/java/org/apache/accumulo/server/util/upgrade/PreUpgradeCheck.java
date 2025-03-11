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
package org.apache.accumulo.server.util.upgrade;

import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.ReadOnlyTStore;
import org.apache.accumulo.core.fate.ZooStore;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PreUpgradeCheck implements KeywordExecutable {

  private static final Logger LOG = LoggerFactory.getLogger(PreUpgradeCheck.class);

  @Override
  public String keyword() {
    return "pre-upgrade";
  }

  @Override
  public String description() {
    return "Utility that prepares an instance to be upgraded to a new version."
        + " This utility must be run before starting any servers.";
  }

  @Override
  public void execute(String[] args) throws Exception {

    ServerUtilOpts opts = new ServerUtilOpts();
    opts.parseArgs(PreUpgradeCheck.class.getName(), args);

    final ServerContext context = opts.getServerContext();

    final int persistentVersion = AccumuloDataVersion.getCurrentVersion(context);
    final int thisVersion = AccumuloDataVersion.get();
    if (persistentVersion == thisVersion) {
      throw new IllegalStateException("Running this utility is unnecessary, this instance"
          + " has already been upgraded to version " + thisVersion);
    }

    final String zkRoot = context.getZooKeeperRoot();
    final ZooSession zs = context.getZooSession();
    final String prepUpgradePath = context.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE;

    if (!zs.asReader().exists(prepUpgradePath)) {
      LOG.info("{} node not found in ZooKeeper, 'ZooZap -prepare-for-upgrade' was likely"
          + " not run after shutting down instance for upgrade. Removing"
          + " server locks and checking for fate transactions.",  prepUpgradePath);

      try {
        final String fatePath = zkRoot + Constants.ZFATE;
        // Adapted from UpgradeCoordinator.abortIfFateTransactions
        final ReadOnlyTStore<PreUpgradeCheck> fate = new ZooStore<>(fatePath, zs);
        if (!fate.list().isEmpty()) {
          throw new IllegalStateException("Cannot continue pre-upgrade checks"
              + " because FATE transactions exist. You can start a tserver, but"
              + " not the Manager, with the old version of Accumulo then use "
              + " the shell to delete completed transactions and fail pending"
              + " or in-progress transactions. Once all of the FATE transactions"
              + " have been removed you can retry this operation.");
        }
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException("Error checking for existing FATE transactions", e);
      }
      LOG.info("No FATE transactions found");

      // Forcefully delete all server locks
      Set<String> serviceLockPaths = Set.of(zkRoot + Constants.ZCOMPACTORS,
          zkRoot + Constants.ZCOORDINATOR_LOCK, zkRoot + Constants.ZGC_LOCK,
          zkRoot + Constants.ZMANAGER_LOCK, zkRoot + Constants.ZMONITOR_LOCK,
          zkRoot + Constants.ZSSERVERS, zkRoot + Constants.ZTSERVERS);
      for (String slp : serviceLockPaths) {
        LOG.info("Deleting all zookeeper entries under {}", slp);
        try {
          List<String> children = zs.asReader().getChildren(slp);
          for (String child : children) {
            LOG.debug("Performing recursive delete on node:  {}", child);
            ZooUtil.recursiveDelete(zs, slp + "/" + child, NodeMissingPolicy.SKIP);
          }
        } catch (KeeperException.NoNodeException e) {
          LOG.warn("{} path does not exist in zookeeper", slp);
        } catch (InterruptedException e) {
          throw new IllegalStateException("Interrupted while trying to find"
              + " and delete children of zookeeper node: " + slp);
        }
      }
    }

    // TODO: Add 3.1 pre-upgrade steps

    // Initialize the UpgradeProgress object in ZooKeeper
    new UpgradeProgressTracker(context).initialize();

    // Delete the upgrade preparation node
    zs.asReaderWriter().delete(prepUpgradePath);

    LOG.info("Upgrade preparation completed, start Manager to continue upgrade to version: {}",
        thisVersion);

  }

}

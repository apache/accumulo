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
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

public class PreUpgradeCheck implements KeywordExecutable {

  private static final Logger LOG = LoggerFactory.getLogger(PreUpgradeCheck.class);

  static class Opts extends ServerUtilOpts {

    @Parameter(names = "--force", description = "if true, will force pre-upgrade to run")
    boolean force = false;
  }

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

    Opts opts = new Opts();
    opts.parseArgs(PreUpgradeCheck.class.getName(), args);

    final ServerContext context = opts.getServerContext();

    final int persistentVersion = AccumuloDataVersion.getCurrentVersion(context);
    final int thisVersion = AccumuloDataVersion.get();
    if (persistentVersion == thisVersion) {
      throw new IllegalStateException("Running this utility is unnecessary, this instance"
          + " has already been upgraded to version " + thisVersion);
    }

    final ZooSession zs = context.getZooSession();
    final ZooReader zr = zs.asReader();
    final String prepUpgradePath = Constants.ZPREPARE_FOR_UPGRADE;

    if (!zr.exists(prepUpgradePath)) {

      if (opts.force) {
        LOG.info("{} node not found in ZooKeeper, 'ZooZap -prepare-for-upgrade' was likely"
            + " not run after shutting down instance for upgrade. Removing"
            + " server locks and checking for fate transactions.", prepUpgradePath);
      } else {
        throw new IllegalStateException(prepUpgradePath + " node not found in ZooKeeper indicating"
            + " that ZooZap -prepare-for-upgrade was not run after shutting down the instance. If"
            + " you wish to continue, then run this command using the --force option.");
      }

      try {
        // Adapted from UpgradeCoordinator.abortIfFateTransactions
        if (!zr.getChildren(Constants.ZFATE).isEmpty()) {
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
      Set<ServiceLockPath> serviceLockPaths =
          context.getServerPaths().getCompactor((g) -> true, AddressSelector.all(), true);
      serviceLockPaths.addAll(
          context.getServerPaths().getTabletServer((g) -> true, AddressSelector.all(), true));
      serviceLockPaths
          .addAll(context.getServerPaths().getScanServer((g) -> true, AddressSelector.all(), true));
      serviceLockPaths.add(context.getServerPaths().getManager(true));
      serviceLockPaths.add(context.getServerPaths().getGarbageCollector(true));
      serviceLockPaths.add(context.getServerPaths().getMonitor(true));

      for (ServiceLockPath slp : serviceLockPaths) {
        LOG.info("Deleting all zookeeper entries under {}", slp);
        try {
          List<String> children = zr.getChildren(slp.toString());
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

    // Initialize the UpgradeProgress object in ZooKeeper
    new UpgradeProgressTracker(context).initialize();

    // Delete the upgrade preparation node
    zs.asReaderWriter().delete(prepUpgradePath);

    LOG.info("Upgrade preparation completed, start Manager to continue upgrade to version: {}",
        thisVersion);

  }

}

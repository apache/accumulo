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
package org.apache.accumulo.server.util;

import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonManager.Mode;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ZooZap implements KeywordExecutable {
  private static final Logger log = LoggerFactory.getLogger(ZooZap.class);

  private static void message(String msg, Opts opts) {
    if (opts.verbose) {
      System.out.println(msg);
    }
  }

  @Override
  public String keyword() {
    return "zoo-zap";
  }

  @Override
  public String description() {
    return "Utility for removing Zookeeper locks and preparing an instance for upgrade";
  }

  static class Opts extends Help {
    @Deprecated(since = "2.1.0")
    @Parameter(names = "-master",
        description = "remove master locks (deprecated -- user -manager instead")
    boolean zapMaster = false;
    @Parameter(names = "-manager", description = "remove manager locks")
    boolean zapManager = false;
    @Parameter(names = "-tservers", description = "remove tablet server locks")
    boolean zapTservers = false;
    @Parameter(names = "-compaction-coordinators",
        description = "remove compaction coordinator locks")
    boolean zapCoordinators = false;
    @Parameter(names = "-compactors", description = "remove compactor locks")
    boolean zapCompactors = false;
    @Parameter(names = "-sservers", description = "remove scan server locks")
    boolean zapScanServers = false;
    @Parameter(names = "-verbose", description = "print out messages about progress")
    boolean verbose = false;
    @Parameter(names = "-prepare-for-upgrade",
        description = "prepare Accumulo for an upgrade to the next non-bugfix release")
    boolean upgrade = false;
    @Parameter(names = "-force", description = "allow prepare-for-upgrade to run again")
    boolean forceUpgradePrep = false;
  }

  public static void main(String[] args) throws Exception {
    new ZooZap().execute(args);
  }

  @Override
  public void execute(String[] args) throws Exception {
    try {
      var siteConf = SiteConfiguration.auto();
      // Login as the server on secure HDFS
      if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
        SecurityUtil.serverLogin(siteConf);
      }
      zap(siteConf, args);
    } finally {
      SingletonManager.setMode(Mode.CLOSED);
    }
  }

  public void zap(SiteConfiguration siteConf, String... args) {
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);

    if (!opts.zapMaster && !opts.zapManager && !opts.zapTservers && !opts.upgrade) {
      new JCommander(opts).usage();
      return;
    }

    String volDir = VolumeConfiguration.getVolumeUris(siteConf).iterator().next();
    Path instanceDir = new Path(volDir, "instance_id");
    InstanceId iid = VolumeManager.getInstanceIDFromHdfs(instanceDir, new Configuration());
    ZooReaderWriter zoo = new ZooReaderWriter(siteConf);

    if (opts.upgrade) {
      final String zUpgradepath = Constants.ZROOT + "/" + iid + Constants.ZPREPARE_FOR_UPGRADE;
      try {
        if (zoo.exists(zUpgradepath)) {
          if (!opts.forceUpgradePrep) {
            throw new IllegalStateException(
                "'ZooZap -prepare-for-upgrade' must have already been run."
                    + " To run again use the 'ZooZap -prepare-for-upgrade -force'");
          } else {
            zoo.delete(zUpgradepath);
          }
        }
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException("Error creating or checking for " + zUpgradepath
            + " node in zookeeper: " + e.getMessage(), e);
      }

      log.info("Upgrade specified, validating that Manager is stopped");
      final ServiceLockPath mgrPath =
          ServiceLock.path(Constants.ZROOT + "/" + iid + Constants.ZMANAGER_LOCK);
      try {
        if (ServiceLock.getLockData(zoo.getZooKeeper(), mgrPath) != null) {
          throw new IllegalStateException(
              "Manager is running, shut it down and retry this operation");
        }
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException("Error trying to determine if Manager lock is held", e);
      }

      log.info("Checking for existing fate transactions");
      try {
        // Adapted from UpgradeCoordinator.abortIfFateTransactions
        if (!zoo.getChildren(Constants.ZFATE).isEmpty()) {
          throw new IllegalStateException("Cannot complete upgrade preparation"
              + " because FATE transactions exist. You can start a tserver, but"
              + " not the Manager, then use the shell to delete completed"
              + " transactions and fail pending or in-progress transactions."
              + " Once all of the FATE transactions have been removed you can"
              + " retry this operation.");
        }
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException("Error checking for existing FATE transactions", e);
      }

      log.info("Creating {} node in zookeeper, servers will be prevented from"
          + " starting while this node exists", zUpgradepath);
      try {
        zoo.putPersistentData(zUpgradepath, new byte[0], NodeExistsPolicy.SKIP);
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException("Error creating " + zUpgradepath
            + " node in zookeeper. Check for any issues and retry.", e);
      }
      log.info("Instance {} prepared for upgrade. Server processes will not start while"
          + " in this state. To undo this state and abort upgrade preparations delete"
          + " the zookeeper node: {}", iid.canonical(), zUpgradepath);

      log.info("Forcing removal of all server locks");
      // modify the options to remove all locks
      opts.zapCompactors = true;
      opts.zapCoordinators = true;
      opts.zapManager = true;
      opts.zapScanServers = true;
      opts.zapTservers = true;
    }

    if (opts.zapMaster) {
      log.warn("The -master option is deprecated. Please use -manager instead.");
    }
    if (opts.zapManager || opts.zapMaster) {
      String managerLockPath = Constants.ZROOT + "/" + iid + Constants.ZMANAGER_LOCK;

      try {
        zapDirectory(zoo, managerLockPath, opts);
      } catch (KeeperException | InterruptedException e) {
        e.printStackTrace();
      }
    }

    if (opts.zapTservers) {
      String tserversPath = Constants.ZROOT + "/" + iid + Constants.ZTSERVERS;
      try {
        List<String> children = zoo.getChildren(tserversPath);
        for (String child : children) {
          message("Deleting " + tserversPath + "/" + child + " from zookeeper", opts);

          if (opts.zapManager || opts.zapMaster) {
            zoo.recursiveDelete(tserversPath + "/" + child, NodeMissingPolicy.SKIP);
          } else {
            var zLockPath = ServiceLock.path(tserversPath + "/" + child);
            if (!zoo.getChildren(zLockPath.toString()).isEmpty()) {
              try {
                ServiceLock.deleteLock(zoo, zLockPath);
              } catch (RuntimeException e) {
                message("Did not delete " + tserversPath + "/" + child, opts);
              }
            }
          }
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("{}", e.getMessage(), e);
      }
    }

    // Remove the tracers, we don't use them anymore.
    @SuppressWarnings("deprecation")
    String path = siteConf.get(Property.TRACE_ZK_PATH);
    try {
      zapDirectory(zoo, path, opts);
    } catch (Exception e) {
      // do nothing if the /tracers node does not exist.
    }

    if (opts.zapCoordinators) {
      final String coordinatorPath = Constants.ZROOT + "/" + iid + Constants.ZCOORDINATOR_LOCK;
      try {
        if (zoo.exists(coordinatorPath)) {
          zapDirectory(zoo, coordinatorPath, opts);
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting coordinator from zookeeper, {}", e.getMessage(), e);
      }
    }

    if (opts.zapCompactors) {
      String compactorsBasepath = Constants.ZROOT + "/" + iid + Constants.ZCOMPACTORS;
      try {
        if (zoo.exists(compactorsBasepath)) {
          List<String> queues = zoo.getChildren(compactorsBasepath);
          for (String queue : queues) {
            message("Deleting " + compactorsBasepath + "/" + queue + " from zookeeper", opts);
            zoo.recursiveDelete(compactorsBasepath + "/" + queue, NodeMissingPolicy.SKIP);
          }
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("Error deleting compactors from zookeeper, {}", e.getMessage(), e);
      }

    }

    if (opts.zapScanServers) {
      String sserversPath = Constants.ZROOT + "/" + iid + Constants.ZSSERVERS;
      try {
        if (zoo.exists(sserversPath)) {
          List<String> children = zoo.getChildren(sserversPath);
          for (String child : children) {
            message("Deleting " + sserversPath + "/" + child + " from zookeeper", opts);

            var zLockPath = ServiceLock.path(sserversPath + "/" + child);
            if (!zoo.getChildren(zLockPath.toString()).isEmpty()) {
              ServiceLock.deleteLock(zoo, zLockPath);
            }
          }
        }
      } catch (KeeperException | InterruptedException e) {
        log.error("{}", e.getMessage(), e);
      }
    }

  }

  private static void zapDirectory(ZooReaderWriter zoo, String path, Opts opts)
      throws KeeperException, InterruptedException {
    List<String> children = zoo.getChildren(path);
    for (String child : children) {
      message("Deleting " + path + "/" + child + " from zookeeper", opts);
      zoo.recursiveDelete(path + "/" + child, NodeMissingPolicy.SKIP);
    }
  }
}

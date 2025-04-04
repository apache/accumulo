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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.Help;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
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
public class UpgradeUtil implements KeywordExecutable {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeUtil.class);

  static class Opts extends Help {
    @Parameter(names = "--prepare",
        description = "prepare an older version instance for an upgrade to a newer non-bugfix release."
            + " This command should be run using the older version of software after the instance is shut down.")
    boolean prepare = false;
  }

  @Override
  public String keyword() {
    return "upgrade";
  }

  @Override
  public String description() {
    return "utility used to perform various upgrade steps for an Accumulo instance";
  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);

    if (!opts.prepare) {
      new JCommander(opts).usage();
      return;
    }

    var siteConf = SiteConfiguration.auto();
    // Login as the server on secure HDFS
    if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      SecurityUtil.serverLogin(siteConf);
    }

    String volDir = VolumeConfiguration.getVolumeUris(siteConf).iterator().next();
    Path instanceDir = new Path(volDir, "instance_id");
    InstanceId iid = VolumeManager.getInstanceIDFromHdfs(instanceDir, new Configuration());
    ZooReaderWriter zoo = new ZooReaderWriter(siteConf);

    if (opts.prepare) {
      final String zUpgradepath = Constants.ZROOT + "/" + iid + Constants.ZPREPARE_FOR_UPGRADE;
      try {
        if (zoo.exists(zUpgradepath)) {
          zoo.delete(zUpgradepath);
        }
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException("Error creating or checking for " + zUpgradepath
            + " node in zookeeper: " + e.getMessage(), e);
      }

      LOG.info("Upgrade specified, validating that Manager is stopped");
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

      LOG.info("Checking for existing fate transactions");
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

      LOG.info("Creating {} node in zookeeper, servers will be prevented from"
          + " starting while this node exists", zUpgradepath);
      try {
        zoo.putPersistentData(zUpgradepath, new byte[0], NodeExistsPolicy.SKIP);
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException("Error creating " + zUpgradepath
            + " node in zookeeper. Check for any issues and retry.", e);
      }

      LOG.info("Forcing removal of all server locks");
      new ZooZap().zap(siteConf, "-manager", "-compaction-coordinators", "-tservers", "-compactors",
          "-sservers");

      LOG.info("Instance {} prepared for upgrade. Server processes will not start while"
          + " in this state. To undo this state and abort upgrade preparations delete"
          + " the zookeeper node: {}. If you abort and restart the instance, then you "
          + " should re-run this utility before upgrading.", iid.canonical(), zUpgradepath);
    }

  }

}

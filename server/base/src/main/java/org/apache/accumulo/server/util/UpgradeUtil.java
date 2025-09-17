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

import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
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

import com.beust.jcommander.DefaultUsageFormatter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class UpgradeUtil implements KeywordExecutable {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeUtil.class);

  static class Opts extends ConfigOpts {
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

  private static class UpgradeUsageFormatter extends DefaultUsageFormatter {

    public UpgradeUsageFormatter(JCommander commander) {
      super(commander);
    }

    @Override
    public void appendMainLine(StringBuilder out, boolean hasOptions, boolean hasCommands,
        int indentCount, String indent) {
      super.appendMainLine(out, hasOptions, hasCommands, indentCount, indent);

      out.append("\n");
      out.append(indent)
          .append("  The upgrade command is intended to be used in the following way :\n");
      out.append(indent).append("    1. Stop older version of accumulo\n");
      out.append(indent)
          .append("    2. Run 'accumulo upgrade --prepare' using the older version of accumulo\n");
      out.append(indent).append("    3. Setup the newer version of the accumulo software\n");
      out.append(indent)
          .append("    4. Run 'accumulo upgrade --start' using the newer version of accumulo\n");
      out.append(indent).append(
          "    5. Start accumulo using the newer version and let the manager complete the upgrade\n");
      out.append("\n");
    }
  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(
        jCommander -> jCommander.setUsageFormatter(new UpgradeUsageFormatter(jCommander)),
        keyword(), args);

    if (!opts.prepare) {
      var jc = new JCommander(opts);
      jc.setProgramName(keyword());
      jc.setUsageFormatter(new UpgradeUsageFormatter(jc));
      jc.usage();
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
      SortedMap<String,String> tablePropsInSite = new TreeMap<>();
      // get table props in site conf excluding default config
      siteConf.getProperties(tablePropsInSite,
          key -> key.startsWith(Property.TABLE_PREFIX.getKey()), false);

      if (!tablePropsInSite.isEmpty()) {
        LOG.warn("Saw table properties in site configuration : {} ", tablePropsInSite.keySet());
        throw new IllegalStateException("Did not start upgrade preparation because table properties"
            + " are present in site config which may cause later versions to fail.  Recommended action"
            + " is to set these properties at the system, namespace, or table level if still needed."
            + " This can be done by starting accumulo and using the shell/api, or using the 'accumulo "
            + "zoo-prop-editor' command.  Site configuration is the lowest level, so when moving "
            + "properties consider if they will override something at a higher level. For example "
            + "if moving a property to the namespace level, check if its set at the system level.");
      }
      LOG.info(
          "Please examine the the accumulo.properties files across your cluster to ensure none "
              + "have table properties that could cause later versions to fail.");

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
        if (!zoo.getChildren(Constants.ZROOT + "/" + iid + Constants.ZFATE).isEmpty()) {
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
          "-sservers", "--monitor", "--gc");

      LOG.info("Instance {} prepared for upgrade. Server processes will not start while"
          + " in this state. To undo this state and abort upgrade preparations delete"
          + " the zookeeper node: {}. If you abort and restart the instance, then you "
          + " should re-run this utility before upgrading.", iid.canonical(), zUpgradepath);
    }

  }

}

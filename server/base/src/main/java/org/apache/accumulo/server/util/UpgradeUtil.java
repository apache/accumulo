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

import static org.apache.accumulo.core.Constants.ZFATE;
import static org.apache.accumulo.core.Constants.ZPREPARE_FOR_UPGRADE;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    try (var context = new ServerContext(siteConf)) {
      final ZooSession zs = context.getZooSession();
      final ZooReaderWriter zoo = zs.asReaderWriter();
      if (opts.prepare) {
        try {
          if (zoo.exists(ZPREPARE_FOR_UPGRADE)) {
            zoo.delete(ZPREPARE_FOR_UPGRADE);
          }
        } catch (KeeperException | InterruptedException e) {
          throw new IllegalStateException("Error creating or checking for " + ZPREPARE_FOR_UPGRADE
              + " node in zookeeper: " + e.getMessage(), e);
        }

        LOG.info("Upgrade specified, validating that Manager is stopped");
        if (context.getServerPaths().getManager(true) != null) {
          throw new IllegalStateException(
              "Manager is running, shut it down and retry this operation");
        }

        LOG.info("Checking for existing fate transactions");
        try {
          // Adapted from UpgradeCoordinator.abortIfFateTransactions
          // TODO: After the 4.0.0 release this code block needs to be
          // modified to account for the new Fate table.
          if (!zoo.getChildren(ZFATE).isEmpty()) {
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
            + " starting while this node exists", ZPREPARE_FOR_UPGRADE);
        try {
          zoo.putPersistentData(ZPREPARE_FOR_UPGRADE, new byte[0], NodeExistsPolicy.SKIP);
        } catch (KeeperException | InterruptedException e) {
          throw new IllegalStateException("Error creating " + ZPREPARE_FOR_UPGRADE
              + " node in zookeeper. Check for any issues and retry.", e);
        }

        LOG.info("Forcing removal of all server locks");
        new ZooZap().zap(context, "-manager", "-tservers", "-compactors", "-sservers");

        LOG.info(
            "Instance {} prepared for upgrade. Server processes will not start while"
                + " in this state. To undo this state and abort upgrade preparations delete"
                + " the zookeeper node: {}. If you abort and restart the instance, then you "
                + " should re-run this utility before upgrading.",
            context.getInstanceID(), ZPREPARE_FOR_UPGRADE);
      }
    }

  }

}

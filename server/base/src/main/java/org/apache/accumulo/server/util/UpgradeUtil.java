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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.ZFATE;
import static org.apache.accumulo.core.Constants.ZPREPARE_FOR_UPGRADE;
import static org.apache.accumulo.core.Constants.ZTABLES;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.compaction.CompactionDispatcher;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactionServiceId;
import org.apache.accumulo.core.spi.compaction.CompactionServices;
import org.apache.accumulo.core.util.compaction.CompactionServicesConfig;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.NamespaceConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.util.upgrade.PreUpgradeValidation;
import org.apache.accumulo.server.util.upgrade.UpgradeProgress;
import org.apache.accumulo.server.util.upgrade.UpgradeProgressTracker;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class UpgradeUtil implements KeywordExecutable {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeUtil.class);
  private static final String ZTABLE_NAME = "/name";

  static class Opts extends ConfigOpts {
    @Parameter(names = "--prepare",
        description = "prepare an older version instance for an upgrade to a newer non-bugfix release."
            + " This command should be run using the older version of software after the instance is shut down.")
    boolean prepare = false;

    @Parameter(names = "--start",
        description = """
            Start an upgrade of an Accumulo instance. The 'start' step is intended to be run on the \
            instance with the new version of software before any server processes are started.  Server processes \
            should fail to start if this step is not run. This will check that 'accumulo upgrade --prepare' \
            was run on the instance after it was shut down, perform pre-upgrade validation, and perform any \
            upgrade steps that need to occur before the Manager is started. Finally, it creates a mandatory \
            marker in ZooKeeper that enables the Manager to complete an upgrade.""")
    boolean start = false;

    @Parameter(names = "--force",
        description = "Continue with 'start' processing if 'prepare' had not been run on the instance.")
    boolean force = false;
  }

  @Override
  public String keyword() {
    return "upgrade";
  }

  @Override
  public String description() {
    return "utility used to perform various upgrade steps for an Accumulo instance.";
  }

  private void prepare(final ServerContext context) {

    final int persistentVersion = AccumuloDataVersion.getCurrentVersion(context);
    final int thisVersion = AccumuloDataVersion.get();
    if (persistentVersion != thisVersion) {
      throw new IllegalStateException("It looks like you are running 'prepare' with "
          + "a different version of software than what the instance was running with."
          + " The 'prepare' command is intended to be run after an instance is shutdown"
          + " with the same version of software before trying to upgrade.");
    }

    final ZooSession zs = context.getZooSession();
    final ZooReaderWriter zoo = zs.asReaderWriter();

    try {
      zoo.delete(ZPREPARE_FOR_UPGRADE);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(
          "Error deleting " + ZPREPARE_FOR_UPGRADE + " node in zookeeper", e);
    }

    LOG.info("Upgrade specified, validating that Manager is stopped");
    if (context.getServerPaths().getManager(true) != null) {
      throw new IllegalStateException("Manager is running, shut it down and retry this operation");
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
    new ZooZap().zap(context, "-manager", "-tservers", "-compactors", "-sservers", "--monitor",
        "--gc");

    LOG.info(
        "Instance {} prepared for upgrade. Server processes will not start while"
            + " in this state. To undo this state and abort upgrade preparations delete"
            + " the zookeeper node: {}. If you abort and restart the instance, then you "
            + " should re-run this utility before upgrading.",
        context.getInstanceID(), ZPREPARE_FOR_UPGRADE);
  }

  private void start(ServerContext context, boolean force) {
    final int persistentVersion = AccumuloDataVersion.getCurrentVersion(context);
    final int thisVersion = AccumuloDataVersion.get();
    if (persistentVersion == thisVersion) {
      throw new IllegalStateException("Running this utility is unnecessary, this instance"
          + " has already been upgraded to version " + thisVersion);
    }

    if (context.getServerPaths().getManager(true) != null) {
      throw new IllegalStateException("Cannot run this command with the Manager running.");
    }

    final ZooSession zs = context.getZooSession();
    final ZooReader zr = zs.asReader();

    // Check to see if the 'start' command has successfully run before. If it has,
    // and the Manager made any progress in upgrading, then fail.
    try {
      if (zr.exists(Constants.ZUPGRADE_PROGRESS)) {
        int persistedVersion = AccumuloDataVersion.getCurrentVersion(context);
        byte[] bytes = zr.getData(Constants.ZUPGRADE_PROGRESS, new Stat());
        UpgradeProgress progress = UpgradeProgress.fromJsonBytes(bytes);
        if (progress.getZooKeeperVersion() != persistedVersion
            || progress.getRootVersion() != persistedVersion
            || progress.getMetadataVersion() != persistedVersion) {
          throw new IllegalStateException(
              "It appears that an upgrade is in progress. 'accumulo upgrade --start'"
                  + " cannot be run again");
        } else {
          ZooUtil.recursiveDelete(zs, Constants.ZUPGRADE_PROGRESS, NodeMissingPolicy.FAIL);
        }
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException(Constants.ZUPGRADE_PROGRESS + " node exists"
          + " in ZooKeeper implying the 'start' command is being re-run. Deleting"
          + " this node has failed. Delete it manually before retrying.", e);
    }

    final String prepUpgradePath = Constants.ZPREPARE_FOR_UPGRADE;
    boolean prepareNodeExists = false;
    try {
      prepareNodeExists = zr.exists(prepUpgradePath);
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Error checking for existence of node: " + prepUpgradePath,
          e);
    }

    if (!prepareNodeExists) {

      if (force) {
        LOG.info("{} node not found in ZooKeeper, 'accumulo upgrade --prepare' was likely"
            + " not run after shutting down instance for upgrade. Removing"
            + " server locks and checking for fate transactions.", prepUpgradePath);
      } else {
        throw new IllegalStateException(prepUpgradePath + " node not found in ZooKeeper indicating"
            + " that 'accumulo upgrade --prepare' was not run after shutting down the instance. If"
            + " you wish to continue, then run this command using the --force option. If you wish"
            + " to cancel, delete, or let your Fate transactions complete, then restart the instance"
            + " with the old version of software.");
      }

      try {
        // Adapted from UpgradeCoordinator.abortIfFateTransactions
        // TODO: After the 4.0.0 release this code block needs to be
        // modified to account for the new Fate table.
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

      // In the case where the user passes the '--force' option, then it's possible
      // that some server processes could be running because the Constants.ZPREPARE_FOR_UPGRADE
      // node was not created in ZooKeeper. Server paths in ZooKeeper could be
      // from the prior version or this one. Delete all paths in ZooKeeper that
      // represent server locks.
      for (String topLevelServerPath : List.of(Constants.ZTSERVERS, Constants.ZCOMPACTORS,
          Constants.ZSSERVERS, Constants.ZGC_LOCK, Constants.ZMANAGER_LOCK,
          Constants.ZMONITOR_LOCK)) {
        try {
          var children = zs.getChildren(topLevelServerPath, null);
          for (var child : children) {
            ZooUtil.recursiveDelete(zs, topLevelServerPath + "/" + child, NodeMissingPolicy.SKIP);
          }
        } catch (KeeperException | InterruptedException e) {
          throw new IllegalStateException(
              "Error deleting server locks under node: " + topLevelServerPath, e);
        }
      }
    }

    try {
      validateCompactionServiceConfiguration(context);
      LOG.info("Validated compaction service configuration");
    } catch (KeeperException | InterruptedException e) {
      LOG.error("Error validating compaction service configuration", e);
      throw new IllegalStateException("Error validating compaction service configuration", e);
    }

    // Run the PreUpgradeValidation code to validate the ZooKeeper ACLs
    try {
      new PreUpgradeValidation().validate(context);
    } catch (RuntimeException e) {
      throw new IllegalStateException("PreUpgradeValidation failure", e);
    }

    // Initialize the UpgradeProgress object in ZooKeeper.
    new UpgradeProgressTracker(context).initialize();

    // Delete the upgrade preparation node
    try {
      ZooUtil.recursiveDelete(zs, prepUpgradePath, NodeMissingPolicy.SKIP);
    } catch (KeeperException | InterruptedException e) {
      LOG.warn(
          "Error deleting {} from ZooKeeper. Instance ready for "
              + "upgrade, but servers will not start while this node exists. Delete it manually.",
          prepUpgradePath, e);
    }

    LOG.info("Upgrade started, start the instance to continue upgrade to version: {}", thisVersion);

  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(keyword(), args);

    if (!opts.prepare && !opts.start) {
      new JCommander(opts).usage();
      return;
    }

    if (opts.prepare && opts.start) {
      throw new IllegalArgumentException("prepare and start options are mutually exclusive");
    }

    var siteConf = SiteConfiguration.auto();
    // Login as the server on secure HDFS
    if (siteConf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      SecurityUtil.serverLogin(siteConf);
    }

    try (var context = new ServerContext(siteConf)) {
      if (opts.prepare) {
        prepare(context);
      } else if (opts.start) {
        start(context, opts.force);
      }
    }

  }

  private void validateCompactionServiceConfiguration(ServerContext ctx)
      throws KeeperException, InterruptedException {

    boolean configurationError = false;

    final CompactionServicesConfig servicesConfig =
        new CompactionServicesConfig(ctx.getConfiguration());
    final Set<CompactionServiceId> definedServiceIds = servicesConfig.getPlanners().keySet()
        .stream().map(CompactionServiceId::of).collect(Collectors.toUnmodifiableSet());

    LOG.info("Defined compaction service ids: {}", definedServiceIds);

    final ZooReader zr = ctx.getZooSession().asReader();
    List<String> zooTableIds = zr.getChildren(ZTABLES);

    for (String tableId : zooTableIds) {

      final String tableName =
          new String(zr.getData(Constants.ZTABLES + "/" + tableId + ZTABLE_NAME), UTF_8);
      final String namespaceId = new String(
          zr.getData(Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_NAMESPACE), UTF_8);

      final NamespaceId nsid = NamespaceId.of(namespaceId);
      final TableId tid = TableId.of(tableId);

      final NamespaceConfiguration nsConf =
          new NamespaceConfiguration(ctx, nsid, ctx.getConfiguration());
      final TableConfiguration tconf = new TableConfiguration(ctx, tid, nsConf);

      final CompactionDispatcher dispatcher = tconf.getCompactionDispatcher();

      for (CompactionKind kind : CompactionKind.values()) {
        final CompactionDispatcher.DispatchParameters dispatchParams =
            new CompactionDispatcher.DispatchParameters() {
              @Override
              public CompactionServices getCompactionServices() {
                return () -> definedServiceIds;
              }

              @Override
              public ServiceEnvironment getServiceEnv() {
                return (ServiceEnvironment) ctx;
              }

              @Override
              public CompactionKind getCompactionKind() {
                return kind;
              }

              @Override
              public Map<String,String> getExecutionHints() {
                return Map.of();
              }
            };
        final CompactionServiceId expectedCompactionService =
            dispatcher.dispatch(dispatchParams).getService();
        LOG.info("Table {} is configured to use service \"{}\" for compaction kind {}", tableName,
            expectedCompactionService, kind);
        if (!servicesConfig.getPlanners().containsKey(expectedCompactionService.canonical())) {
          if ((tid.equals(SystemTables.ROOT.tableId())
              && expectedCompactionService.canonical().equals("root"))
              || (tid.equals(SystemTables.METADATA.tableId())
                  && expectedCompactionService.canonical().equals("meta"))) {
            LOG.warn(
                "Table {} is using a default compaction service configuration from a prior version."
                    + " The \"{}\" compaction service configuration is no longer defined. You can either define"
                    + " it now in the accumulo.properties file, or the compaction service configuration will"
                    + " be removed to adopt the new default in this version during the upgrade.",
                tableName, expectedCompactionService);
          } else {
            LOG.error(
                "Table {} returned non-existent compaction service \"{}\"  for compaction type {}.",
                tid, expectedCompactionService, kind);
            configurationError = true;
          }
        }
      }
    }
    if (configurationError) {
      LOG.error("Compaction configuration is incorrect. One or more tables is configured to use a"
          + " compaction service that does not exist in the configuration. Configured compaction"
          + " services are: {}", definedServiceIds);
      throw new IllegalStateException(
          "Compaction configuration is not correct. Continuing with upgrade"
              + " will leave the instance in a state where compactions will not start for some tables. Please fix the system"
              + " configuration by defining the expected compaction services in accumulo.properties and run"
              + " --start again.");
    }
  }

}

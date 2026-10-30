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

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.ServiceLockPath;
import org.apache.accumulo.core.fate.zookeeper.ZooCache;
import org.apache.accumulo.core.fate.zookeeper.ZooCache.ZcStat;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
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
import com.google.common.collect.Iterators;

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
        LOG.warn("Saw table properties in accumulo.properties file : {} ",
            tablePropsInSite.keySet());
        throw new IllegalStateException("Did not start upgrade preparation because table properties"
            + " are present in the accumulo.properties which may cause later versions to fail.  Recommended action"
            + " is to set these properties at the system, namespace, or table level if still needed."
            + " This can be done by starting accumulo and using the shell/api, or using the 'accumulo"
            + " zoo-prop-editor' command.  The accumulo.properties file is the lowest level of configuration, so when moving"
            + " properties consider if they will override something at a higher level. For example"
            + " if moving a property to the namespace level, check if its set at the system level.");
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
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
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
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
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
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IllegalStateException("Error checking for existing FATE transactions", e);
      }

      LOG.info("Creating {} node in zookeeper, servers will be prevented from"
          + " starting while this node exists", zUpgradepath);
      try {
        zoo.putPersistentData(zUpgradepath, new byte[0], NodeExistsPolicy.SKIP);
      } catch (KeeperException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        throw new IllegalStateException("Error creating " + zUpgradepath
            + " node in zookeeper. Check for any issues and retry.", e);
      }

      LOG.info("Forcing removal of all server locks");
      new ZooZap().zap(siteConf, "-manager", "-compaction-coordinators", "-tservers", "-compactors",
          "-sservers", "--monitor", "--gc");

      // Wait for tserver locks to be removed, then ensure that no tablets have logs
      // as this indicates recovery needs to be performed.
      final ZooCache zc = new ZooCache(zoo, null);
      try {
        waitForTServerLocks(Constants.ZROOT + "/" + iid, zoo, zc);
        List<KeyExtent> recoveries = findTabletsNeedingRecovery(Constants.ZROOT + "/" + iid, zc);
        if (!recoveries.isEmpty()) {
          LOG.info(
              "Found {} Tablets that need recovery. Continuing with the upgrade is not advisable."
                  + " It's recommended to abort this upgrade using the instructions below and restart the"
                  + " instance to allow Tablet recovery to occur.",
              recoveries.size());
          recoveries.forEach(t -> LOG.info("{} needs recovery", t));
        }
      } finally {
        zc.close();
      }

      LOG.info("Instance {} prepared for upgrade. Server processes will not start while"
          + " in this state. To undo this state and abort upgrade preparations delete"
          + " the zookeeper node: {}. If you abort and restart the instance, then you "
          + " should re-run this utility before upgrading.", iid.canonical(), zUpgradepath);
    }

  }

  private void waitForTServerLocks(String zkRoot, ZooReader zr, ZooCache cache)
      throws KeeperException, InterruptedException {
    final String path = zkRoot + Constants.ZTSERVERS;
    List<String> tserverNodes = zr.getChildren(path);

    int tserversWithLocks = 0;
    do {
      Thread.sleep(5000);
      tserversWithLocks = 0;
      for (String zPath : tserverNodes) {
        final var zLockPath = ServiceLock.path(path + "/" + zPath);
        ZcStat stat = new ZcStat();
        byte[] lockData = ServiceLock.getLockData(cache, zLockPath, stat);
        if (lockData != null) {
          tserversWithLocks++;
        }
        LOG.info("{} TabletServer locks still exist in ZooKeeper", tserversWithLocks);
      }
    } while (tserversWithLocks != 0);
  }

  private Iterator<TabletMetadata> readTabletMetadata(List<String> rfiles, Range range) {
    // RFileScanner sets up the system iterator stack by default
    Scanner rfileScanner =
        RFile.newScanner().from(new ArrayList<String>(rfiles).toArray(new String[] {})).build();
    rfileScanner.setRange(range);
    RowIterator rowIter = new RowIterator(rfileScanner);
    return Iterators.transform(rowIter,
        ri -> TabletMetadata.convertRow(ri, EnumSet.allOf(ColumnType.class), false));
  }

  List<KeyExtent> findTabletsNeedingRecovery(String zkRoot, ZooCache cache) throws IOException {
    final List<KeyExtent> extentsNeedingRecovery = new ArrayList<>();

    // Read the root tablet metadata to see if it has logs and needs recovery
    TabletMetadata rootTabletMetadata = TabletsMetadata.getRootMetadata(zkRoot, cache);
    if (!rootTabletMetadata.getLogs().isEmpty()) {
      extentsNeedingRecovery.add(rootTabletMetadata.getExtent());
    }

    // Collect the rfiles for the root tablet so that we can collect the files for the
    // metadata table and also check if the metadata table has logs and needs recovery
    List<String> rootFiles = rootTabletMetadata.getFiles().stream().map(stf -> stf.getPathStr())
        .collect(Collectors.toList());
    Iterator<TabletMetadata> iter =
        readTabletMetadata(rootFiles, MetadataSchema.TabletsSection.getRange(MetadataTable.ID));
    List<String> metadataFiles = new ArrayList<>();
    while (iter.hasNext()) {
      TabletMetadata metadataTablet = iter.next();
      metadataTablet.getFiles().stream().map(stf -> stf.getPathStr()).forEach(metadataFiles::add);
      if (!metadataTablet.getLogs().isEmpty()) {
        extentsNeedingRecovery.add(metadataTablet.getExtent());
      }
    }

    // Read the rfiles for the metadata table to see if any user tables have
    // logs and need recovery
    iter = readTabletMetadata(metadataFiles, MetadataSchema.TabletsSection.getRange());
    while (iter.hasNext()) {
      TabletMetadata userTablet = iter.next();
      if (!userTablet.getLogs().isEmpty()) {
        extentsNeedingRecovery.add(userTablet.getExtent());
      }
    }

    return extentsNeedingRecovery;
  }

}

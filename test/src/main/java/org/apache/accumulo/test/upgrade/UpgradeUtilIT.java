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
package org.apache.accumulo.test.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.UpgradeUtil;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

public class UpgradeUtilIT extends AccumuloClusterHarness {

  @Test
  public void testPrepareFailsDueToManagerUp() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    createPrepareMarker(ctx);

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--prepare"}));
    assertEquals("Manager is running, shut it down and retry this operation", ise.getMessage());
    assertFalse(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

  }

  @Test
  public void testPrepareFailsDueToFateTransactions() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    createPrepareMarker(ctx);

    assertTrue(ctx.getZooReader().getChildren(ctx.getZooKeeperRoot() + Constants.ZFATE).isEmpty());
    ctx.getZooReaderWriter().putEphemeralData(
        ctx.getZooKeeperRoot() + Constants.ZFATE + "/" + UUID.randomUUID(), new byte[0]);
    assertFalse(ctx.getZooReader().getChildren(ctx.getZooKeeperRoot() + Constants.ZFATE).isEmpty());

    stopManagerAndWait(ctx);

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--prepare"}));
    assertTrue(ise.getMessage()
        .startsWith("Cannot complete upgrade preparation because FATE transactions exist."));
    assertFalse(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

  }

  @Test
  public void testPrepareSucceeds() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    createPrepareMarker(ctx);

    cluster.getClusterControl().adminStopAll();
    Wait.waitFor(() -> ctx.getZooReader()
        .getChildren(ctx.getZooKeeperRoot() + Constants.ZMANAGER_LOCK).isEmpty());

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    new UpgradeUtil().execute(new String[] {"--prepare"});
    runUpgradeAndCheckOutput(ctx, null, false, false);
  }

  private void createPrepareMarker(ServerContext ctx) throws Exception {
    ctx.getZooReaderWriter().putPersistentData(
        ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE, new byte[0],
        NodeExistsPolicy.SKIP);
    assertTrue(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));
  }

  private void stopManagerAndWait(ServerContext ctx) throws Exception {
    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Wait.waitFor(() -> ctx.getZooReader()
        .getChildren(ctx.getZooKeeperRoot() + Constants.ZMANAGER_LOCK).isEmpty());
  }

  private TabletMetadata getFirstTablet(ServerContext ctx, TableId tableId, ColumnType... columns) {
    try (TabletsMetadata tablets =
        ctx.getAmple().readTablets().forTable(tableId).fetch(columns).build()) {
      Iterator<TabletMetadata> iter = tablets.iterator();
      assertTrue(iter.hasNext());
      TabletMetadata tablet = iter.next();
      assertNotNull(tablet);
      return tablet;
    }
  }

  private void insertLogsRemoveLocation(ServerContext ctx, KeyExtent extent, Location loc)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    Ample ample = ctx.getAmple();
    String wal = "file://" + ctx.getZooKeeperRoot() + Path.SEPARATOR + Constants.WAL_DIR
        + Path.SEPARATOR + "localhost+9997" + Path.SEPARATOR + UUID.randomUUID().toString();
    ample.mutateTablet(extent).putWal(new LogEntry(extent, System.currentTimeMillis(), wal))
        .deleteLocation(loc).mutate();
  }

  private void removeTabletLocation(ServerContext ctx, KeyExtent extent, Location loc) {
    Ample ample = ctx.getAmple();
    ample.mutateTablet(extent).deleteLocation(loc).mutate();
  }

  private void insertLocationRemoveLogs(ServerContext ctx, KeyExtent extent,
      Collection<LogEntry> logs, Location loc) {
    Ample ample = ctx.getAmple();
    TabletMutator tm = ample.mutateTablet(extent);
    tm.putLocation(loc);
    for (LogEntry log : logs) {
      tm.deleteWal(log);
    }
    tm.mutate();
  }

  private void flushMetadataTables(AccumuloClient client) throws Exception {
    client.tableOperations().flush(MetadataTable.NAME, null, null, true);
    client.tableOperations().flush(RootTable.NAME, null, null, true);
  }

  private void clearMetadataTabletLocationsAndLogs(ServerContext ctx) {
    try (TabletsMetadata tablets = ctx.getAmple().readTablets().forTable(MetadataTable.ID)
        .fetch(ColumnType.PREV_ROW, ColumnType.LOCATION, ColumnType.LOGS).build()) {
      for (TabletMetadata tablet : tablets) {
        insertLocationRemoveLogs(ctx, tablet.getExtent(), tablet.getLogs(), tablet.getLocation());
        removeTabletLocation(ctx, tablet.getExtent(), tablet.getLocation());
      }
    }
  }

  private void stopTabletServersGracefully(ServerContext ctx) {
    for (String tabletServer : ctx.instanceOperations().getTabletServers()) {
      Admin.signalGracefulShutdown(ctx, HostAndPort.fromString(tabletServer));
    }
  }

  private void removeRootTabletLocation(ServerContext ctx) {
    TabletMetadata rootTablet = getRootTablet(ctx);
    removeTabletLocation(ctx, rootTablet.getExtent(), rootTablet.getLocation());
  }

  private TabletMetadata getRootTablet(ServerContext ctx) {
    TabletMetadata rootTablet =
        TabletsMetadata.getRootMetadata(ctx.getZooKeeperRoot(), ctx.getZooCache());
    assertNotNull(rootTablet);
    return rootTablet;
  }

  private void stopAllServersBeforeRootTabletMutation() throws Exception {
    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    getCluster().getClusterControl().stopAllServers(ServerType.TABLET_SERVER);
  }

  private void stopServersAndRemoveRootLocation(ServerContext ctx) throws Exception {
    stopManagerAndWait(ctx);
    stopTabletServersGracefully(ctx);
    removeRootTabletLocation(ctx);
  }

  private void runUpgradeAndCheckOutput(ServerContext ctx, KeyExtent extent,
      boolean failureExpected, boolean failureDueToLogs) throws Exception {
    StringWriter writer = new StringWriter();
    LoggerContext loggerContext = LoggerContext.getContext(false);
    Configuration cfg = loggerContext.getConfiguration();
    PatternLayout layout = PatternLayout.newBuilder().setConfiguration(cfg)
        .setPattern(PatternLayout.SIMPLE_CONVERSION_PATTERN).build();
    Appender appender =
        WriterAppender.createAppender(layout, null, writer, "UpgradeUtilITAppender", false, true);
    appender.start();
    cfg.addAppender(appender);
    cfg.getLoggerConfig(UpgradeUtil.class.getName()).addAppender(appender, null, null);

    try {
      System.setProperty("accumulo.properties",
          "file://" + getCluster().getAccumuloPropertiesPath());
      new UpgradeUtil().execute(new String[] {"--prepare"});
      assertTrue(
          ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));
      String logMessages = writer.toString();
      if (failureExpected) {
        if (failureDueToLogs) {
          assertTrue(logMessages.contains(extent + " needs recovery due to walogs being present"),
              "Expected --prepare to report recovery for " + extent + " logs, got " + logMessages);
        } else {
          assertTrue(logMessages.contains(extent + " needs recovery due to location being set"),
              "Expected --prepare to report recovery for " + extent + " location, got "
                  + logMessages);
        }
      } else {
        assertFalse(logMessages.contains(" needs recovery due to walogs being present"),
            "Expected --prepare to report recovery for logs, got " + logMessages);
        assertFalse(logMessages.contains(" needs recovery due to location being set"),
            "Expected --prepare to report recovery for location, got " + logMessages);
      }
    } finally {
      cfg.getLoggerConfig(UpgradeUtil.class.getName()).removeAppender(appender.getName());
      loggerContext.updateLoggers();
      appender.stop();
    }
  }

  @Test
  public void testPrepareFailsRootTabletLogs() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().waitForBalance();
      TabletMetadata rtm = getRootTablet(ctx);
      // Shutdown the Manager and Tablet Servers before modifying the
      // root tablet metadata. Leaving them up causes the test to hang.
      stopAllServersBeforeRootTabletMutation();
      insertLogsRemoveLocation(ctx, rtm.getExtent(), rtm.getLocation());
      runUpgradeAndCheckOutput(ctx, rtm.getExtent(), true, true);
    }
  }

  @Test
  public void testPrepareFailsRootTabletLocation() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().waitForBalance();
      TabletMetadata rtm = getRootTablet(ctx);
      // Shutdown the Manager and Tablet Servers before modifying the
      // root tablet metadata. Leaving them up causes the test to hang.
      stopAllServersBeforeRootTabletMutation();
      insertLocationRemoveLogs(ctx, rtm.getExtent(), rtm.getLogs(), rtm.getLocation());
      runUpgradeAndCheckOutput(ctx, rtm.getExtent(), true, false);
    }
  }

  @Test
  public void testPrepareFailsMetadataTabletLogs() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().waitForBalance();
      TabletMetadata metadataTablet =
          getFirstTablet(ctx, MetadataTable.ID, ColumnType.PREV_ROW, ColumnType.LOCATION);
      insertLogsRemoveLocation(ctx, metadataTablet.getExtent(), metadataTablet.getLocation());

      // The recovery checker reads metadata from RFiles, so flush both metadata levels after
      // injecting the synthetic WAL reference.
      flushMetadataTables(client);

      stopServersAndRemoveRootLocation(ctx);

      runUpgradeAndCheckOutput(ctx, metadataTablet.getExtent(), true, true);
    }
  }

  @Test
  public void testPrepareFailsMetadataTabletLocation() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().waitForBalance();
      TabletMetadata metadataTablet = getFirstTablet(ctx, MetadataTable.ID, ColumnType.PREV_ROW,
          ColumnType.LOCATION, ColumnType.LOGS);
      insertLocationRemoveLogs(ctx, metadataTablet.getExtent(), metadataTablet.getLogs(),
          metadataTablet.getLocation());

      // The recovery checker reads metadata from RFiles, so flush both metadata levels after
      // injecting the synthetic WAL reference.
      flushMetadataTables(client);

      stopServersAndRemoveRootLocation(ctx);

      runUpgradeAndCheckOutput(ctx, metadataTablet.getExtent(), true, false);
    }
  }

  @Test
  public void testPrepareFailsUserTabletLogs() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    String tableName = getUniqueNames(1)[0];
    TabletMetadata userTablet;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);
      client.instanceOperations().waitForBalance();
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

      userTablet = getFirstTablet(ctx, tableId, ColumnType.PREV_ROW, ColumnType.LOCATION);
      insertLogsRemoveLocation(ctx, userTablet.getExtent(), userTablet.getLocation());

      // Remove Metadata tablets location and logs
      clearMetadataTabletLocationsAndLogs(ctx);

      // The recovery checker reads metadata from RFiles, so flush both metadata levels after
      // injecting the synthetic WAL reference.
      flushMetadataTables(client);

      stopServersAndRemoveRootLocation(ctx);

      runUpgradeAndCheckOutput(ctx, userTablet.getExtent(), true, true);
    }
  }

  @Test
  public void testPrepareFailsUserTabletLocation() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    String tableName = getUniqueNames(1)[0];
    TabletMetadata userTablet;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);
      client.instanceOperations().waitForBalance();
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

      userTablet =
          getFirstTablet(ctx, tableId, ColumnType.PREV_ROW, ColumnType.LOCATION, ColumnType.LOGS);
      insertLocationRemoveLogs(ctx, userTablet.getExtent(), userTablet.getLogs(),
          userTablet.getLocation());

      // Remove Metadata tablets location and logs
      clearMetadataTabletLocationsAndLogs(ctx);
      // The recovery checker reads metadata from RFiles, so flush both metadata levels after
      // injecting the synthetic WAL reference.
      client.tableOperations().flush(tableName, null, null, true);
      flushMetadataTables(client);

      stopServersAndRemoveRootLocation(ctx);

      runUpgradeAndCheckOutput(ctx, userTablet.getExtent(), true, false);
    }
  }

  @Test
  public void testStartWhenAlreadyPrepared() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    ctx.getZooReaderWriter().putPersistentData(
        ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE, new byte[0],
        NodeExistsPolicy.SKIP);

    getCluster().stop();
    IllegalStateException ise = assertThrows(IllegalStateException.class, getCluster()::start);
    assertTrue(ise.getMessage().startsWith("Cannot start, instance is prepared for upgrade"));

  }

}

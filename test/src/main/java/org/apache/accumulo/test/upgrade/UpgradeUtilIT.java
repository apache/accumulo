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

import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.UpgradeUtil;
import org.apache.accumulo.test.util.Wait;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Test;

public class UpgradeUtilIT extends AccumuloClusterHarness {

  @Test
  public void testPrepareFailsDueToManagerUp() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    ctx.getZooReaderWriter().putPersistentData(
        ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE, new byte[0],
        NodeExistsPolicy.SKIP);
    assertTrue(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--prepare"}));
    assertEquals("Manager is running, shut it down and retry this operation", ise.getMessage());
    assertFalse(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

  }

  @Test
  public void testPrepareFailsDueToFateTransactions() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    ctx.getZooReaderWriter().putPersistentData(
        ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE, new byte[0],
        NodeExistsPolicy.SKIP);
    assertTrue(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

    assertTrue(ctx.getZooReader().getChildren(ctx.getZooKeeperRoot() + Constants.ZFATE).isEmpty());
    ctx.getZooReaderWriter().putEphemeralData(
        ctx.getZooKeeperRoot() + Constants.ZFATE + "/" + UUID.randomUUID(), new byte[0]);
    assertFalse(ctx.getZooReader().getChildren(ctx.getZooKeeperRoot() + Constants.ZFATE).isEmpty());

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Wait.waitFor(() -> ctx.getZooReader()
        .getChildren(ctx.getZooKeeperRoot() + Constants.ZMANAGER_LOCK).isEmpty());

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

    ctx.getZooReaderWriter().putPersistentData(
        ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE, new byte[0],
        NodeExistsPolicy.SKIP);
    assertTrue(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Wait.waitFor(() -> ctx.getZooReader()
        .getChildren(ctx.getZooKeeperRoot() + Constants.ZMANAGER_LOCK).isEmpty());

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    new UpgradeUtil().execute(new String[] {"--prepare"});
    assertTrue(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

  }

  @Test
  public void testFindsTabletRecoveryMarkerInMetadataFiles() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    String tableName = getUniqueNames(1)[0];
    KeyExtent extent;
    KeyExtent firstMetadataExtent = null;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      Ample ample = ctx.getAmple();

      // Get the first metadata table extent
      try (TabletsMetadata tm =
          ample.readTablets().forTable(MetadataTable.ID).fetch(ColumnType.PREV_ROW).build()) {
        Iterator<TabletMetadata> iter = tm.iterator();
        assertTrue(iter.hasNext());
        firstMetadataExtent = iter.next().getExtent();
      }
      assertNotNull(firstMetadataExtent);

      client.tableOperations().create(tableName);
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      extent = new KeyExtent(tableId, null, null);

      String wal = "file:/fake-recovery-wal";
      ample.mutateTablet(extent).putWal(new LogEntry(extent, System.currentTimeMillis(), wal))
          .mutate();
      ample.mutateTablet(firstMetadataExtent)
          .putWal(new LogEntry(extent, System.currentTimeMillis(), wal)).mutate();
      ample.mutateTablet(RootTable.EXTENT)
          .putWal(new LogEntry(extent, System.currentTimeMillis(), wal)).mutate();

      // The recovery checker reads metadata from RFiles, so flush both metadata levels after
      // injecting the synthetic WAL reference.
      client.tableOperations().flush(MetadataTable.NAME, null, null, true);
      client.tableOperations().flush(RootTable.NAME, null, null, true);
    }

    CapturingAppender appender = new CapturingAppender();
    LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = loggerContext.getConfiguration();
    LoggerConfig loggerConfig = configuration.getLoggerConfig(UpgradeUtil.class.getName());
    appender.start();
    loggerConfig.addAppender(appender, Level.INFO, null);
    loggerContext.updateLoggers();

    try {
      getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
      Wait.waitFor(() -> ctx.getZooReader()
          .getChildren(ctx.getZooKeeperRoot() + Constants.ZMANAGER_LOCK).isEmpty());

      System.setProperty("accumulo.properties",
          "file://" + getCluster().getAccumuloPropertiesPath());
      new UpgradeUtil().execute(new String[] {"--prepare"});
      assertTrue(
          ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));
      assertTrue(appender.messages().contains(RootTable.EXTENT + " needs recovery"),
          "Expected --prepare to report recovery for " + RootTable.EXTENT + ", got "
              + appender.messages());
      assertTrue(appender.messages().contains(firstMetadataExtent + " needs recovery"),
          "Expected --prepare to report recovery for " + firstMetadataExtent + ", got "
              + appender.messages());
      assertTrue(appender.messages().contains(extent + " needs recovery"),
          "Expected --prepare to report recovery for " + extent + ", got " + appender.messages());
    } finally {
      loggerConfig.removeAppender(appender.getName());
      loggerContext.updateLoggers();
      appender.stop();
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

  private static class CapturingAppender extends AbstractAppender {
    private final List<String> messages = new CopyOnWriteArrayList<>();

    private CapturingAppender() {
      super("UpgradeUtilITAppender", null, PatternLayout.createDefaultLayout(), false, null);
    }

    @Override
    public void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
    }

    private List<String> messages() {
      return messages;
    }
  }

}

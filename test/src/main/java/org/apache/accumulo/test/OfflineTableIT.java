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
package org.apache.accumulo.test;

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class OfflineTableIT extends SharedMiniClusterBase {

  private static class OfflineTableITConfiguration implements MiniClusterConfigurationCallback {

    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg,
        org.apache.hadoop.conf.Configuration coreSite) {
      // Timeout scan sessions after being idle for 3 seconds
      cfg.setProperty(Property.TSERV_SESSION_MAXIDLE, "3s");
    }
  }

  @BeforeAll
  public static void start() throws Exception {
    OfflineTableITConfiguration c = new OfflineTableITConfiguration();
    SharedMiniClusterBase.startMiniClusterWithConfig(c);
  }

  @AfterAll
  public static void stop() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testScanOffline() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf");
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));

      assertThrows(TableOfflineException.class,
          () -> client.createScanner(tableName, Authorizations.EMPTY));
    }
  }

  @Test
  public void testBatchScanOffline() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf");
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));

      assertThrows(TableOfflineException.class,
          () -> client.createBatchScanner(tableName, Authorizations.EMPTY));
    }
  }

  @Test
  public void testSplitOffline() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      client.tableOperations().offline(tableName, true);
      assertFalse(client.tableOperations().isOnline(tableName));
      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text("m"));
      assertThrows(TableOfflineException.class, () -> {
        client.tableOperations().addSplits(tableName, splits);
      });
    }
  }

  @Test
  public void testEcompWaitForOffline() throws Exception {
    final var ctx = getCluster().getServerContext();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf");
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      final var tabletMeta = ctx.getAmple().readTablet(new KeyExtent(tableId, null, null));

      final var ecid = ExternalCompactionId.generate(UUID.randomUUID());

      // Insert a fake External compaction which should prevent the wait for offline
      // from returning
      try (var mutator = ctx.getAmple().mutateTablets()) {
        var tabletDir =
            tabletMeta.getFiles().stream().findFirst().orElseThrow().getPath().getParent();
        var tmpFile = new Path(tabletDir, "C1234.rf_tmp");
        var cm = new CompactionMetadata(tabletMeta.getFiles(), ReferencedTabletFile.of(tmpFile),
            "localhost:16789", CompactionKind.SYSTEM, (short) 10, CompactorGroupId.of(GROUP1),
            false, null);
        mutator.mutateTablet(tabletMeta.getExtent()).putExternalCompaction(ecid, cm).mutate();
      }

      // test the ecomp prevents the wait for the offline() table operation from finishing
      // until the ecomp is deleted
      testWaitForOffline(ctx, client, tableId, tableName, mutator -> mutator
          .mutateTablet(tabletMeta.getExtent()).deleteExternalCompaction(ecid).mutate());
    }
  }

  @Test
  public void testOpidWaitForOffline() throws Exception {
    final var ctx = getCluster().getServerContext();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      ScanServerIT.createTableAndIngest(client, tableName, null, 10, 10, "colf");
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
      final var tabletMeta = ctx.getAmple().readTablet(new KeyExtent(tableId, null, null));

      // Insert a fake opid to prevent going offline
      try (var mutator = ctx.getAmple().mutateTablets()) {
        mutator.mutateTablet(tabletMeta.getExtent())
            .putOperation(TabletOperationId.from(TabletOperationType.SPLITTING,
                FateId.from(FateInstanceType.META, UUID.randomUUID())))
            .mutate();
      }

      // test the opid prevents the wait for the offline() table operation from finishing
      // until the opid is deleted
      testWaitForOffline(ctx, client, tableId, tableName,
          mutator -> mutator.mutateTablet(tabletMeta.getExtent()).deleteOperation().mutate());
    }
  }

  private void testWaitForOffline(ServerContext ctx, AccumuloClient client, TableId tableId,
      String tableName, Consumer<TabletsMutator> clear) throws Exception {

    // Try and take the table offline. At this point this should hang because there is a condition
    // preventing waitForTableStateTransition call from returning (either opid or ecomp)
    final var service = Executors.newSingleThreadExecutor();
    try {
      var tabletMeta = ctx.getAmple().readTablet(new KeyExtent(tableId, null, null));
      Future<?> f = service.submit(() -> {
        try {
          client.tableOperations().offline(tableName, true);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      // Check that the wait times out for the offline operation
      assertThrows(TimeoutException.class, () -> f.get(10, TimeUnit.SECONDS));

      // Clear the condition that is preventing the waitForTableStateTransition method
      // in TableOperationsImpl from finishing
      try (var mutator = ctx.getAmple().mutateTablets()) {
        clear.accept(mutator);
      }

      // The future should now finish and we should be offline
      f.get();
      tabletMeta = ctx.getAmple().readTablet(new KeyExtent(tableId, null, null));

      // Should have no location, ecomp, or opid
      assertFalse(tabletMeta.hasCurrent());
      assertNull(tabletMeta.getLocation());
      assertTrue(tabletMeta.getExternalCompactions().isEmpty());
      assertNull(tabletMeta.getOperationId());
      assertFalse(client.tableOperations().isOnline(tableName));
    } finally {
      service.shutdownNow();
    }
  }

}

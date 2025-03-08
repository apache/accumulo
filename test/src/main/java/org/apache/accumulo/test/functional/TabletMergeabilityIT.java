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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.test.TestIngest.generateRow;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.countTablets;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletMergeability;
import org.apache.accumulo.core.clientImpl.TabletMergeabilityUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.accumulo.test.util.Wait;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TabletMergeabilityIT extends SharedMiniClusterBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new Callback());
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static class Callback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      // Configure a short period of time to run the auto merge thread for testing
      cfg.setProperty(Property.MANAGER_TABLET_MERGEABILITY_INTERVAL, "3s");
    }
  }

  @Test
  public void testMergeabilityAll() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);
      var tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text(String.format("%09d", 333)));
      splits.add(new Text(String.format("%09d", 666)));
      splits.add(new Text(String.format("%09d", 999)));

      // create splits with mergeabilty disabled so the task does not merge them away
      // The default tablet is always mergeable, but it is currently the only one that is mergeable,
      // so nothing will merge
      c.tableOperations().putSplits(tableName, TabletMergeabilityUtil.userDefaultSplits(splits));
      Wait.waitFor(() -> countTablets(getCluster().getServerContext(), tableName, tm -> true) == 4,
          5000, 200);

      // update to always mergeable so the task can now merge tablets
      c.tableOperations().putSplits(tableName, TabletMergeabilityUtil.systemDefaultSplits(splits));

      // Wait for merge, we should have one tablet
      Wait.waitFor(() -> hasExactTablets(getCluster().getServerContext(), tableId,
          Set.of(new KeyExtent(tableId, null, null))), 10000, 200);

    }
  }

  @Test
  public void testMergeabilityMultipleRanges() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);
      var tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      SortedMap<Text,TabletMergeability> splits = new TreeMap<>();
      splits.put(new Text(String.format("%09d", 333)), TabletMergeability.never());
      splits.put(new Text(String.format("%09d", 555)), TabletMergeability.never());
      splits.put(new Text(String.format("%09d", 666)), TabletMergeability.never());
      splits.put(new Text(String.format("%09d", 999)), TabletMergeability.never());

      c.tableOperations().putSplits(tableName, splits);
      Wait.waitFor(() -> countTablets(getCluster().getServerContext(), tableName, tm -> true) == 5,
          5000, 500);

      splits.put(new Text(String.format("%09d", 333)), TabletMergeability.always());
      splits.put(new Text(String.format("%09d", 555)), TabletMergeability.always());
      // Keep tablet 666 as never, this should cause two fate jobs for merging
      splits.put(new Text(String.format("%09d", 999)), TabletMergeability.always());
      c.tableOperations().putSplits(tableName, splits);

      // Wait for merge, we should have 3 tablets
      // 333 and 555 should be merged into 555
      // 666
      // 999 and default merged into default
      Wait.waitFor(() -> hasExactTablets(getCluster().getServerContext(), tableId,
          Set.of(new KeyExtent(tableId, new Text(String.format("%09d", 555)), null),
              new KeyExtent(tableId, new Text(String.format("%09d", 666)),
                  new Text(String.format("%09d", 555))),
              new KeyExtent(tableId, null, new Text(String.format("%09d", 666))))),
          10000, 200);

    }
  }

  @Test
  public void testMergeabilityThresholdMultipleRanges() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "32K");
      props.put(Property.TABLE_MAX_MERGEABILITY_THRESHOLD.getKey(), ".5");
      c.tableOperations().create(tableName, new NewTableConfiguration()
          .withInitialTabletAvailability(TabletAvailability.HOSTED).setProperties(props));
      var tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      SortedMap<Text,TabletMergeability> splits = new TreeMap<>();
      // Create new tablets that won't merge automatically
      for (int i = 10000; i <= 90000; i += 10000) {
        splits.put(row(i), TabletMergeability.never());
      }

      c.tableOperations().putSplits(tableName, splits);
      // Verify we now have 10 tablets
      // [row_0000010000, row_0000020000, row_0000030000, row_0000040000, row_0000050000,
      // row_0000060000, row_0000070000, row_0000080000, row_0000090000, default]
      Wait.waitFor(() -> countTablets(getCluster().getServerContext(), tableName, tm -> true) == 10,
          5000, 500);

      // Insert rows into each tablet with different numbers of rows
      // Tablets with end rows row_0000020000 - row_0000040000, row_0000060000 - row_0000080000,
      // default will have 1000 rows
      // Tablets with end rows row_0000010000, row_0000050000, row_0000090000 will have 5000 rows
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        final var value = StringUtils.repeat("a", 1024);
        for (int i = 0; i < 100000; i += 10000) {
          var rows = 1000;
          if (i % 40000 == 0) {
            rows = 5000;
          }
          for (int j = 0; j < rows; j++) {
            Mutation m = new Mutation(row(i + j));
            m.put(new Text("cf1"), new Text("cq1"), new Value(value));
            bw.addMutation(m);
          }
        }
      }
      c.tableOperations().flush(tableName, null, null, true);

      // Set all 10 tablets to be auto-mergeable
      for (int i = 10000; i <= 90000; i += 10000) {
        splits.put(row(i), TabletMergeability.always());
      }
      c.tableOperations().putSplits(tableName, splits);

      // With the mergeability threshold set to 50% of 32KB we should be able to merge together
      // the tablets with 1000 rows, but not 5000 rows. This should produce the following
      // 6 tablets after merger.
      Wait.waitFor(() -> hasExactTablets(getCluster().getServerContext(), tableId,
          Set.of(new KeyExtent(tableId, row(10000), null),
              new KeyExtent(tableId, row(40000), row(10000)),
              new KeyExtent(tableId, row(50000), row(40000)),
              new KeyExtent(tableId, row(80000), row(50000)),
              new KeyExtent(tableId, row(90000), row(80000)),
              new KeyExtent(tableId, null, row(90000)))),
          10000, 200);
    }
  }

  @Test
  public void testSplitAndMergeAll() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "16K");
      props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K");
      c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props)
          .withInitialTabletAvailability(TabletAvailability.HOSTED));
      var tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      // Ingest data so tablet will split
      VerifyParams params = new VerifyParams(getClientProps(), tableName, 5_000);
      TestIngest.ingest(c, params);
      c.tableOperations().flush(tableName);
      VerifyIngest.verifyIngest(c, params);

      // Wait for table to split, should be more than 10 tablets
      Wait.waitFor(() -> c.tableOperations().listSplits(tableName).size() > 10, 30000, 200);

      // Delete all the data - We can't use deleteRows() as that would merge empty tablets
      // Instead, we want the mergeability thread to merge so use a batch deleter and
      // compact away the deleted data
      var bd = c.createBatchDeleter(tableName, Authorizations.EMPTY, 1);
      bd.setRanges(List.of(new Range()));
      bd.delete();
      c.tableOperations().compact(tableName, new CompactionConfig().setFlush(true));

      // Wait for merge back to default tablet
      Wait.waitFor(() -> hasExactTablets(getCluster().getServerContext(), tableId,
          Set.of(new KeyExtent(tableId, null, null))), 30000, 200);
    }
  }

  @Test
  public void testMergeabilityThreshold() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,String> props = new HashMap<>();
      props.put(Property.TABLE_SPLIT_THRESHOLD.getKey(), "16K");
      props.put(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K");
      // Set a low threshold to 1% of the split threshold
      props.put(Property.TABLE_MAX_MERGEABILITY_THRESHOLD.getKey(), ".01");
      c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props)
          .withInitialTabletAvailability(TabletAvailability.HOSTED));
      var tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      // Ingest data so tablet will split
      VerifyParams params = new VerifyParams(getClientProps(), tableName, 5_000);
      TestIngest.ingest(c, params);
      c.tableOperations().flush(tableName);
      VerifyIngest.verifyIngest(c, params);

      // Wait for table to split, should be more than 10 tablets
      Wait.waitFor(() -> c.tableOperations().listSplits(tableName).size() > 10, 10000, 200);

      // Set the split threshold back to the default of 5 MB. There's not a lot of data so normally
      // we could merge back to 1 tablet, but the threshold is too low at 1% so it should not merge
      // yet.
      c.tableOperations().setProperty(tableName, Property.TABLE_SPLIT_THRESHOLD.getKey(), "5m");

      // Should not merge so make sure it throws IllegalStateException
      assertThrows(IllegalStateException.class,
          () -> Wait.waitFor(() -> hasExactTablets(getCluster().getServerContext(), tableId,
              Set.of(new KeyExtent(tableId, null, null))), 5000, 500));
      // Make sure we failed because of exact tablets and not a different IllegalStateException
      assertFalse(hasExactTablets(getCluster().getServerContext(), tableId,
          Set.of(new KeyExtent(tableId, null, null))));

      // With a 10% threshold we should be able to merge
      c.tableOperations().setProperty(tableName, Property.TABLE_MAX_MERGEABILITY_THRESHOLD.getKey(),
          ".1");

      // Wait for merge back to default tablet
      Wait.waitFor(() -> hasExactTablets(getCluster().getServerContext(), tableId,
          Set.of(new KeyExtent(tableId, null, null))), 10000, 200);
    }
  }

  @Test
  public void testMergeAfter() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);
      var tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      TreeSet<Text> splits = new TreeSet<>();
      splits.add(new Text(String.format("%09d", 333)));
      splits.add(new Text(String.format("%09d", 666)));
      splits.add(new Text(String.format("%09d", 999)));

      var delay = Duration.ofSeconds(5);
      var startTime = c.instanceOperations().getManagerTime();
      c.tableOperations().putSplits(tableName, TabletMergeabilityUtil.splitsWithDefault(splits,
          TabletMergeability.after(Duration.ofSeconds(5))));

      Wait.waitFor(() -> countTablets(getCluster().getServerContext(), tableName, tm -> true) == 4,
          5000, 200);

      // Wait for merge back to default tablet
      Wait.waitFor(() -> hasExactTablets(getCluster().getServerContext(), tableId,
          Set.of(new KeyExtent(tableId, null, null))), 10000, 200);

      var elapsed = c.instanceOperations().getManagerTime().minus(startTime);
      assertTrue(elapsed.compareTo(delay) > 0);
    }
  }

  @Test
  public void testMergeabilityMaxFiles() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      Map<String,String> props = new HashMap<>();
      // disable compactions and set a low merge file max
      props.put(Property.TABLE_MAJC_RATIO.getKey(), "9999");
      props.put(Property.TABLE_MERGE_FILE_MAX.getKey(), "3");
      c.tableOperations().create(tableName, new NewTableConfiguration().setProperties(props)
          .withInitialTabletAvailability(TabletAvailability.HOSTED));
      var tableId = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      // Create new tablets that won't merge automatically
      SortedMap<Text,TabletMergeability> splits = new TreeMap<>();
      for (int i = 500; i < 5000; i += 500) {
        splits.put(row(i), TabletMergeability.never());
      }
      c.tableOperations().putSplits(tableName, splits);

      // Verify we now have 10 tablets
      Wait.waitFor(() -> countTablets(getCluster().getServerContext(), tableName, tm -> true) == 10,
          5000, 500);

      // Ingest data so tablet will split, each tablet will have several files because
      // of the flush setting
      VerifyParams params = new VerifyParams(getClientProps(), tableName, 5_000);
      params.startRow = 0;
      params.flushAfterRows = 100;
      TestIngest.ingest(c, params);
      VerifyIngest.verifyIngest(c, params);

      // Mark all tablets as mergeable
      for (int i = 500; i < 5000; i += 500) {
        splits.put(row(i), TabletMergeability.always());
      }
      c.tableOperations().putSplits(tableName, splits);

      // Should not merge as we set max file count to only 3 and there are more files than that
      // per tablet, so make sure it throws IllegalStateException
      assertThrows(IllegalStateException.class,
          () -> Wait.waitFor(() -> hasExactTablets(getCluster().getServerContext(), tableId,
              Set.of(new KeyExtent(tableId, null, null))), 5000, 500));
      // Make sure tablets is still 10, not merged
      assertEquals(10, countTablets(getCluster().getServerContext(), tableName, tm -> true));

      // Set max merge file count back to default of 10k
      c.tableOperations().setProperty(tableName, Property.TABLE_MERGE_FILE_MAX.getKey(), "10000");

      // Should merge back to 1 tablet
      Wait.waitFor(() -> hasExactTablets(getCluster().getServerContext(), tableId,
          Set.of(new KeyExtent(tableId, null, null))), 10000, 200);
    }
  }

  private static boolean hasExactTablets(ServerContext ctx, TableId tableId,
      Set<KeyExtent> expected) {
    try (var tabletsMetadata = ctx.getAmple().readTablets().forTable(tableId).build()) {
      // check for exact tablets by counting tablets that match the expected rows and also
      // making sure the number seen equals exactly expected
      final var expectedTablets = new HashSet<>(expected);
      for (TabletMetadata tm : tabletsMetadata) {
        // make sure every tablet seen is contained in the expected set
        if (!expectedTablets.remove(tm.getExtent())) {
          return false;
        }
      }
      // Verify all tablets seen
      return expectedTablets.isEmpty();
    }
  }

  private static Text row(int row) {
    return generateRow(row, 0);
  }
}

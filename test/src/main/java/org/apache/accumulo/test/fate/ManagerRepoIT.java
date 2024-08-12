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
package org.apache.accumulo.test.fate;

import static org.apache.accumulo.core.client.ConditionalWriter.Status.ACCEPTED;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.UNKNOWN;
import static org.apache.accumulo.test.ample.TestAmpleUtil.mockWithAmple;
import static org.apache.accumulo.test.ample.metadata.ConditionalWriterInterceptor.withStatus;
import static org.apache.accumulo.test.ample.metadata.TestAmple.not;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.assertNoCompactionMetadata;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SplitColumnFamily;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.compact.CompactionDriver;
import org.apache.accumulo.manager.tableOps.merge.DeleteRows;
import org.apache.accumulo.manager.tableOps.merge.MergeInfo;
import org.apache.accumulo.manager.tableOps.merge.MergeInfo.Operation;
import org.apache.accumulo.manager.tableOps.merge.MergeTablets;
import org.apache.accumulo.manager.tableOps.merge.ReserveTablets;
import org.apache.accumulo.manager.tableOps.split.AllocateDirsAndEnsureOnline;
import org.apache.accumulo.manager.tableOps.split.FindSplits;
import org.apache.accumulo.manager.tableOps.split.PreSplit;
import org.apache.accumulo.manager.tableOps.split.SplitInfo;
import org.apache.accumulo.test.LargeSplitRowIT;
import org.apache.accumulo.test.ample.metadata.TestAmple;
import org.apache.accumulo.test.ample.metadata.TestAmple.TestServerAmpleImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.collect.Sets;

public class ManagerRepoIT extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @ParameterizedTest
  @EnumSource(MergeInfo.Operation.class)
  public void testNoWalsMergeRepos(MergeInfo.Operation operation) throws Exception {
    String[] tableNames = getUniqueNames(2);
    String metadataTable = tableNames[0] + operation;
    String userTable = tableNames[1] + operation;

    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(userTable);
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(userTable));

      // Set up Test ample and manager
      TestAmple.createMetadataTable(client, metadataTable);
      TestServerAmpleImpl testAmple = (TestServerAmpleImpl) TestAmple
          .create(getCluster().getServerContext(), Map.of(DataLevel.USER, metadataTable));
      testAmple.createMetadataFromExisting(client, tableId);
      Manager manager = mockWithAmple(getCluster().getServerContext(), testAmple);

      // Create a test operation and fate id for testing merge and delete rows
      // and add operation to test metadata for the tablet
      var fateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
      var opid = TabletOperationId.from(TabletOperationType.MERGING, fateId);
      var extent = new KeyExtent(tableId, null, null);

      try (TabletsMutator tm = testAmple.mutateTablets()) {
        tm.mutateTablet(extent).putOperation(opid).mutate();
      }

      // Build either MergeTablets or DeleteRows repo for testing no WALs, both should check this
      // condition
      final MergeInfo mergeInfo = new MergeInfo(tableId,
          manager.getContext().getNamespaceId(tableId), null, null, operation);
      final ManagerRepo repo =
          operation == Operation.MERGE ? new MergeTablets(mergeInfo) : new DeleteRows(mergeInfo);
      // Also test ReserveTablets isReady()
      final ManagerRepo reserve = new ReserveTablets(mergeInfo);

      // First, check no errors with the default case
      assertEquals(0, reserve.isReady(fateId, manager));
      assertNotNull(repo.call(fateId, manager));

      // Write a WAL to the test metadata and then re-run the repo to check for an error
      try (TabletsMutator tm = testAmple.mutateTablets()) {
        var walFilePath = Path.of("tserver+8080", UUID.randomUUID().toString()).toString();
        tm.mutateTablet(extent).putWal(LogEntry.fromPath(walFilePath)).mutate();
      }

      // Should not be ready due to the presence of a WAL
      assertTrue(reserve.isReady(fateId, manager) > 0);

      // Repo should throw an exception due to the WAL existence
      var thrown = assertThrows(IllegalStateException.class, () -> repo.call(fateId, manager));
      assertTrue(thrown.getMessage().contains("has unexpected walogs"));
    }
  }

  @Test
  public void testSplitOffline() throws Exception {
    String[] tableNames = getUniqueNames(2);
    String metadataTable = tableNames[0];
    String userTable = tableNames[1];

    // This test ensures a repo involved in splitting a tablet handles an offline table correctly

    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      TestAmple.createMetadataTable(client, metadataTable);

      // create a new table that is initially offline
      client.tableOperations().create(userTable, new NewTableConfiguration().createOffline());

      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(userTable));

      TestServerAmpleImpl testAmple = (TestServerAmpleImpl) TestAmple
          .create(getCluster().getServerContext(), Map.of(DataLevel.USER, metadataTable));

      testAmple.createMetadataFromExisting(client, tableId,
          not(SplitColumnFamily.UNSPLITTABLE_COLUMN));

      var fateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
      KeyExtent extent = new KeyExtent(tableId, null, null);

      // manually set an operation id on the tablet
      var opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId);
      testAmple.mutateTablet(extent)
          .putOperation(TabletOperationId.from(TabletOperationType.SPLITTING, fateId)).mutate();

      Manager manager = mockWithAmple(getCluster().getServerContext(), testAmple);

      assertEquals(opid, testAmple.readTablet(extent).getOperationId());

      var eoRepo = new AllocateDirsAndEnsureOnline(
          new SplitInfo(extent, new TreeSet<>(List.of(new Text("sp1")))));

      // The repo should delete the opid and throw an exception
      assertThrows(ThriftTableOperationException.class, () -> eoRepo.call(fateId, manager));

      // the operation id should have been cleaned up before the exception was thrown
      assertNull(testAmple.readTablet(extent).getOperationId());
    }
  }

  @Test
  public void testFindSplitsUnsplittable() throws Exception {

    String[] tableNames = getUniqueNames(2);
    String metadataTable = tableNames[0];
    String userTable = tableNames[1];

    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      TestAmple.createMetadataTable(client, metadataTable);

      // Create table with a smaller max end row size
      createUnsplittableTable(client, userTable);
      populateUnsplittableTable(client, userTable);

      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(userTable));

      TestServerAmpleImpl testAmple =
          (TestServerAmpleImpl) TestAmple.create(getCluster().getServerContext(),
              Map.of(DataLevel.USER, metadataTable), () -> withStatus(ACCEPTED, UNKNOWN, 1));
      // Prevent UNSPLITTABLE_COLUMN just in case a system split tried to run on the table
      // before we copied it and inserted the column
      testAmple.createMetadataFromExisting(client, tableId,
          not(SplitColumnFamily.UNSPLITTABLE_COLUMN));

      KeyExtent extent = new KeyExtent(tableId, null, null);
      Manager manager = mockWithAmple(getCluster().getServerContext(), testAmple);

      FindSplits findSplits = new FindSplits(extent);
      PreSplit preSplit = (PreSplit) findSplits
          .call(FateId.from(FateInstanceType.USER, UUID.randomUUID()), manager);

      // The table should not need splitting
      assertNull(preSplit);

      // Verify metadata has unsplittable column
      var metadata = testAmple.readTablet(new KeyExtent(tableId, null, null)).getUnSplittable();
      assertNotNull(metadata);

      // Bump max end row size and verify split occurs and unsplittable column is cleaned up
      client.tableOperations().setProperty(userTable, Property.TABLE_MAX_END_ROW_SIZE.getKey(),
          "500");

      findSplits = new FindSplits(extent);
      preSplit = (PreSplit) findSplits.call(FateId.from(FateInstanceType.USER, UUID.randomUUID()),
          manager);

      // The table SHOULD now need splitting
      assertNotNull(preSplit);

      // Verify unsplittable metadata is still the same and exists
      // This will not be cleared until the UpdateTablets repo runs
      // so if the test is updated to test UpdateTablets this can be checked
      assertEquals(metadata,
          testAmple.readTablet(new KeyExtent(tableId, null, null)).getUnSplittable());
    }
  }

  /**
   * The test {@link LargeSplitRowIT#testUnsplittableCleanup()} is similar to this test.
   */
  @Test
  public void testFindSplitsDeleteUnsplittable() throws Exception {

    String[] tableNames = getUniqueNames(2);
    String metadataTable = tableNames[0];
    String userTable = tableNames[1];

    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {
      TestAmple.createMetadataTable(client, metadataTable);

      // Create table with a smaller max end row size
      createUnsplittableTable(client, userTable);
      populateUnsplittableTable(client, userTable);

      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(userTable));

      TestServerAmpleImpl testAmple =
          (TestServerAmpleImpl) TestAmple.create(getCluster().getServerContext(),
              Map.of(DataLevel.USER, metadataTable), () -> withStatus(ACCEPTED, UNKNOWN, 1));
      // Prevent UNSPLITTABLE_COLUMN just in case a system split tried to run on the table
      // before we copied it and inserted the column
      testAmple.createMetadataFromExisting(client, tableId,
          not(SplitColumnFamily.UNSPLITTABLE_COLUMN));

      KeyExtent extent = new KeyExtent(tableId, null, null);
      Manager manager = mockWithAmple(getCluster().getServerContext(), testAmple);

      FindSplits findSplits = new FindSplits(extent);
      PreSplit preSplit = (PreSplit) findSplits
          .call(FateId.from(FateInstanceType.USER, UUID.randomUUID()), manager);

      // The table should not need splitting
      assertNull(preSplit);

      // Verify metadata has unsplittable column
      var metadata = testAmple.readTablet(new KeyExtent(tableId, null, null)).getUnSplittable();
      assertNotNull(metadata);

      // Increase the split threshold such that the tablet no longer needs to split. This will also
      // make the config differ from what is in the unsplittable column.
      client.tableOperations().setProperty(userTable, Property.TABLE_SPLIT_THRESHOLD.getKey(),
          "1M");

      findSplits = new FindSplits(extent);
      preSplit = (PreSplit) findSplits.call(FateId.from(FateInstanceType.USER, UUID.randomUUID()),
          manager);

      // The table SHOULD not need splitting
      assertNull(preSplit);

      // The tablet no longer needs to split so the unsplittable column should have been deleted
      assertNull(testAmple.readTablet(new KeyExtent(tableId, null, null)).getUnSplittable());
    }
  }

  @ParameterizedTest
  @MethodSource("compactionDriverRanges")
  public void testCompactionDriverCleanup(Pair<Text,Text> rangeText) throws Exception {
    // Create a range for the test and generate table names
    String[] tableNames = getUniqueNames(2);
    var range = new Range(rangeText.getFirst(), true, rangeText.getSecond(), false);
    var tableSuffix = (range.isInfiniteStartKey() ? "" : rangeText.getFirst().toString())
        + (range.isInfiniteStopKey() ? "" : rangeText.getSecond().toString());
    String metadataTable = tableNames[0] + tableSuffix;
    String userTable = tableNames[1] + tableSuffix;

    try (ClientContext client =
        (ClientContext) Accumulo.newClient().from(getClientProps()).build()) {

      // Create a table with 4 splits
      var splits = Sets.newTreeSet(Arrays.asList(new Text("d"), new Text("m"), new Text("s")));
      client.tableOperations().create(userTable, new NewTableConfiguration().withSplits(splits));
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(userTable));

      // Set up Test ample and manager
      TestAmple.createMetadataTable(client, metadataTable);
      TestServerAmpleImpl testAmple = (TestServerAmpleImpl) TestAmple
          .create(getCluster().getServerContext(), Map.of(DataLevel.USER, metadataTable));
      testAmple.createMetadataFromExisting(client, tableId);
      Manager manager = mockWithAmple(getCluster().getServerContext(), testAmple);
      var ctx = manager.getContext();

      // Create the CompactionDriver to test with the given range passed into the method
      final ManagerRepo repo = new CompactionDriver(ctx.getNamespaceId(tableId), tableId,
          !range.isInfiniteStartKey() ? range.getStartKey().getRow().getBytes() : null,
          !range.isInfiniteStopKey() ? range.getEndKey().getRow().getBytes() : null);

      // Create a couple random fateIds and generate compaction metadata for
      // the first fateId for all 4 tablets in the table
      var fateId1 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
      var fateId2 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
      createCompactionMetadata(testAmple, tableId, fateId1);

      // Verify there are 4 tablets and each tablet has compaction metadata generated
      // for the first fateId and store all the extents in a set
      Set<KeyExtent> extents = new HashSet<>();
      try (TabletsMetadata tabletsMetadata = testAmple.readTablets().forTable(tableId).build()) {
        assertEquals(4, tabletsMetadata.stream().count());
        tabletsMetadata.forEach(tm -> {
          extents.add(tm.getExtent());
          assertHasCompactionMetadata(fateId1, tm);
        });
      }
      assertEquals(4, extents.size());

      // First call undo using the second fateId and verify there's still metadata for the first one
      repo.undo(fateId2, manager);
      try (TabletsMetadata tabletsMetadata = testAmple.readTablets().forTable(tableId).build()) {
        tabletsMetadata.forEach(tm -> {
          assertHasCompactionMetadata(fateId1, tm);
        });
      }

      // Now call undo on the first fateId which would clean up all the metadata for all the
      // tablets that overlap with the given range that was provided to the CompactionDriver
      // during the creation of the repo
      repo.undo(fateId1, manager);

      // First, iterate over only the overlapping tablets and verify that those tablets
      // were cleaned up and remove any visited tablets from the extents set
      try (var tabletsMetadata = testAmple.readTablets().forTable(tableId)
          .overlapping(rangeText.getFirst(), rangeText.getSecond()).build()) {
        tabletsMetadata.forEach(tm -> {
          extents.remove(tm.getExtent());
          assertNoCompactionMetadata(tm);
        });
      }

      // Second, for any remaining tablets that did not overlap the range provided,
      // verify that metadata still exists as the CompactionDriver should not have cleaned
      // up tablets that did not overlap the given range
      try (var tabletsMetadata =
          testAmple.readTablets().forTablets(extents, Optional.empty()).build()) {
        tabletsMetadata.forEach(tm -> {
          extents.remove(tm.getExtent());
          assertHasCompactionMetadata(fateId1, tm);
        });
      }

      // Verify all the tablets in the table were checked for correct metadata after undo
      assertTrue(extents.isEmpty());
    }
  }

  private void createCompactionMetadata(Ample testAmple, TableId tableId, FateId fateId) {
    var stf1 = StoredTabletFile.of(new org.apache.hadoop.fs.Path(
        "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000050.rf"));
    var stf2 = StoredTabletFile.of(new org.apache.hadoop.fs.Path(
        "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf"));

    try (TabletsMetadata tabletsMetadata = testAmple.readTablets().forTable(tableId).build();
        TabletsMutator tabletsMutator = testAmple.mutateTablets()) {
      var selectedFiles = new SelectedFiles(Set.of(stf1, stf2), true, fateId,
          SteadyTime.from(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
      tabletsMetadata.forEach(tm -> tabletsMutator.mutateTablet(tm.getExtent()).putCompacted(fateId)
          .putUserCompactionRequested(fateId).putSelectedFiles(selectedFiles).mutate());
    }
  }

  private void assertHasCompactionMetadata(FateId fateId, TabletMetadata tm) {
    assertEquals(Set.of(fateId), tm.getCompacted());
    assertNotNull(tm.getSelectedFiles());
    assertEquals(Set.of(fateId), tm.getUserCompactionsRequested());
  }

  // Create a few ranges to test the CompactionDriver cleanup against
  private static Stream<Pair<Text,Text>> compactionDriverRanges() {
    return Stream.of(new Pair<>(null, null), new Pair<>(null, new Text("d")),
        new Pair<>(new Text("dd"), new Text("mm")), new Pair<>(new Text("s"), null));
  }

  private void createUnsplittableTable(ClientContext client, String table) throws Exception {
    // make a table and lower the configuration properties
    // @formatter:off
    Map<String,String> props = Map.of(
        Property.TABLE_SPLIT_THRESHOLD.getKey(), "1K",
        Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none",
        Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "64",
        Property.TABLE_MAX_END_ROW_SIZE.getKey(), "" + 100,
        Property.TABLE_MAJC_RATIO.getKey(), "9999"
    );
    // @formatter:on
    client.tableOperations().create(table, new NewTableConfiguration().setProperties(props));

  }

  private void populateUnsplittableTable(ClientContext client, String table) throws Exception {
    byte[] data = new byte[101];
    Arrays.fill(data, 0, data.length - 2, (byte) 'm');

    final int numOfMutations = 20;
    try (BatchWriter batchWriter = client.createBatchWriter(table)) {
      // Make the last place in the key different for every entry added to the table
      for (int i = 0; i < numOfMutations; i++) {
        data[data.length - 1] = (byte) i;
        Mutation m = new Mutation(data);
        m.put("cf", "cq", "value");
        batchWriter.addMutation(m);
      }
    }
    client.tableOperations().flush(table, null, null, true);
  }
}

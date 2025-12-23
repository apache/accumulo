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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP1;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP2;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP3;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP4;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP6;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.GROUP8;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.MAX_DATA;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.addCompactionIterators;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.assertNoCompactionMetadata;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.compact;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.createTable;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.row;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.verify;
import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.writeData;
import static org.apache.accumulo.test.fate.FateTestUtil.seedTransaction;
import static org.apache.accumulo.test.util.FileMetadataUtil.countFencedFiles;
import static org.apache.accumulo.test.util.FileMetadataUtil.splitFilesIntoRanges;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.accumulo.compactor.ExtCEnv.CompactorIterEnv;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.PluginConfig;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.admin.compaction.CompressionConfigurer;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.RowRange;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.FateKey;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.file.FilePrefix;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.SimpleCompactionDispatcher;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.FindCompactionTmpFiles;
import org.apache.accumulo.test.fate.TestLock;
import org.apache.accumulo.test.functional.CompactionIT.ErrorThrowingSelector;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ExternalCompaction_1_IT extends SharedMiniClusterBase {
  private static ServiceLock testLock;

  private static final Logger log = LoggerFactory.getLogger(ExternalCompaction_1_IT.class);

  public static class ExternalCompaction1Config implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration coreSite) {
      ExternalCompactionTestUtils.configureMiniCluster(cfg, coreSite);
    }
  }

  @BeforeAll
  public static void beforeTests() throws Exception {
    startMiniClusterWithConfig(new ExternalCompaction1Config());
    testLock = new TestLock().createTestLock(getCluster().getServerContext());
  }

  @AfterAll
  public static void afterTests() throws Exception {
    if (testLock != null) {
      testLock.unlock();
    }
    stopMiniCluster();
  }

  public static class TestFilter extends Filter {

    int modulus = 1;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

      // this cast should fail if the compaction is running in the tserver
      CompactorIterEnv cienv = (CompactorIterEnv) env;

      Preconditions.checkArgument(cienv.getQueueName() != null);
      Preconditions.checkArgument(
          options.getOrDefault("expectedQ", "").equals(cienv.getQueueName().canonical()));
      Preconditions.checkArgument(cienv.isUserCompaction());
      Preconditions.checkArgument(cienv.getIteratorScope() == IteratorScope.majc);
      Preconditions.checkArgument(!cienv.isSamplingEnabled());

      // if the init function is never called at all, then not setting the modulus option should
      // cause the test to fail
      if (options.containsKey("modulus")) {
        Preconditions.checkArgument(!options.containsKey("pmodulus"));
        Preconditions.checkArgument(cienv.isFullMajorCompaction());
        modulus = Integer.parseInt(options.get("modulus"));
      }

      // use when partial compaction is expected
      if (options.containsKey("pmodulus")) {
        Preconditions.checkArgument(!options.containsKey("modulus"));
        Preconditions.checkArgument(!cienv.isFullMajorCompaction());
        modulus = Integer.parseInt(options.get("pmodulus"));
      }
    }

    @Override
    public boolean accept(Key k, Value v) {
      return Integer.parseInt(v.toString()) % modulus == 0;
    }

  }

  @Test
  public void testBadSelector() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      final String tableName = getUniqueNames(1)[0];
      NewTableConfiguration tc = new NewTableConfiguration();
      // Ensure compactions don't kick off
      tc.setProperties(Map.of(Property.TABLE_MAJC_RATIO.getKey(), "10.0"));
      c.tableOperations().create(tableName, tc);
      // Create multiple RFiles
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 1; i <= 4; i++) {
          Mutation m = new Mutation(Integer.toString(i));
          m.put("cf", "cq", new Value());
          bw.addMutation(m);
          bw.flush();
          // flush often to create multiple files to compact
          c.tableOperations().flush(tableName, null, null, true);
        }
      }

      CompactionConfig config = new CompactionConfig()
          .setSelector(new PluginConfig(ErrorThrowingSelector.class.getName(), Map.of()))
          .setWait(true);
      assertThrows(AccumuloException.class, () -> c.tableOperations().compact(tableName, config));

      List<String> rows = new ArrayList<>();
      c.createScanner(tableName).forEach((k, v) -> rows.add(k.getRow().toString()));
      assertEquals(List.of("1", "2", "3", "4"), rows);

      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);
    }
  }

  @Test
  public void testExternalCompaction() throws Exception {
    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      String table1 = names[0];
      createTable(client, table1, "cs1");

      String table2 = names[1];
      createTable(client, table2, "cs2");

      writeData(client, table1);
      writeData(client, table2);

      compact(client, table1, 2, GROUP1, true);
      verify(client, table1, 2);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      compact(client, table2, 3, GROUP2, true);
      verify(client, table2, 3);

    }
  }

  /**
   * This test verifies the dead compaction detector does not remove compactions that are committing
   * in fate for the Root table.
   */
  @Test
  public void testCompactionCommitAndDeadDetectionRoot() throws Exception {
    var ctx = getCluster().getServerContext();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build();
        FateStore<FateEnv> metaFateStore =
            new MetaFateStore<>(ctx.getZooSession(), testLock.getLockID(), null)) {
      var tableId = ctx.getTableId(SystemTables.ROOT.tableName());
      var allCids = new HashMap<TableId,List<ExternalCompactionId>>();
      var fateId = createCompactionCommitAndDeadMetadata(c, metaFateStore,
          SystemTables.ROOT.tableName(), allCids);
      verifyCompactionCommitAndDead(metaFateStore, tableId, fateId, allCids.get(tableId));
    }
  }

  /**
   * This test verifies the dead compaction detector does not remove compactions that are committing
   * in fate for the Metadata table.
   */
  @Test
  public void testCompactionCommitAndDeadDetectionMeta() throws Exception {
    var ctx = getCluster().getServerContext();

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build();
        FateStore<FateEnv> metaFateStore =
            new MetaFateStore<>(ctx.getZooSession(), testLock.getLockID(), null)) {
      // Metadata table by default already has 2 tablets
      var tableId = ctx.getTableId(SystemTables.METADATA.tableName());
      var allCids = new HashMap<TableId,List<ExternalCompactionId>>();
      var fateId = createCompactionCommitAndDeadMetadata(c, metaFateStore,
          SystemTables.METADATA.tableName(), allCids);
      verifyCompactionCommitAndDead(metaFateStore, tableId, fateId, allCids.get(tableId));
    }
  }

  /**
   * This test verifies the dead compaction detector does not remove compactions that are committing
   * in fate for a User table.
   */
  @Test
  public void testCompactionCommitAndDeadDetectionUser() throws Exception {
    var ctx = getCluster().getServerContext();
    final String tableName = getUniqueNames(1)[0];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build();
        UserFateStore<FateEnv> userFateStore =
            new UserFateStore<>(ctx, SystemTables.FATE.tableName(), testLock.getLockID(), null)) {
      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      c.tableOperations().create(tableName, new NewTableConfiguration().withSplits(splits));
      writeData(c, tableName);

      var tableId = ctx.getTableId(tableName);
      var allCids = new HashMap<TableId,List<ExternalCompactionId>>();
      var fateId = createCompactionCommitAndDeadMetadata(c, userFateStore, tableName, allCids);
      verifyCompactionCommitAndDead(userFateStore, tableId, fateId, allCids.get(tableId));
    }
  }

  /**
   * This test verifies the dead compaction detector does not remove compactions that are committing
   * in fate when all data levels have compactions
   */
  @Test
  public void testCompactionCommitAndDeadDetectionAll() throws Exception {
    var ctx = getCluster().getServerContext();
    final String userTable = getUniqueNames(1)[0];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build();
        FateStore<FateEnv> userFateStore =
            new UserFateStore<>(ctx, SystemTables.FATE.tableName(), testLock.getLockID(), null);
        FateStore<FateEnv> metaFateStore =
            new MetaFateStore<>(ctx.getZooSession(), testLock.getLockID(), null)) {

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      c.tableOperations().create(userTable, new NewTableConfiguration().withSplits(splits));
      writeData(c, userTable);

      Map<TableId,FateId> fateIds = new HashMap<>();
      Map<TableId,List<ExternalCompactionId>> allCids = new HashMap<>();

      // create compaction metadata for each data level to test
      for (String tableName : List.of(SystemTables.ROOT.tableName(),
          SystemTables.METADATA.tableName(), userTable)) {
        var tableId = ctx.getTableId(tableName);
        var fateStore = FateInstanceType.fromTableId(tableId) == FateInstanceType.USER
            ? userFateStore : metaFateStore;
        fateIds.put(tableId,
            createCompactionCommitAndDeadMetadata(c, fateStore, tableName, allCids));
      }

      // verify the dead compaction was removed for each level
      // but not the compaction associated with a fate id
      for (Entry<TableId,FateId> entry : fateIds.entrySet()) {
        var tableId = entry.getKey();
        var fateStore = FateInstanceType.fromTableId(tableId) == FateInstanceType.USER
            ? userFateStore : metaFateStore;
        verifyCompactionCommitAndDead(fateStore, tableId, entry.getValue(), allCids.get(tableId));
      }
    }
  }

  public static class FakeFateOperation extends AbstractFateOperation {

    private static final long serialVersionUID = 1234L;

    @Override
    public long isReady(FateId fateId, FateEnv environment) throws Exception {
      return 1000;
    }

    @Override
    public Repo<FateEnv> call(FateId fateId, FateEnv environment) throws Exception {
      return null;
    }
  }

  private FateId createCompactionCommitAndDeadMetadata(AccumuloClient c,
      FateStore<FateEnv> fateStore, String tableName,
      Map<TableId,List<ExternalCompactionId>> allCids) throws Exception {
    var ctx = getCluster().getServerContext();
    c.tableOperations().flush(tableName, null, null, true);
    var tableId = ctx.getTableId(tableName);

    allCids.put(tableId, List.of(ExternalCompactionId.generate(UUID.randomUUID()),
        ExternalCompactionId.generate(UUID.randomUUID())));

    // Create a fate transaction for one of the compaction ids that is in the new state, it
    // should never run. Its purpose is to prevent the dead compaction detector
    // from deleting the id.
    Repo<FateEnv> repo = new FakeFateOperation();
    var fateId = seedTransaction(fateStore, Fate.FateOperation.COMMIT_COMPACTION,
        FateKey.forCompactionCommit(allCids.get(tableId).get(0)), repo, true).orElseThrow();

    // Read the tablet metadata
    var tabletsMeta = ctx.getAmple().readTablets().forTable(tableId).build().stream()
        .collect(Collectors.toList());
    // Root is always 1 tablet
    if (!tableId.equals(SystemTables.ROOT.tableId())) {
      assertEquals(2, tabletsMeta.size());
    }

    // Insert fake compaction entries in the metadata table. No compactor will report ownership
    // of these, so they should look like dead compactions and be removed. However, one of
    // them hasan associated fate tx that should prevent its removal.
    try (var mutator = ctx.getAmple().mutateTablets()) {
      for (int i = 0; i < tabletsMeta.size(); i++) {
        var tabletMeta = tabletsMeta.get(0);
        var tabletDir =
            tabletMeta.getFiles().stream().findFirst().orElseThrow().getPath().getParent();
        var tmpFile = new Path(tabletDir, "C1234.rf_tmp");

        CompactionMetadata cm = new CompactionMetadata(tabletMeta.getFiles(),
            ReferencedTabletFile.of(tmpFile), "localhost:16789", CompactionKind.SYSTEM, (short) 10,
            ResourceGroupId.of(GROUP1), false, null);

        mutator.mutateTablet(tabletMeta.getExtent())
            .putExternalCompaction(allCids.get(tableId).get(i), cm).mutate();
      }
    }

    return fateId;
  }

  private void verifyCompactionCommitAndDead(FateStore<FateEnv> fateStore, TableId tableId,
      FateId fateId, List<ExternalCompactionId> cids) {
    var ctx = getCluster().getServerContext();

    log.info("cids:{}", cids);
    var compactionWithFate = cids.get(0);
    var compactionWithoutFate = cids.get(1);

    // Wait until the compaction id w/o a fate transaction is removed, should still see the one
    // with a fate transaction
    Wait.waitFor(() -> {
      Set<ExternalCompactionId> currentIds = ctx.getAmple().readTablets().forTable(tableId).build()
          .stream().map(TabletMetadata::getExternalCompactions)
          .flatMap(ecm -> ecm.keySet().stream()).collect(Collectors.toSet());
      log.info("currentIds1:{}", currentIds);
      assertTrue(currentIds.contains(compactionWithFate));
      return currentIds.contains(compactionWithFate) && !currentIds.contains(compactionWithoutFate);
    });

    // Delete the fate transaction, should allow the dead compaction detector to clean up the
    // remaining external compaction id
    var fateTx = fateStore.reserve(fateId);
    fateTx.delete();
    fateTx.unreserve(Duration.ZERO);

    // wait for the remaining compaction id to be removed
    Wait.waitFor(() -> {
      Set<ExternalCompactionId> currentIds = ctx.getAmple().readTablets().forTable(tableId).build()
          .stream().map(TabletMetadata::getExternalCompactions)
          .flatMap(ecm -> ecm.keySet().stream()).collect(Collectors.toSet());
      log.info("currentIds2:{}", currentIds);
      return !currentIds.contains(compactionWithFate);
    });
  }

  @Test
  public void testCompactionAndCompactorDies() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs3", 2);
      writeData(client, table1);
      verify(client, table1, 1);

      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().start(ServerType.COMPACTOR, null, 1,
          ExternalDoNothingCompactor.class);

      compact(client, table1, 2, GROUP3, false);
      TableId tid = getCluster().getServerContext().getTableId(table1);

      // Wait for the compaction to start by waiting for 1 external compaction column
      var ecids = ExternalCompactionTestUtils
          .waitForCompactionStartAndReturnEcids(getCluster().getServerContext(), tid);

      assertFalse(ecids.isEmpty());

      // Verify that a tmp file is created
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 1);

      // Kill the compactor
      getCluster().getClusterControl().stop(ServerType.COMPACTOR);

      // DeadCompactionDetector in the CompactionCoordinator should fail the compaction and delete
      // it from the tablet.
      ExternalCompactionTestUtils.waitForRunningCompactions(getCluster().getServerContext(), tid,
          ecids);

      // Verify that the tmp file are cleaned up
      Wait.waitFor(() -> FindCompactionTmpFiles
          .findTempFiles(getCluster().getServerContext(), tid.canonical()).size() == 0);

      // If the compaction actually ran it would have filtered data, so lets make sure all the data
      // written is there. This check provides evidence the compaction did not run.
      verify(client, table1, 1);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);
      getCluster().getClusterControl().stop(ServerType.TABLET_SERVER);
    } finally {
      // We stopped the TServer and started our own, restart the original TabletServers
      getCluster().getClusterControl().start(ServerType.TABLET_SERVER);
      // Restart the regular compactors
      getCluster().getClusterControl().stopAllServers(ServerType.COMPACTOR);
      getCluster().getClusterControl().start(ServerType.COMPACTOR);

    }

  }

  @Test
  public void testManytablets() throws Exception {
    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, table1, "cs4", 200);

      writeData(client, table1);

      compact(client, table1, 3, GROUP4, true);

      verify(client, table1, 3);

      List<TabletId> tabletIds;
      // start a compaction on each tablet
      try (var tablets =
          client.tableOperations().getTabletInformation(table1, List.of(RowRange.all()))) {
        tabletIds = tablets.map(TabletInformation::getTabletId).collect(Collectors.toList());
      }
      // compact the even tablet with a modulus filter of 2
      List<Range> evenRanges = new ArrayList<>();
      for (int i = 0; i < tabletIds.size(); i += 2) {
        var tabletId = tabletIds.get(i);
        CompactionConfig compactionConfig = new CompactionConfig()
            .setStartRow(tabletId.getPrevEndRow()).setEndRow(tabletId.getEndRow()).setWait(false);
        addCompactionIterators(compactionConfig, 2, GROUP4);
        client.tableOperations().compact(table1, compactionConfig);
        evenRanges.add(tabletId.toRange());
      }

      // compact the odd tablets with a modulus filter of 5
      List<Range> oddRanges = new ArrayList<>();
      for (int i = 1; i < tabletIds.size(); i += 2) {
        var tabletId = tabletIds.get(i);
        CompactionConfig compactionConfig = new CompactionConfig()
            .setStartRow(tabletId.getPrevEndRow()).setEndRow(tabletId.getEndRow()).setWait(false);
        addCompactionIterators(compactionConfig, 5, GROUP4);
        client.tableOperations().compact(table1, compactionConfig);
        oddRanges.add(tabletId.toRange());
      }

      Wait.waitFor(() -> {
        try (BatchScanner scanner = client.createBatchScanner(table1)) {
          scanner.setRanges(evenRanges);
          // filtered out data that was divisible by 3 and then 2 by compactions, so should end up
          // w/ only data divisible by 6
          int matching = 0;
          int nonMatching = 0;
          for (var entry : scanner) {
            int val = Integer.parseInt(entry.getValue().toString());
            if (val % 6 == 0) {
              matching++;
            } else {
              nonMatching++;
            }
          }
          boolean evenDone = matching > 0 && nonMatching == 0;
          // filtered out data that was divisible by 3 and then 5 by compactions, so should end up
          // w/ only data divisible by 15
          scanner.setRanges(oddRanges);
          matching = 0;
          nonMatching = 0;
          for (var entry : scanner) {
            int val = Integer.parseInt(entry.getValue().toString());
            if (val % 15 == 0) {
              matching++;
            } else {
              nonMatching++;
            }
          }
          boolean oddDone = matching > 0 && nonMatching == 0;
          return evenDone && oddDone;
        }
      });

    }
  }

  public static class FourConfigurer implements CompactionConfigurer {

    @Override
    public void init(InitParameters iparams) {}

    @Override
    public Overrides override(InputParameters params) {
      if (new Text("4").equals(params.getTabletId().getEndRow())) {
        return new Overrides(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "gz"));
      } else {
        return new Overrides(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none"));
      }
    }
  }

  public static class UriConfigurer implements CompactionConfigurer {

    @Override
    public void init(InitParameters iparams) {}

    @Override
    public Overrides override(InputParameters params) {
      // This will validate the paths looks like a tablet file path and throw an exception if it
      // does not
      var parts = ReferencedTabletFile.of(new Path(params.getOutputFile()));
      // For this test should be producing A files
      Preconditions.checkArgument(
          parts.getFileName().startsWith(FilePrefix.FULL_COMPACTION.getPrefix() + ""));
      Preconditions.checkArgument(parts.getFileName().endsWith(RFile.EXTENSION));

      if (parts.getTabletDir()
          .equals(MetadataSchema.TabletsSection.ServerColumnFamily.DEFAULT_TABLET_DIR_NAME)) {
        return new Overrides(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "gz"));
      } else {
        return new Overrides(Map.of(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none"));
      }
    }
  }

  @Test
  public void testConfigurer() throws Exception {
    String tableName = this.getUniqueNames(1)[0];

    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      Map<String,String> props = Map.of("table.compaction.dispatcher",
          SimpleCompactionDispatcher.class.getName(), "table.compaction.dispatcher.opts.service",
          "cs5", Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none");
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);
      client.tableOperations().create(tableName, ntc);

      byte[] data = new byte[100000];
      Arrays.fill(data, (byte) 65);
      try (var writer = client.createBatchWriter(tableName)) {
        for (int row = 0; row < 10; row++) {
          Mutation m = new Mutation(row + "");
          m.at().family("big").qualifier("stuff").put(data);
          writer.addMutation(m);
        }
      }
      client.tableOperations().flush(tableName, null, null, true);

      // without compression, expect file to be large
      long sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

      client.tableOperations().compact(tableName,
          new CompactionConfig().setWait(true)
              .setConfigurer(new PluginConfig(CompressionConfigurer.class.getName(),
                  Map.of(CompressionConfigurer.LARGE_FILE_COMPRESSION_TYPE, "gz",
                      CompressionConfigurer.LARGE_FILE_COMPRESSION_THRESHOLD, data.length + ""))));
      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      // after compacting with compression, expect small file
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes < data.length,
          "Unexpected files sizes: data: " + data.length + ", file:" + sizes);

      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      // after compacting without compression, expect big files again
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(tableName);

      // compact using a custom configurer that considers tablet end row
      client.tableOperations().addSplits(tableName, new TreeSet<>(List.of(new Text("4"))));
      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true)
          .setConfigurer(new PluginConfig(FourConfigurer.class.getName())));
      var tabletSizes = getFilesSizesPerTablet(client, tableName);
      assertTrue(tabletSizes.get("4") > 0 && tabletSizes.get("4") < 1000, tabletSizes.toString());
      assertTrue(tabletSizes.get("null") > 500_000 && tabletSizes.get("4") < 510_000,
          tabletSizes.toString());

      // compact using a custom configurer that considers the output path, should invert which file
      // is compressed
      client.tableOperations().addSplits(tableName, new TreeSet<>(List.of(new Text("4"))));
      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true)
          .setConfigurer(new PluginConfig(UriConfigurer.class.getName())));
      tabletSizes = getFilesSizesPerTablet(client, tableName);
      assertTrue(tabletSizes.get("4") > 500_000 && tabletSizes.get("4") < 510_000,
          tabletSizes.toString());
      assertTrue(tabletSizes.get("null") > 0 && tabletSizes.get("null") < 1000,
          tabletSizes.toString());

    }
  }

  private Map<String,Long> getFilesSizesPerTablet(AccumuloClient client, String table)
      throws Exception {
    var ctx = (ClientContext) client;
    var ample = ctx.getAmple();
    var id = ctx.getTableId(table);

    try (var tablets = ample.readTablets().forTable(id).build()) {
      Map<String,Long> sizes = new TreeMap<>();
      for (var tablet : tablets) {
        var tsize = tablet.getFilesMap().values().stream().mapToLong(DataFileValue::getSize).sum();
        var endRow = tablet.getEndRow();
        sizes.put(endRow == null ? "null" : endRow.toString(), tsize);
      }
      return sizes;
    }
  }

  @Test
  public void testConfigurerSetOnTable() throws Exception {
    String tableName = this.getUniqueNames(1)[0];

    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      byte[] data = new byte[100000];

      Map<String,String> props = Map.of("table.compaction.dispatcher",
          SimpleCompactionDispatcher.class.getName(), "table.compaction.dispatcher.opts.service",
          "cs5", Property.TABLE_FILE_COMPRESSION_TYPE.getKey(), "none",
          Property.TABLE_COMPACTION_CONFIGURER.getKey(), CompressionConfigurer.class.getName(),
          Property.TABLE_COMPACTION_CONFIGURER_OPTS.getKey()
              + CompressionConfigurer.LARGE_FILE_COMPRESSION_TYPE,
          "gz", Property.TABLE_COMPACTION_CONFIGURER_OPTS.getKey()
              + CompressionConfigurer.LARGE_FILE_COMPRESSION_THRESHOLD,
          "" + data.length);
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(props);
      client.tableOperations().create(tableName, ntc);

      Arrays.fill(data, (byte) 65);
      try (var writer = client.createBatchWriter(tableName)) {
        for (int row = 0; row < 10; row++) {
          Mutation m = new Mutation(row + "");
          m.at().family("big").qualifier("stuff").put(data);
          writer.addMutation(m);
        }
      }
      client.tableOperations().flush(tableName, null, null, true);

      // without compression, expect file to be large
      long sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes > data.length * 10 && sizes < data.length * 11,
          "Unexpected files sizes : " + sizes);

      client.tableOperations().compact(tableName, new CompactionConfig().setWait(true));
      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      // after compacting with compression, expect small file
      sizes = CompactionExecutorIT.getFileSizes(client, tableName);
      assertTrue(sizes < data.length,
          "Unexpected files sizes: data: " + data.length + ", file:" + sizes);

      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(tableName);
    }
  }

  public static class ExtDevNull extends DevNull {
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) throws IOException {
      super.init(source, options, env);

      // this cast should fail if the compaction is running in the tserver
      CompactorIterEnv cienv = (CompactorIterEnv) env;

      Preconditions.checkArgument(cienv.getQueueName() != null);
    }
  }

  @Test
  public void testExternalCompactionWithTableIterator() throws Exception {
    // in addition to testing table configured iters w/ external compaction, this also tests an
    // external compaction that deletes everything

    String table1 = this.getUniqueNames(1)[0];
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {
      createTable(client, table1, "cs6");
      writeData(client, table1);
      compact(client, table1, 2, GROUP6, true);
      verify(client, table1, 2);

      IteratorSetting setting = new IteratorSetting(50, "delete", ExtDevNull.class);
      client.tableOperations().attachIterator(table1, setting, EnumSet.of(IteratorScope.majc));
      client.tableOperations().compact(table1, new CompactionConfig().setWait(true));

      try (Scanner s = client.createScanner(table1)) {
        assertFalse(s.iterator().hasNext());
      }

      assertNoCompactionMetadata(getCluster().getServerContext(), table1);

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(table1);
    }
  }

  @Test
  public void testExternalCompactionWithFencedFiles() throws Exception {
    String[] names = this.getUniqueNames(2);
    try (AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      String table1 = names[0];
      createTable(client, table1, "cs1");

      String table2 = names[1];
      createTable(client, table2, "cs2");

      writeData(client, table1);
      writeData(client, table2);

      // Verify that all data can be seen
      verify(client, table1, 1, MAX_DATA);
      verify(client, table2, 1, MAX_DATA);

      // Split file in table1 into two files each fenced off by 100 rows for a total of 200
      splitFilesIntoRanges(getCluster().getServerContext(), table1,
          Set.of(new Range(new Text(row(99)), false, new Text(row(199)), true),
              new Range(new Text(row(299)), false, new Text(row(399)), true)));
      assertEquals(2, countFencedFiles(getCluster().getServerContext(), table1));

      // Fence file in table2 to 600 rows
      splitFilesIntoRanges(getCluster().getServerContext(), table2,
          Set.of(new Range(new Text(row(199)), false, new Text(row(799)), true)));
      assertEquals(1, countFencedFiles(getCluster().getServerContext(), table2));

      // Verify that a subset of the data is now seen after fencing
      verify(client, table1, 1, 200);
      verify(client, table2, 1, 600);

      // Compact and verify previousy fenced data didn't come back
      compact(client, table1, 2, GROUP1, true);
      verify(client, table1, 2, 200);

      SortedSet<Text> splits = new TreeSet<>();
      splits.add(new Text(row(MAX_DATA / 2)));
      client.tableOperations().addSplits(table2, splits);

      // Compact and verify previousy fenced data didn't come back
      compact(client, table2, 3, GROUP2, true);
      verify(client, table2, 3, 600);

      // should be no more fenced files after compaction
      assertEquals(0, countFencedFiles(getCluster().getServerContext(), table1));
      assertEquals(0, countFencedFiles(getCluster().getServerContext(), table2));
    }
  }

  public static class FSelector implements CompactionSelector {

    @Override
    public void init(InitParameters iparams) {}

    @Override
    public Selection select(SelectionParameters sparams) {
      List<CompactableFile> toCompact = sparams.getAvailableFiles().stream()
          .filter(cf -> cf.getFileName().startsWith("F")).collect(Collectors.toList());
      return new Selection(toCompact);
    }

  }

  @Test
  public void testPartialCompaction() throws Exception {
    String tableName = getUniqueNames(1)[0];
    try (final AccumuloClient client =
        Accumulo.newClient().from(getCluster().getClientProperties()).build()) {

      createTable(client, tableName, "cs8");

      writeData(client, tableName);
      // This should create an A file
      compact(client, tableName, 17, GROUP8, true);
      verify(client, tableName, 17);

      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = MAX_DATA; i < MAX_DATA * 2; i++) {
          Mutation m = new Mutation(row(i));
          m.put("", "", "" + i);
          bw.addMutation(m);
        }
      }

      // this should create an F file
      client.tableOperations().flush(tableName);

      // run a compaction that only compacts F files
      IteratorSetting iterSetting = new IteratorSetting(100, TestFilter.class);
      // make sure iterator options make it to compactor process
      iterSetting.addOption("expectedQ", GROUP8);
      // compact F file w/ different modulus and user pmodulus option for partial compaction
      iterSetting.addOption("pmodulus", 19 + "");
      CompactionConfig config = new CompactionConfig().setIterators(List.of(iterSetting))
          .setWait(true).setSelector(new PluginConfig(FSelector.class.getName()));
      client.tableOperations().compact(tableName, config);
      assertNoCompactionMetadata(getCluster().getServerContext(), tableName);

      try (Scanner scanner = client.createScanner(tableName)) {
        int count = 0;
        for (Entry<Key,Value> entry : scanner) {

          int v = Integer.parseInt(entry.getValue().toString());
          int modulus = v < MAX_DATA ? 17 : 19;

          assertEquals(0, Integer.parseInt(entry.getValue().toString()) % modulus,
              String.format("%s %s %d != 0", entry.getValue(), "%", modulus));
          count++;
        }

        int expectedCount = 0;
        for (int i = 0; i < MAX_DATA * 2; i++) {
          int modulus = i < MAX_DATA ? 17 : 19;
          if (i % modulus == 0) {
            expectedCount++;
          }
        }

        assertEquals(expectedCount, count);
      }

      // We need to cancel the compaction or delete the table here because we initiate a user
      // compaction above in the test. Even though the external compaction was cancelled
      // because we split the table, FaTE will continue to queue up a compaction
      client.tableOperations().cancelCompaction(tableName);
    }

  }

}

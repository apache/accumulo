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

import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.SELECTED_COLUMN;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;
import static org.apache.accumulo.core.util.LazySingletons.GSON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.metadata.ConditionalTabletsMutatorImpl;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AmpleConditionalWriterIT extends AccumuloClusterHarness {

  // ELASTICITY_TODO ensure that all conditional updates are tested

  private TableId tid;
  private KeyExtent e1;
  private KeyExtent e2;
  private KeyExtent e3;
  private KeyExtent e4;

  @BeforeEach
  public void setupTable() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("j")));
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).createOffline());

      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);

      tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      e1 = new KeyExtent(tid, new Text("c"), null);
      e2 = new KeyExtent(tid, new Text("f"), new Text("c"));
      e3 = new KeyExtent(tid, new Text("j"), new Text("f"));
      e4 = new KeyExtent(tid, null, new Text("j"));
    }
  }

  @Test
  public void testLocations() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      var ts1 = new TServerInstance("localhost:9997", 5000L);
      var ts2 = new TServerInstance("localhost:9997", 6000L);

      var context = cluster.getServerContext();

      assertNull(context.getAmple().readTablet(e1).getLocation());

      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit(tm -> false);
      var results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Location.future(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Location.future(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.future(ts1))
          .putLocation(Location.current(ts1)).deleteLocation(Location.future(ts1))
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Location.current(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.future(ts1))
          .putLocation(Location.current(ts1)).deleteLocation(Location.future(ts1))
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Location.current(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.future(ts2))
          .putLocation(Location.current(ts2)).deleteLocation(Location.future(ts2))
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Location.current(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.current(ts1))
          .deleteLocation(Location.current(ts1)).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertNull(context.getAmple().readTablet(e1).getLocation());
    }
  }

  @Test
  public void testFiles() throws Exception {

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var ts1 = new TServerInstance("localhost:9997", 5000L);
      var ts2 = new TServerInstance("localhost:9997", 6000L);

      var context = cluster.getServerContext();

      var stf1 = StoredTabletFile
          .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf"));
      var stf2 = StoredTabletFile
          .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000071.rf"));
      var stf3 = StoredTabletFile
          .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000072.rf"));
      var stf4 = StoredTabletFile
          .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/C0000073.rf"));
      var dfv = new DataFileValue(100, 100);

      System.out.println(context.getAmple().readTablet(e1).getLocation());

      // simulate a compaction where the tablet location is not set
      var ctmi = new ConditionalTabletsMutatorImpl(context);

      var tm1 = TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv)
          .build();
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm1, PREV_ROW, FILES)
          .putFile(stf4, new DataFileValue(0, 0)).submit(tm -> false);
      var results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Set.of(), context.getAmple().readTablet(e1).getFiles());

      var tm2 = TabletMetadata.builder(e1).putLocation(Location.current(ts1)).build();
      // simulate minor compacts where the tablet location is not set
      for (StoredTabletFile file : List.of(stf1, stf2, stf3)) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm2, LOCATION)
            .putFile(file, new DataFileValue(0, 0)).submit(tm -> false);
        results = ctmi.process();
        assertEquals(Status.REJECTED, results.get(e1).getStatus());
      }

      assertEquals(Set.of(), context.getAmple().readTablet(e1).getFiles());

      // set the location
      var tm3 = TabletMetadata.builder(e1).build(LOCATION);
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm3, LOCATION)
          .putLocation(Location.current(ts1)).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      var tm4 = TabletMetadata.builder(e1).putLocation(Location.current(ts2)).build();
      // simulate minor compacts where the tablet location is wrong
      for (StoredTabletFile file : List.of(stf1, stf2, stf3)) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm4, LOCATION)
            .putFile(file, new DataFileValue(0, 0)).submit(tm -> false);
        results = ctmi.process();
        assertEquals(Status.REJECTED, results.get(e1).getStatus());
      }

      assertEquals(Set.of(), context.getAmple().readTablet(e1).getFiles());

      // simulate minor compacts where the tablet location is set
      for (StoredTabletFile file : List.of(stf1, stf2, stf3)) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm2, LOCATION)
            .putFile(file, new DataFileValue(0, 0)).submit(tm -> false);
        results = ctmi.process();
        assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      }

      assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());

      // simulate a compaction and test a subset and superset of files
      for (var tabletMeta : List.of(
          TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).build(),
          TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv)
              .putFile(stf4, dfv).build())) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta, FILES)
            .putFile(stf4, new DataFileValue(0, 0)).deleteFile(stf1).deleteFile(stf2)
            .deleteFile(stf3).submit(tm -> false);
        results = ctmi.process();
        assertEquals(Status.REJECTED, results.get(e1).getStatus());
        assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());
      }

      // simulate a compaction
      ctmi = new ConditionalTabletsMutatorImpl(context);
      var tm5 = TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv)
          .build();
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm5, FILES)
          .putFile(stf4, new DataFileValue(0, 0)).deleteFile(stf1).deleteFile(stf2).deleteFile(stf3)
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf4), context.getAmple().readTablet(e1).getFiles());

      // without this the metadata constraint will not allow the bulk file to be added to metadata
      TransactionWatcher.ZooArbitrator.start(context, Constants.BULK_ARBITRATOR_TYPE, 9L);

      // simulate a bulk import
      var stf5 = StoredTabletFile
          .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/b-0000009/I0000074.rf"));
      ctmi = new ConditionalTabletsMutatorImpl(context);
      var tm6 = TabletMetadata.builder(e1).build(LOADED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm6, LOADED)
          .putFile(stf5, new DataFileValue(0, 0)).putBulkFile(stf5.getTabletFile(), 9L)
          .putFile(stf5, new DataFileValue(0, 0)).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf4, stf5), context.getAmple().readTablet(e1).getFiles());

      // simulate a compaction
      var stf6 = StoredTabletFile
          .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/A0000075.rf"));
      ctmi = new ConditionalTabletsMutatorImpl(context);
      var tm7 = TabletMetadata.builder(e1).putFile(stf4, dfv).putFile(stf5, dfv).build();
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm7, FILES)
          .putFile(stf6, new DataFileValue(0, 0)).deleteFile(stf4).deleteFile(stf5)
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf6), context.getAmple().readTablet(e1).getFiles());

      // simulate trying to re bulk import file after a compaction
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm6, LOADED)
          .putFile(stf5, new DataFileValue(0, 0)).putBulkFile(stf5.getTabletFile(), 9L)
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf6), context.getAmple().readTablet(e1).getFiles());
    }
  }

  @Test
  public void testSelectedFiles() throws Exception {
    var context = cluster.getServerContext();

    var stf1 = StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf"));
    var stf2 = StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000071.rf"));
    var stf3 = StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000072.rf"));
    var stf4 = StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/C0000073.rf"));
    var dfv = new DataFileValue(100, 100);

    System.out.println(context.getAmple().readTablet(e1).getLocation());

    // simulate a compaction where the tablet location is not set
    var ctmi = new ConditionalTabletsMutatorImpl(context);
    var tm1 = TabletMetadata.builder(e1).build(FILES, SELECTED);
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm1, PREV_ROW, FILES)
        .putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv).submit(tm -> false);
    var results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

    assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());

    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm1, PREV_ROW, FILES, SELECTED)
        .putSelectedFiles(new SelectedFiles(Set.of(stf1, stf2, stf3), true, 2L))
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.REJECTED, results.get(e1).getStatus());

    assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());
    assertNull(context.getAmple().readTablet(e1).getSelectedFiles());

    var tm2 = TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv)
        .build(SELECTED);
    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm2, PREV_ROW, FILES, SELECTED)
        .putSelectedFiles(new SelectedFiles(Set.of(stf1, stf2, stf3), true, 2L))
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

    assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());
    assertEquals(Set.of(stf1, stf2, stf3),
        context.getAmple().readTablet(e1).getSelectedFiles().getFiles());

    // a list of selected files objects that are not the same as the current tablet and expected to
    // fail
    var expectedToFail = new ArrayList<SelectedFiles>();

    expectedToFail.add(new SelectedFiles(Set.of(stf1, stf2), true, 2L));
    expectedToFail.add(new SelectedFiles(Set.of(stf1, stf2, stf3, stf4), true, 2L));
    expectedToFail.add(new SelectedFiles(Set.of(stf1, stf2, stf3), false, 2L));
    expectedToFail.add(new SelectedFiles(Set.of(stf1, stf2, stf3), true, 3L));

    for (var selectedFiles : expectedToFail) {
      var tm3 = TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv)
          .putSelectedFiles(selectedFiles).build();
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm3, PREV_ROW, FILES, SELECTED)
          .deleteSelectedFiles().submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());
      assertEquals(Set.of(stf1, stf2, stf3),
          context.getAmple().readTablet(e1).getSelectedFiles().getFiles());
    }

    var tm5 = TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv)
        .putSelectedFiles(new SelectedFiles(Set.of(stf1, stf2, stf3), true, 2L)).build();
    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm5, PREV_ROW, FILES, SELECTED)
        .deleteSelectedFiles().submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

    assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());
    assertNull(context.getAmple().readTablet(e1).getSelectedFiles());
  }

  /**
   * Verifies that if selected files have been manually changed in the metadata, the files will be
   * re-ordered before being read
   */
  @Test
  public void testSelectedFilesReordering() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      String pathPrefix = "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/";
      StoredTabletFile stf1 = StoredTabletFile.of(new Path(pathPrefix + "F0000070.rf"));
      StoredTabletFile stf2 = StoredTabletFile.of(new Path(pathPrefix + "F0000071.rf"));
      StoredTabletFile stf3 = StoredTabletFile.of(new Path(pathPrefix + "F0000072.rf"));

      final Set<StoredTabletFile> storedTabletFiles = Set.of(stf1, stf2, stf3);
      final boolean initiallySelectedAll = true;
      final long fateTxId = 2L;
      final SelectedFiles selectedFiles =
          new SelectedFiles(storedTabletFiles, initiallySelectedAll, fateTxId);

      ConditionalTabletsMutatorImpl ctmi = new ConditionalTabletsMutatorImpl(context);

      // write the SelectedFiles to the keyextent
      ctmi.mutateTablet(e1).requireAbsentOperation().putSelectedFiles(selectedFiles)
          .submit(tm -> false);

      // verify we can read the selected files
      Status mutationStatus = ctmi.process().get(e1).getStatus();
      assertEquals(Status.ACCEPTED, mutationStatus, "Failed to put selected files to tablet");
      assertEquals(selectedFiles, context.getAmple().readTablet(e1).getSelectedFiles(),
          "Selected files should match those that were written");

      final Text row = e1.toMetaRow();
      final Text selectedColumnFamily = SELECTED_COLUMN.getColumnFamily();
      final Text selectedColumnQualifier = SELECTED_COLUMN.getColumnQualifier();

      Supplier<String> selectedMetadataValue = () -> {
        try (Scanner scanner = client.createScanner(MetadataTable.NAME)) {
          scanner.fetchColumn(selectedColumnFamily, selectedColumnQualifier);
          scanner.setRange(new Range(row));

          return scanner.stream().map(Map.Entry::getValue).map(Value::get)
              .map(entry -> new String(entry, UTF_8)).collect(onlyElement());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      String actualMetadataValue = selectedMetadataValue.get();
      final String expectedMetadata = selectedFiles.getMetadataValue();
      assertEquals(expectedMetadata, actualMetadataValue,
          "Value should be equal to metadata of SelectedFiles object that was written");

      // get the string value of the paths
      List<String> filesPathList = storedTabletFiles.stream().map(StoredTabletFile::toString)
          .sorted().collect(Collectors.toList());

      // verify we have the format of the json correct
      String newJson = createSelectedFilesJson(fateTxId, initiallySelectedAll, filesPathList);
      assertEquals(actualMetadataValue, newJson,
          "Test json should be identical to actual metadata at this point");

      // reverse the order of the files and create a new json
      Collections.reverse(filesPathList);
      newJson = createSelectedFilesJson(fateTxId, initiallySelectedAll, filesPathList);
      assertNotEquals(actualMetadataValue, newJson,
          "Test json should have reverse file order of actual metadata");

      // write the json with reverse file order
      try (BatchWriter bw = client.createBatchWriter(MetadataTable.NAME)) {
        Mutation mutation = new Mutation(row);
        mutation.put(selectedColumnFamily, selectedColumnQualifier,
            new Value(newJson.getBytes(UTF_8)));
        bw.addMutation(mutation);
      }

      // verify the metadata has been changed
      actualMetadataValue = selectedMetadataValue.get();
      assertEquals(newJson, actualMetadataValue,
          "Value should be equal to the new json we manually wrote above");

      TabletMetadata tm1 =
          TabletMetadata.builder(e1).putSelectedFiles(selectedFiles).build(SELECTED);
      ctmi = new ConditionalTabletsMutatorImpl(context);
      StoredTabletFile stf4 = StoredTabletFile.of(new Path(pathPrefix + "F0000073.rf"));
      // submit a mutation with the condition that the selected files match what was originally
      // written
      DataFileValue dfv = new DataFileValue(100, 100);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm1, SELECTED).putFile(stf4, dfv)
          .submit(tm -> false);

      mutationStatus = ctmi.process().get(e1).getStatus();

      // with a SortedFilesIterator attached to the Condition for SELECTED, the metadata should be
      // re-ordered and once again match what was originally written meaning the conditional
      // mutation should have been accepted and the file added should be present
      assertEquals(Status.ACCEPTED, mutationStatus);
      assertEquals(Set.of(stf4), context.getAmple().readTablet(e1).getFiles(),
          "Expected to see the file that was added by the mutation");
    }
  }

  /**
   * Creates a json suitable to create a SelectedFiles object from. The given parameters will be
   * inserted into the returned json String. Example of returned json String:
   *
   * <pre>
   * {
   *   "txid": "FATE[123456]",
   *   "selAll": true,
   *   "files": ["/path/to/file1.rf", "/path/to/file2.rf"]
   * }
   * </pre>
   */
  public static String createSelectedFilesJson(Long txid, boolean selAll,
      Collection<String> paths) {
    String filesJsonArray = GSON.get().toJson(paths);
    String formattedTxid = FateTxId.formatTid(Long.parseLong(Long.toString(txid), 16));
    return ("{'txid':'" + formattedTxid + "','selAll':" + selAll + ",'files':" + filesJsonArray
        + "}").replace('\'', '\"');
  }

  @Test
  public void testMultipleExtents() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var ts1 = new TServerInstance("localhost:9997", 5000L);
      var ts2 = new TServerInstance("localhost:9997", 6000L);

      var context = cluster.getServerContext();

      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit(tm -> false);
      ctmi.mutateTablet(e2).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit(tm -> false);
      var results = ctmi.process();

      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());

      assertEquals(Location.future(ts1), context.getAmple().readTablet(e1).getLocation());
      assertEquals(Location.future(ts2), context.getAmple().readTablet(e2).getLocation());
      assertNull(context.getAmple().readTablet(e3).getLocation());
      assertNull(context.getAmple().readTablet(e4).getLocation());

      assertEquals(Set.of(e1, e2), results.keySet());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit(tm -> false);
      ctmi.mutateTablet(e2).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit(tm -> false);
      ctmi.mutateTablet(e3).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit(tm -> false);
      ctmi.mutateTablet(e4).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit(tm -> false);
      results = ctmi.process();

      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Status.REJECTED, results.get(e2).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e3).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e4).getStatus());

      assertEquals(Location.future(ts1), context.getAmple().readTablet(e1).getLocation());
      assertEquals(Location.future(ts2), context.getAmple().readTablet(e2).getLocation());
      assertEquals(Location.future(ts1), context.getAmple().readTablet(e3).getLocation());
      assertEquals(Location.future(ts2), context.getAmple().readTablet(e4).getLocation());

      assertEquals(Set.of(e1, e2, e3, e4), results.keySet());

    }
  }

  @Test
  public void testOperations() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      var opid1 = TabletOperationId.from("SPLITTING:FATE[1234]");
      var opid2 = TabletOperationId.from("MERGING:FATE[5678]");

      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().putOperation(opid1).submit(tm -> false);
      ctmi.mutateTablet(e2).requireAbsentOperation().putOperation(opid2).submit(tm -> false);
      ctmi.mutateTablet(e3).requireOperation(opid1).deleteOperation().submit(tm -> false);
      var results = ctmi.process();

      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());
      assertEquals(Status.REJECTED, results.get(e3).getStatus());
      assertEquals(TabletOperationType.SPLITTING,
          context.getAmple().readTablet(e1).getOperationId().getType());
      assertEquals(opid1, context.getAmple().readTablet(e1).getOperationId());
      assertEquals(TabletOperationType.MERGING,
          context.getAmple().readTablet(e2).getOperationId().getType());
      assertEquals(opid2, context.getAmple().readTablet(e2).getOperationId());
      assertNull(context.getAmple().readTablet(e3).getOperationId());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireOperation(opid2).deleteOperation().submit(tm -> false);
      ctmi.mutateTablet(e2).requireOperation(opid1).deleteOperation().submit(tm -> false);
      results = ctmi.process();

      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Status.REJECTED, results.get(e2).getStatus());
      assertEquals(TabletOperationType.SPLITTING,
          context.getAmple().readTablet(e1).getOperationId().getType());
      assertEquals(TabletOperationType.MERGING,
          context.getAmple().readTablet(e2).getOperationId().getType());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireOperation(opid1).deleteOperation().submit(tm -> false);
      ctmi.mutateTablet(e2).requireOperation(opid2).deleteOperation().submit(tm -> false);
      results = ctmi.process();

      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());
      assertNull(context.getAmple().readTablet(e1).getOperationId());
      assertNull(context.getAmple().readTablet(e2).getOperationId());
    }
  }

  @Test
  public void testCompacted() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      var ctmi = new ConditionalTabletsMutatorImpl(context);

      var tabletMeta1 = TabletMetadata.builder(e1).build(COMPACTED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .putCompacted(55L).submit(tabletMetadata -> tabletMetadata.getCompacted().contains(55L));
      var tabletMeta2 = TabletMetadata.builder(e2).putCompacted(45L).build(COMPACTED);
      ctmi.mutateTablet(e2).requireAbsentOperation().requireSame(tabletMeta2, COMPACTED)
          .putCompacted(56L).submit(tabletMetadata -> tabletMetadata.getCompacted().contains(56L));

      var results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.REJECTED, results.get(e2).getStatus());

      tabletMeta1 = context.getAmple().readTablet(e1);
      assertEquals(Set.of(55L), tabletMeta1.getCompacted());
      assertEquals(Set.of(), context.getAmple().readTablet(e2).getCompacted());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .putCompacted(65L).putCompacted(75L).submit(tabletMetadata -> false);

      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      tabletMeta1 = context.getAmple().readTablet(e1);
      assertEquals(Set.of(55L, 65L, 75L), tabletMeta1.getCompacted());

      // test require same with a superset
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tabletMeta1 = TabletMetadata.builder(e2).putCompacted(55L).putCompacted(65L).putCompacted(75L)
          .putCompacted(45L).build(COMPACTED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .deleteCompacted(55L).deleteCompacted(65L).deleteCompacted(75L)
          .submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Set.of(55L, 65L, 75L), context.getAmple().readTablet(e1).getCompacted());

      // test require same with a subset
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tabletMeta1 = TabletMetadata.builder(e2).putCompacted(55L).putCompacted(65L).build(COMPACTED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .deleteCompacted(55L).deleteCompacted(65L).deleteCompacted(75L)
          .submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Set.of(55L, 65L, 75L), context.getAmple().readTablet(e1).getCompacted());

      // now use the exact set the tablet has
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tabletMeta1 = TabletMetadata.builder(e2).putCompacted(55L).putCompacted(65L).putCompacted(75L)
          .build(COMPACTED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .deleteCompacted(55L).deleteCompacted(65L).deleteCompacted(75L)
          .submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Set.of(), context.getAmple().readTablet(e1).getCompacted());
    }
  }

  @Test
  public void testRootTabletUpdate() {
    var context = cluster.getServerContext();

    var rootMeta = context.getAmple().readTablet(RootTable.EXTENT);
    var loc = rootMeta.getLocation();

    assertEquals(LocationType.CURRENT, loc.getType());
    assertFalse(rootMeta.getCompactId().isPresent());

    var ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(RootTable.EXTENT).requireAbsentOperation().requireAbsentLocation()
        .putCompactionId(7).submit(tm -> false);
    var results = ctmi.process();
    assertEquals(Status.REJECTED, results.get(RootTable.EXTENT).getStatus());
    assertFalse(context.getAmple().readTablet(RootTable.EXTENT).getCompactId().isPresent());

    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(RootTable.EXTENT).requireAbsentOperation()
        .requireLocation(Location.future(loc.getServerInstance())).putCompactionId(7)
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.REJECTED, results.get(RootTable.EXTENT).getStatus());
    assertFalse(context.getAmple().readTablet(RootTable.EXTENT).getCompactId().isPresent());

    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(RootTable.EXTENT).requireAbsentOperation()
        .requireLocation(Location.current(loc.getServerInstance())).putCompactionId(7)
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(RootTable.EXTENT).getStatus());
    assertEquals(7L, context.getAmple().readTablet(RootTable.EXTENT).getCompactId().getAsLong());
  }

  @Test
  public void testTime() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      for (var time : List.of(new MetadataTime(100, TimeType.LOGICAL),
          new MetadataTime(100, TimeType.MILLIS), new MetadataTime(0, TimeType.LOGICAL))) {
        var ctmi = new ConditionalTabletsMutatorImpl(context);
        var tabletMeta1 = TabletMetadata.builder(e1).putTime(time).build();
        ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, TIME)
            .putTime(new MetadataTime(101, TimeType.LOGICAL)).submit(tabletMetadata -> false);
        var results = ctmi.process();
        assertEquals(Status.REJECTED, results.get(e1).getStatus());
        assertEquals(new MetadataTime(0, TimeType.MILLIS),
            context.getAmple().readTablet(e1).getTime());
      }

      for (int i = 0; i < 10; i++) {
        var ctmi = new ConditionalTabletsMutatorImpl(context);
        var tabletMeta1 =
            TabletMetadata.builder(e1).putTime(new MetadataTime(i, TimeType.MILLIS)).build();
        ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, TIME)
            .putTime(new MetadataTime(i + 1, TimeType.MILLIS)).submit(tabletMetadata -> false);
        var results = ctmi.process();
        assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
        assertEquals(new MetadataTime(i + 1, TimeType.MILLIS),
            context.getAmple().readTablet(e1).getTime());
      }
    }
  }

  @Test
  public void testFlushId() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      assertTrue(context.getAmple().readTablet(e1).getFlushId().isEmpty());

      var ctmi = new ConditionalTabletsMutatorImpl(context);

      var tabletMeta1 = TabletMetadata.builder(e1).putFlushId(42L).build();
      assertTrue(tabletMeta1.getFlushId().isPresent());
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, FLUSH_ID)
          .putFlushId(43L).submit(tabletMetadata -> tabletMetadata.getFlushId().orElse(-1) == 43L);
      var results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertTrue(context.getAmple().readTablet(e1).getFlushId().isEmpty());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      var tabletMeta2 = TabletMetadata.builder(e1).build(FLUSH_ID);
      assertFalse(tabletMeta2.getFlushId().isPresent());
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta2, FLUSH_ID)
          .putFlushId(43L).submit(tabletMetadata -> tabletMetadata.getFlushId().orElse(-1) == 43L);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(43L, context.getAmple().readTablet(e1).getFlushId().getAsLong());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      var tabletMeta3 = TabletMetadata.builder(e1).putFlushId(43L).build();
      assertTrue(tabletMeta1.getFlushId().isPresent());
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta3, FLUSH_ID)
          .putFlushId(44L).submit(tabletMetadata -> tabletMetadata.getFlushId().orElse(-1) == 44L);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(44L, context.getAmple().readTablet(e1).getFlushId().getAsLong());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta3, FLUSH_ID)
          .putFlushId(45L).submit(tabletMetadata -> tabletMetadata.getFlushId().orElse(-1) == 45L);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(44L, context.getAmple().readTablet(e1).getFlushId().getAsLong());
    }
  }
}

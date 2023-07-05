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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
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

      var stf1 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf");
      var stf2 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000071.rf");
      var stf3 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000072.rf");
      var stf4 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/C0000073.rf");
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
      var stf5 =
          new StoredTabletFile("hdfs://localhost:8020/accumulo/tables/2a/b-0000009/I0000074.rf");
      ctmi = new ConditionalTabletsMutatorImpl(context);
      var tm6 = TabletMetadata.builder(e1).build(LOADED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm6, LOADED)
          .putFile(stf5, new DataFileValue(0, 0)).putBulkFile(stf5.getTabletFile(), 9L)
          .putFile(stf5, new DataFileValue(0, 0)).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf4, stf5), context.getAmple().readTablet(e1).getFiles());

      // simulate a compaction
      var stf6 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/A0000075.rf");
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

    var stf1 =
        new StoredTabletFile("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf");
    var stf2 =
        new StoredTabletFile("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000071.rf");
    var stf3 =
        new StoredTabletFile("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000072.rf");
    var stf4 =
        new StoredTabletFile("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/C0000073.rf");
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

  @Test
  public void testMultipleExtents() throws Exception {
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
  public void testOperations() throws Exception {
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
  public void testRootTabletUpdate() throws Exception {
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
}

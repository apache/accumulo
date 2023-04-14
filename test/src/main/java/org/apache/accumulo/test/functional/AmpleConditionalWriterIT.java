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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletOperation;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.metadata.ConditionalTabletsMutatorImpl;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class AmpleConditionalWriterIT extends AccumuloClusterHarness {

  @Test
  public void testLocations() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("j")));
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).createOffline());

      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);

      var tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      var e1 = new KeyExtent(tid, new Text("c"), null);

      var ts1 = new TServerInstance("localhost:9997", 5000L);
      var ts2 = new TServerInstance("localhost:9997", 6000L);

      var context = cluster.getServerContext();

      assertNull(context.getAmple().readTablet(e1).getLocation());

      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit();
      var results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Location.future(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit();
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Location.future(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.future(ts1))
          .putLocation(Location.current(ts1)).deleteLocation(Location.future(ts1)).submit();
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Location.current(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.future(ts1))
          .putLocation(Location.current(ts1)).deleteLocation(Location.future(ts1)).submit();
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Location.current(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.future(ts2))
          .putLocation(Location.current(ts2)).deleteLocation(Location.future(ts2)).submit();
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Location.current(ts1), context.getAmple().readTablet(e1).getLocation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.current(ts1))
          .deleteLocation(Location.current(ts1)).submit();
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertNull(context.getAmple().readTablet(e1).getLocation());
    }
  }

  @Test
  public void testFiles() throws Exception {

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("j")));
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).createOffline());

      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);

      var tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      var e1 = new KeyExtent(tid, new Text("c"), null);

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

      System.out.println(context.getAmple().readTablet(e1).getLocation());

      // simulate a compaction where the tablet location is not set
      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireFile(stf1).requireFile(stf2)
          .requireFile(stf3).putFile(stf4, new DataFileValue(0, 0)).submit();
      var results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Set.of(), context.getAmple().readTablet(e1).getFiles());

      // simulate minor compacts where the tablet location is not set
      for (StoredTabletFile file : List.of(stf1, stf2, stf3)) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.current(ts1))
            .putFile(file, new DataFileValue(0, 0)).submit();
        results = ctmi.process();
        assertEquals(Status.REJECTED, results.get(e1).getStatus());
      }

      assertEquals(Set.of(), context.getAmple().readTablet(e1).getFiles());

      // set the location
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.current(ts1)).submit();
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      // simulate minor compacts where the tablet location is wrong
      for (StoredTabletFile file : List.of(stf1, stf2, stf3)) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.current(ts2))
            .putFile(file, new DataFileValue(0, 0)).submit();
        results = ctmi.process();
        assertEquals(Status.REJECTED, results.get(e1).getStatus());
      }

      assertEquals(Set.of(), context.getAmple().readTablet(e1).getFiles());

      // simulate minor compacts where the tablet location is set
      for (StoredTabletFile file : List.of(stf1, stf2, stf3)) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.current(ts1))
            .putFile(file, new DataFileValue(0, 0)).submit();
        results = ctmi.process();
        assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      }

      assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());

      // simulate a compaction
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireFile(stf1).requireFile(stf2)
          .requireFile(stf3).putFile(stf4, new DataFileValue(0, 0)).deleteFile(stf1)
          .deleteFile(stf2).deleteFile(stf3).submit();
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf4), context.getAmple().readTablet(e1).getFiles());

      // without this the metadata constraint will not allow the bulk file to be added to metadata
      TransactionWatcher.ZooArbitrator.start(context, Constants.BULK_ARBITRATOR_TYPE, 9L);

      // simulate a bulk import
      var stf5 =
          new StoredTabletFile("hdfs://localhost:8020/accumulo/tables/2a/b-0000009/I0000074.rf");
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentBulkFile(stf5)
          .putFile(stf5, new DataFileValue(0, 0)).putBulkFile(stf5, 9L)
          .putFile(stf5, new DataFileValue(0, 0)).submit();
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf4, stf5), context.getAmple().readTablet(e1).getFiles());

      // simulate a compaction
      var stf6 = new StoredTabletFile(
          "hdfs://localhost:8020/accumulo/tables/2a/default_tablet/A0000075.rf");
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireFile(stf4).requireFile(stf5)
          .putFile(stf6, new DataFileValue(0, 0)).deleteFile(stf4).deleteFile(stf5).submit();
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf6), context.getAmple().readTablet(e1).getFiles());

      // simulate trying to re bulk import file after a compaction
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentBulkFile(stf5)
          .putFile(stf5, new DataFileValue(0, 0)).putBulkFile(stf5, 9L)
          .putFile(stf5, new DataFileValue(0, 0)).submit();
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf6), context.getAmple().readTablet(e1).getFiles());
    }
  }

  @Test
  public void testMultipleExtents() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("j")));
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).createOffline());

      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);

      var tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      var e1 = new KeyExtent(tid, new Text("c"), null);
      var e2 = new KeyExtent(tid, new Text("f"), new Text("c"));
      var e3 = new KeyExtent(tid, new Text("j"), new Text("f"));
      var e4 = new KeyExtent(tid, null, new Text("j"));

      var ts1 = new TServerInstance("localhost:9997", 5000L);
      var ts2 = new TServerInstance("localhost:9997", 6000L);

      var context = cluster.getServerContext();

      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit();
      ctmi.mutateTablet(e2).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit();
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
          .putLocation(Location.future(ts2)).submit();
      ctmi.mutateTablet(e2).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit();
      ctmi.mutateTablet(e3).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit();
      ctmi.mutateTablet(e4).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit();
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
      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splits = new TreeSet<>(List.of(new Text("c"), new Text("f"), new Text("j")));
      c.tableOperations().create(tableName,
          new NewTableConfiguration().withSplits(splits).createOffline());

      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);

      var tid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      var e1 = new KeyExtent(tid, new Text("c"), null);
      var e2 = new KeyExtent(tid, new Text("f"), new Text("c"));
      var e3 = new KeyExtent(tid, new Text("j"), new Text("f"));

      var context = cluster.getServerContext();

      var opid1 = new TabletOperationId("1234");
      var opid2 = new TabletOperationId("5678");

      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().putOperation(TabletOperation.SPLITTING, opid1)
          .submit();
      ctmi.mutateTablet(e2).requireAbsentOperation().putOperation(TabletOperation.MERGING, opid2)
          .submit();
      ctmi.mutateTablet(e3).requireOperation(TabletOperation.SPLITTING, opid1).deleteOperation()
          .submit();
      var results = ctmi.process();

      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());
      assertEquals(Status.REJECTED, results.get(e3).getStatus());
      assertEquals(TabletOperation.SPLITTING, context.getAmple().readTablet(e1).getOperation());
      assertEquals(opid1, context.getAmple().readTablet(e1).getOperationId());
      assertEquals(TabletOperation.MERGING, context.getAmple().readTablet(e2).getOperation());
      assertEquals(opid2, context.getAmple().readTablet(e2).getOperationId());
      assertEquals(null, context.getAmple().readTablet(e3).getOperation());
      assertEquals(null, context.getAmple().readTablet(e3).getOperationId());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireOperation(TabletOperation.MERGING, opid2).deleteOperation()
          .submit();
      ctmi.mutateTablet(e2).requireOperation(TabletOperation.SPLITTING, opid1).deleteOperation()
          .submit();
      results = ctmi.process();

      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Status.REJECTED, results.get(e2).getStatus());
      assertEquals(TabletOperation.SPLITTING, context.getAmple().readTablet(e1).getOperation());
      assertEquals(TabletOperation.MERGING, context.getAmple().readTablet(e2).getOperation());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireOperation(TabletOperation.SPLITTING, opid1).deleteOperation()
          .submit();
      ctmi.mutateTablet(e2).requireOperation(TabletOperation.MERGING, opid2).deleteOperation()
          .submit();
      results = ctmi.process();

      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());
      assertEquals(null, context.getAmple().readTablet(e1).getOperation());
      assertEquals(null, context.getAmple().readTablet(e1).getOperationId());
      assertEquals(null, context.getAmple().readTablet(e2).getOperation());
      assertEquals(null, context.getAmple().readTablet(e2).getOperationId());
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
        .putCompactionId(7).submit();
    var results = ctmi.process();
    assertEquals(Status.REJECTED, results.get(RootTable.EXTENT).getStatus());
    assertFalse(context.getAmple().readTablet(RootTable.EXTENT).getCompactId().isPresent());

    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(RootTable.EXTENT).requireAbsentOperation()
        .requireLocation(Location.future(loc.getServerInstance())).putCompactionId(7).submit();
    results = ctmi.process();
    assertEquals(Status.REJECTED, results.get(RootTable.EXTENT).getStatus());
    assertFalse(context.getAmple().readTablet(RootTable.EXTENT).getCompactId().isPresent());

    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(RootTable.EXTENT).requireAbsentOperation()
        .requireLocation(Location.current(loc.getServerInstance())).putCompactionId(7).submit();
    results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(RootTable.EXTENT).getStatus());
    assertEquals(7L, context.getAmple().readTablet(RootTable.EXTENT).getCompactId().getAsLong());
  }

}

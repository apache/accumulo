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
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FLUSH_ID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.HOSTING_REQUESTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.TIME;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.UNSPLITTABLE;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.USER_COMPACTION_REQUESTED;
import static org.apache.accumulo.core.util.LazySingletons.GSON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletMetadataBuilder;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.metadata.schema.UnSplittableMetadata;
import org.apache.accumulo.core.metadata.schema.filters.GcWalsFilter;
import org.apache.accumulo.core.metadata.schema.filters.HasCurrentFilter;
import org.apache.accumulo.core.metadata.schema.filters.TabletMetadataFilter;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.spi.balancer.TableLoadBalancer;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.metadata.AsyncConditionalTabletsMutatorImpl;
import org.apache.accumulo.server.metadata.ConditionalTabletsMutatorImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class AmpleConditionalWriterIT extends AccumuloClusterHarness {

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

      c.securityOperations().grantTablePermission("root", AccumuloTable.METADATA.tableName(),
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

      // test require absent with a future location set
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Location.future(ts1), context.getAmple().readTablet(e1).getLocation());

      try (TabletsMetadata tablets =
          context.getAmple().readTablets().forTable(tid).filter(new HasCurrentFilter()).build()) {
        List<KeyExtent> actual =
            tablets.stream().map(TabletMetadata::getExtent).collect(Collectors.toList());
        assertEquals(List.of(), actual);
      }

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.future(ts1))
          .putLocation(Location.current(ts1)).deleteLocation(Location.future(ts1))
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Location.current(ts1), context.getAmple().readTablet(e1).getLocation());

      // test require absent with a current location set
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Location.current(ts1), context.getAmple().readTablet(e1).getLocation());

      try (TabletsMetadata tablets =
          context.getAmple().readTablets().forTable(tid).filter(new HasCurrentFilter()).build()) {
        List<KeyExtent> actual =
            tablets.stream().map(TabletMetadata::getExtent).collect(Collectors.toList());
        assertEquals(List.of(e1), actual);
      }

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

      // Set two current locations, this puts the tablet in a bad state as its only expected that
      // single location should be set
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().putLocation(Location.current(ts1))
          .putLocation(Location.current(ts2)).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      // When a tablet has two locations reading it should throw an exception
      assertThrows(IllegalStateException.class, () -> context.getAmple().readTablet(e1));

      // Try to update the tablet requiring one of the locations that is set on the tablet. Even
      // though the required location exists, the presence of the other location in the tablet
      // metadata should cause the update to fail.
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(Location.current(ts1))
          .deleteLocation(Location.current(ts1)).deleteLocation(Location.current(ts2))
          .submit(tm -> false);
      results = ctmi.process();
      var finalResult1 = results.get(e1);
      // The update should be rejected because of the two locations. When a conditional mutation is
      // rejected an attempt is made to read the tablet metadata and examine. This read of the
      // tablet metadata will fail because the tablet has two locations.
      assertThrows(IllegalStateException.class, finalResult1::getStatus);

      // tablet should still have two location set, so reading it should fail
      assertThrows(IllegalStateException.class, () -> context.getAmple().readTablet(e1));

      // Requiring an absent location should fail when two locations are set
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .deleteLocation(Location.current(ts1)).deleteLocation(Location.current(ts2))
          .submit(tm -> false);
      results = ctmi.process();
      var finalResult2 = results.get(e1);
      assertThrows(IllegalStateException.class, finalResult2::getStatus);

      // tablet should still have two location set, so reading it should fail
      assertThrows(IllegalStateException.class, () -> context.getAmple().readTablet(e1));

      // Change the tablet to have a futre and current location set.
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().deleteLocation(Location.current(ts1))
          .putLocation(Location.future(ts1)).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      // tablet should still have two location set, so reading it should fail
      assertThrows(IllegalStateException.class, () -> context.getAmple().readTablet(e1));

      // Test requiring different locations. Some of the required locations are actually set and
      // some are not. All should fail because the tablet has multiple locations set and/or the
      // required location does not exist.
      for (var loc : List.of(Location.current(ts1), Location.current(ts2), Location.future(ts1),
          Location.future(ts2))) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(e1).requireAbsentOperation().requireLocation(loc)
            .deleteLocation(Location.future(ts1)).deleteLocation(Location.current(ts2))
            .submit(tm -> false);
        results = ctmi.process();
        var finalResult3 = results.get(e1);
        // tablet should still have two location set, so reading it should fail
        assertThrows(IllegalStateException.class, finalResult3::getStatus);
      }

      // Requiring an absent location should fail when a future and current location are set
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .deleteLocation(Location.current(ts1)).deleteLocation(Location.current(ts2))
          .submit(tm -> false);
      results = ctmi.process();
      var finalResult4 = results.get(e1);
      assertThrows(IllegalStateException.class, finalResult4::getStatus);

      // tablet should still have two location set, so reading it should fail
      assertThrows(IllegalStateException.class, () -> context.getAmple().readTablet(e1));

      // Delete one of the locations w/o any location requirements, this should succeed.
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().deleteLocation(Location.future(ts1))
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      // This check validates the expected state of the tablet as some of the previous test were
      // catching an exception and making assumption about the state of the tablet metadata based on
      // the fact that an exception was thrown.
      assertEquals(Location.current(ts2), context.getAmple().readTablet(e1).getLocation());
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
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm1, FILES)
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
      // First attempt should fail because the dfvs were replaced in the test
      // so the values of the files will not match
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      // Try again with the correct comapcted datafiles
      var compactedDv = new DataFileValue(0, 0);
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tm5 = TabletMetadata.builder(e1).putFile(stf1, compactedDv).putFile(stf2, compactedDv)
          .putFile(stf3, compactedDv).build();
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm5, FILES)
          .putFile(stf4, new DataFileValue(0, 0)).deleteFile(stf1).deleteFile(stf2).deleteFile(stf3)
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf4), context.getAmple().readTablet(e1).getFiles());

      // simulate a bulk import
      var stf5 = StoredTabletFile
          .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/b-0000009/I0000074.rf"));
      ctmi = new ConditionalTabletsMutatorImpl(context);
      var tm6 = TabletMetadata.builder(e1).build(LOADED);
      FateInstanceType type = FateInstanceType.fromTableId(tid);
      FateId fateId = FateId.from(type, UUID.randomUUID());
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm6, LOADED)
          .putFile(stf5, new DataFileValue(0, 0)).putBulkFile(stf5.getTabletFile(), fateId)
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf4, stf5), context.getAmple().readTablet(e1).getFiles());

      // simulate a compaction
      var stf6 = StoredTabletFile
          .of(new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/A0000075.rf"));
      ctmi = new ConditionalTabletsMutatorImpl(context);
      var tm7 =
          TabletMetadata.builder(e1).putFile(stf4, compactedDv).putFile(stf5, compactedDv).build();
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm7, FILES)
          .putFile(stf6, new DataFileValue(0, 0)).deleteFile(stf4).deleteFile(stf5)
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf6), context.getAmple().readTablet(e1).getFiles());

      // simulate trying to re bulk import file after a compaction
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm6, LOADED)
          .putFile(stf5, new DataFileValue(0, 0)).putBulkFile(stf5.getTabletFile(), fateId)
          .submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf6), context.getAmple().readTablet(e1).getFiles());
    }
  }

  @Test
  public void testWALs() {
    var context = cluster.getServerContext();

    // Test adding a WAL to a tablet and verifying its presence
    String walFilePath =
        java.nio.file.Path.of("tserver+8080", UUID.randomUUID().toString()).toString();
    final LogEntry originalLogEntry = LogEntry.fromPath(walFilePath);
    ConditionalTabletsMutatorImpl ctmi = new ConditionalTabletsMutatorImpl(context);
    // create a tablet metadata with no write ahead logs
    var tmEmptySet = TabletMetadata.builder(e1).build(LOGS);
    // tablet should not have any logs to start with so requireSame with the empty logs should pass
    assertTrue(context.getAmple().readTablet(e1).getLogs().isEmpty());
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tmEmptySet, LOGS)
        .putWal(originalLogEntry).submit(tm -> false);
    var results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

    Set<LogEntry> expectedLogs = new HashSet<>();
    expectedLogs.add(originalLogEntry);
    assertEquals(expectedLogs, new HashSet<>(context.getAmple().readTablet(e1).getLogs()),
        "The original LogEntry should be present.");

    // Test adding another WAL and verifying the update
    String walFilePath2 =
        java.nio.file.Path.of("tserver+8080", UUID.randomUUID().toString()).toString();
    LogEntry newLogEntry = LogEntry.fromPath(walFilePath2);
    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(e1).requireAbsentOperation().putWal(newLogEntry).submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

    // Verify that both the original and new WALs are present
    expectedLogs.add(newLogEntry);
    HashSet<LogEntry> actualLogs = new HashSet<>(context.getAmple().readTablet(e1).getLogs());
    assertEquals(expectedLogs, actualLogs, "Both original and new LogEntry should be present.");

    String walFilePath3 =
        java.nio.file.Path.of("tserver+8080", UUID.randomUUID().toString()).toString();
    LogEntry otherLogEntry = LogEntry.fromPath(walFilePath3);

    // create a powerset to ensure all possible subsets fail when using requireSame except the
    // expected current state
    Set<LogEntry> allLogs = Set.of(originalLogEntry, newLogEntry, otherLogEntry);
    Set<Set<LogEntry>> allSubsets = Sets.powerSet(allLogs);

    for (Set<LogEntry> subset : allSubsets) {
      // Skip the subset that matches the current state of the tablet
      if (subset.equals(expectedLogs)) {
        continue;
      }

      final TabletMetadataBuilder builder = TabletMetadata.builder(e1);
      subset.forEach(builder::putWal);
      TabletMetadata tmSubset = builder.build(LOGS);

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tmSubset, LOGS)
          .deleteWal(originalLogEntry).submit(t -> false);
      results = ctmi.process();

      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      // ensure the operation did not go through
      actualLogs = new HashSet<>(context.getAmple().readTablet(e1).getLogs());
      assertEquals(expectedLogs, actualLogs, "Both original and new LogEntry should be present.");
    }

    // Test that requiring the current WALs gets accepted when making an update (deleting a WAL in
    // this example)
    TabletMetadata tm2 =
        TabletMetadata.builder(e1).putWal(originalLogEntry).putWal(newLogEntry).build(LOGS);
    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm2, LOGS)
        .deleteWal(originalLogEntry).submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(e1).getStatus(),
        "Requiring the current WALs should result in acceptance when making an update.");

    // Verify that the update went through as expected
    assertEquals(List.of(newLogEntry), context.getAmple().readTablet(e1).getLogs(),
        "Only the new LogEntry should remain after deleting the original.");

    // test the requireAbsentLogs() function when logs are not present in the tablet metadata
    assertTrue(context.getAmple().readTablet(e2).getLogs().isEmpty());
    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(e2).requireAbsentOperation().requireAbsentLogs().putWal(originalLogEntry)
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(e2).getStatus());

    // test the requireAbsentLogs() function when logs are present in the tablet metadata
    assertFalse(context.getAmple().readTablet(e2).getLogs().isEmpty());
    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(e2).requireAbsentOperation().requireAbsentLogs().deleteWal(originalLogEntry)
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.REJECTED, results.get(e2).getStatus());
    assertFalse(context.getAmple().readTablet(e2).getLogs().isEmpty());
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
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm1, FILES).putFile(stf1, dfv)
        .putFile(stf2, dfv).putFile(stf3, dfv).submit(tm -> false);
    var results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

    assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());

    ctmi = new ConditionalTabletsMutatorImpl(context);
    FateInstanceType type = FateInstanceType.fromTableId(tid);
    FateId fateId1 = FateId.from(type, UUID.randomUUID());
    FateId fateId2 = FateId.from(type, UUID.randomUUID());
    var time = SteadyTime.from(100_100, TimeUnit.NANOSECONDS);
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm1, FILES, SELECTED)
        .putSelectedFiles(new SelectedFiles(Set.of(stf1, stf2, stf3), true, fateId1, time))
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.REJECTED, results.get(e1).getStatus());

    assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());
    assertNull(context.getAmple().readTablet(e1).getSelectedFiles());

    var tm2 = TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv)
        .build(SELECTED);
    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm2, FILES, SELECTED)
        .putSelectedFiles(new SelectedFiles(Set.of(stf1, stf2, stf3), true, fateId1, time))
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

    assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());
    assertEquals(Set.of(stf1, stf2, stf3),
        context.getAmple().readTablet(e1).getSelectedFiles().getFiles());

    // a list of selected files objects that are not the same as the current tablet and expected to
    // fail
    var expectedToFail = new ArrayList<SelectedFiles>();

    expectedToFail.add(new SelectedFiles(Set.of(stf1, stf2), true, fateId1, time));
    expectedToFail.add(new SelectedFiles(Set.of(stf1, stf2, stf3, stf4), true, fateId1, time));
    expectedToFail.add(new SelectedFiles(Set.of(stf1, stf2, stf3), false, fateId1, time));
    expectedToFail.add(new SelectedFiles(Set.of(stf1, stf2, stf3), true, fateId2, time));

    for (var selectedFiles : expectedToFail) {
      var tm3 = TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv)
          .putSelectedFiles(selectedFiles).build();
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm3, FILES, SELECTED)
          .deleteSelectedFiles().submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());

      assertEquals(Set.of(stf1, stf2, stf3), context.getAmple().readTablet(e1).getFiles());
      assertEquals(Set.of(stf1, stf2, stf3),
          context.getAmple().readTablet(e1).getSelectedFiles().getFiles());
    }

    var tm5 = TabletMetadata.builder(e1).putFile(stf1, dfv).putFile(stf2, dfv).putFile(stf3, dfv)
        .putSelectedFiles(new SelectedFiles(Set.of(stf1, stf2, stf3), true, fateId1, time)).build();
    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tm5, FILES, SELECTED)
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
      final FateInstanceType type = FateInstanceType.fromTableId(tid);
      final FateId fateId = FateId.from(type, UUID.randomUUID());
      final SteadyTime time = SteadyTime.from(100, TimeUnit.NANOSECONDS);
      final SelectedFiles selectedFiles =
          new SelectedFiles(storedTabletFiles, initiallySelectedAll, fateId, time);

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
        try (Scanner scanner = client.createScanner(AccumuloTable.METADATA.tableName())) {
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
      String newJson =
          createSelectedFilesJson(fateId, initiallySelectedAll, filesPathList, 0, time.getNanos());
      assertEquals(actualMetadataValue, newJson,
          "Test json should be identical to actual metadata at this point");

      // reverse the order of the files and create a new json
      Collections.reverse(filesPathList);
      newJson =
          createSelectedFilesJson(fateId, initiallySelectedAll, filesPathList, 0, time.getNanos());
      assertNotEquals(actualMetadataValue, newJson,
          "Test json should have reverse file order of actual metadata");

      // write the json with reverse file order
      try (BatchWriter bw = client.createBatchWriter(AccumuloTable.METADATA.tableName())) {
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
   *   "fateId": "FATE:META:12345678-9abc-def1-2345-6789abcdef12",
   *   "selAll": true,
   *   "files": ["/path/to/file1.rf", "/path/to/file2.rf"]
   * }
   * </pre>
   */
  public static String createSelectedFilesJson(FateId fateId, boolean selAll,
      Collection<String> paths, int compJobs, long selTime) {
    String filesJsonArray = GSON.get().toJson(paths);
    return ("{'fateId':'" + fateId + "','selAll':" + selAll + ",'compJobs':" + compJobs
        + ",'selTimeNanos':" + selTime + ",'files':" + filesJsonArray + "}").replace('\'', '\"');
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

      var e5 = new KeyExtent(tid, new Text("yz"), new Text("ya"));
      assertNull(context.getAmple().readTablet(e5));

      // in addition to testing multiple tablets also testing requireAbsentTablet() which is
      // currently not called elsewhere in this IT
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit(tm -> false);
      ctmi.mutateTablet(e2).requireAbsentTablet().putLocation(Location.future(ts1))
          .submit(tm -> false);
      ctmi.mutateTablet(e3).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit(tm -> false);
      ctmi.mutateTablet(e4).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit(tm -> false);
      ctmi.mutateTablet(e5).requireAbsentTablet().putDirName("t-54321")
          .putPrevEndRow(e5.prevEndRow()).putTabletAvailability(TabletAvailability.ONDEMAND)
          .submit(tm -> false);
      results = ctmi.process();

      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Status.REJECTED, results.get(e2).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e3).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e4).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e5).getStatus());

      assertEquals(Location.future(ts1), context.getAmple().readTablet(e1).getLocation());
      assertEquals(Location.future(ts2), context.getAmple().readTablet(e2).getLocation());
      assertEquals(Location.future(ts1), context.getAmple().readTablet(e3).getLocation());
      assertEquals(Location.future(ts2), context.getAmple().readTablet(e4).getLocation());
      assertEquals("t-54321", context.getAmple().readTablet(e5).getDirName());

      assertEquals(Set.of(e1, e2, e3, e4, e5), results.keySet());
    }
  }

  @Test
  public void testOperations() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      FateInstanceType type = FateInstanceType.fromTableId(tid);
      FateId fateId1 = FateId.from(type, UUID.randomUUID());
      FateId fateId2 = FateId.from(type, UUID.randomUUID());
      var opid1 = TabletOperationId.from(TabletOperationType.SPLITTING, fateId1);
      var opid2 = TabletOperationId.from(TabletOperationType.MERGING, fateId2);

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
  public void testCompaction() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      ExternalCompactionId ecid1 = ExternalCompactionId.generate(UUID.randomUUID());
      ExternalCompactionId ecid2 = ExternalCompactionId.generate(UUID.randomUUID());

      FateInstanceType type = FateInstanceType.fromTableId(tid);
      FateId fateId = FateId.from(type, UUID.randomUUID());

      ReferencedTabletFile tmpFile =
          ReferencedTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/c1.rf"));
      CompactorGroupId ceid = CompactorGroupId.of("G1");
      Set<StoredTabletFile> jobFiles =
          Set.of(StoredTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/b2.rf")));
      CompactionMetadata ecMeta = new CompactionMetadata(jobFiles, tmpFile, "localhost:4444",
          CompactionKind.SYSTEM, (short) 2, ceid, false, fateId);

      // create a tablet metadata w/ en empty set of compactions
      var tabletMetaNoCompactions = TabletMetadata.builder(e1).build(ECOMP);
      var tabletMetaCompactions =
          TabletMetadata.builder(e1).putExternalCompaction(ecid1, ecMeta).build();

      // Test different compaction requirement scenarios for tablets w/o any compactions in the
      // metadata table
      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMetaNoCompactions, ECOMP)
          .putExternalCompaction(ecid1, ecMeta).submit(tm -> false);
      ctmi.mutateTablet(e2).requireAbsentOperation().requireCompaction(ecid2)
          .putExternalCompaction(ecid1, ecMeta).submit(tm -> false);
      ctmi.mutateTablet(e3).requireAbsentOperation().requireSame(tabletMetaNoCompactions, ECOMP)
          .putExternalCompaction(ecid1, ecMeta).putExternalCompaction(ecid2, ecMeta)
          .submit(tm -> false);
      ctmi.mutateTablet(e4).requireAbsentOperation().requireSame(tabletMetaCompactions, ECOMP)
          .putExternalCompaction(ecid1, ecMeta).submit(tm -> false);
      var results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.REJECTED, results.get(e2).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e3).getStatus());
      assertEquals(Status.REJECTED, results.get(e4).getStatus());
      assertEquals(Set.of(ecid1),
          context.getAmple().readTablet(e1).getExternalCompactions().keySet());
      assertEquals(Set.of(), context.getAmple().readTablet(e2).getExternalCompactions().keySet());
      assertEquals(Set.of(ecid1, ecid2),
          context.getAmple().readTablet(e3).getExternalCompactions().keySet());
      assertEquals(Set.of(), context.getAmple().readTablet(e4).getExternalCompactions().keySet());

      // Test compaction requirements that do not match the compaction ids that exists in the
      // metadata table.
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireCompaction(ecid2)
          .deleteExternalCompaction(ecid1).submit(tm -> false);
      ctmi.mutateTablet(e3).requireAbsentOperation().requireSame(tabletMetaCompactions, ECOMP)
          .deleteExternalCompaction(ecid1).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Status.REJECTED, results.get(e3).getStatus());
      assertEquals(Set.of(ecid1),
          context.getAmple().readTablet(e1).getExternalCompactions().keySet());
      assertEquals(Set.of(ecid1, ecid2),
          context.getAmple().readTablet(e3).getExternalCompactions().keySet());

      // Test compaction requirements that match the compaction ids that exists in the metadata
      // table.
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMetaCompactions, ECOMP)
          .deleteExternalCompaction(ecid1).submit(tm -> false);
      ctmi.mutateTablet(e3).requireAbsentOperation().requireCompaction(ecid2)
          .deleteExternalCompaction(ecid1).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e3).getStatus());
      assertEquals(Set.of(), context.getAmple().readTablet(e1).getExternalCompactions().keySet());
      assertEquals(Set.of(ecid2),
          context.getAmple().readTablet(e3).getExternalCompactions().keySet());

    }
  }

  @Test
  public void testCompacted() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      var ctmi = new ConditionalTabletsMutatorImpl(context);

      FateInstanceType type = FateInstanceType.fromTableId(tid);
      FateId fateId1 = FateId.from(type, UUID.randomUUID());
      FateId fateId2 = FateId.from(type, UUID.randomUUID());
      FateId fateId3 = FateId.from(type, UUID.randomUUID());
      FateId fateId4 = FateId.from(type, UUID.randomUUID());
      FateId fateId5 = FateId.from(type, UUID.randomUUID());

      var tabletMeta1 = TabletMetadata.builder(e1).build(COMPACTED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .putCompacted(fateId2)
          .submit(tabletMetadata -> tabletMetadata.getCompacted().contains(fateId2));
      var tabletMeta2 = TabletMetadata.builder(e2).putCompacted(fateId1).build(COMPACTED);
      ctmi.mutateTablet(e2).requireAbsentOperation().requireSame(tabletMeta2, COMPACTED)
          .putCompacted(fateId3)
          .submit(tabletMetadata -> tabletMetadata.getCompacted().contains(fateId3));

      var results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.REJECTED, results.get(e2).getStatus());

      tabletMeta1 = context.getAmple().readTablet(e1);
      assertEquals(Set.of(fateId2), tabletMeta1.getCompacted());
      assertEquals(Set.of(), context.getAmple().readTablet(e2).getCompacted());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .putCompacted(fateId4).putCompacted(fateId5).submit(tabletMetadata -> false);

      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      tabletMeta1 = context.getAmple().readTablet(e1);
      assertEquals(Set.of(fateId2, fateId4, fateId5), tabletMeta1.getCompacted());

      // test require same with a superset
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tabletMeta1 = TabletMetadata.builder(e2).putCompacted(fateId2).putCompacted(fateId4)
          .putCompacted(fateId5).putCompacted(fateId1).build(COMPACTED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .deleteCompacted(fateId2).deleteCompacted(fateId4).deleteCompacted(fateId5)
          .submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Set.of(fateId2, fateId4, fateId5),
          context.getAmple().readTablet(e1).getCompacted());

      // test require same with a subset
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tabletMeta1 =
          TabletMetadata.builder(e2).putCompacted(fateId2).putCompacted(fateId4).build(COMPACTED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .deleteCompacted(fateId2).deleteCompacted(fateId4).deleteCompacted(fateId5)
          .submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Set.of(fateId2, fateId4, fateId5),
          context.getAmple().readTablet(e1).getCompacted());

      // now use the exact set the tablet has
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tabletMeta1 = TabletMetadata.builder(e2).putCompacted(fateId2).putCompacted(fateId4)
          .putCompacted(fateId5).build(COMPACTED);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, COMPACTED)
          .deleteCompacted(fateId2).deleteCompacted(fateId4).deleteCompacted(fateId5)
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
    assertNull(rootMeta.getOperationId());

    FateInstanceType type = FateInstanceType.fromTableId(RootTable.EXTENT.tableId());
    TabletOperationId opid =
        TabletOperationId.from(TabletOperationType.MERGING, FateId.from(type, UUID.randomUUID()));

    var ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(RootTable.EXTENT).requireAbsentOperation().requireAbsentLocation()
        .putOperation(opid).submit(tm -> false);
    var results = ctmi.process();
    assertEquals(Status.REJECTED, results.get(RootTable.EXTENT).getStatus());
    assertNull(context.getAmple().readTablet(RootTable.EXTENT).getOperationId());

    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(RootTable.EXTENT).requireAbsentOperation()
        .requireLocation(Location.future(loc.getServerInstance())).putOperation(opid)
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.REJECTED, results.get(RootTable.EXTENT).getStatus());
    assertNull(context.getAmple().readTablet(RootTable.EXTENT).getOperationId());

    ctmi = new ConditionalTabletsMutatorImpl(context);
    ctmi.mutateTablet(RootTable.EXTENT).requireAbsentOperation()
        .requireLocation(Location.current(loc.getServerInstance())).putOperation(opid)
        .submit(tm -> false);
    results = ctmi.process();
    assertEquals(Status.ACCEPTED, results.get(RootTable.EXTENT).getStatus());
    assertEquals(opid.canonical(),
        context.getAmple().readTablet(RootTable.EXTENT).getOperationId().canonical());
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
  public void testUserCompactionRequested() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      var ctmi = new ConditionalTabletsMutatorImpl(context);

      FateInstanceType type = FateInstanceType.fromTableId(tid);
      FateId fateId1 = FateId.from(type, UUID.randomUUID());
      FateId fateId2 = FateId.from(type, UUID.randomUUID());
      FateId fateId3 = FateId.from(type, UUID.randomUUID());
      FateId fateId4 = FateId.from(type, UUID.randomUUID());
      FateId fateId5 = FateId.from(type, UUID.randomUUID());

      var tabletMeta1 = TabletMetadata.builder(e1).build(USER_COMPACTION_REQUESTED);
      ctmi.mutateTablet(e1).requireAbsentOperation()
          .requireSame(tabletMeta1, USER_COMPACTION_REQUESTED).putUserCompactionRequested(fateId2)
          .submit(tabletMetadata -> tabletMetadata.getUserCompactionsRequested().contains(fateId2));
      var tabletMeta2 = TabletMetadata.builder(e2).putUserCompactionRequested(fateId1)
          .build(USER_COMPACTION_REQUESTED);
      ctmi.mutateTablet(e2).requireAbsentOperation()
          .requireSame(tabletMeta2, USER_COMPACTION_REQUESTED).putUserCompactionRequested(fateId3)
          .submit(tabletMetadata -> tabletMetadata.getUserCompactionsRequested().contains(fateId3));

      var results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.REJECTED, results.get(e2).getStatus());

      tabletMeta1 = context.getAmple().readTablet(e1);
      assertEquals(Set.of(fateId2), tabletMeta1.getUserCompactionsRequested());
      assertEquals(Set.of(), context.getAmple().readTablet(e2).getUserCompactionsRequested());

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation()
          .requireSame(tabletMeta1, USER_COMPACTION_REQUESTED).putUserCompactionRequested(fateId4)
          .putUserCompactionRequested(fateId5).submit(tabletMetadata -> false);

      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());

      tabletMeta1 = context.getAmple().readTablet(e1);
      assertEquals(Set.of(fateId2, fateId4, fateId5), tabletMeta1.getUserCompactionsRequested());

      // test require same with a superset
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tabletMeta1 = TabletMetadata.builder(e2).putUserCompactionRequested(fateId2)
          .putUserCompactionRequested(fateId4).putUserCompactionRequested(fateId5)
          .putUserCompactionRequested(fateId1).build(USER_COMPACTION_REQUESTED);
      ctmi.mutateTablet(e1).requireAbsentOperation()
          .requireSame(tabletMeta1, USER_COMPACTION_REQUESTED)
          .deleteUserCompactionRequested(fateId2).deleteUserCompactionRequested(fateId4)
          .deleteUserCompactionRequested(fateId5).submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Set.of(fateId2, fateId4, fateId5),
          context.getAmple().readTablet(e1).getUserCompactionsRequested());

      // test require same with a subset
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tabletMeta1 = TabletMetadata.builder(e2).putUserCompactionRequested(fateId2)
          .putUserCompactionRequested(fateId4).build(USER_COMPACTION_REQUESTED);
      ctmi.mutateTablet(e1).requireAbsentOperation()
          .requireSame(tabletMeta1, USER_COMPACTION_REQUESTED)
          .deleteUserCompactionRequested(fateId2).deleteUserCompactionRequested(fateId4)
          .deleteUserCompactionRequested(fateId5).submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Set.of(fateId2, fateId4, fateId5),
          context.getAmple().readTablet(e1).getUserCompactionsRequested());

      // now use the exact set the tablet has
      ctmi = new ConditionalTabletsMutatorImpl(context);
      tabletMeta1 = TabletMetadata.builder(e2).putUserCompactionRequested(fateId2)
          .putUserCompactionRequested(fateId4).putUserCompactionRequested(fateId5)
          .build(USER_COMPACTION_REQUESTED);
      ctmi.mutateTablet(e1).requireAbsentOperation()
          .requireSame(tabletMeta1, USER_COMPACTION_REQUESTED)
          .deleteUserCompactionRequested(fateId2).deleteUserCompactionRequested(fateId4)
          .deleteUserCompactionRequested(fateId5).submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Set.of(), context.getAmple().readTablet(e1).getUserCompactionsRequested());
    }
  }

  @Nested
  public class TestFilter {

    /**
     * @param filters set of filters to apply to the readTablets operation
     * @param expectedTablets set of tablets expected to be returned with the filters applied
     */
    private void testFilterApplied(ServerContext context, Set<TabletMetadataFilter> filters,
        Set<KeyExtent> expectedTablets, String message) {
      // test with just the needed columns fetched and then with all columns fetched. both should
      // yield the same result
      addFiltersFetchAndAssert(context, filters, true, expectedTablets, message);
      addFiltersFetchAndAssert(context, filters, false, expectedTablets, message);
    }

    private void addFiltersFetchAndAssert(ServerContext context, Set<TabletMetadataFilter> filters,
        boolean shouldFetchNeededCols, Set<KeyExtent> expectedTablets, String message) {
      TabletsMetadata.TableRangeOptions options = context.getAmple().readTablets().forTable(tid);
      // add the filter(s) to the operation before building
      for (TabletMetadataFilter filter : filters) {
        options.filter(filter);
        // test fetching just the needed columns
        if (shouldFetchNeededCols) {
          for (TabletMetadata.ColumnType columnType : filter.getColumns()) {
            options.fetch(columnType);
          }
        }
      }
      if (shouldFetchNeededCols) {
        // if some columns were fetched, also need to fetch PREV_ROW in order to use getExtent()
        options.fetch(PREV_ROW);
      }
      try (TabletsMetadata tablets = options.build()) {
        Set<KeyExtent> actual =
            tablets.stream().map(TabletMetadata::getExtent).collect(Collectors.toSet());
        assertEquals(expectedTablets, actual,
            message + (shouldFetchNeededCols
                ? ". Only needed columns were fetched in the readTablets operation."
                : ". All columns were fetched in the readTablets operation."));
      }
    }

    @Test
    public void multipleFilters() {
      ServerContext context = cluster.getServerContext();
      ConditionalTabletsMutatorImpl ctmi;

      // make sure we read all tablets on table initially with no filters
      testFilterApplied(context, Set.of(), Set.of(e1, e2, e3, e4),
          "Initially, all tablets should be present");

      String server = "server1+8555";

      String walFilePath = java.nio.file.Path.of(server, UUID.randomUUID().toString()).toString();
      LogEntry wal = LogEntry.fromPath(walFilePath);

      // add wal compact and flush ID to these tablets
      final Set<KeyExtent> tabletsWithWalCompactFlush = Set.of(e1, e2, e3);
      for (KeyExtent ke : tabletsWithWalCompactFlush) {
        FateInstanceType type = FateInstanceType.fromTableId(ke.tableId());
        FateId fateId = FateId.from(type, UUID.randomUUID());
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(ke).requireAbsentOperation().putCompacted(fateId)
            .putFlushId(TestTabletMetadataFilter.VALID_FLUSH_ID).putWal(wal)
            .submit(tabletMetadata -> false);
        var results = ctmi.process();
        assertEquals(Status.ACCEPTED, results.get(ke).getStatus());
      }
      // check that applying a combination of filters returns only tablets that meet the criteria
      testFilterApplied(context, Set.of(new TestTabletMetadataFilter(), new GcWalsFilter(Set.of())),
          tabletsWithWalCompactFlush, "Combination of filters did not return the expected tablets");

      TServerInstance serverInstance = new TServerInstance(server, 1L);

      // on a subset of the tablets, put a location
      final Set<KeyExtent> tabletsWithLocation = Set.of(e2, e3, e4);
      for (KeyExtent ke : tabletsWithLocation) {
        ctmi = new ConditionalTabletsMutatorImpl(context);
        ctmi.mutateTablet(ke).requireAbsentOperation().requireAbsentLocation()
            .putLocation(Location.current(serverInstance)).submit(tabletMetadata -> false);
        var results = ctmi.process();
        assertEquals(Status.ACCEPTED, results.get(ke).getStatus());
        assertEquals(Location.current(serverInstance),
            context.getAmple().readTablet(ke).getLocation(),
            "Did not see expected location after adding it");
      }

      // test that the new subset is returned with all 3 filters applied
      Set<KeyExtent> expected = Sets.intersection(tabletsWithWalCompactFlush, tabletsWithLocation);
      assertFalse(expected.isEmpty());
      testFilterApplied(context,
          Set.of(new HasCurrentFilter(), new GcWalsFilter(Set.of()),
              new TestTabletMetadataFilter()),
          expected, "Combination of filters did not return the expected tablets");
    }

    @Test
    public void testCompactedAndFlushIdFilter() {
      ServerContext context = cluster.getServerContext();
      ConditionalTabletsMutatorImpl ctmi = new ConditionalTabletsMutatorImpl(context);
      Set<TabletMetadataFilter> filter = Set.of(new TestTabletMetadataFilter());
      FateInstanceType type = FateInstanceType.fromTableId(tid);
      FateId fateId1 = FateId.from(type, UUID.randomUUID());
      FateId fateId2 = FateId.from(type, UUID.randomUUID());

      // make sure we read all tablets on table initially with no filters
      testFilterApplied(context, Set.of(), Set.of(e1, e2, e3, e4),
          "Initially, all tablets should be present");

      // Set compacted on e2 but with no flush ID
      ctmi.mutateTablet(e2).requireAbsentOperation().putCompacted(fateId1)
          .submit(tabletMetadata -> false);
      var results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());
      testFilterApplied(context, filter, Set.of(),
          "Compacted but no flush ID should return no tablets");

      // Set incorrect flush ID on e2
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e2).requireAbsentOperation().putFlushId(45L)
          .submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());
      testFilterApplied(context, filter, Set.of(),
          "Compacted with incorrect flush ID should return no tablets");

      // Set correct flush ID on e2
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e2).requireAbsentOperation()
          .putFlushId(TestTabletMetadataFilter.VALID_FLUSH_ID).submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());
      testFilterApplied(context, filter, Set.of(e2),
          "Compacted with correct flush ID should return e2");

      // Set compacted and correct flush ID on e3
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e3).requireAbsentOperation().putCompacted(fateId2)
          .putFlushId(TestTabletMetadataFilter.VALID_FLUSH_ID).submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e3).getStatus());
      testFilterApplied(context, filter, Set.of(e2, e3),
          "Compacted with correct flush ID should return e2 and e3");
    }

    @Test
    public void walFilter() {
      ServerContext context = cluster.getServerContext();
      ConditionalTabletsMutatorImpl ctmi = new ConditionalTabletsMutatorImpl(context);
      Set<TabletMetadataFilter> filter = Set.of(new GcWalsFilter(Set.of()));

      // make sure we read all tablets on table initially with no filters
      testFilterApplied(context, Set.of(), Set.of(e1, e2, e3, e4),
          "Initially, all tablets should be present");

      // add a wal to e2
      String walFilePath =
          java.nio.file.Path.of("tserver+8080", UUID.randomUUID().toString()).toString();
      LogEntry wal = LogEntry.fromPath(walFilePath);
      ctmi.mutateTablet(e2).requireAbsentOperation().putWal(wal).submit(tabletMetadata -> false);
      var results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());

      // test that the filter works
      testFilterApplied(context, filter, Set.of(e2), "Only tablets with wals should be returned");

      // add wal to tablet e4
      ctmi = new ConditionalTabletsMutatorImpl(context);
      walFilePath = java.nio.file.Path.of("tserver+8080", UUID.randomUUID().toString()).toString();
      wal = LogEntry.fromPath(walFilePath);
      ctmi.mutateTablet(e4).requireAbsentOperation().putWal(wal).submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e4).getStatus());

      // now, when using the wal filter, should see both e2 and e4
      testFilterApplied(context, filter, Set.of(e2, e4),
          "Only tablets with wals should be returned");

      // remove the wal from e4
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e4).requireAbsentOperation().deleteWal(wal).submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e4).getStatus());

      // test that now only the tablet with a wal is returned when using filter()
      testFilterApplied(context, filter, Set.of(e2), "Only tablets with wals should be returned");

      var ts1 = new TServerInstance("localhost:9997", 5000L);
      var ts2 = new TServerInstance("localhost:9997", 6000L);

      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts1)).submit(tabletMetadata -> false);
      ctmi.mutateTablet(e2).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.current(ts1)).submit(tabletMetadata -> false);
      ctmi.mutateTablet(e3).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.future(ts2)).submit(tabletMetadata -> false);
      ctmi.mutateTablet(e4).requireAbsentOperation().requireAbsentLocation()
          .putLocation(Location.current(ts2)).submit(tabletMetadata -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e3).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e4).getStatus());

      testFilterApplied(context, filter, Set.of(e1, e2, e3, e4),
          "All tablets should appear to be assigned to dead tservers and be returned");

      // add ts1 to live tservers set and make ts2 look like a dead tserver
      filter = Set.of(new GcWalsFilter(Set.of(ts1)));
      testFilterApplied(context, filter, Set.of(e2, e3, e4),
          "Tablets assigned to ts2 or with a wal should be returned");

      // add ts2 to live tservers set and make ts1 look like a dead tserver
      filter = Set.of(new GcWalsFilter(Set.of(ts2)));
      testFilterApplied(context, filter, Set.of(e1, e2),
          "Tablets assigned to ts1 or with a wal should be returned");

      // add ts1 and ts2 to live tserver set, so nothing should look dead
      filter = Set.of(new GcWalsFilter(Set.of(ts1, ts2)));
      testFilterApplied(context, filter, Set.of(e2), "Only tablets with a wal should be returned");
    }

    @Test
    public void partialFetch() {
      ServerContext context = cluster.getServerContext();
      TestTabletMetadataFilter filter = new TestTabletMetadataFilter();
      // if we only fetch some columns needed by the filter, we should get an exception
      TabletsMetadata.Options options =
          context.getAmple().readTablets().forTable(tid).fetch(FLUSH_ID).filter(filter);
      var ise = assertThrows(IllegalStateException.class, () -> options.build().close());
      String expectedMsg = String.format("%s needs cols %s however only %s were fetched",
          TestTabletMetadataFilter.class.getSimpleName(), filter.getColumns(), Set.of(FLUSH_ID));
      assertTrue(ise.getMessage().contains(expectedMsg));
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

  @Test
  public void testAsyncMutator() throws Exception {
    var table = getUniqueNames(2)[1];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      // The AsyncConditionalTabletsMutatorImpl processes batches of conditional mutations. Run
      // tests where more than the batch size is processed an ensure this handled correctly.

      TreeSet<Text> splits =
          IntStream.range(1, (int) (AsyncConditionalTabletsMutatorImpl.BATCH_SIZE * 2.5))
              .mapToObj(i -> new Text(String.format("%06d", i)))
              .collect(Collectors.toCollection(TreeSet::new));

      assertTrue(splits.size() > AsyncConditionalTabletsMutatorImpl.BATCH_SIZE);

      c.tableOperations().create(table, new NewTableConfiguration().withSplits(splits));
      var tableId = TableId.of(c.tableOperations().tableIdMap().get(table));

      var ample = cluster.getServerContext().getAmple();

      AtomicLong accepted = new AtomicLong(0);
      AtomicLong total = new AtomicLong(0);
      Consumer<Ample.ConditionalResult> resultsConsumer = result -> {
        if (result.getStatus() == Status.ACCEPTED) {
          accepted.incrementAndGet();
        }
        total.incrementAndGet();
      };

      // run a test where a subset of tablets are modified, all modifications should be accepted
      FateInstanceType type = FateInstanceType.fromTableId(tableId);
      var opid1 =
          TabletOperationId.from(TabletOperationType.MERGING, FateId.from(type, UUID.randomUUID()));

      int expected = 0;
      try (var tablets = ample.readTablets().forTable(tableId).fetch(OPID, PREV_ROW).build();
          var mutator = ample.conditionallyMutateTablets(resultsConsumer)) {
        for (var tablet : tablets) {
          if (tablet.getEndRow() != null
              && Integer.parseInt(tablet.getEndRow().toString()) % 2 == 0) {
            mutator.mutateTablet(tablet.getExtent()).requireAbsentOperation().putOperation(opid1)
                .submit(tm -> opid1.equals(tm.getOperationId()));
            expected++;
          }
        }
      }

      assertTrue(expected > 0);
      assertEquals(expected, accepted.get());
      assertEquals(total.get(), accepted.get());

      // run test where some will be accepted and some will be rejected and ensure the counts come
      // out as expected.
      var opid2 =
          TabletOperationId.from(TabletOperationType.MERGING, FateId.from(type, UUID.randomUUID()));

      accepted.set(0);
      total.set(0);

      try (var tablets = ample.readTablets().forTable(tableId).fetch(OPID, PREV_ROW).build();
          var mutator = ample.conditionallyMutateTablets(resultsConsumer)) {
        for (var tablet : tablets) {
          mutator.mutateTablet(tablet.getExtent()).requireAbsentOperation().putOperation(opid2)
              .submit(tm -> opid2.equals(tm.getOperationId()));
        }
      }

      var numTablets = splits.size() + 1;
      assertEquals(numTablets - expected, accepted.get());
      assertEquals(numTablets, total.get());
    }
  }

  @Test
  public void testSuspendMarker() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      final String SUSPEND_RG = "SUSPEND";

      MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
      cluster.getConfig().getClusterServerConfiguration().addTabletServerResourceGroup(SUSPEND_RG,
          1);
      cluster.getClusterControl().start(ServerType.TABLET_SERVER);
      List<Process> processes = cluster.getClusterControl().getTabletServers(SUSPEND_RG);
      assertNotNull(processes);
      assertEquals(1, processes.size());

      String tableName = getUniqueNames(2)[1];
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.withInitialTabletAvailability(TabletAvailability.HOSTED);
      ntc.setProperties(Map.of(Property.TABLE_SUSPEND_DURATION.getKey(), "30s",
          TableLoadBalancer.TABLE_ASSIGNMENT_GROUP_PROPERTY, SUSPEND_RG));
      c.tableOperations().create(tableName, ntc);

      c.instanceOperations().waitForBalance();

      TableId suspendTableTid = TableId.of(c.tableOperations().tableIdMap().get(tableName));

      TabletMetadata originalTM = null;
      try (TabletsMetadata tms = TabletsMetadata.builder(c).forTable(suspendTableTid).build()) {
        assertEquals(1, Iterables.size(tms));
        originalTM = tms.iterator().next();
        assertNull(originalTM.getSuspend());
      }

      cluster.getClusterControl().stopTabletServerGroup(SUSPEND_RG);

      Wait.waitFor(() -> getSuspendedColumn(c, suspendTableTid) != null, 60_000);

      try (var tabletsMutator = getServerContext().getAmple().conditionallyMutateTablets()) {
        tabletsMutator.mutateTablet(originalTM.getExtent()).requireAbsentOperation()
            .requireSame(originalTM, SUSPEND).putTabletAvailability(TabletAvailability.ONDEMAND)
            .submit(tabletMetadata -> false);

        // This should fail because the original tablet metadata does not have a suspend column
        // and the current tablet metadata does.
        assertTrue(tabletsMutator.process().get(originalTM.getExtent()).getStatus()
            .equals(Status.REJECTED));
      }

      // test require same when the tablet metadata passed in has a suspend column set
      var suspendedTM = getServerContext().getAmple().readTablet(originalTM.getExtent());
      assertNotNull(suspendedTM.getSuspend());
      try (var tabletsMutator = getServerContext().getAmple().conditionallyMutateTablets()) {
        tabletsMutator.mutateTablet(originalTM.getExtent()).requireAbsentOperation()
            .requireSame(suspendedTM, SUSPEND).putFlushNonce(6789).submit(tabletMetadata -> false);

        // This should succeed because the tablet metadata does have a suspend column and the
        // current tablet metadata does.
        assertTrue(tabletsMutator.process().get(originalTM.getExtent()).getStatus()
            .equals(Status.ACCEPTED));
      }

      cluster.getClusterControl().start(ServerType.TABLET_SERVER);

      Wait.waitFor(() -> getSuspendedColumn(c, suspendTableTid) == null, 60_000);

      try (var tabletsMutator = getServerContext().getAmple().conditionallyMutateTablets()) {
        tabletsMutator.mutateTablet(originalTM.getExtent()).requireAbsentOperation()
            .requireSame(originalTM, SUSPEND).putTabletAvailability(TabletAvailability.ONDEMAND)
            .submit(tabletMetadata -> false);

        // This should succeed because the original tablet metadata does not have a suspend column
        // and the current tablet metadata does not also because the tablet server for the SUSPEND
        // resource group was restarted.
        assertTrue(tabletsMutator.process().get(originalTM.getExtent()).getStatus()
            .equals(Status.ACCEPTED));

      }
    }
  }

  private static SuspendingTServer getSuspendedColumn(AccumuloClient c, TableId tid) {
    try (TabletsMetadata tms = TabletsMetadata.builder(c).forTable(tid).build()) {
      assertEquals(1, Iterables.size(tms));
      TabletMetadata tm = tms.iterator().next();
      return tm.getSuspend();
    }
  }

  @Test
  public void testTabletAvailability() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      assertEquals(TabletAvailability.ONDEMAND,
          context.getAmple().readTablet(e1).getTabletAvailability());
      assertEquals(TabletAvailability.ONDEMAND,
          context.getAmple().readTablet(e2).getTabletAvailability());

      var ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation()
          .requireTabletAvailability(TabletAvailability.HOSTED)
          .putTabletAvailability(TabletAvailability.UNHOSTED).submit(tm -> false);
      ctmi.mutateTablet(e2).requireAbsentOperation()
          .requireTabletAvailability(TabletAvailability.ONDEMAND)
          .putTabletAvailability(TabletAvailability.UNHOSTED).submit(tm -> false);
      var results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(Status.ACCEPTED, results.get(e2).getStatus());

      assertEquals(TabletAvailability.ONDEMAND,
          context.getAmple().readTablet(e1).getTabletAvailability());
      assertEquals(TabletAvailability.UNHOSTED,
          context.getAmple().readTablet(e2).getTabletAvailability());
    }
  }

  @Test
  public void testUnsplittable() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      var ctmi = new ConditionalTabletsMutatorImpl(context);

      var tabletMeta1 = TabletMetadata.builder(e1).build(UNSPLITTABLE);

      // require the UNSPLITTABLE column to be absent when it is absent
      UnSplittableMetadata usm1 =
          UnSplittableMetadata.toUnSplittable(e1, 1000, 100000, 32, Set.of());
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, UNSPLITTABLE)
          .setUnSplittable(usm1).submit(tm -> false);
      var results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      var tabletMeta2 = context.getAmple().readTablet(e1);
      assertEquals(usm1.toBase64(), tabletMeta2.getUnSplittable().toBase64());

      // require the UNSPLITTABLE column to be absent when it is not absent
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta1, UNSPLITTABLE)
          .deleteUnSplittable().submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(usm1.toBase64(), context.getAmple().readTablet(e1).getUnSplittable().toBase64());

      UnSplittableMetadata usm2 =
          UnSplittableMetadata.toUnSplittable(e1, 1001, 100001, 33, Set.of());
      var tabletMeta3 = TabletMetadata.builder(e1).setUnSplittable(usm2).build(UNSPLITTABLE);
      // require the UNSPLITTABLE column to be usm2 when it is actually usm1
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta3, UNSPLITTABLE)
          .deleteUnSplittable().submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertEquals(usm1.toBase64(), context.getAmple().readTablet(e1).getUnSplittable().toBase64());

      // require the UNSPLITTABLE column to be usm1 when it is actually usm1
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta2, UNSPLITTABLE)
          .deleteUnSplittable().submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.ACCEPTED, results.get(e1).getStatus());
      assertNull(context.getAmple().readTablet(e1).getUnSplittable());

      // require the UNSPLITTABLE column to be usm1 when it is actually absent
      ctmi = new ConditionalTabletsMutatorImpl(context);
      ctmi.mutateTablet(e1).requireAbsentOperation().requireSame(tabletMeta2, UNSPLITTABLE)
          .setUnSplittable(usm2).submit(tm -> false);
      results = ctmi.process();
      assertEquals(Status.REJECTED, results.get(e1).getStatus());
      assertNull(context.getAmple().readTablet(e1).getUnSplittable());
    }
  }

  @Test
  public void testErrors() {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      var context = cluster.getServerContext();

      var ctmi1 = new ConditionalTabletsMutatorImpl(context);
      ctmi1.mutateTablet(e1).requireAbsentTablet().putDirName("d1").submit(tm -> false);
      // making multiple updates for the same tablet is not supported
      assertThrows(IllegalStateException.class,
          () -> ctmi1.mutateTablet(e1).requireAbsentTablet().putDirName("d2").submit(tm -> false));
      // attempt to use a column that requireSame does not support
      TabletMetadata tabletMetadata = TabletMetadata.builder(e2).build();
      assertThrows(UnsupportedOperationException.class,
          () -> ctmi1.mutateTablet(e2).requireAbsentOperation()
              .requireSame(tabletMetadata, HOSTING_REQUESTED).putDirName("d2").submit(tm -> false));

      var ctmi2 = new ConditionalTabletsMutatorImpl(context);
      // the following end prev end row update should cause a constraint violation because a tablets
      // prev end row must be less than its end row
      ctmi2.mutateTablet(e1).requireAbsentOperation().putPrevEndRow(e1.endRow())
          .submit(tm -> false);
      var results = ctmi2.process();
      // getting status when a constraint violation happened should throw an exception
      assertThrows(IllegalStateException.class, () -> results.get(e1).getStatus());

      var ctmi3 = new ConditionalTabletsMutatorImpl(context);
      ctmi3.mutateTablet(e1).requireAbsentOperation().putFlushNonce(1234).submit(tm -> false);
      var results2 = ctmi3.process();
      assertEquals(Status.ACCEPTED, results2.get(e1).getStatus());

      // attempting to use after calling proccess() should throw an exception
      assertThrows(IllegalStateException.class, () -> ctmi3.mutateTablet(e1)
          .requireAbsentOperation().putFlushNonce(1234).submit(tm -> false));
    }
  }
}

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
package org.apache.accumulo.core.metadata.schema;

import static java.util.stream.Collectors.toSet;
import static org.apache.accumulo.core.metadata.StoredTabletFile.serialize;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.MergedColumnFamily.MERGED_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.MergedColumnFamily.MERGED_VALUE;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.COMPACT_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn.SUSPEND_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Builder;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class TabletMetadataTest {

  @Test
  public void testAllColumns() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);

    COMPACT_COLUMN.put(mutation, new Value("5"));
    DIRECTORY_COLUMN.put(mutation, new Value("t-0001757"));
    FLUSH_COLUMN.put(mutation, new Value("6"));
    TIME_COLUMN.put(mutation, new Value("M123456789"));

    String bf1 = serialize("hdfs://nn1/acc/tables/1/t-0001/bf1");
    String bf2 = serialize("hdfs://nn1/acc/tables/1/t-0001/bf2");
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf1).put(FateTxId.formatTid(56));
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf2).put(FateTxId.formatTid(59));

    mutation.at().family(ClonedColumnFamily.NAME).qualifier("").put("OK");

    DataFileValue dfv1 = new DataFileValue(555, 23);
    StoredTabletFile tf1 = StoredTabletFile.of(new Path("hdfs://nn1/acc/tables/1/t-0001/df1.rf"));
    StoredTabletFile tf2 = StoredTabletFile.of(new Path("hdfs://nn1/acc/tables/1/t-0001/df2.rf"));
    mutation.at().family(DataFileColumnFamily.NAME).qualifier(tf1.getMetadata()).put(dfv1.encode());
    DataFileValue dfv2 = new DataFileValue(234, 13);
    mutation.at().family(DataFileColumnFamily.NAME).qualifier(tf2.getMetadata()).put(dfv2.encode());

    mutation.at().family(CurrentLocationColumnFamily.NAME).qualifier("s001").put("server1:8555");

    mutation.at().family(LastLocationColumnFamily.NAME).qualifier("s000").put("server2:8555");

    LogEntry le1 = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());
    le1.addToMutation(mutation);
    LogEntry le2 = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());
    le2.addToMutation(mutation);

    StoredTabletFile sf1 = StoredTabletFile.of(new Path("hdfs://nn1/acc/tables/1/t-0001/sf1.rf"));
    StoredTabletFile sf2 = StoredTabletFile.of(new Path("hdfs://nn1/acc/tables/1/t-0001/sf2.rf"));
    mutation.at().family(ScanFileColumnFamily.NAME).qualifier(sf1.getMetadata()).put("");
    mutation.at().family(ScanFileColumnFamily.NAME).qualifier(sf2.getMetadata()).put("");

    MERGED_COLUMN.put(mutation, new Value());

    OLD_PREV_ROW_COLUMN.put(mutation, TabletColumnFamily.encodePrevEndRow(new Text("oldPrev")));
    long suspensionTime = System.currentTimeMillis();
    TServerInstance ser1 = new TServerInstance(HostAndPort.fromParts("server1", 8555), "s001");
    Value suspend = SuspendingTServer.toValue(ser1, suspensionTime);
    SUSPEND_COLUMN.put(mutation, suspend);
    double splitRatio = .3;
    SPLIT_RATIO_COLUMN.put(mutation, new Value(Double.toString(splitRatio)));

    ExternalCompactionId ecid = ExternalCompactionId.generate(UUID.randomUUID());
    ReferencedTabletFile tmpFile =
        ReferencedTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/c1.rf"));
    CompactionExecutorId ceid = CompactionExecutorIdImpl.externalId("G1");
    Set<StoredTabletFile> jobFiles =
        Set.of(StoredTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/b2.rf")));
    ExternalCompactionMetadata ecMeta = new ExternalCompactionMetadata(jobFiles, jobFiles, tmpFile,
        "localhost:4444", CompactionKind.SYSTEM, (short) 2, ceid, false, false, 44L);
    mutation.put(ExternalCompactionColumnFamily.STR_NAME, ecid.canonical(), ecMeta.toJson());

    SortedMap<Key,Value> rowMap = toRowMap(mutation);

    TabletMetadata tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(),
        EnumSet.allOf(ColumnType.class), true);

    assertEquals("OK", tm.getCloned());
    assertEquals(5L, tm.getCompactId().getAsLong());
    assertEquals("t-0001757", tm.getDirName());
    assertEquals(extent.endRow(), tm.getEndRow());
    assertEquals(extent, tm.getExtent());
    assertEquals(Set.of(tf1, tf2), Set.copyOf(tm.getFiles()));
    assertEquals(Map.of(tf1, dfv1, tf2, dfv2), tm.getFilesMap());
    assertEquals(6L, tm.getFlushId().getAsLong());
    assertEquals(rowMap, tm.getKeyValues());
    assertEquals(Map.of(new StoredTabletFile(bf1), 56L, new StoredTabletFile(bf2), 59L),
        tm.getLoaded());
    assertEquals(HostAndPort.fromParts("server1", 8555), tm.getLocation().getHostAndPort());
    assertEquals("s001", tm.getLocation().getSession());
    assertEquals(LocationType.CURRENT, tm.getLocation().getType());
    assertTrue(tm.hasCurrent());
    assertEquals(HostAndPort.fromParts("server2", 8555), tm.getLast().getHostAndPort());
    assertEquals("s000", tm.getLast().getSession());
    assertEquals(LocationType.LAST, tm.getLast().getType());
    assertEquals(Set.of(le1, le2), tm.getLogs().stream().collect(toSet()));
    assertEquals(extent.prevEndRow(), tm.getPrevEndRow());
    assertEquals(extent.tableId(), tm.getTableId());
    assertTrue(tm.sawPrevEndRow());
    assertEquals("M123456789", tm.getTime().encode());
    assertEquals(Set.of(sf1, sf2), Set.copyOf(tm.getScans()));
    assertTrue(tm.hasMerged());
    assertEquals(new Text("oldPrev"), tm.getOldPrevEndRow());
    assertTrue(tm.sawOldPrevEndRow());
    assertEquals(SuspendingTServer.fromValue(suspend), tm.getSuspend());
    assertEquals(splitRatio, tm.getSplitRatio());
    assertEquals(ecMeta.toJson(), tm.getExternalCompactions().get(ecid).toJson());
  }

  @Test
  public void testFuture() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(FutureLocationColumnFamily.NAME).qualifier("s001").put("server1:8555");

    SortedMap<Key,Value> rowMap = toRowMap(mutation);

    TabletMetadata tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(),
        EnumSet.allOf(ColumnType.class), false);

    assertEquals(extent, tm.getExtent());
    assertEquals(HostAndPort.fromParts("server1", 8555), tm.getLocation().getHostAndPort());
    assertEquals("s001", tm.getLocation().getSession());
    assertEquals(LocationType.FUTURE, tm.getLocation().getType());
    assertFalse(tm.hasCurrent());
  }

  @Test
  public void testFutureAndCurrent() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(CurrentLocationColumnFamily.NAME).qualifier("s001").put("server1:8555");
    mutation.at().family(FutureLocationColumnFamily.NAME).qualifier("s001").put("server1:8555");

    SortedMap<Key,Value> rowMap = toRowMap(mutation);

    assertThrows(IllegalStateException.class, () -> TabletMetadata
        .convertRow(rowMap.entrySet().iterator(), EnumSet.allOf(ColumnType.class), false));
  }

  @Test
  public void testLocationStates() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));
    TServerInstance ser1 = new TServerInstance(HostAndPort.fromParts("server1", 8555), "s001");
    TServerInstance ser2 = new TServerInstance(HostAndPort.fromParts("server2", 8111), "s002");
    TServerInstance deadSer = new TServerInstance(HostAndPort.fromParts("server3", 8000), "s003");
    Set<TServerInstance> tservers = new LinkedHashSet<>();
    tservers.add(ser1);
    tservers.add(ser2);
    EnumSet<ColumnType> colsToFetch = EnumSet.of(LOCATION, LAST, SUSPEND);

    // test assigned
    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(FutureLocationColumnFamily.NAME).qualifier(ser1.getSession())
        .put(ser1.getHostPort());
    SortedMap<Key,Value> rowMap = toRowMap(mutation);

    // PREV_ROW was not fetched
    final var missingPrevRow =
        TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false);
    assertThrows(IllegalStateException.class, () -> missingPrevRow.getTabletState(tservers));

    // This should now work as PREV_ROW has been included
    colsToFetch = EnumSet.of(LOCATION, LAST, SUSPEND, PREV_ROW);
    TabletMetadata tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false);
    TabletState state = tm.getTabletState(tservers);

    assertEquals(TabletState.ASSIGNED, state);
    assertEquals(ser1, tm.getLocation().getServerInstance());
    assertEquals(ser1.getSession(), tm.getLocation().getSession());
    assertEquals(LocationType.FUTURE, tm.getLocation().getType());
    assertFalse(tm.hasCurrent());

    // test hosted
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(CurrentLocationColumnFamily.NAME).qualifier(ser2.getSession())
        .put(ser2.getHostPort());
    rowMap = toRowMap(mutation);

    tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false);

    assertEquals(TabletState.HOSTED, tm.getTabletState(tservers));
    assertEquals(ser2, tm.getLocation().getServerInstance());
    assertEquals(ser2.getSession(), tm.getLocation().getSession());
    assertEquals(LocationType.CURRENT, tm.getLocation().getType());
    assertTrue(tm.hasCurrent());

    // test ASSIGNED_TO_DEAD_SERVER
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(CurrentLocationColumnFamily.NAME).qualifier(deadSer.getSession())
        .put(deadSer.getHostPort());
    rowMap = toRowMap(mutation);

    tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false);

    assertEquals(TabletState.ASSIGNED_TO_DEAD_SERVER, tm.getTabletState(tservers));
    assertEquals(deadSer, tm.getLocation().getServerInstance());
    assertEquals(deadSer.getSession(), tm.getLocation().getSession());
    assertEquals(LocationType.CURRENT, tm.getLocation().getType());
    assertTrue(tm.hasCurrent());

    // test UNASSIGNED
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    rowMap = toRowMap(mutation);

    tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false);

    assertEquals(TabletState.UNASSIGNED, tm.getTabletState(tservers));
    assertNull(tm.getLocation());
    assertFalse(tm.hasCurrent());

    // test SUSPENDED
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(SUSPEND_COLUMN.getColumnFamily())
        .qualifier(SUSPEND_COLUMN.getColumnQualifier()).put(SuspendingTServer.toValue(ser2, 1000L));
    rowMap = toRowMap(mutation);

    tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false);

    assertEquals(TabletState.SUSPENDED, tm.getTabletState(tservers));
    assertEquals(1000L, tm.getSuspend().suspensionTime);
    assertEquals(ser2.getHostAndPort(), tm.getSuspend().server);
    assertNull(tm.getLocation());
    assertFalse(tm.hasCurrent());
  }

  @Test
  public void testMergedColumn() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    // Test merged column set
    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);
    MERGED_COLUMN.put(mutation, MERGED_VALUE);
    TabletMetadata tm = TabletMetadata.convertRow(toRowMap(mutation).entrySet().iterator(),
        EnumSet.of(ColumnType.MERGED), true);
    assertTrue(tm.hasMerged());

    // Column not set
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    tm = TabletMetadata.convertRow(toRowMap(mutation).entrySet().iterator(),
        EnumSet.of(ColumnType.MERGED), true);
    assertFalse(tm.hasMerged());

    // MERGED Column not fetched
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    tm = TabletMetadata.convertRow(toRowMap(mutation).entrySet().iterator(),
        EnumSet.of(ColumnType.PREV_ROW), true);
    assertThrows(IllegalStateException.class, tm::hasMerged);
  }

  @Test
  public void testTabletsMetadataAutoClose() throws Exception {
    AtomicBoolean closeCalled = new AtomicBoolean();
    AutoCloseable autoCloseable = () -> closeCalled.set(true);
    Constructor<TabletsMetadata> tmConstructor =
        TabletsMetadata.class.getDeclaredConstructor(AutoCloseable.class, Iterable.class);
    tmConstructor.setAccessible(true);

    try (TabletsMetadata ignored = tmConstructor.newInstance(autoCloseable, List.of())) {
      // test autoCloseable used directly on TabletsMetadata
    }
    assertTrue(closeCalled.get());

    closeCalled.set(false);
    try (Stream<TabletMetadata> ignored =
        tmConstructor.newInstance(autoCloseable, List.of()).stream()) {
      // test stream delegates to close on TabletsMetadata
    }
    assertTrue(closeCalled.get());
  }

  @Test
  public void testTmBuilderImmutable() {
    TabletMetadata.Builder b = new Builder();
    var tm = b.build(EnumSet.allOf(ColumnType.class));

    ExternalCompactionId ecid = ExternalCompactionId.generate(UUID.randomUUID());
    ReferencedTabletFile tmpFile =
        ReferencedTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/c1.rf"));
    CompactionExecutorId ceid = CompactionExecutorIdImpl.externalId("G1");
    StoredTabletFile stf = StoredTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/b2.rf"));
    Set<StoredTabletFile> jobFiles = Set.of(stf);
    ExternalCompactionMetadata ecMeta = new ExternalCompactionMetadata(jobFiles, jobFiles, tmpFile,
        "localhost:4444", CompactionKind.SYSTEM, (short) 2, ceid, false, false, 44L);

    // Verify the various collections are immutable and non-null (except for getKeyValues) if
    // nothing set on the builder
    assertTrue(tm.getExternalCompactions().isEmpty());
    assertThrows(UnsupportedOperationException.class,
        () -> tm.getExternalCompactions().put(ecid, ecMeta));
    assertTrue(tm.getFiles().isEmpty());
    assertTrue(tm.getFilesMap().isEmpty());
    assertThrows(UnsupportedOperationException.class, () -> tm.getFiles().add(stf));
    assertThrows(UnsupportedOperationException.class,
        () -> tm.getFilesMap().put(stf, new DataFileValue(0, 0, 0)));
    assertTrue(tm.getLogs().isEmpty());
    assertThrows(UnsupportedOperationException.class,
        () -> tm.getLogs().add(LogEntry.fromPath("localhost+8020/" + UUID.randomUUID())));
    assertTrue(tm.getScans().isEmpty());
    assertThrows(UnsupportedOperationException.class, () -> tm.getScans().add(stf));
    assertTrue(tm.getLoaded().isEmpty());
    assertThrows(UnsupportedOperationException.class, () -> tm.getLoaded().put(stf, 0L));
    assertThrows(IllegalStateException.class, tm::getKeyValues);

    // Set some data in the collections and very they are not empty but still immutable
    b.extCompaction(ecid, ecMeta);
    b.file(stf, new DataFileValue(0, 0, 0));
    b.log(LogEntry.fromPath("localhost+8020/" + UUID.randomUUID()));
    b.scan(stf);
    b.loadedFile(stf, 0L);
    b.keyValue(new Key(), new Value());
    var tm2 = b.build(EnumSet.allOf(ColumnType.class));

    assertEquals(1, tm2.getExternalCompactions().size());
    assertThrows(UnsupportedOperationException.class,
        () -> tm2.getExternalCompactions().put(ecid, ecMeta));
    assertEquals(1, tm2.getFiles().size());
    assertEquals(1, tm2.getFilesMap().size());
    assertThrows(UnsupportedOperationException.class, () -> tm2.getFiles().add(stf));
    assertThrows(UnsupportedOperationException.class,
        () -> tm2.getFilesMap().put(stf, new DataFileValue(0, 0, 0)));
    assertEquals(1, tm2.getLogs().size());
    assertThrows(UnsupportedOperationException.class,
        () -> tm2.getLogs().add(LogEntry.fromPath("localhost+8020/" + UUID.randomUUID())));
    assertEquals(1, tm2.getScans().size());
    assertThrows(UnsupportedOperationException.class, () -> tm2.getScans().add(stf));
    assertEquals(1, tm2.getLoaded().size());
    assertThrows(UnsupportedOperationException.class, () -> tm2.getLoaded().put(stf, 0L));
    assertEquals(1, tm2.getKeyValues().size());
    assertThrows(UnsupportedOperationException.class,
        () -> tm2.getKeyValues().put(new Key(), new Value()));

  }

  private SortedMap<Key,Value> toRowMap(Mutation mutation) {
    SortedMap<Key,Value> rowMap = new TreeMap<>();
    mutation.getUpdates().forEach(cu -> {
      Key k = new Key(mutation.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
          cu.getTimestamp());
      Value v = new Value(cu.getValue());
      rowMap.put(k, v);
    });
    return rowMap;
  }
}

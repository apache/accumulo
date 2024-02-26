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
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.AVAILABILITY;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.HOSTING_REQUESTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.MERGED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.USER_COMPACTION_REQUESTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SuspendingTServer;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.FutureLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.UserCompactionRequestedColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.compaction.CompactorGroupIdImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class TabletMetadataTest {

  @Test
  public void testAllColumns() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);

    FateInstanceType type = FateInstanceType.fromTableId(extent.tableId());
    FateId fateId56L = FateId.from(type, 56L);
    FateId fateId59L = FateId.from(type, 59L);

    DIRECTORY_COLUMN.put(mutation, new Value("t-0001757"));
    FLUSH_COLUMN.put(mutation, new Value("6"));
    TIME_COLUMN.put(mutation, new Value("M123456789"));

    String bf1 = serialize("hdfs://nn1/acc/tables/1/t-0001/bf1");
    String bf2 = serialize("hdfs://nn1/acc/tables/1/t-0001/bf2");
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf1).put(fateId56L.canonical());
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf2).put(fateId59L.canonical());

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
    mutation.put(UserCompactionRequestedColumnFamily.STR_NAME, FateId.from(type, 17).canonical(),
        "");

    SortedMap<Key,Value> rowMap = toRowMap(mutation);

    TabletMetadata tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(),
        EnumSet.allOf(ColumnType.class), true, false);

    assertEquals("OK", tm.getCloned());
    assertEquals("t-0001757", tm.getDirName());
    assertEquals(extent.endRow(), tm.getEndRow());
    assertEquals(extent, tm.getExtent());
    assertEquals(Set.of(tf1, tf2), Set.copyOf(tm.getFiles()));
    assertEquals(Map.of(tf1, dfv1, tf2, dfv2), tm.getFilesMap());
    assertEquals(6L, tm.getFlushId().getAsLong());
    assertEquals(rowMap, tm.getKeyValues());
    assertEquals(Map.of(new StoredTabletFile(bf1), fateId56L, new StoredTabletFile(bf2), fateId59L),
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
    assertTrue(tm.getUserCompactionsRequested().contains(FateId.from(type, 17)));
  }

  @Test
  public void testFuture() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(FutureLocationColumnFamily.NAME).qualifier("s001").put("server1:8555");

    SortedMap<Key,Value> rowMap = toRowMap(mutation);

    TabletMetadata tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(),
        EnumSet.allOf(ColumnType.class), false, false);

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
        .convertRow(rowMap.entrySet().iterator(), EnumSet.allOf(ColumnType.class), false, false));

    TabletMetadata tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(),
        EnumSet.allOf(ColumnType.class), false, true);
    assertTrue(tm.isFutureAndCurrentLocationSet());
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
    EnumSet<ColumnType> colsToFetch =
        EnumSet.of(LOCATION, LAST, SUSPEND, AVAILABILITY, HOSTING_REQUESTED);

    // test assigned
    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(FutureLocationColumnFamily.NAME).qualifier(ser1.getSession())
        .put(ser1.getHostPort());
    SortedMap<Key,Value> rowMap = toRowMap(mutation);

    TabletMetadata tm =
        TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false, false);
    TabletState state = TabletState.compute(tm, tservers);

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

    tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false, false);

    assertEquals(TabletState.HOSTED, TabletState.compute(tm, tservers));
    assertEquals(ser2, tm.getLocation().getServerInstance());
    assertEquals(ser2.getSession(), tm.getLocation().getSession());
    assertEquals(LocationType.CURRENT, tm.getLocation().getType());
    assertTrue(tm.hasCurrent());

    // test ASSIGNED_TO_DEAD_SERVER
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(CurrentLocationColumnFamily.NAME).qualifier(deadSer.getSession())
        .put(deadSer.getHostPort());
    rowMap = toRowMap(mutation);

    tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false, false);

    assertEquals(TabletState.ASSIGNED_TO_DEAD_SERVER, TabletState.compute(tm, tservers));
    assertEquals(deadSer, tm.getLocation().getServerInstance());
    assertEquals(deadSer.getSession(), tm.getLocation().getSession());
    assertEquals(LocationType.CURRENT, tm.getLocation().getType());
    assertTrue(tm.hasCurrent());

    // test UNASSIGNED
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    rowMap = toRowMap(mutation);

    tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false, false);

    assertEquals(TabletState.UNASSIGNED, TabletState.compute(tm, tservers));
    assertNull(tm.getLocation());
    assertFalse(tm.hasCurrent());

    // test SUSPENDED
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.at().family(SuspendLocationColumn.SUSPEND_COLUMN.getColumnFamily())
        .qualifier(SuspendLocationColumn.SUSPEND_COLUMN.getColumnQualifier())
        .put(SuspendingTServer.toValue(ser2, 1000L));
    rowMap = toRowMap(mutation);

    tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false, false);

    assertEquals(TabletState.SUSPENDED, TabletState.compute(tm, tservers));
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
        EnumSet.of(MERGED), true, false);
    assertTrue(tm.hasMerged());

    // Column not set
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    tm = TabletMetadata.convertRow(toRowMap(mutation).entrySet().iterator(), EnumSet.of(MERGED),
        true, false);
    assertFalse(tm.hasMerged());

    // MERGED Column not fetched
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    tm = TabletMetadata.convertRow(toRowMap(mutation).entrySet().iterator(),
        EnumSet.of(ColumnType.PREV_ROW), true, false);
    assertThrows(IllegalStateException.class, tm::hasMerged);
  }

  @Test
  public void testCompactionRequestedColumn() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));
    FateInstanceType type = FateInstanceType.fromTableId(extent.tableId());

    // Test column set
    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);
    mutation.put(UserCompactionRequestedColumnFamily.STR_NAME, FateId.from(type, 17).canonical(),
        "");
    mutation.put(UserCompactionRequestedColumnFamily.STR_NAME, FateId.from(type, 18).canonical(),
        "");

    TabletMetadata tm = TabletMetadata.convertRow(toRowMap(mutation).entrySet().iterator(),
        EnumSet.of(USER_COMPACTION_REQUESTED), true, false);
    assertEquals(2, tm.getUserCompactionsRequested().size());
    assertTrue(tm.getUserCompactionsRequested().contains(FateId.from(type, 17)));
    assertTrue(tm.getUserCompactionsRequested().contains(FateId.from(type, 18)));

    // Column not set
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    tm = TabletMetadata.convertRow(toRowMap(mutation).entrySet().iterator(),
        EnumSet.of(USER_COMPACTION_REQUESTED), true, false);
    assertTrue(tm.getUserCompactionsRequested().isEmpty());

    // Column not fetched
    mutation = TabletColumnFamily.createPrevRowMutation(extent);
    tm = TabletMetadata.convertRow(toRowMap(mutation).entrySet().iterator(),
        EnumSet.of(ColumnType.PREV_ROW), true, false);
    assertThrows(IllegalStateException.class, tm::getUserCompactionsRequested);
  }

  @Test
  public void testUnkownColFamily() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));
    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);

    mutation.put("1234567890abcdefg", "xyz", "v1");
    assertThrows(IllegalStateException.class, () -> TabletMetadata
        .convertRow(toRowMap(mutation).entrySet().iterator(), EnumSet.of(MERGED), true, false));
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

  @Test
  public void testBuilder() {
    TServerInstance ser1 = new TServerInstance(HostAndPort.fromParts("server1", 8555), "s001");

    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    FateInstanceType type = FateInstanceType.fromTableId(extent.tableId());

    StoredTabletFile sf1 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/sf1.rf")).insert();
    DataFileValue dfv1 = new DataFileValue(89, 67);

    StoredTabletFile sf2 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/sf2.rf")).insert();
    DataFileValue dfv2 = new DataFileValue(890, 670);

    ReferencedTabletFile rf1 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/imp1.rf"));
    ReferencedTabletFile rf2 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/imp2.rf"));

    StoredTabletFile sf3 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/sf3.rf")).insert();
    StoredTabletFile sf4 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/sf4.rf")).insert();

    TabletMetadata tm = TabletMetadata.builder(extent)
        .putTabletAvailability(TabletAvailability.UNHOSTED).putLocation(Location.future(ser1))
        .putFile(sf1, dfv1).putFile(sf2, dfv2).putBulkFile(rf1, FateId.from(type, 25))
        .putBulkFile(rf2, FateId.from(type, 35)).putFlushId(27).putDirName("dir1").putScan(sf3)
        .putScan(sf4).putCompacted(FateId.from(type, 17)).putCompacted(FateId.from(type, 23))
        .build(ECOMP, HOSTING_REQUESTED, MERGED, USER_COMPACTION_REQUESTED);

    assertEquals(extent, tm.getExtent());
    assertEquals(TabletAvailability.UNHOSTED, tm.getTabletAvailability());
    assertEquals(Location.future(ser1), tm.getLocation());
    assertEquals(27L, tm.getFlushId().orElse(-1));
    assertEquals(Map.of(sf1, dfv1, sf2, dfv2), tm.getFilesMap());
    assertEquals(Map.of(rf1.insert(), FateId.from(type, 25L), rf2.insert(), FateId.from(type, 35L)),
        tm.getLoaded());
    assertEquals("dir1", tm.getDirName());
    assertEquals(Set.of(sf3, sf4), Set.copyOf(tm.getScans()));
    assertEquals(Set.of(), tm.getExternalCompactions().keySet());
    assertEquals(Set.of(FateId.from(type, 17L), FateId.from(type, 23L)), tm.getCompacted());
    assertFalse(tm.getHostingRequested());
    assertTrue(tm.getUserCompactionsRequested().isEmpty());
    assertFalse(tm.hasMerged());
    assertThrows(IllegalStateException.class, tm::getOperationId);
    assertThrows(IllegalStateException.class, tm::getSuspend);
    assertThrows(IllegalStateException.class, tm::getTime);

    TabletOperationId opid1 =
        TabletOperationId.from(TabletOperationType.SPLITTING, FateId.from(type, 55));
    TabletMetadata tm2 = TabletMetadata.builder(extent).putOperation(opid1).build(LOCATION);

    assertEquals(extent, tm2.getExtent());
    assertEquals(opid1, tm2.getOperationId());
    assertNull(tm2.getLocation());
    assertThrows(IllegalStateException.class, tm2::getFiles);
    assertThrows(IllegalStateException.class, tm2::getTabletAvailability);
    assertThrows(IllegalStateException.class, tm2::getFlushId);
    assertThrows(IllegalStateException.class, tm2::getFiles);
    assertThrows(IllegalStateException.class, tm2::getLogs);
    assertThrows(IllegalStateException.class, tm2::getLoaded);
    assertThrows(IllegalStateException.class, tm2::getDirName);
    assertThrows(IllegalStateException.class, tm2::getScans);
    assertThrows(IllegalStateException.class, tm2::getExternalCompactions);
    assertThrows(IllegalStateException.class, tm2::getHostingRequested);
    assertThrows(IllegalStateException.class, tm2::getSelectedFiles);
    assertThrows(IllegalStateException.class, tm2::getCompacted);

    var ecid1 = ExternalCompactionId.generate(UUID.randomUUID());
    CompactionMetadata ecm =
        new CompactionMetadata(Set.of(sf1, sf2), rf1, "cid1", CompactionKind.USER, (short) 3,
            CompactorGroupIdImpl.groupId("Q1"), true, FateId.from(type, 99L));

    LogEntry le1 = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());
    LogEntry le2 = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());

    SelectedFiles selFiles = new SelectedFiles(Set.of(sf1, sf4), false, FateId.from(type, 159L));

    TabletMetadata tm3 = TabletMetadata.builder(extent).putExternalCompaction(ecid1, ecm)
        .putSuspension(ser1, 45L).putTime(new MetadataTime(479, TimeType.LOGICAL)).putWal(le1)
        .putWal(le2).setHostingRequested().putSelectedFiles(selFiles).setMerged()
        .putUserCompactionRequested(FateId.from(type, 159L)).build();

    assertEquals(Set.of(ecid1), tm3.getExternalCompactions().keySet());
    assertEquals(Set.of(sf1, sf2), tm3.getExternalCompactions().get(ecid1).getJobFiles());
    assertEquals(ser1.getHostAndPort(), tm3.getSuspend().server);
    assertEquals(45L, tm3.getSuspend().suspensionTime);
    assertEquals(new MetadataTime(479, TimeType.LOGICAL), tm3.getTime());
    assertTrue(tm3.getHostingRequested());
    assertEquals(Stream.of(le1, le2).map(LogEntry::toString).collect(toSet()),
        tm3.getLogs().stream().map(LogEntry::toString).collect(toSet()));
    assertEquals(Set.of(sf1, sf4), tm3.getSelectedFiles().getFiles());
    assertEquals(FateId.from(type, 159L), tm3.getSelectedFiles().getFateId());
    assertFalse(tm3.getSelectedFiles().initiallySelectedAll());
    assertEquals(selFiles.getMetadataValue(), tm3.getSelectedFiles().getMetadataValue());
    assertTrue(tm3.hasMerged());
    assertTrue(tm3.getUserCompactionsRequested().contains(FateId.from(type, 159L)));
  }

}

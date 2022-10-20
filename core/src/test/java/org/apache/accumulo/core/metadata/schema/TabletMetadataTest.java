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
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.SuspendLocationColumn;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.COMPACT_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LAST;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SUSPEND;
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

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
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
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TabletMetadataTest {

  @Test
  public void testAllColumns() {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);

    COMPACT_COLUMN.put(mutation, new Value("5"));
    DIRECTORY_COLUMN.put(mutation, new Value("t-0001757"));
    FLUSH_COLUMN.put(mutation, new Value("6"));
    TIME_COLUMN.put(mutation, new Value("M123456789"));

    String bf1 = "hdfs://nn1/acc/tables/1/t-0001/bf1";
    String bf2 = "hdfs://nn1/acc/tables/1/t-0001/bf2";
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf1).put(FateTxId.formatTid(56));
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf2).put(FateTxId.formatTid(59));

    mutation.at().family(ClonedColumnFamily.NAME).qualifier("").put("OK");

    DataFileValue dfv1 = new DataFileValue(555, 23);
    StoredTabletFile tf1 = new StoredTabletFile("hdfs://nn1/acc/tables/1/t-0001/df1.rf");
    StoredTabletFile tf2 = new StoredTabletFile("hdfs://nn1/acc/tables/1/t-0001/df2.rf");
    mutation.at().family(DataFileColumnFamily.NAME).qualifier(tf1.getMetaUpdateDelete())
        .put(dfv1.encode());
    DataFileValue dfv2 = new DataFileValue(234, 13);
    mutation.at().family(DataFileColumnFamily.NAME).qualifier(tf2.getMetaUpdateDelete())
        .put(dfv2.encode());

    mutation.at().family(CurrentLocationColumnFamily.NAME).qualifier("s001").put("server1:8555");

    mutation.at().family(LastLocationColumnFamily.NAME).qualifier("s000").put("server2:8555");

    LogEntry le1 = new LogEntry(extent, 55, "lf1");
    mutation.at().family(le1.getColumnFamily()).qualifier(le1.getColumnQualifier())
        .timestamp(le1.timestamp).put(le1.getValue());
    LogEntry le2 = new LogEntry(extent, 57, "lf2");
    mutation.at().family(le2.getColumnFamily()).qualifier(le2.getColumnQualifier())
        .timestamp(le2.timestamp).put(le2.getValue());

    StoredTabletFile sf1 = new StoredTabletFile("hdfs://nn1/acc/tables/1/t-0001/sf1.rf");
    StoredTabletFile sf2 = new StoredTabletFile("hdfs://nn1/acc/tables/1/t-0001/sf2.rf");
    mutation.at().family(ScanFileColumnFamily.NAME).qualifier(sf1.getMetaUpdateDelete()).put("");
    mutation.at().family(ScanFileColumnFamily.NAME).qualifier(sf2.getMetaUpdateDelete()).put("");

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
    assertEquals(Set.of(le1.getValue() + " " + le1.timestamp, le2.getValue() + " " + le2.timestamp),
        tm.getLogs().stream().map(le -> le.getValue() + " " + le.timestamp).collect(toSet()));
    assertEquals(extent.prevEndRow(), tm.getPrevEndRow());
    assertEquals(extent.tableId(), tm.getTableId());
    assertTrue(tm.sawPrevEndRow());
    assertEquals("M123456789", tm.getTime().encode());
    assertEquals(Set.of(sf1, sf2), Set.copyOf(tm.getScans()));
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

    TabletMetadata tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false);
    TabletState state = tm.getTabletState(tservers);

    assertEquals(TabletState.ASSIGNED, state);
    assertEquals(ser1, tm.getLocation());
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
    assertEquals(ser2, tm.getLocation());
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
    assertEquals(deadSer, tm.getLocation());
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
    mutation.at().family(SuspendLocationColumn.SUSPEND_COLUMN.getColumnFamily())
        .qualifier(SuspendLocationColumn.SUSPEND_COLUMN.getColumnQualifier())
        .put(SuspendingTServer.toValue(ser2, 1000L));
    rowMap = toRowMap(mutation);

    tm = TabletMetadata.convertRow(rowMap.entrySet().iterator(), colsToFetch, false);

    assertEquals(TabletState.SUSPENDED, tm.getTabletState(tservers));
    assertEquals(1000L, tm.getSuspend().suspensionTime);
    assertEquals(ser2.getHostAndPort(), tm.getSuspend().server);
    assertNull(tm.getLocation());
    assertFalse(tm.hasCurrent());
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

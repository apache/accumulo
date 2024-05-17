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
package org.apache.accumulo.server.manager.state;

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.FLUSH_COLUMN;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.manager.state.TabletManagement;
import org.apache.accumulo.core.manager.state.TabletManagement.ManagementAction;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.BulkFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ClonedColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.CurrentLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LastLocationColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ScanFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class TabletManagementTest {

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

  private SortedMap<Key,Value> createMetadataEntryKV(KeyExtent extent) {

    Mutation mutation = TabletColumnFamily.createPrevRowMutation(extent);

    FateInstanceType type = FateInstanceType.fromTableId(extent.tableId());
    FateId fateId1 = FateId.from(type, UUID.randomUUID());
    FateId fateId2 = FateId.from(type, UUID.randomUUID());

    DIRECTORY_COLUMN.put(mutation, new Value("t-0001757"));
    FLUSH_COLUMN.put(mutation, new Value("6"));
    TIME_COLUMN.put(mutation, new Value("M123456789"));

    StoredTabletFile bf1 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/bf1")).insert();
    StoredTabletFile bf2 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/bf2")).insert();
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf1.getMetadata())
        .put(fateId1.canonical());
    mutation.at().family(BulkFileColumnFamily.NAME).qualifier(bf2.getMetadata())
        .put(fateId2.canonical());

    mutation.at().family(ClonedColumnFamily.NAME).qualifier("").put("OK");

    DataFileValue dfv1 = new DataFileValue(555, 23);
    StoredTabletFile tf1 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/df1.rf")).insert();
    StoredTabletFile tf2 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/df2.rf")).insert();
    mutation.at().family(DataFileColumnFamily.NAME).qualifier(tf1.getMetadata()).put(dfv1.encode());
    DataFileValue dfv2 = new DataFileValue(234, 13);
    mutation.at().family(DataFileColumnFamily.NAME).qualifier(tf2.getMetadata()).put(dfv2.encode());

    mutation.at().family(CurrentLocationColumnFamily.NAME).qualifier("s001").put("server1:8555");

    mutation.at().family(LastLocationColumnFamily.NAME).qualifier("s000").put("server2:8555");

    LogEntry le1 = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());
    le1.addToMutation(mutation);
    LogEntry le2 = LogEntry.fromPath("localhost+8020/" + UUID.randomUUID());
    le2.addToMutation(mutation);

    StoredTabletFile sf1 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/sf1.rf")).insert();
    StoredTabletFile sf2 =
        new ReferencedTabletFile(new Path("hdfs://nn1/acc/tables/1/t-0001/sf2.rf")).insert();
    mutation.at().family(ScanFileColumnFamily.NAME).qualifier(sf1.getMetadata()).put("");
    mutation.at().family(ScanFileColumnFamily.NAME).qualifier(sf2.getMetadata()).put("");

    return toRowMap(mutation);

  }

  @Test
  public void testEncodeDecodeWithReasons() throws Exception {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    final Set<ManagementAction> actions =
        Set.of(ManagementAction.NEEDS_LOCATION_UPDATE, ManagementAction.NEEDS_SPLITTING);

    final SortedMap<Key,Value> entries = createMetadataEntryKV(extent);

    TabletManagement.addActions(entries::put, entries.firstKey().getRow(), actions);
    Key key = entries.firstKey();
    Value val = WholeRowIterator.encodeRow(new ArrayList<>(entries.keySet()),
        new ArrayList<>(entries.values()));

    // Remove the REASONS column from the entries map for the comparison check
    // below
    entries.remove(new Key(key.getRow().toString(), "REASONS", ""));

    TabletManagement tmi = new TabletManagement(key, val, true);
    assertEquals(entries, tmi.getTabletMetadata().getKeyValues());
    assertEquals(actions, tmi.getActions());
  }

  @Test
  public void testEncodeDecodeWithErrors() throws Exception {
    KeyExtent extent = new KeyExtent(TableId.of("5"), new Text("df"), new Text("da"));

    final SortedMap<Key,Value> entries = createMetadataEntryKV(extent);

    TabletManagement.addError(entries::put, entries.firstKey().getRow(),
        new UnsupportedOperationException("Not supported."));
    Key key = entries.firstKey();
    Value val = WholeRowIterator.encodeRow(new ArrayList<>(entries.keySet()),
        new ArrayList<>(entries.values()));

    // Remove the ERROR column from the entries map for the comparison check
    // below
    entries.remove(new Key(key.getRow().toString(), "ERROR", ""));

    TabletManagement tmi = new TabletManagement(key, val, true);
    assertEquals(entries, tmi.getTabletMetadata().getKeyValues());
    assertEquals("Not supported.", tmi.getErrorMessage());
  }

  @Test
  public void testBinary() throws Exception {
    // test end row with non ascii data
    Text endRow = new Text(new byte[] {'m', (byte) 0xff});
    KeyExtent extent = new KeyExtent(TableId.of("5"), endRow, new Text("da"));

    final Set<ManagementAction> actions =
        Set.of(ManagementAction.NEEDS_LOCATION_UPDATE, ManagementAction.NEEDS_SPLITTING);

    final SortedMap<Key,Value> entries = createMetadataEntryKV(extent);

    TabletManagement.addActions(entries::put, entries.firstKey().getRow(), actions);
    Key key = entries.firstKey();
    Value val = WholeRowIterator.encodeRow(new ArrayList<>(entries.keySet()),
        new ArrayList<>(entries.values()));

    assertTrue(entries.keySet().stream().allMatch(k -> k.getRow().equals(extent.toMetaRow())));

    // Remove the REASONS column from the entries map for the comparison check
    // below
    entries.remove(new Key(key.getRow(), new Text("REASONS"), new Text("")));

    TabletManagement tmi = new TabletManagement(key, val, true);
    assertEquals(entries, tmi.getTabletMetadata().getKeyValues());
    assertEquals(actions, tmi.getActions());

  }
}

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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TableDiskUsageTest {

  private static final String volume1 = "hdfs://nn1/acc";
  private static final String volume2 = "hdfs://nn2/acc";

  private static final TableId tableId1 = TableId.of("1");
  private static final TableId tableId2 = TableId.of("2");
  private static final TableId tableId3 = TableId.of("3");
  private static final String tabletName1 = "t-0001";
  private static final String tabletName2 = "t-0002";
  private static final String tabletName3 = "t-0003";
  private static final String tabletName4 = "t-0004";

  private static final Map<TableId,String> tableIdToNameMap = new HashMap<>();

  @BeforeAll
  public static void beforeClass() {
    tableIdToNameMap.put(RootTable.ID, MetadataTable.NAME);
    tableIdToNameMap.put(MetadataTable.ID, MetadataTable.NAME);
    tableIdToNameMap.put(tableId1, "table1");
    tableIdToNameMap.put(tableId2, "table2");
    tableIdToNameMap.put(tableId3, "table3");
  }

  @Test
  public void testSingleTableMultipleTablets() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);
    final TabletsMetadata mockTabletsMetadata = mockTabletsMetadata(client, tableId1);

    List<TabletMetadata> realTabletsMetadata = new ArrayList<>();
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume1, tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume1, tableId1, tabletName1, "C0002.rf"), 1024);
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume1, tableId1, tabletName2, "C0003.rf"), 2048);
    mockTabletsMetadataIter(mockTabletsMetadata, realTabletsMetadata.iterator());

    EasyMock.replay(client, mockTabletsMetadata);

    Map<SortedSet<String>,Long> result = TableDiskUsage.getDiskUsage(tableSet(tableId1), client);

    assertEquals(4096, getTotalUsage(result, tableId1));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult =
        result.entrySet().stream().findFirst().orElseThrow();
    assertEquals(1, firstResult.getKey().size());
    assertTrue(firstResult.getKey().contains(getTableName(tableId1)));
    assertEquals(4096, firstResult.getValue());

    EasyMock.verify(client, mockTabletsMetadata);
  }

  @Test
  public void testMultipleVolumes() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);
    final TabletsMetadata mockTabletsMetadata = mockTabletsMetadata(client, tableId1);

    List<TabletMetadata> realTabletsMetadata = new ArrayList<>();
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume1, tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume1, tableId1, tabletName1, "C0002.rf"), 1024);
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume2, tableId1, tabletName2, "C0003.rf"), 2048);
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume2, tableId1, tabletName2, "C0004.rf"), 10000);
    mockTabletsMetadataIter(mockTabletsMetadata, realTabletsMetadata.iterator());

    EasyMock.replay(client, mockTabletsMetadata);

    Map<SortedSet<String>,Long> result = TableDiskUsage.getDiskUsage(tableSet(tableId1), client);

    assertEquals(14096, getTotalUsage(result, tableId1));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult =
        result.entrySet().stream().findFirst().orElseThrow();
    assertEquals(1, firstResult.getKey().size());
    assertEquals(14096, firstResult.getValue());

    EasyMock.verify(client, mockTabletsMetadata);
  }

  @Test
  public void testMetadataTable() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);
    final TabletsMetadata mockTabletsMetadata = mockTabletsMetadata(client, MetadataTable.ID);

    List<TabletMetadata> realTabletsMetadata = new ArrayList<>();
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume1, MetadataTable.ID, MetadataTable.NAME, "C0001.rf"), 1024);
    mockTabletsMetadataIter(mockTabletsMetadata, realTabletsMetadata.iterator());

    EasyMock.replay(client, mockTabletsMetadata);

    Map<SortedSet<String>,Long> result =
        TableDiskUsage.getDiskUsage(tableSet(MetadataTable.ID), client);

    assertEquals(1024, getTotalUsage(result, MetadataTable.ID));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult =
        result.entrySet().stream().findFirst().orElseThrow();
    assertEquals(1024, firstResult.getValue());

    EasyMock.verify(client, mockTabletsMetadata);
  }

  @Test
  public void testDuplicateFile() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);
    final TabletsMetadata mockTabletsMetadata = mockTabletsMetadata(client, tableId1);

    List<TabletMetadata> realTabletsMetadata = new ArrayList<>();
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume1, tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(realTabletsMetadata,
        getTabletFile(volume1, tableId1, tabletName1, "C0001.rf"), 1024);
    mockTabletsMetadataIter(mockTabletsMetadata, realTabletsMetadata.iterator());

    EasyMock.replay(client, mockTabletsMetadata);

    Map<SortedSet<String>,Long> result = TableDiskUsage.getDiskUsage(tableSet(tableId1), client);

    assertEquals(1024, getTotalUsage(result, tableId1));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult =
        result.entrySet().stream().findFirst().orElseThrow();
    assertEquals(1, firstResult.getKey().size());
    assertTrue(firstResult.getKey().contains(getTableName(tableId1)));
    assertEquals(1024, firstResult.getValue());

    EasyMock.verify(client, mockTabletsMetadata);
  }

  @Test
  public void testEmptyTable() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);
    final TabletsMetadata mockTabletsMetadata = mockTabletsMetadata(client, tableId1);

    List<TabletMetadata> realTabletsMetadata = new ArrayList<>();
    mockTabletsMetadataIter(mockTabletsMetadata, realTabletsMetadata.iterator());

    EasyMock.replay(client, mockTabletsMetadata);

    Map<SortedSet<String>,Long> result = TableDiskUsage.getDiskUsage(tableSet(tableId1), client);

    assertEquals(0, getTotalUsage(result, tableId1));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult =
        result.entrySet().stream().findFirst().orElseThrow();
    assertEquals(1, firstResult.getKey().size());
    assertEquals(0, firstResult.getValue());

    EasyMock.verify(client, mockTabletsMetadata);
  }

  @Test
  public void testMultipleTables() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);

    final TabletsMetadata mockTabletsMetadata1 = mockTabletsMetadata(client, tableId1);
    List<TabletMetadata> realTabletsMetadata1 = new ArrayList<>();
    appendFileMetadata(realTabletsMetadata1,
        getTabletFile(volume1, tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(realTabletsMetadata1,
        getTabletFile(volume1, tableId1, tabletName1, "C0002.rf"), 4096);
    mockTabletsMetadataIter(mockTabletsMetadata1, realTabletsMetadata1.iterator());

    final TabletsMetadata mockTabletsMetadata2 = mockTabletsMetadata(client, tableId2);
    List<TabletMetadata> realTabletsMetadata2 = new ArrayList<>();
    appendFileMetadata(realTabletsMetadata2,
        getTabletFile(volume1, tableId2, tabletName2, "C0003.rf"), 2048);
    appendFileMetadata(realTabletsMetadata2,
        getTabletFile(volume1, tableId2, tabletName2, "C0004.rf"), 3000);
    mockTabletsMetadataIter(mockTabletsMetadata2, realTabletsMetadata2.iterator());

    final TabletsMetadata mockTabletsMetadata3 = mockTabletsMetadata(client, tableId3);
    List<TabletMetadata> realTabletsMetadata3 = new ArrayList<>();
    // shared file
    appendFileMetadata(realTabletsMetadata3,
        getTabletFile(volume1, tableId2, tabletName2, "C0003.rf"), 2048);
    appendFileMetadata(realTabletsMetadata3,
        getTabletFile(volume1, tableId3, tabletName3, "C0005.rf"), 84520);
    appendFileMetadata(realTabletsMetadata3,
        getTabletFile(volume1, tableId3, tabletName3, "C0006.rf"), 3000);
    appendFileMetadata(realTabletsMetadata3,
        getTabletFile(volume1, tableId3, tabletName4, "C0007.rf"), 98456);
    mockTabletsMetadataIter(mockTabletsMetadata3, realTabletsMetadata3.iterator());

    EasyMock.replay(client, mockTabletsMetadata1, mockTabletsMetadata2, mockTabletsMetadata3);

    Map<SortedSet<String>,Long> result =
        TableDiskUsage.getDiskUsage(tableSet(tableId1, tableId2, tableId3), client);

    assertEquals(5120, getTotalUsage(result, tableId1));
    assertEquals(5048, getTotalUsage(result, tableId2));
    assertEquals(188024, getTotalUsage(result, tableId3));

    // Make sure all shared tables exist in map
    assertEquals(4, result.size());
    assertTrue(result.containsKey(tableNameSet(tableId1)));
    assertTrue(result.containsKey(tableNameSet(tableId2)));
    assertTrue(result.containsKey(tableNameSet(tableId2, tableId3)));
    assertTrue(result.containsKey(tableNameSet(tableId3)));

    // Make sure all the shared disk usage computations are correct
    assertEquals(5120, result.get(tableNameSet(tableId1)));
    assertEquals(3000, result.get(tableNameSet(tableId2)));
    assertEquals(2048, result.get(tableNameSet(tableId2, tableId3)));
    assertEquals(185976, result.get(tableNameSet(tableId3)));

    EasyMock.verify(client, mockTabletsMetadata1, mockTabletsMetadata2, mockTabletsMetadata3);
  }

  private static TreeSet<String> tableNameSet(TableId... tableIds) {
    return Set.of(tableIds).stream().map(TableDiskUsageTest::getTableName)
        .collect(Collectors.toCollection(TreeSet::new));
  }

  // Need to use a LinkedHashSet for predictable order due to the fact that
  // we are using mock scanners that always return results in the same order
  private static Set<TableId> tableSet(TableId... tableIds) {
    return new LinkedHashSet<>(List.of(tableIds));
  }

  private static Long getTotalUsage(Map<SortedSet<String>,Long> result, TableId tableId) {
    return result.entrySet().stream()
        .filter(entry -> entry.getKey().contains(getTableName(tableId)))
        .mapToLong(Map.Entry::getValue).sum();
  }

  private static String getTableName(TableId tableId) {
    return tableIdToNameMap.get(tableId);
  }

  private static void appendFileMetadata(List<TabletMetadata> realTabletsMetadata,
      StoredTabletFile file, long size) {
    Key key = new Key((file.getTableId() + "<").getBytes(UTF_8),
        MetadataSchema.TabletsSection.DataFileColumnFamily.STR_NAME.getBytes(UTF_8),
        file.getMetaInsert().getBytes(UTF_8), 123L);
    Value val = new DataFileValue(size, 1).encodeAsValue();
    SortedMap<Key,Value> map = new TreeMap<>();
    map.put(key, val);

    TabletMetadata tm = TabletMetadata.convertRow(map.entrySet().iterator(),
        EnumSet.of(TabletMetadata.ColumnType.FILES), true);
    realTabletsMetadata.add(tm);
  }

  private static StoredTabletFile getTabletFile(String volume, TableId tableId, String tablet,
      String fileName) {
    return new StoredTabletFile(
        volume + Constants.HDFS_TABLES_DIR + "/" + tableId + "/" + tablet + "/" + fileName);
  }

  private TabletsMetadata mockTabletsMetadata(ClientContext client, TableId tableId) {
    final Ample ample = EasyMock.createMock(Ample.class);
    final TabletsMetadata.TableOptions tableOptions =
        EasyMock.createMock(TabletsMetadata.TableOptions.class);
    final TabletsMetadata.TableRangeOptions tableRangeOptions =
        EasyMock.createMock(TabletsMetadata.TableRangeOptions.class);
    final TabletsMetadata.Options options = EasyMock.createMock(TabletsMetadata.Options.class);
    final TabletsMetadata tabletsMetadata = EasyMock.createMock(TabletsMetadata.class);
    EasyMock.expect(client.getAmple()).andReturn(ample);
    EasyMock.expect(ample.readTablets()).andReturn(tableOptions);
    EasyMock.expect(tableOptions.forTable(tableId)).andReturn(tableRangeOptions);
    EasyMock.expect(tableRangeOptions.fetch(TabletMetadata.ColumnType.FILES)).andReturn(options);
    EasyMock.expect(options.build()).andReturn(tabletsMetadata);
    EasyMock.replay(ample, tableOptions, tableRangeOptions, options);
    return tabletsMetadata;
  }

  private void mockTabletsMetadataIter(TabletsMetadata tabletsMetadata,
      Iterator<TabletMetadata> realTabletsMetadata) {
    EasyMock.expect(tabletsMetadata.iterator()).andReturn(realTabletsMetadata);
    tabletsMetadata.close();
    EasyMock.expectLastCall().andAnswer(() -> null);
  }
}

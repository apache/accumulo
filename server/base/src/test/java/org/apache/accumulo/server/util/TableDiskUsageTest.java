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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
    final ServerContext client = EasyMock.createMock(ServerContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 1);

    Map<Key,Value> tableEntries = new HashMap<>();
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0002.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName2, "C0003.rf"), 2048);
    mockTableScan(scanner, tableEntries, tableId1);

    EasyMock.replay(client, scanner);

    Map<SortedSet<String>,Long> result = TableDiskUsage.getDiskUsage(tableSet(tableId1), client);

    assertEquals(4096, getTotalUsage(result, tableId1));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult = result.entrySet().stream().findFirst().get();
    assertEquals(1, firstResult.getKey().size());
    assertTrue(firstResult.getKey().contains(getTableName(tableId1)));
    assertEquals(4096, firstResult.getValue());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testMultipleVolumes() throws Exception {
    final ServerContext client = EasyMock.createMock(ServerContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 1);

    Map<Key,Value> tableEntries = new HashMap<>();
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0002.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(volume2, tableId1, tabletName2, "C0003.rf"),
        2048);
    appendFileMetadata(tableEntries, getTabletFile(volume2, tableId1, tabletName2, "C0004.rf"),
        10000);
    mockTableScan(scanner, tableEntries, tableId1);

    EasyMock.replay(client, scanner);

    Map<SortedSet<String>,Long> result = TableDiskUsage.getDiskUsage(tableSet(tableId1), client);

    assertEquals(14096, getTotalUsage(result, tableId1));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult = result.entrySet().stream().findFirst().get();
    assertEquals(1, firstResult.getKey().size());
    assertEquals(14096, firstResult.getValue());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testMetadataTable() throws Exception {
    final ServerContext client = EasyMock.createMock(ServerContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);

    // Expect root table instead to be scanned
    EasyMock.expect(client.createScanner(RootTable.NAME, Authorizations.EMPTY)).andReturn(scanner);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);

    Map<Key,Value> tableEntries = new HashMap<>();
    appendFileMetadata(tableEntries,
        getTabletFile(MetadataTable.ID, MetadataTable.NAME, "C0001.rf"), 1024);
    mockTableScan(scanner, tableEntries, MetadataTable.ID);

    EasyMock.replay(client, scanner);

    Map<SortedSet<String>,Long> result =
        TableDiskUsage.getDiskUsage(tableSet(MetadataTable.ID), client);

    assertEquals(1024, getTotalUsage(result, MetadataTable.ID));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult = result.entrySet().stream().findFirst().get();
    assertEquals(1024, firstResult.getValue());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testDuplicateFile() throws Exception {
    final ServerContext client = EasyMock.createMock(ServerContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 1);

    Map<Key,Value> tableEntries = new HashMap<>();
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    mockTableScan(scanner, tableEntries, tableId1);

    EasyMock.replay(client, scanner);

    Map<SortedSet<String>,Long> result = TableDiskUsage.getDiskUsage(tableSet(tableId1), client);

    assertEquals(1024, getTotalUsage(result, tableId1));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult = result.entrySet().stream().findFirst().get();
    assertEquals(1, firstResult.getKey().size());
    assertTrue(firstResult.getKey().contains(getTableName(tableId1)));
    assertEquals(1024, firstResult.getValue());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testEmptyTable() throws Exception {
    final ServerContext client = EasyMock.createMock(ServerContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 1);

    Map<Key,Value> tableEntries = new HashMap<>();
    mockTableScan(scanner, tableEntries, tableId1);

    EasyMock.replay(client, scanner);

    Map<SortedSet<String>,Long> result = TableDiskUsage.getDiskUsage(tableSet(tableId1), client);

    assertEquals(0, getTotalUsage(result, tableId1));
    assertEquals(1, result.size());
    Map.Entry<SortedSet<String>,Long> firstResult = result.entrySet().stream().findFirst().get();
    assertEquals(1, firstResult.getKey().size());
    assertEquals(0, firstResult.getValue());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testMultipleTables() throws Exception {
    final ServerContext client = EasyMock.createMock(ServerContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 3);

    Map<Key,Value> tableEntries1 = new HashMap<>();
    appendFileMetadata(tableEntries1, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(tableEntries1, getTabletFile(tableId1, tabletName1, "C0002.rf"), 4096);
    mockTableScan(scanner, tableEntries1, tableId1);

    Map<Key,Value> tableEntries2 = new HashMap<>();
    appendFileMetadata(tableEntries2, getTabletFile(tableId2, tabletName2, "C0003.rf"), 2048);
    appendFileMetadata(tableEntries2, getTabletFile(tableId2, tabletName2, "C0004.rf"), 3000);
    mockTableScan(scanner, tableEntries2, tableId2);

    Map<Key,Value> tableEntries3 = new HashMap<>();
    // shared file
    appendFileMetadata(tableEntries3, getTabletFile(tableId2, tabletName2, "C0003.rf"), 2048);
    appendFileMetadata(tableEntries3, getTabletFile(tableId3, tabletName3, "C0005.rf"), 84520);
    appendFileMetadata(tableEntries3, getTabletFile(tableId3, tabletName3, "C0006.rf"), 3000);
    appendFileMetadata(tableEntries3, getTabletFile(tableId3, tabletName4, "C0007.rf"), 98456);
    mockTableScan(scanner, tableEntries3, tableId3);

    EasyMock.replay(client, scanner);

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

    EasyMock.verify(client, scanner);
  }

  private static TreeSet<String> tableNameSet(TableId... tableIds) {
    return Set.of(tableIds).stream().map(tableId -> getTableName(tableId))
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
        .mapToLong(entry -> entry.getValue()).sum();
  }

  private static String getTableName(TableId tableId) {
    return tableIdToNameMap.get(tableId);
  }

  private static void appendFileMetadata(Map<Key,Value> tableEntries, TabletFile file, long size) {
    tableEntries.put(
        new Key(new Text(file.getTableId() + "<"),
            MetadataSchema.TabletsSection.DataFileColumnFamily.NAME, file.getMetaInsertText()),
        new DataFileValue(size, 1).encodeAsValue());
  }

  private static TabletFile getTabletFile(String volume, TableId tableId, String tablet,
      String fileName) {
    return new TabletFile(new Path(
        volume + Constants.HDFS_TABLES_DIR + "/" + tableId + "/" + tablet + "/" + fileName));
  }

  private static TabletFile getTabletFile(TableId tableId, String tablet, String fileName) {
    return getTabletFile(volume1, tableId, tablet, fileName);
  }

  private void mockScan(ServerContext client, Scanner scanner, int times) throws Exception {
    EasyMock.expect(client.createScanner(MetadataTable.NAME, Authorizations.EMPTY))
        .andReturn(scanner).times(times);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);
  }

  private void mockTableScan(Scanner scanner, Map<Key,Value> tableEntries, TableId tableId) {
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    EasyMock.expectLastCall().once();
    scanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());
    EasyMock.expectLastCall().once();
    EasyMock.expect(scanner.iterator()).andReturn(tableEntries.entrySet().iterator());
    scanner.close();
    EasyMock.expectLastCall().once();
  }
}

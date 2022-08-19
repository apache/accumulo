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
package org.apache.accumulo.core.util.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableDiskUsageResult;
import org.apache.accumulo.core.clientImpl.ClientContext;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MetadataTableDiskUsageTest {

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
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 1);

    Map<Key,Value> tableEntries = new HashMap<>();
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0002.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName2, "C0003.rf"), 2048);
    mockTableScan(client, scanner, tableEntries, tableId1);

    EasyMock.replay(client, scanner);

    TableDiskUsageResult result = MetadataTableDiskUsage
        .getDiskUsage(Set.of(getTableName(tableId1)), client, Authorizations.EMPTY);

    assertEquals(4096, result.getTableUsages().get(tableId1).get());
    assertEquals(1, result.getSharedDiskUsages().size());
    assertEquals(4096, result.getSharedDiskUsages().get(0).getUsage());
    assertEquals(4096, result.getVolumeUsages().get(tableId1).get(volume1).get());
    assertEquals(0, result.getSharedTables().size());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testMultipleVolumes() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 1);

    Map<Key,Value> tableEntries = new HashMap<>();
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0002.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(volume2, tableId1, tabletName2, "C0003.rf"),
        2048);
    appendFileMetadata(tableEntries, getTabletFile(volume2, tableId1, tabletName2, "C0004.rf"),
        10000);
    mockTableScan(client, scanner, tableEntries, tableId1);

    EasyMock.replay(client, scanner);

    TableDiskUsageResult result = MetadataTableDiskUsage
        .getDiskUsage(Set.of(getTableName(tableId1)), client, Authorizations.EMPTY);

    assertEquals(14096, result.getTableUsages().get(tableId1).get());
    assertEquals(1, result.getSharedDiskUsages().size());
    assertEquals(14096, result.getSharedDiskUsages().get(0).getUsage());
    assertEquals(2048, result.getVolumeUsages().get(tableId1).get(volume1).get());
    assertEquals(12048, result.getVolumeUsages().get(tableId1).get(volume2).get());
    assertEquals(0, result.getSharedTables().size());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testMetadataTable() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);

    // Expect root table instead to be scanned
    EasyMock.expect(client.createScanner(RootTable.NAME, Authorizations.EMPTY)).andReturn(scanner);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);

    Map<Key,Value> tableEntries = new HashMap<>();
    appendFileMetadata(tableEntries,
        getTabletFile(MetadataTable.ID, MetadataTable.NAME, "C0001.rf"), 1024);
    mockTableScan(client, scanner, tableEntries, MetadataTable.ID);

    EasyMock.replay(client, scanner);

    TableDiskUsageResult result = MetadataTableDiskUsage
        .getDiskUsage(Set.of(getTableName(MetadataTable.ID)), client, Authorizations.EMPTY);

    assertEquals(1024, result.getTableUsages().get(MetadataTable.ID).get());
    assertEquals(1, result.getSharedDiskUsages().size());
    assertEquals(1024, result.getSharedDiskUsages().get(0).getUsage());
    assertEquals(1024, result.getVolumeUsages().get(MetadataTable.ID).get(volume1).get());
    assertEquals(0, result.getSharedTables().size());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testDuplicateFile() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 1);

    Map<Key,Value> tableEntries = new HashMap<>();
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(tableEntries, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    mockTableScan(client, scanner, tableEntries, tableId1);

    EasyMock.replay(client, scanner);

    TableDiskUsageResult result = MetadataTableDiskUsage
        .getDiskUsage(Set.of(getTableName(tableId1)), client, Authorizations.EMPTY);

    assertEquals(1024, result.getTableUsages().get(tableId1).get());
    assertEquals(1, result.getSharedDiskUsages().size());
    assertEquals(1024, result.getSharedDiskUsages().get(0).getUsage());
    assertEquals(1024, result.getVolumeUsages().get(tableId1).get(volume1).get());
    assertEquals(0, result.getSharedTables().size());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testEmptyTable() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 1);

    Map<Key,Value> tableEntries = new HashMap<>();
    mockTableScan(client, scanner, tableEntries, tableId1);

    EasyMock.replay(client, scanner);

    TableDiskUsageResult result = MetadataTableDiskUsage
        .getDiskUsage(Set.of(getTableName(tableId1)), client, Authorizations.EMPTY);

    assertEquals(0, result.getTableUsages().get(tableId1).get());
    assertEquals(1, result.getSharedDiskUsages().size());
    assertEquals(0, result.getSharedDiskUsages().get(0).getUsage());
    assertEquals(0, result.getVolumeUsages().size());
    assertEquals(0, result.getSharedTables().size());

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testMultipleTables() throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 3);

    Map<Key,Value> tableEntries1 = new HashMap<>();
    appendFileMetadata(tableEntries1, getTabletFile(tableId1, tabletName1, "C0001.rf"), 1024);
    appendFileMetadata(tableEntries1, getTabletFile(tableId1, tabletName1, "C0002.rf"), 4096);
    mockTableScan(client, scanner, tableEntries1, tableId1);

    Map<Key,Value> tableEntries2 = new HashMap<>();
    appendFileMetadata(tableEntries2, getTabletFile(tableId2, tabletName2, "C0003.rf"), 2048);
    appendFileMetadata(tableEntries2, getTabletFile(tableId2, tabletName2, "C0004.rf"), 3000);
    mockTableScan(client, scanner, tableEntries2, tableId2);

    Map<Key,Value> tableEntries3 = new HashMap<>();
    // shared file
    appendFileMetadata(tableEntries3, getTabletFile(tableId2, tabletName2, "C0003.rf"), 2048);
    appendFileMetadata(tableEntries3, getTabletFile(tableId3, tabletName3, "C0005.rf"), 84520);
    appendFileMetadata(tableEntries3, getTabletFile(tableId3, tabletName3, "C0006.rf"), 3000);
    appendFileMetadata(tableEntries3, getTabletFile(tableId3, tabletName4, "C0007.rf"), 98456);
    mockTableScan(client, scanner, tableEntries3, tableId3);

    EasyMock.replay(client, scanner);

    TableDiskUsageResult result = MetadataTableDiskUsage.getDiskUsage(
        Set.of(getTableName(tableId1), getTableName(tableId2), getTableName(tableId3)), client,
        Authorizations.EMPTY);

    assertEquals(5120, result.getTableUsages().get(tableId1).get());
    assertEquals(5048, result.getTableUsages().get(tableId2).get());
    assertEquals(188024, result.getTableUsages().get(tableId3).get());

    assertEquals(4, result.getSharedDiskUsages().size());
    assertEquals(Set.of(getTableName(tableId1)), result.getSharedDiskUsages().get(0).getTables());
    assertEquals(Set.of(getTableName(tableId2)), result.getSharedDiskUsages().get(1).getTables());
    assertEquals(Set.of(getTableName(tableId2), getTableName(tableId3)),
        result.getSharedDiskUsages().get(2).getTables());
    assertEquals(Set.of(getTableName(tableId3)), result.getSharedDiskUsages().get(3).getTables());

    // Make sure all the shared disk usage computations are correct
    assertEquals(5120, result.getSharedDiskUsages().get(0).getUsage());
    assertEquals(3000, result.getSharedDiskUsages().get(1).getUsage());
    assertEquals(2048, result.getSharedDiskUsages().get(2).getUsage());
    assertEquals(185976, result.getSharedDiskUsages().get(3).getUsage());

    // check volume usage
    assertEquals(5120, result.getVolumeUsages().get(tableId1).get(volume1).get());
    assertEquals(5048, result.getVolumeUsages().get(tableId2).get(volume1).get());
    assertEquals(188024, result.getVolumeUsages().get(tableId3).get(volume1).get());

    assertEquals(1, result.getSharedTables().size());
    assertEquals(Set.of(tableId2), result.getSharedTables().get(tableId3));

    EasyMock.verify(client, scanner);
  }

  @Test
  public void testSharedFileComputeSharedFalse() throws Exception {
    testSharedFile(false);
  }

  @Test
  public void testSharedFile() throws Exception {
    testSharedFile(true);
  }

  private void testSharedFile(boolean computeShared) throws Exception {
    final ClientContext client = EasyMock.createMock(ClientContext.class);
    final Scanner scanner = EasyMock.createMock(Scanner.class);
    mockScan(client, scanner, 1);

    Map<Key,Value> tableEntries1 = new HashMap<>();
    appendFileMetadata(tableEntries1, getTabletFile(tableId1, tabletName1, "C0002.rf"), 1024);
    appendFileMetadata(tableEntries1, getTabletFile(tableId2, tabletName2, "C0003.rf"), 2048);
    appendFileMetadata(tableEntries1, getTabletFile(tableId2, tabletName2, "C0004.rf"), 3000);
    mockTableScan(client, scanner, tableEntries1, tableId2);

    EasyMock.replay(client, scanner);

    TableDiskUsageResult result = MetadataTableDiskUsage
        .getDiskUsage(Set.of(getTableName(tableId2)), computeShared, client, Authorizations.EMPTY);

    assertEquals(1, result.getTableUsages().size());
    assertEquals(6072, result.getTableUsages().get(tableId2).get());
    assertEquals(6072, result.getVolumeUsages().get(tableId2).get(volume1).get());

    // If compute shared is true then make sure metrics have been calculated across files
    if (computeShared) {
      assertEquals(2, result.getSharedDiskUsages().size());
      assertEquals(Set.of(getTableName(tableId1), getTableName(tableId2)),
          result.getSharedDiskUsages().get(0).getTables());
      assertEquals(Set.of(getTableName(tableId2)), result.getSharedDiskUsages().get(1).getTables());
      assertEquals(1024, result.getSharedDiskUsages().get(0).getUsage());
      assertEquals(5048, result.getSharedDiskUsages().get(1).getUsage());
    } else {
      assertEquals(0, result.getSharedDiskUsages().size());
    }

    assertEquals(1, result.getSharedTables().size());
    assertEquals(Set.of(tableId1), result.getSharedTables().get(tableId2));

    EasyMock.verify(client, scanner);
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

  private void mockScan(ClientContext client, Scanner scanner, int times) throws Exception {
    EasyMock.expect(client.createScanner(MetadataTable.NAME, Authorizations.EMPTY))
        .andReturn(scanner).times(times);
    EasyMock.expect(client.getTableIdToNameMap()).andReturn(tableIdToNameMap);
  }

  private void mockTableScan(ClientContext client, Scanner scanner, Map<Key,Value> tableEntries,
      TableId tableId) throws Exception {
    EasyMock.expect(client.getTableId(getTableName(tableId))).andReturn(tableId);
    scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    EasyMock.expectLastCall().once();
    scanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());
    EasyMock.expectLastCall().once();
    EasyMock.expect(scanner.iterator()).andReturn(tableEntries.entrySet().iterator());
    scanner.close();
    EasyMock.expectLastCall().once();
  }
}

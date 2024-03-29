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
package org.apache.accumulo.core.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.TableId;
import org.easymock.EasyMock;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MetadataServicerTest {

  private static final String userTableName = "tableName";
  private static final TableId userTableId = TableId.of("tableId");
  private static ClientContext context;

  @BeforeAll
  public static void setupContext() {
    HashMap<String,String> tableNameToIdMap = new HashMap<>();
    tableNameToIdMap.put(AccumuloTable.ROOT.tableName(), AccumuloTable.ROOT.tableId().canonical());
    tableNameToIdMap.put(AccumuloTable.METADATA.tableName(),
        AccumuloTable.METADATA.tableId().canonical());
    tableNameToIdMap.put(userTableName, userTableId.canonical());

    context = EasyMock.createMock(ClientContext.class);
    TableOperations tableOps = EasyMock.createMock(TableOperations.class);
    EasyMock.expect(tableOps.tableIdMap()).andReturn(tableNameToIdMap).anyTimes();
    EasyMock.expect(context.tableOperations()).andReturn(tableOps).anyTimes();
    EasyMock.replay(context, tableOps);
  }

  @Test
  public void checkSystemTableIdentifiers() {
    assertNotEquals(AccumuloTable.ROOT.tableId(), AccumuloTable.METADATA.tableId());
    assertNotEquals(AccumuloTable.ROOT.tableName(), AccumuloTable.METADATA.tableName());
  }

  @Test
  public void testGetCorrectServicer() throws AccumuloException, AccumuloSecurityException {
    MetadataServicer ms = MetadataServicer.forTableId(context, AccumuloTable.ROOT.tableId());
    assertTrue(ms instanceof ServicerForRootTable);
    assertFalse(ms instanceof TableMetadataServicer);
    assertEquals(AccumuloTable.ROOT.tableId(), ms.getServicedTableId());

    ms = MetadataServicer.forTableId(context, AccumuloTable.METADATA.tableId());
    assertTrue(ms instanceof ServicerForMetadataTable);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(AccumuloTable.ROOT.tableName(),
        ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(AccumuloTable.METADATA.tableId(), ms.getServicedTableId());

    ms = MetadataServicer.forTableId(context, userTableId);
    assertTrue(ms instanceof ServicerForUserTables);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(AccumuloTable.METADATA.tableName(),
        ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(userTableId, ms.getServicedTableId());

    ms = MetadataServicer.forTableName(context, AccumuloTable.ROOT.tableName());
    assertTrue(ms instanceof ServicerForRootTable);
    assertFalse(ms instanceof TableMetadataServicer);
    assertEquals(AccumuloTable.ROOT.tableId(), ms.getServicedTableId());

    ms = MetadataServicer.forTableName(context, AccumuloTable.METADATA.tableName());
    assertTrue(ms instanceof ServicerForMetadataTable);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(AccumuloTable.ROOT.tableName(),
        ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(AccumuloTable.METADATA.tableId(), ms.getServicedTableId());

    ms = MetadataServicer.forTableName(context, userTableName);
    assertTrue(ms instanceof ServicerForUserTables);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(AccumuloTable.METADATA.tableName(),
        ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(userTableId, ms.getServicedTableId());
  }
}

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
    tableNameToIdMap.put(RootTable.NAME, RootTable.ID.canonical());
    tableNameToIdMap.put(MetadataTable.NAME, MetadataTable.ID.canonical());
    tableNameToIdMap.put(userTableName, userTableId.canonical());

    context = EasyMock.createMock(ClientContext.class);
    TableOperations tableOps = EasyMock.createMock(TableOperations.class);
    EasyMock.expect(tableOps.tableIdMap()).andReturn(tableNameToIdMap).anyTimes();
    EasyMock.expect(context.tableOperations()).andReturn(tableOps).anyTimes();
    EasyMock.replay(context, tableOps);
  }

  @Test
  public void checkSystemTableIdentifiers() {
    assertNotEquals(RootTable.ID, MetadataTable.ID);
    assertNotEquals(RootTable.NAME, MetadataTable.NAME);
  }

  @Test
  public void testGetCorrectServicer() throws AccumuloException, AccumuloSecurityException {
    MetadataServicer ms = MetadataServicer.forTableId(context, RootTable.ID);
    assertTrue(ms instanceof ServicerForRootTable);
    assertFalse(ms instanceof TableMetadataServicer);
    assertEquals(RootTable.ID, ms.getServicedTableId());

    ms = MetadataServicer.forTableId(context, MetadataTable.ID);
    assertTrue(ms instanceof ServicerForMetadataTable);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(RootTable.NAME, ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(MetadataTable.ID, ms.getServicedTableId());

    ms = MetadataServicer.forTableId(context, userTableId);
    assertTrue(ms instanceof ServicerForUserTables);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(MetadataTable.NAME, ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(userTableId, ms.getServicedTableId());

    ms = MetadataServicer.forTableName(context, RootTable.NAME);
    assertTrue(ms instanceof ServicerForRootTable);
    assertFalse(ms instanceof TableMetadataServicer);
    assertEquals(RootTable.ID, ms.getServicedTableId());

    ms = MetadataServicer.forTableName(context, MetadataTable.NAME);
    assertTrue(ms instanceof ServicerForMetadataTable);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(RootTable.NAME, ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(MetadataTable.ID, ms.getServicedTableId());

    ms = MetadataServicer.forTableName(context, userTableName);
    assertTrue(ms instanceof ServicerForUserTables);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(MetadataTable.NAME, ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(userTableId, ms.getServicedTableId());
  }
}

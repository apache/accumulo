/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Test;

public class MetadataServicerTest {

  private static final String userTableName = "tableName";
  private static final Table.ID userTableId = Table.ID.of("tableId");
  private static ClientContext context;

  @BeforeClass
  public static void setupContext() throws Exception {
    HashMap<String,String> tableNameToIdMap = new HashMap<>();
    tableNameToIdMap.put(RootTable.NAME, RootTable.ID.canonicalID());
    tableNameToIdMap.put(MetadataTable.NAME, MetadataTable.ID.canonicalID());
    tableNameToIdMap.put(ReplicationTable.NAME, ReplicationTable.ID.canonicalID());
    tableNameToIdMap.put(userTableName, userTableId.canonicalID());

    context = EasyMock.createMock(ClientContext.class);
    Connector conn = EasyMock.createMock(Connector.class);
    Instance inst = EasyMock.createMock(Instance.class);
    TableOperations tableOps = EasyMock.createMock(TableOperations.class);
    EasyMock.expect(tableOps.tableIdMap()).andReturn(tableNameToIdMap).anyTimes();
    EasyMock.expect(conn.tableOperations()).andReturn(tableOps).anyTimes();
    EasyMock.expect(context.getInstance()).andReturn(inst).anyTimes();
    EasyMock.expect(context.getConnector()).andReturn(conn).anyTimes();
    EasyMock.replay(context, conn, inst, tableOps);
  }

  @Test
  public void checkSystemTableIdentifiers() {
    assertNotEquals(RootTable.ID, MetadataTable.ID);
    assertNotEquals(RootTable.NAME, MetadataTable.NAME);
  }

  @Test
  public void testGetCorrectServicer() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    MetadataServicer ms = MetadataServicer.forTableId(context, RootTable.ID);
    assertTrue(ms instanceof ServicerForRootTable);
    assertFalse(ms instanceof TableMetadataServicer);
    assertEquals(RootTable.ID, ms.getServicedTableId());

    ms = MetadataServicer.forTableId(context, MetadataTable.ID);
    assertTrue(ms instanceof ServicerForMetadataTable);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(RootTable.NAME, ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(MetadataTable.ID, ms.getServicedTableId());

    ms = MetadataServicer.forTableId(context, ReplicationTable.ID);
    assertTrue(ms instanceof ServicerForUserTables);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(MetadataTable.NAME, ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(ReplicationTable.ID, ms.getServicedTableId());

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

    ms = MetadataServicer.forTableName(context, ReplicationTable.NAME);
    assertTrue(ms instanceof ServicerForUserTables);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(MetadataTable.NAME, ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(ReplicationTable.ID, ms.getServicedTableId());

    ms = MetadataServicer.forTableName(context, userTableName);
    assertTrue(ms instanceof ServicerForUserTables);
    assertTrue(ms instanceof TableMetadataServicer);
    assertEquals(MetadataTable.NAME, ((TableMetadataServicer) ms).getServicingTableName());
    assertEquals(userTableId, ms.getServicedTableId());
  }
}

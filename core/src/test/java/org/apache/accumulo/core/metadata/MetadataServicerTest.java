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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.Test;

public class MetadataServicerTest {

  @Test
  public void checkSystemTableIdentifiers() {
    assertNotEquals(RootTable.ID, MetadataTable.ID);
    assertNotEquals(RootTable.NAME, MetadataTable.NAME);
  }

  @Test
  public void testGetCorrectServicer() throws AccumuloException, AccumuloSecurityException, TableExistsException, TableNotFoundException {
    String userTableName = "A";
    MockInstance instance = new MockInstance("metadataTest");
    Connector connector = instance.getConnector("root", new PasswordToken(""));
    connector.tableOperations().create(userTableName);
    String userTableId = connector.tableOperations().tableIdMap().get(userTableName);
    Credentials credentials = new Credentials("root", new PasswordToken(""));
    ClientContext context = new ClientContext(instance, credentials, new ClientConfiguration());

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

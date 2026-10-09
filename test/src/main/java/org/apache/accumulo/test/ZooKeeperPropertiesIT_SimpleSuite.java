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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.accumulo.test.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class ZooKeeperPropertiesIT_SimpleSuite extends SharedMiniClusterBase {

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testNoFiles() {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      // Should throw an error as this property can't be changed in ZooKeeper
      assertThrows(AccumuloException.class, () -> client.instanceOperations()
          .setProperty(Property.GENERAL_RPC_TIMEOUT.getKey(), "60s"));
    }
  }

  @Test
  @Timeout(30)
  public void testTablePropUtils() throws AccumuloException, TableExistsException,
      AccumuloSecurityException, TableNotFoundException {
    ServerContext context = getCluster().getServerContext();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      final String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      Map<String,String> idMap = client.tableOperations().tableIdMap();
      String tid = idMap.get(tableName);

      Map<String,String> properties = client.tableOperations().getConfiguration(tableName);
      assertEquals("false", properties.get(Property.TABLE_BLOOM_ENABLED.getKey()));

      final TablePropKey tablePropKey = TablePropKey.of(TableId.of(tid));
      PropUtil.setProperties(context, tablePropKey,
          Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

      Wait.waitFor(
          () -> !client.tableOperations().getConfiguration(tableName)
              .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"),
          30_000, 250, "Table property update did not propagate");

      PropUtil.removeProperties(context, tablePropKey,
          List.of(Property.TABLE_BLOOM_ENABLED.getKey()));

      Wait.waitFor(
          () -> !client.tableOperations().getConfiguration(tableName)
              .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"),
          30_000, 250, "Table property removal did not propagate");

      // Add invalid property
      assertThrows(IllegalArgumentException.class,
          () -> PropUtil.setProperties(context, tablePropKey,
              Map.of("NOT_A_PROPERTY", "not_a_value")),
          "Expected IllegalArgumentException to be thrown.");
    }
  }

  @Test
  @Timeout(30)
  public void testNamespacePropUtils() throws AccumuloException, AccumuloSecurityException,
      NamespaceExistsException, NamespaceNotFoundException {
    ServerContext context = getCluster().getServerContext();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      final String namespace = getUniqueNames(1)[0];
      client.namespaceOperations().create(namespace);
      Map<String,String> nsMap = client.namespaceOperations().namespaceIdMap();
      String nid = nsMap.get(namespace);

      Map<String,String> properties = client.namespaceOperations().getConfiguration(namespace);
      assertEquals("15", properties.get(Property.TABLE_FILE_MAX.getKey()));

      final NamespaceId namespaceId = NamespaceId.of(nid);
      final NamespacePropKey namespacePropKey = NamespacePropKey.of(namespaceId);
      PropUtil.setProperties(context, namespacePropKey,
          Map.of(Property.TABLE_FILE_MAX.getKey(), "31"));

      Wait.waitFor(
          () -> client.namespaceOperations().getConfiguration(namespace)
              .get(Property.TABLE_FILE_MAX.getKey()).equals("31"),
          30_000, 250, "Namespace property update did not propagate");

      PropUtil.removeProperties(context, namespacePropKey,
          List.of(Property.TABLE_FILE_MAX.getKey()));

      Wait.waitFor(
          () -> client.namespaceOperations().getConfiguration(namespace)
              .get(Property.TABLE_FILE_MAX.getKey()).equals("15"),
          30_000, 250, "Namespace property removal did not propagate");

      // Add invalid property
      assertThrows(IllegalArgumentException.class,
          () -> PropUtil.setProperties(context, namespacePropKey,
              Map.of("NOT_A_PROPERTY", "not_a_value")),
          "Expected IllegalArgumentException to be thrown.");
    }
  }

}

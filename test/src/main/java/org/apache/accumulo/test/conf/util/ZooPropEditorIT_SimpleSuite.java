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
package org.apache.accumulo.test.conf.util;

import static org.apache.accumulo.core.conf.SiteConfiguration.ACCUMULO_PROPERTIES_PROPERTY;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.conf.util.ZooPropEditor;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
public class ZooPropEditorIT_SimpleSuite extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(ZooPropEditorIT_SimpleSuite.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterAll
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void modifyPropTest() throws Exception {
    String[] names = getUniqueNames(2);
    String namespace = names[0];
    String table = namespace + "." + names[1];

    assertNull(System.getProperties().get(ACCUMULO_PROPERTIES_PROPERTY));
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {

      client.namespaceOperations().create(namespace);
      client.tableOperations().create(table);

      LOG.debug("Tables: {}", client.tableOperations().list());

      // override default in namespace, and then over-ride that for table prop
      AccumuloException ae = assertThrows(AccumuloException.class, () -> client.instanceOperations()
          .setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));
      assertTrue(ae.getMessage()
          .contains("Table property " + Property.TABLE_BLOOM_ENABLED.getKey()
              + " cannot be set at the system or resource group level."
              + " Set table properties at the namespace or table level."));
      client.namespaceOperations().setProperty(namespace, Property.TABLE_BLOOM_ENABLED.getKey(),
          "true");
      client.tableOperations().setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey(), "false");

      Wait.waitFor(() -> client.namespaceOperations().getConfiguration(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500);

      // before - check setup correct
      Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500);

      System.setProperty(ACCUMULO_PROPERTIES_PROPERTY,
          "file:///" + getCluster().getAccumuloPropertiesPath());

      // set table property (table.bloom.enabled=true)
      String[] setTablePropArgs =
          {"-t", table, "-s", Property.TABLE_BLOOM_ENABLED.getKey() + "=true"};
      new ZooPropEditor().execute(setTablePropArgs);

      // after set - check prop changed in ZooKeeper
      Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500);

      String[] deleteTablePropArgs = {"-t", table, "-d", Property.TABLE_BLOOM_ENABLED.getKey()};
      new ZooPropEditor().execute(deleteTablePropArgs);

      // after delete - check map entry is null (removed from ZooKeeper)
      Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()) == null, 5000, 500);

      // set namespace property (changed from setup)
      Wait.waitFor(() -> client.namespaceOperations().getConfiguration(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500);

      String[] setSystemPropArgs = {"-s", Property.TABLE_BLOOM_ENABLED.getKey() + "=false"};
      IllegalStateException ise = assertThrows(IllegalStateException.class,
          () -> new ZooPropEditor().execute(setSystemPropArgs));
      assertTrue(ise.getMessage().startsWith("Failed to set property for system"));

      // after set - check map entry is false
      Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500);

      // set namespace property (changed from setup)
      Wait.waitFor(() -> client.namespaceOperations().getNamespaceProperties(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500);

      String[] setNamespacePropArgs =
          {"-ns", namespace, "-s", Property.TABLE_BLOOM_ENABLED.getKey() + "=false"};
      new ZooPropEditor().execute(setNamespacePropArgs);

      // after set - check map entry is false
      Wait.waitFor(() -> client.namespaceOperations().getNamespaceProperties(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500);

      String[] deleteNamespacePropArgs =
          {"-ns", namespace, "-d", Property.TABLE_BLOOM_ENABLED.getKey()};
      new ZooPropEditor().execute(deleteNamespacePropArgs);

      // after set - check map entry is false
      Wait.waitFor(() -> client.namespaceOperations().getNamespaceProperties(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()) == null, 5000, 500);

    } finally {
      System.clearProperty(ACCUMULO_PROPERTIES_PROPERTY);
    }
  }

  @Test
  public void testTablePropInSystemConfigFails() {
    assertNull(System.getProperties().get(ACCUMULO_PROPERTIES_PROPERTY));
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      DefaultConfiguration dc = DefaultConfiguration.getInstance();
      Map<String,String> defaultProperties = dc.getAllPropertiesWithPrefix(Property.TABLE_PREFIX);
      System.setProperty(ACCUMULO_PROPERTIES_PROPERTY,
          "file:///" + getCluster().getAccumuloPropertiesPath());
      for (Entry<String,String> e : defaultProperties.entrySet()) {
        ZooPropEditor tool = new ZooPropEditor();
        String[] setSystemPropArgs = {"-s", e.getKey() + "=" + e.getValue()};
        IllegalStateException ise =
            assertThrows(IllegalStateException.class, () -> tool.execute(setSystemPropArgs));
        assertTrue(ise.getMessage().startsWith("Failed to set property for system"),
            ise::getMessage);
      }
    } finally {
      System.clearProperty(ACCUMULO_PROPERTIES_PROPERTY);
    }
  }

  @Test
  public void testTablePropInResourceGroupConfigFails() {
    assertNull(System.getProperties().get(ACCUMULO_PROPERTIES_PROPERTY));
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      DefaultConfiguration dc = DefaultConfiguration.getInstance();
      Map<String,String> defaultProperties = dc.getAllPropertiesWithPrefix(Property.TABLE_PREFIX);
      System.setProperty(ACCUMULO_PROPERTIES_PROPERTY,
          "file:///" + getCluster().getAccumuloPropertiesPath());
      for (Entry<String,String> e : defaultProperties.entrySet()) {
        ZooPropEditor tool = new ZooPropEditor();
        String[] setRGPropArgs =
            {"-r", ResourceGroupId.DEFAULT.canonical(), "-s", e.getKey() + "=" + e.getValue()};
        IllegalStateException ise =
            assertThrows(IllegalStateException.class, () -> tool.execute(setRGPropArgs));
        assertTrue(ise.getMessage().startsWith("Failed to set property for default"),
            ise::getMessage);
      }
    } finally {
      System.clearProperty(ACCUMULO_PROPERTIES_PROPERTY);
    }
  }

}

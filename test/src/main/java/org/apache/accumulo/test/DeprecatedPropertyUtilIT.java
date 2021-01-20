/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.conf.DeprecatedPropertyUtil.PropertyRenamer;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.server.util.NamespacePropUtil;
import org.apache.accumulo.server.util.SystemPropUtil;
import org.apache.accumulo.server.util.TablePropUtil;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DeprecatedPropertyUtilIT extends ConfigurableMacBase {
  private static final String OLD_SYSTEM_PREFIX = "old.system.custom.";
  private static final String OLD_TABLE_PREFIX = "old.table.custom.";
  private static final String OLD_NS_PREFIX = "old.ns.custom.";

  private static final PropertyRenamer TEST_SYS_RENAMER = new PropertyRenamer() {
    @Override
    public boolean matches(String property) {
      return property.startsWith(OLD_SYSTEM_PREFIX);
    }

    @Override
    public String rename(String property) {
      return Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey()
          + property.substring(OLD_SYSTEM_PREFIX.length());
    }
  };
  private static final PropertyRenamer TEST_TABLE_RENAMER = new PropertyRenamer() {
    @Override
    public boolean matches(String property) {
      return property.startsWith(OLD_TABLE_PREFIX);
    }

    @Override
    public String rename(String property) {
      return Property.TABLE_ARBITRARY_PROP_PREFIX.getKey()
          + property.substring(OLD_TABLE_PREFIX.length());
    }
  };
  private static final PropertyRenamer TEST_NAMESPACE_RENAMER = new PropertyRenamer() {
    @Override
    public boolean matches(String property) {
      return property.startsWith(OLD_NS_PREFIX);
    }

    @Override
    public String rename(String property) {
      return Property.TABLE_ARBITRARY_PROP_PREFIX.getKey()
          + property.substring(OLD_NS_PREFIX.length());
    }
  };

  private static class TestPropertyUtil extends DeprecatedPropertyUtil {
    public static void registerTestRenamer() {
      renamers.add(TEST_SYS_RENAMER);
      renamers.add(TEST_TABLE_RENAMER);
      renamers.add(TEST_NAMESPACE_RENAMER);
    }

    public static void removeTestRenamer() {
      renamers.removeIf(
          r -> r == TEST_SYS_RENAMER || r == TEST_TABLE_RENAMER || r == TEST_NAMESPACE_RENAMER);
    }
  }

  @Before
  public void setUp() throws Exception {
    super.setUp();
    TestPropertyUtil.registerTestRenamer();
  }

  @After
  public void tearDown() {
    super.tearDown();
    TestPropertyUtil.removeTestRenamer();
  }

  @Test
  public void testSystemProperty() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String oldProp = OLD_SYSTEM_PREFIX + "test.prop";
      String newProp = Property.GENERAL_ARBITRARY_PROP_PREFIX.getKey() + "test.prop";
      String propValue = "dummy prop value";
      Map<String,String> config = client.instanceOperations().getSystemConfiguration();
      assertFalse(oldProp + " was in the config!", config.containsKey(newProp));
      assertFalse(newProp + " was in the config!", config.containsKey(newProp));

      SystemPropUtil.setSystemProperty(getServerContext(), oldProp, propValue);

      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(oldProp + " was in the config after set call!", config.containsKey(oldProp));
      assertTrue(newProp + " was not in the config after set call!", config.containsKey(newProp));
      assertEquals(propValue, config.get(newProp));

      SystemPropUtil.removeSystemProperty(getServerContext(), oldProp);

      config = client.instanceOperations().getSystemConfiguration();
      assertFalse(oldProp + " was in the config after remove call!", config.containsKey(oldProp));
      assertFalse(newProp + " was in the config after remove call!", config.containsKey(newProp));
    }
  }

  @Test
  public void testTableProperty() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String oldProp = OLD_TABLE_PREFIX + "test.prop";
      String newProp = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "test.prop";
      String propValue = "dummy prop value";
      Map<String,String> props = new HashMap<>();
      client.tableOperations().getProperties(MetadataTable.NAME)
          .forEach(e -> props.put(e.getKey(), e.getValue()));
      assertFalse(oldProp + " was in the config!", props.containsKey(newProp));
      assertFalse(newProp + " was in the config!", props.containsKey(newProp));

      TablePropUtil.setTableProperty(getServerContext(), MetadataTable.ID, oldProp, propValue);

      props.clear();
      client.tableOperations().getProperties(MetadataTable.NAME)
          .forEach(e -> props.put(e.getKey(), e.getValue()));
      assertFalse(oldProp + " was in the config after set call!", props.containsKey(oldProp));
      assertTrue(newProp + " was not in the config after set call!", props.containsKey(newProp));
      assertEquals(propValue, props.get(newProp));

      TablePropUtil.removeTableProperty(getServerContext(), MetadataTable.ID, oldProp);

      props.clear();
      client.tableOperations().getProperties(MetadataTable.NAME)
          .forEach(e -> props.put(e.getKey(), e.getValue()));
      assertFalse(oldProp + " was in the config after remove call!", props.containsKey(oldProp));
      assertFalse(newProp + " was in the config after remove call!", props.containsKey(newProp));
    }
  }

  @Test
  public void testNamespaceProperty() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String namespace = "testNamespace";
      client.namespaceOperations().create(namespace);
      NamespaceId nsID =
          NamespaceId.of(client.namespaceOperations().namespaceIdMap().get(namespace));

      String oldProp = OLD_NS_PREFIX + "test.prop";
      String newProp = Property.TABLE_ARBITRARY_PROP_PREFIX.getKey() + "test.prop";
      String propValue = "dummy prop value";
      Map<String,String> props = new HashMap<>();
      client.namespaceOperations().getProperties(namespace)
          .forEach(e -> props.put(e.getKey(), e.getValue()));
      assertFalse(oldProp + " was in the config!", props.containsKey(newProp));
      assertFalse(newProp + " was in the config!", props.containsKey(newProp));

      NamespacePropUtil.setNamespaceProperty(getServerContext(), nsID, oldProp, propValue);

      props.clear();
      client.namespaceOperations().getProperties(namespace)
          .forEach(e -> props.put(e.getKey(), e.getValue()));

      assertFalse(oldProp + " was in the config after set call!", props.containsKey(oldProp));
      assertTrue(newProp + " was not in the config after set call!", props.containsKey(newProp));
      assertEquals(propValue, props.get(newProp));

      NamespacePropUtil.removeNamespaceProperty(getServerContext(), nsID, oldProp);

      props.clear();
      client.namespaceOperations().getProperties(namespace)
          .forEach(e -> props.put(e.getKey(), e.getValue()));
      assertFalse(oldProp + " was in the config after remove call!", props.containsKey(oldProp));
      assertFalse(newProp + " was in the config after remove call!", props.containsKey(newProp));
    }
  }
}

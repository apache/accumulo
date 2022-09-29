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
package org.apache.accumulo.test.conf;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.clientImpl.thrift.TVersionedProperties;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
@Tag(SUNNY_DAY)
public class PropStoreConfigIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(PropStoreConfigIT.class);

  @Test
  public void setTablePropTest() throws Exception {
    String table = getUniqueNames(1)[0];

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      log.info("Tables: {}", client.tableOperations().list());

      client.instanceOperations().setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
      client.tableOperations().setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey(), "true");

      Thread.sleep(SECONDS.toMillis(3L));

      var props = client.tableOperations().getProperties(table);
      log.info("Props: {}", props);
      for (Map.Entry<String,String> e : props) {
        if (e.getKey().contains("table.bloom.enabled")) {
          log.info("after bloom property: {}={}", e.getKey(), e.getValue());
          assertEquals("true", e.getValue());
        }
      }
    }
  }

  @Test
  public void deletePropsTest() throws Exception {
    String[] names = getUniqueNames(2);
    String namespace = names[0];
    String table = namespace + "." + names[1];

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {

      client.namespaceOperations().create(namespace);
      client.tableOperations().create(table);

      log.info("Tables: {}", client.tableOperations().list());

      client.instanceOperations().setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
      client.tableOperations().setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey(), "true");

      Thread.sleep(SECONDS.toMillis(1L));

      var props = client.tableOperations().getProperties(table);
      log.info("Props: {}", props);
      for (Map.Entry<String,String> e : props) {
        if (e.getKey().contains("table.bloom.enabled")) {
          log.info("after bloom property: {}={}", e.getKey(), e.getValue());
          assertEquals("true", e.getValue());
        }
      }

      var tableIdMap = client.tableOperations().tableIdMap();
      var nsIdMap = client.namespaceOperations().namespaceIdMap();

      ServerContext context = getServerContext();

      NamespaceId nid = NamespaceId.of(nsIdMap.get(namespace));
      TableId tid = TableId.of(tableIdMap.get(table));

      // check zk nodes exist
      assertTrue(context.getPropStore().exists(NamespacePropKey.of(context, nid)));
      assertTrue(context.getPropStore().exists(TablePropKey.of(context, tid)));
      // check ServerConfigurationFactory
      assertNotNull(context.getNamespaceConfiguration(nid));
      assertNotNull(context.getTableConfiguration(tid));

      client.tableOperations().delete(table);
      client.namespaceOperations().delete(namespace);
      Thread.sleep(100);

      // check zk nodes deleted
      assertFalse(context.getPropStore().exists(NamespacePropKey.of(context, nid)));
      assertFalse(context.getPropStore().exists(TablePropKey.of(context, tid)));
      // check ServerConfigurationFactory deleted - should return null
      assertNull(context.getTableConfiguration(tid));
    }
  }

  /**
   * Validate that property nodes have an ACL set to restrict world access.
   */
  @Test
  public void permissionsTest() throws Exception {
    var names = getUniqueNames(3);
    String namespace = names[0];
    String table1 = namespace + "." + names[1];
    String table2 = names[2];

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {

      client.namespaceOperations().create(namespace);
      client.tableOperations().create(table1);
      client.tableOperations().create(table2);

      client.instanceOperations().setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
      client.tableOperations().setProperty(table1, Property.TABLE_BLOOM_ENABLED.getKey(), "true");

      Thread.sleep(SECONDS.toMillis(3L));

      ServerContext serverContext = cluster.getServerContext();
      ZooReaderWriter zrw = serverContext.getZooReaderWriter();

      // validate that a world-readable node has expected perms to validate test method
      var noAcl = zrw.getACL(ZooUtil.getRoot(serverContext.getInstanceID()));
      assertTrue(noAcl.size() > 1);
      assertTrue(
          noAcl.get(0).toString().contains("world") || noAcl.get(1).toString().contains("world"));

      var sysAcl = zrw.getACL(SystemPropKey.of(serverContext).getNodePath());
      assertEquals(1, sysAcl.size());
      assertFalse(sysAcl.get(0).toString().contains("world"));

      for (Map.Entry<String,String> nsEntry : client.namespaceOperations().namespaceIdMap()
          .entrySet()) {
        log.debug("Check acl on namespace name: {}, id: {}", nsEntry.getKey(), nsEntry.getValue());
        var namespaceAcl = zrw.getACL(
            NamespacePropKey.of(serverContext, NamespaceId.of(nsEntry.getValue())).getNodePath());
        assertEquals(1, namespaceAcl.size());
        assertFalse(namespaceAcl.get(0).toString().contains("world"));
      }

      for (Map.Entry<String,String> tEntry : client.tableOperations().tableIdMap().entrySet()) {
        log.debug("Check acl on table name: {}, id: {}", tEntry.getKey(), tEntry.getValue());
        var tableAcl =
            zrw.getACL(TablePropKey.of(serverContext, TableId.of(tEntry.getValue())).getNodePath());
        assertEquals(1, tableAcl.size());
        assertFalse(tableAcl.get(0).toString().contains("world"));
      }
    }
  }

  @Test
  public void modifyInstancePropertiesTest() throws Exception {

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      // Grab original default config
      Map<String,String> config = client.instanceOperations().getSystemConfiguration();
      TVersionedProperties properties = client.instanceOperations().getSystemProperties();

      // should be empty to start
      assertEquals(0, properties.getProperties().size());

      final String originalClientPort = config.get(Property.TSERV_CLIENTPORT.getKey());
      final String originalMaxMem = config.get(Property.TSERV_MAXMEM.getKey());

      // Set properties in ZK
      client.instanceOperations().modifyProperties(original -> {
        original.put(Property.TSERV_CLIENTPORT.getKey(), "9998");
        original.put(Property.TSERV_MAXMEM.getKey(), "35%");
      });

      // Verify system properties added
      assertTrue(Wait.waitFor(
          () -> client.instanceOperations().getSystemProperties().getProperties().size() > 0, 5000,
          500));

      // verify properties updated
      properties = client.instanceOperations().getSystemProperties();
      assertEquals("9998", properties.getProperties().get(Property.TSERV_CLIENTPORT.getKey()));
      assertEquals("35%", properties.getProperties().get(Property.TSERV_MAXMEM.getKey()));

      // verify properties updated in config as well
      config = client.instanceOperations().getSystemConfiguration();
      assertEquals("9998", config.get(Property.TSERV_CLIENTPORT.getKey()));
      assertEquals("35%", config.get(Property.TSERV_MAXMEM.getKey()));

      // Verify removal by sending empty map - only removes props that were set in previous values
      // should be restored
      client.instanceOperations().modifyProperties(Map::clear);

      assertTrue(Wait.waitFor(
          () -> client.instanceOperations().getSystemProperties().getProperties().size() == 0, 5000,
          500));

      // verify default system config restored
      config = client.instanceOperations().getSystemConfiguration();
      assertEquals(originalClientPort, config.get(Property.TSERV_CLIENTPORT.getKey()));
      assertEquals(originalMaxMem, config.get(Property.TSERV_MAXMEM.getKey()));
    }
  }

  @Test
  public void modifyTablePropTest() throws Exception {
    String table = getUniqueNames(1)[0];

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);
      log.info("Tables: {}", client.tableOperations().list());

      testModifyProperties(() -> {
        try {
          // Method to grab full config
          return client.tableOperations().getConfiguration(table);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }, () -> {
        try {
          // Method to grab only properties
          return client.tableOperations().getTableProperties(table);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }, mapMutator -> {
        try {
          // Modify props on table
          client.tableOperations().modifyProperties(table, mapMutator);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      });
    }
  }

  @Test
  public void modifyNamespacePropTest() throws Exception {
    String namespace = "modifyNamespacePropTest";
    String table = namespace + "testtable";
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.namespaceOperations().create(namespace);
      client.tableOperations().create(table);

      log.info("Tables: {}", client.tableOperations().list());

      testModifyProperties(() -> {
        try {
          // Method to grab full config
          return client.namespaceOperations().getConfiguration(namespace);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }, () -> {
        try {
          // Method to grab only properties
          return client.namespaceOperations().getNamespaceProperties(namespace);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }, mapMutator -> {
        try {
          // Modify props on namespace
          client.namespaceOperations().modifyProperties(namespace, mapMutator);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      });
    }
  }

  private void testModifyProperties(Supplier<Map<String,String>> fullConfig,
      Supplier<Map<String,String>> props, Consumer<Consumer<Map<String,String>>> modifyProperties)
      throws Exception {
    // Grab original default config
    Map<String,String> config = fullConfig.get();
    final String originalBloomEnabled = config.get(Property.TABLE_BLOOM_ENABLED.getKey());
    final String originalBloomSize = config.get(Property.TABLE_BLOOM_SIZE.getKey());

    var properties = props.get();
    final int propsSize = properties.size();

    // Modify table properties in ZK
    modifyProperties.accept(original -> {
      original.put(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
      original.put(Property.TABLE_BLOOM_SIZE.getKey(), "1000");
    });

    assertTrue(Wait.waitFor(() -> props.get().size() > propsSize, 5000, 500));

    // verify properties updated
    properties = props.get();
    assertEquals("true", properties.get(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertEquals("1000", properties.get(Property.TABLE_BLOOM_SIZE.getKey()));

    // verify properties updated in full configuration also
    config = fullConfig.get();
    assertEquals("true", config.get(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertEquals("1000", config.get(Property.TABLE_BLOOM_SIZE.getKey()));

    // Verify removal
    modifyProperties.accept(original -> {
      original.remove(Property.TABLE_BLOOM_ENABLED.getKey());
      original.remove(Property.TABLE_BLOOM_SIZE.getKey());
    });

    // Wait for clear
    assertTrue(Wait.waitFor(() -> props.get().size() == propsSize, 5000, 500));

    // verify default system config restored
    config = fullConfig.get();
    assertEquals(originalBloomEnabled, config.get(Property.TABLE_BLOOM_ENABLED.getKey()));
    assertEquals(originalBloomSize, config.get(Property.TABLE_BLOOM_SIZE.getKey()));
  }
}

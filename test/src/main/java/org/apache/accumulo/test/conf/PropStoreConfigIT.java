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
import static org.apache.accumulo.core.conf.ConfigurationTypeHelper.getMemoryAsBytes;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
@Tag(SUNNY_DAY)
public class PropStoreConfigIT extends SharedMiniClusterBase {

  private static final Logger log = LoggerFactory.getLogger(PropStoreConfigIT.class);

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

  @BeforeEach
  public void clear() throws Exception {
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.instanceOperations().modifyProperties(Map::clear);
      assertTrue(Wait.waitFor(() -> getStoredConfiguration().size() == 0, 5000, 500));
    }
  }

  @Test
  public void setTablePropTest() throws Exception {
    String table = getUniqueNames(1)[0];

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      log.debug("Tables: {}", client.tableOperations().list());

      // override default in sys, and then over-ride that for table prop
      client.instanceOperations().setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
      client.tableOperations().setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey(), "false");

      assertTrue(Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500));
      assertTrue(Wait.waitFor(() -> client.tableOperations().getConfiguration(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500));

      // revert sys, and then over-ride to true with table prop
      client.instanceOperations().removeProperty(Property.TABLE_BLOOM_ENABLED.getKey());
      client.tableOperations().setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey(), "true");

      assertTrue(Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500));
      assertTrue(Wait.waitFor(() -> client.tableOperations().getConfiguration(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500));

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

      client.instanceOperations().setProperty(Property.TABLE_BLOOM_SIZE.getKey(), "12345");
      assertTrue(Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_SIZE.getKey()).equals("12345"), 5000, 500));
      assertEquals("12345",
          client.tableOperations().getConfiguration(table).get(Property.TABLE_BLOOM_SIZE.getKey()));

      client.namespaceOperations().setProperty(namespace, Property.TABLE_BLOOM_SIZE.getKey(),
          "23456");
      assertTrue(Wait.waitFor(() -> client.namespaceOperations().getConfiguration(namespace)
          .get(Property.TABLE_BLOOM_SIZE.getKey()).equals("23456"), 5000, 500));
      assertEquals("23456",
          client.tableOperations().getConfiguration(table).get(Property.TABLE_BLOOM_SIZE.getKey()));

      client.tableOperations().setProperty(table, Property.TABLE_BLOOM_SIZE.getKey(), "34567");
      assertTrue(Wait.waitFor(() -> client.tableOperations().getConfiguration(table)
          .get(Property.TABLE_BLOOM_SIZE.getKey()).equals("34567"), 5000, 500));
      assertEquals("12345", client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_SIZE.getKey()));
      assertEquals("23456", client.namespaceOperations().getConfiguration(namespace)
          .get(Property.TABLE_BLOOM_SIZE.getKey()));

      var tableIdMap = client.tableOperations().tableIdMap();
      var nsIdMap = client.namespaceOperations().namespaceIdMap();

      ServerContext context = getCluster().getServerContext();

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

      Thread.sleep(SECONDS.toMillis(3L));

      ServerContext serverContext = getCluster().getServerContext();
      ZooReaderWriter zrw = serverContext.getZooReaderWriter();

      // validate that a world-readable node has expected perms to validate test method
      var noAcl = zrw.getACL(ZooUtil.getRoot(serverContext.getInstanceID()));
      assertTrue(noAcl.size() > 1);
      assertTrue(
          noAcl.get(0).toString().contains("world") || noAcl.get(1).toString().contains("world"));

      var sysAcl = zrw.getACL(SystemPropKey.of(serverContext).getPath());
      assertEquals(1, sysAcl.size());
      assertFalse(sysAcl.get(0).toString().contains("world"));

      for (Map.Entry<String,String> nsEntry : client.namespaceOperations().namespaceIdMap()
          .entrySet()) {
        log.debug("Check acl on namespace name: {}, id: {}", nsEntry.getKey(), nsEntry.getValue());
        var namespaceAcl = zrw.getACL(
            NamespacePropKey.of(serverContext, NamespaceId.of(nsEntry.getValue())).getPath());
        log.debug("namespace permissions: {}", namespaceAcl);
        assertEquals(1, namespaceAcl.size());
        assertFalse(namespaceAcl.get(0).toString().contains("world"));
      }

      for (Map.Entry<String,String> tEntry : client.tableOperations().tableIdMap().entrySet()) {
        log.debug("Check acl on table name: {}, id: {}", tEntry.getKey(), tEntry.getValue());
        var tableAcl =
            zrw.getACL(TablePropKey.of(serverContext, TableId.of(tEntry.getValue())).getPath());
        log.debug("Received ACLs of: {}", tableAcl);
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
      Map<String,String> properties = getStoredConfiguration();

      final String origMaxOpenFiles = config.get(Property.TSERV_SCAN_MAX_OPENFILES.getKey());
      final String origMaxMem = config.get(Property.TSERV_MAXMEM.getKey());
      // final long origMaxMem = getMemoryAsBytes(config.get(Property.TSERV_MAXMEM.getKey()));

      client.instanceOperations().modifyProperties(Map::clear);
      assertTrue(Wait.waitFor(() -> getStoredConfiguration().size() == 0, 5000, 500));

      // should be empty to start
      final int numProps = properties.size();

      final String expectedMaxOpenFiles = "" + (Integer.parseInt(origMaxOpenFiles) + 1);
      final String expectMaxMem = (getMemoryAsBytes(origMaxMem) + 1024) + "M";

      // Set properties in ZK
      client.instanceOperations().modifyProperties(original -> {
        original.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), expectedMaxOpenFiles);
        original.put(Property.TSERV_MAXMEM.getKey(), expectMaxMem);
      });

      // Verify system properties added
      assertTrue(Wait.waitFor(() -> getStoredConfiguration().size() > numProps, 5000, 500));

      // verify properties updated
      properties = getStoredConfiguration();
      assertEquals(expectedMaxOpenFiles,
          properties.get(Property.TSERV_SCAN_MAX_OPENFILES.getKey()));
      assertEquals(expectMaxMem, properties.get(Property.TSERV_MAXMEM.getKey()));

      // verify properties updated in config as well
      config = client.instanceOperations().getSystemConfiguration();
      assertEquals(expectedMaxOpenFiles, config.get(Property.TSERV_SCAN_MAX_OPENFILES.getKey()));
      assertEquals(expectMaxMem, config.get(Property.TSERV_MAXMEM.getKey()));

      // Verify removal by sending empty map - only removes props that were set in previous values
      // should be restored
      client.instanceOperations().modifyProperties(Map::clear);

      assertTrue(Wait.waitFor(() -> getStoredConfiguration().size() == 0, 5000, 500));

      // verify default system config restored
      config = client.instanceOperations().getSystemConfiguration();
      assertEquals(origMaxOpenFiles, config.get(Property.TSERV_SCAN_MAX_OPENFILES.getKey()));
      assertEquals(origMaxMem, config.get(Property.TSERV_MAXMEM.getKey()));
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

  private Map<String,String> getStoredConfiguration() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    return ThriftClientTypes.CLIENT
        .execute(ctx,
            client -> client.getVersionedSystemProperties(TraceUtil.traceInfo(), ctx.rpcCreds()))
        .getProperties();
  }

  interface PropertyShim {
    Map<String,String> modifyProperties(Consumer<Map<String,String>> modifier) throws Exception;

    Map<String,String> getProperties() throws Exception;
  }

  @Test
  public void concurrentTablePropsModificationTest() throws Exception {
    String table = getUniqueNames(1)[0];
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(table);

      var propShim = new PropertyShim() {

        @Override
        public Map<String,String> modifyProperties(Consumer<Map<String,String>> modifier)
            throws Exception {
          return client.tableOperations().modifyProperties(table, modifier);
        }

        @Override
        public Map<String,String> getProperties() throws Exception {
          return client.tableOperations().getTableProperties(table);
        }
      };

      runConcurrentPropsModificationTest(propShim);
    }
  }

  @Test
  public void concurrentNamespacePropsModificationTest() throws Exception {
    String namespace = getUniqueNames(1)[0];
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      client.namespaceOperations().create(namespace);

      var propShim = new PropertyShim() {

        @Override
        public Map<String,String> modifyProperties(Consumer<Map<String,String>> modifier)
            throws Exception {
          return client.namespaceOperations().modifyProperties(namespace, modifier);
        }

        @Override
        public Map<String,String> getProperties() throws Exception {
          return client.namespaceOperations().getNamespaceProperties(namespace);
        }
      };

      runConcurrentPropsModificationTest(propShim);
    }
  }

  @Test
  public void concurrentInstancePropsModificationTest() throws Exception {
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {
      var propShim = new PropertyShim() {

        @Override
        public Map<String,String> modifyProperties(Consumer<Map<String,String>> modifier)
            throws Exception {
          return client.instanceOperations().modifyProperties(modifier);
        }

        @Override
        public Map<String,String> getProperties() throws Exception {
          return client.instanceOperations().getSystemConfiguration();
        }
      };

      runConcurrentPropsModificationTest(propShim);
    }
  }

  /*
   * Test concurrently modifying properties in many threads with each thread making many
   * modifications. The modifications build on each other and the test is written in such a way that
   * if any single modification is lost it can be detected.
   */
  private static void runConcurrentPropsModificationTest(PropertyShim propShim) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(4);

    final int iterations = 151;

    Callable<Void> task1 = () -> {
      for (int i = 0; i < iterations; i++) {

        Map<String,String> prevProps = null;
        if (i % 10 == 0) {
          prevProps = propShim.getProperties();
        }

        Map<String,String> acceptedProps = propShim.modifyProperties(tableProps -> {
          int A = Integer.parseInt(tableProps.getOrDefault("table.custom.A", "0"));
          int B = Integer.parseInt(tableProps.getOrDefault("table.custom.B", "0"));
          int C = Integer.parseInt(tableProps.getOrDefault("table.custom.C", "0"));
          int D = Integer.parseInt(tableProps.getOrDefault("table.custom.D", "0"));

          tableProps.put("table.custom.A", A + 2 + "");
          tableProps.put("table.custom.B", B + 3 + "");
          tableProps.put("table.custom.C", C + 5 + "");
          tableProps.put("table.custom.D", D + 7 + "");
        });

        if (prevProps != null) {
          var beforeA = Integer.parseInt(prevProps.getOrDefault("table.custom.A", "0"));
          var beforeB = Integer.parseInt(prevProps.getOrDefault("table.custom.B", "0"));
          var beforeC = Integer.parseInt(prevProps.getOrDefault("table.custom.C", "0"));
          var beforeD = Integer.parseInt(prevProps.getOrDefault("table.custom.D", "0"));

          var afterA = Integer.parseInt(acceptedProps.get("table.custom.A"));
          var afterB = Integer.parseInt(acceptedProps.get("table.custom.B"));
          var afterC = Integer.parseInt(acceptedProps.get("table.custom.C"));
          var afterD = Integer.parseInt(acceptedProps.get("table.custom.D"));

          // because there are other thread possibly making changes since reading prevProps, can
          // only do >= as opposed to == check. Should at a minimum see the changes made by this
          // thread.
          assertTrue(afterA >= beforeA + 2);
          assertTrue(afterB >= beforeB + 3);
          assertTrue(afterC >= beforeC + 5);
          assertTrue(afterD >= beforeD + 7);
        }
      }
      return null;
    };

    Callable<Void> task2 = () -> {
      for (int i = 0; i < iterations; i++) {
        propShim.modifyProperties(tableProps -> {
          int B = Integer.parseInt(tableProps.getOrDefault("table.custom.B", "0"));
          int C = Integer.parseInt(tableProps.getOrDefault("table.custom.C", "0"));

          tableProps.put("table.custom.B", B + 11 + "");
          tableProps.put("table.custom.C", C + 13 + "");
        });
      }
      return null;
    };

    Callable<Void> task3 = () -> {
      for (int i = 0; i < iterations; i++) {
        propShim.modifyProperties(tableProps -> {
          int B = Integer.parseInt(tableProps.getOrDefault("table.custom.B", "0"));

          tableProps.put("table.custom.B", B + 17 + "");
        });
      }
      return null;
    };

    Callable<Void> task4 = () -> {
      for (int i = 0; i < iterations; i++) {
        propShim.modifyProperties(tableProps -> {
          int E = Integer.parseInt(tableProps.getOrDefault("table.custom.E", "0"));
          tableProps.put("table.custom.E", E + 19 + "");
        });
      }
      return null;
    };

    // run all of the above task concurrently
    for (Future<Void> future : executor.invokeAll(List.of(task1, task2, task3, task4))) {
      // see if there were any exceptions in the background thread and wait for it to finish
      future.get();
    }

    Map<String,String> expected = new HashMap<>();

    // determine the expected sum for all the additions done by the separate threads for each
    // property
    expected.put("table.custom.A", iterations * 2 + "");
    expected.put("table.custom.B", iterations * (3 + 11 + 17) + "");
    expected.put("table.custom.C", iterations * (5 + 13) + "");
    expected.put("table.custom.D", iterations * 7 + "");
    expected.put("table.custom.E", iterations * 19 + "");

    assertTrue(Wait.waitFor(() -> {
      var tableProps = new HashMap<>(propShim.getProperties());
      tableProps.keySet().removeIf(key -> !key.matches("table[.]custom[.][ABCDEF]"));
      boolean equal = expected.equals(tableProps);
      if (!equal) {
        log.info(
            "Waiting for properties to converge. Actual:" + tableProps + " Expected:" + expected);
      }
      return equal;
    }));

    // now that there are not other thread modifying properties, make a modification to check that
    // the returned map
    // is exactly as expected.
    Map<String,String> acceptedProps = propShim.modifyProperties(tableProps -> {
      int A = Integer.parseInt(tableProps.getOrDefault("table.custom.A", "0"));
      int B = Integer.parseInt(tableProps.getOrDefault("table.custom.B", "0"));
      int C = Integer.parseInt(tableProps.getOrDefault("table.custom.C", "0"));
      int D = Integer.parseInt(tableProps.getOrDefault("table.custom.D", "0"));

      tableProps.put("table.custom.A", A + 2 + "");
      tableProps.put("table.custom.B", B + 3 + "");
      tableProps.put("table.custom.C", C + 5 + "");
      tableProps.put("table.custom.D", D + 7 + "");
    });

    var afterA = Integer.parseInt(acceptedProps.get("table.custom.A"));
    var afterB = Integer.parseInt(acceptedProps.get("table.custom.B"));
    var afterC = Integer.parseInt(acceptedProps.get("table.custom.C"));
    var afterD = Integer.parseInt(acceptedProps.get("table.custom.D"));
    var afterE = Integer.parseInt(acceptedProps.get("table.custom.E"));

    assertEquals(iterations * 2 + 2, afterA);
    assertEquals(iterations * (3 + 11 + 17) + 3, afterB);
    assertEquals(iterations * (5 + 13) + 5, afterC);
    assertEquals(iterations * 7 + 7, afterD);
    assertEquals(iterations * 19, afterE);

    executor.shutdown();
  }
}

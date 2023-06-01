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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.conf.util.ZooPropSetTool;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
@Tag(SUNNY_DAY)
public class ZooPropSetToolIT extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(ZooPropSetToolIT.class);

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

    try (var client = Accumulo.newClient().from(getClientProps()).build()) {

      client.namespaceOperations().create(namespace);
      client.tableOperations().create(table);

      LOG.debug("Tables: {}", client.tableOperations().list());

      // override default in sys, and then over-ride that for table prop
      client.instanceOperations().setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
      client.namespaceOperations().setProperty(namespace, Property.TABLE_BLOOM_ENABLED.getKey(),
          "true");
      client.tableOperations().setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey(), "false");

      assertTrue(Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500));

      ZooPropSetTool tool = new ZooPropSetTool();
      InstanceId iid = getCluster().getServerContext().getInstanceID();

      // before - check setup correct
      assertTrue(Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500));

      // set table property (table.bloom.enabled=true)
      String[] setTablePropArgs = {"-p", getCluster().getAccumuloPropertiesPath(), "--instanceId",
          iid.toString(), "-t", table, "-s", Property.TABLE_BLOOM_ENABLED.getKey() + "=true"};
      tool.execute(setTablePropArgs);

      // after set - check prop changed in ZooKeeper
      assertTrue(Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500));

      String[] deleteTablePropArgs = {"-p", getCluster().getAccumuloPropertiesPath(),
          "--instanceId", iid.toString(), "-t", table, "-d", Property.TABLE_BLOOM_ENABLED.getKey()};
      tool.execute(deleteTablePropArgs);

      // after delete - check map entry is null (removed from ZooKeeper)
      assertTrue(Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()) == null, 5000, 500));

      // set system property (changed from setup)
      assertTrue(Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500));

      String[] setSystemPropArgs = {"-p", getCluster().getAccumuloPropertiesPath(), "--instanceId",
          iid.toString(), "-s", Property.TABLE_BLOOM_ENABLED.getKey() + "=false"};
      tool.execute(setSystemPropArgs);

      // after set - check map entry is false
      assertTrue(Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500));

      // set namespace property (changed from setup)
      assertTrue(Wait.waitFor(() -> client.namespaceOperations().getNamespaceProperties(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500));

      String[] setNamespacePropArgs =
          {"-p", getCluster().getAccumuloPropertiesPath(), "--instanceId", iid.toString(), "-ns",
              namespace, "-s", Property.TABLE_BLOOM_ENABLED.getKey() + "=false"};
      tool.execute(setNamespacePropArgs);

      // after set - check map entry is false
      assertTrue(Wait.waitFor(() -> client.namespaceOperations().getNamespaceProperties(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500));

      String[] deleteNamespacePropArgs =
          {"-p", getCluster().getAccumuloPropertiesPath(), "--instanceId", iid.toString(), "-ns",
              namespace, "-d", Property.TABLE_BLOOM_ENABLED.getKey()};
      tool.execute(deleteNamespacePropArgs);

      // after set - check map entry is false
      assertTrue(Wait.waitFor(() -> client.namespaceOperations().getNamespaceProperties(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()) == null, 5000, 500));

    }
  }

  @Test
  public void filterPropTest() throws Exception {
    String table = getUniqueNames(1)[0];

    var origStream = System.out;
    try (var client = Accumulo.newClient().from(getClientProps()).build()) {

      client.tableOperations().create(table);

      LOG.debug("Tables: {}", client.tableOperations().list());

      // override default in sys, and then over-ride that for table prop
      client.instanceOperations().setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true");
      client.tableOperations().setProperty(table, Property.TABLE_BLOOM_ENABLED.getKey(), "false");

      assertTrue(Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500));

      ZooPropSetTool tool = new ZooPropSetTool();
      InstanceId iid = getCluster().getServerContext().getInstanceID();

      // before - check setup correct
      assertTrue(Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500));

      var baos = new ByteArrayOutputStream();
      PrintStream testStream = new PrintStream(baos);
      // Redirecting console output to file
      System.setOut(testStream);

      // filter on property name
      String[] args = {"-p", getCluster().getAccumuloPropertiesPath(), "--instanceId",
          iid.toString(), "-t", table, "-f", "bloom"};
      tool.execute(args);
      testStream.flush();

      LOG.debug("Stream: {}", baos.toString(UTF_8));
      assertTrue(baos.toString(UTF_8).contains(Property.TABLE_BLOOM_ENABLED.getKey()));

      baos.reset();
      testStream = new PrintStream(baos);
      // Redirecting console output to file
      System.setOut(testStream);

      // filter based on value
      String[] args2 = {"-p", getCluster().getAccumuloPropertiesPath(), "--instanceId",
          iid.toString(), "-t", table, "-fv", "false"};
      tool.execute(args2);

      testStream.flush();

      LOG.debug("Stream2: {}", baos.toString(UTF_8));
      assertTrue(baos.toString(UTF_8).contains(Property.TABLE_BLOOM_ENABLED.getKey()));

      baos.reset();
      testStream = new PrintStream(baos);
      // Redirecting console output to file
      System.setOut(testStream);

      // filter on value - not found
      String[] args3 = {"-p", getCluster().getAccumuloPropertiesPath(), "--instanceId",
          iid.toString(), "-t", table, "-fv", "foo"};
      tool.execute(args3);

      testStream.flush();

      LOG.debug("Stream3: {}", baos.toString(UTF_8));
      assertTrue(baos.toString(UTF_8).contains("none"));
      assertFalse(baos.toString(UTF_8).contains(Property.TABLE_BLOOM_ENABLED.getKey()));

      System.setOut(origStream);
    }
  }
}

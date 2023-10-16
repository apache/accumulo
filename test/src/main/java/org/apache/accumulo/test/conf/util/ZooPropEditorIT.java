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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.conf.Property;
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
public class ZooPropEditorIT extends SharedMiniClusterBase {

  private static final Logger LOG = LoggerFactory.getLogger(ZooPropEditorIT.class);

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

      Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500);

      ZooPropEditor tool = new ZooPropEditor();
      // before - check setup correct
      Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500);

      // set table property (table.bloom.enabled=true)
      String[] setTablePropArgs = {"-p", getCluster().getAccumuloPropertiesPath(), "-t", table,
          "-s", Property.TABLE_BLOOM_ENABLED.getKey() + "=true"};
      tool.execute(setTablePropArgs);

      // after set - check prop changed in ZooKeeper
      Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500);

      String[] deleteTablePropArgs = {"-p", getCluster().getAccumuloPropertiesPath(), "-t", table,
          "-d", Property.TABLE_BLOOM_ENABLED.getKey()};
      tool.execute(deleteTablePropArgs);

      // after delete - check map entry is null (removed from ZooKeeper)
      Wait.waitFor(() -> client.tableOperations().getTableProperties(table)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()) == null, 5000, 500);

      // set system property (changed from setup)
      Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500);

      String[] setSystemPropArgs = {"-p", getCluster().getAccumuloPropertiesPath(), "-s",
          Property.TABLE_BLOOM_ENABLED.getKey() + "=false"};
      tool.execute(setSystemPropArgs);

      // after set - check map entry is false
      Wait.waitFor(() -> client.instanceOperations().getSystemConfiguration()
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500);

      // set namespace property (changed from setup)
      Wait.waitFor(() -> client.namespaceOperations().getNamespaceProperties(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true"), 5000, 500);

      String[] setNamespacePropArgs = {"-p", getCluster().getAccumuloPropertiesPath(), "-ns",
          namespace, "-s", Property.TABLE_BLOOM_ENABLED.getKey() + "=false"};
      tool.execute(setNamespacePropArgs);

      // after set - check map entry is false
      Wait.waitFor(() -> client.namespaceOperations().getNamespaceProperties(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false"), 5000, 500);

      String[] deleteNamespacePropArgs = {"-p", getCluster().getAccumuloPropertiesPath(), "-ns",
          namespace, "-d", Property.TABLE_BLOOM_ENABLED.getKey()};
      tool.execute(deleteNamespacePropArgs);

      // after set - check map entry is false
      Wait.waitFor(() -> client.namespaceOperations().getNamespaceProperties(namespace)
          .get(Property.TABLE_BLOOM_ENABLED.getKey()) == null, 5000, 500);

    }
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;

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
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperPropertiesIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperPropertiesIT.class);

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
      AccumuloSecurityException, TableNotFoundException, InterruptedException {
    ServerContext context = getServerContext();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      log.info(">>>> ");
      log.info(">>>> TABLE TESTING...");
      log.info(">>>> ");

      String TABLENAME = "propUtilTable";

      client.tableOperations().create(TABLENAME);
      client.tableOperations().tableIdMap().forEach((k, v) -> log.info(">>>> {} -> {}", k, v));
      Map<String,String> idMap = client.tableOperations().tableIdMap();
      String TID = idMap.get(TABLENAME);

      log.info(">>>> Retrieve and verify initial property value...");
      Iterable<Map.Entry<String,String>> prop1 = null;
      Map<String,String> properties = client.tableOperations().getConfiguration(TABLENAME);
      assertEquals("false", properties.get(Property.TABLE_BLOOM_ENABLED.getKey()));

      log.info(">>>> update and verify updated property...");
      context.tablePropUtil().setProperties(TableId.of(TID),
          Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

      properties = client.tableOperations().getConfiguration(TABLENAME);
      while (!properties.get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("true")) {
        Thread.sleep(250);
        log.info(">>>> 250");
        properties = client.tableOperations().getConfiguration(TABLENAME);
      }
      assertEquals("true", properties.get(Property.TABLE_BLOOM_ENABLED.getKey()));

      log.info(">>>> remove property and re-check");
      context.tablePropUtil().removeProperties(TableId.of(TID),
          List.of(Property.TABLE_BLOOM_ENABLED.getKey()));

      properties = client.tableOperations().getConfiguration(TABLENAME);
      while (!properties.get(Property.TABLE_BLOOM_ENABLED.getKey()).equals("false")) {
        Thread.sleep(250);
        log.info(">>>> 250");
        properties = client.tableOperations().getConfiguration(TABLENAME);
      }
      assertEquals("false", properties.get(Property.TABLE_BLOOM_ENABLED.getKey()));

      // Add invalid property
      assertThrows(IllegalArgumentException.class, () -> {
        context.tablePropUtil().setProperties(TableId.of(TID),
            Map.of("NOT_A_PROPERTY", "not_a_value"));
      }, "Expected IllegalArgumentException to be thrown.");
    } // try-w/resources end
  }

  @Test
  @Timeout(30)
  public void testNamespacePropUtils() throws AccumuloException, AccumuloSecurityException,
      NamespaceExistsException, NamespaceNotFoundException, InterruptedException {
    ServerContext context = getServerContext();

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      log.info(">>>> ");
      log.info(">>>> NAMESPACE TESTING...");
      log.info(">>>> ");

      String NAMESPACE = "testNamespace";
      client.namespaceOperations().create(NAMESPACE);

      SortedSet<String> list = client.namespaceOperations().list();
      list.forEach(p -> log.info(">>>> list: {}", p));

      client.namespaceOperations().namespaceIdMap()
          .forEach((k, v) -> log.info(">>>> {} -> {}", k, v));
      Map<String,String> nsMap = client.namespaceOperations().namespaceIdMap();
      nsMap.forEach((k, v) -> log.info(">>>> {}:{}", k, v));
      String NID = nsMap.get(NAMESPACE);
      log.info(">>>> NID: {}", NID);

      log.info(">>>> Retrieve and verify initial property value...");
      Map<String,String> properties = client.namespaceOperations().getConfiguration(NAMESPACE);
      assertEquals("15", properties.get(Property.TABLE_FILE_MAX.getKey()));

      log.info(">>>> update and verify updated property...");
      context.namespacePropUtil().setProperties(NamespaceId.of(NID),
          Map.of(Property.TABLE_FILE_MAX.getKey(), "31"));

      properties = client.namespaceOperations().getConfiguration(NAMESPACE);
      while (!properties.get(Property.TABLE_FILE_MAX.getKey()).equals("31")) {
        Thread.sleep(250);
        log.info(">>>> 250");
        properties = client.namespaceOperations().getConfiguration(NAMESPACE);
      }
      assertEquals("31", properties.get(Property.TABLE_FILE_MAX.getKey()));

      log.info(">>>> remove property and re-check");
      context.namespacePropUtil().removeProperties(NamespaceId.of(NID),
          List.of(Property.TABLE_FILE_MAX.getKey()));

      properties = client.namespaceOperations().getConfiguration(NAMESPACE);
      while (!properties.get(Property.TABLE_FILE_MAX.getKey()).equals("15")) {
        Thread.sleep(250);
        log.info(">>>> 250");
        properties = client.namespaceOperations().getConfiguration(NAMESPACE);
      }
      assertEquals("15", properties.get(Property.TABLE_FILE_MAX.getKey()));

      // Add invalid property
      assertThrows(IllegalArgumentException.class, () -> {
        context.namespacePropUtil().setProperties(NamespaceId.of(NID),
            Map.of("NOT_A_PROPERTY", "not_a_value"));
      }, "Expected IllegalArgumentException to be thrown.");
    }
  }

}

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
  public void testTablePropUtils()
      throws AccumuloException, TableExistsException, AccumuloSecurityException,
      TableNotFoundException {
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

      log.info(">>>> Retrieve existing property value...");
      Iterable<Map.Entry<String,String>> prop1 = client.tableOperations().getProperties(TABLENAME);
      prop1.forEach(k -> {
        if (k.getKey().equals(Property.TABLE_BLOOM_ENABLED.getKey())) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          assertEquals("false", k.getValue());
        }
      });

      log.info(">>>> Set property to true...");
      context.tablePropUtil().setProperties(TableId.of(TID),
          Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

      log.info(">>>> verify property set...");
      Iterable<Map.Entry<String,String>> prop2 = client.tableOperations().getProperties(TABLENAME);
      prop2.forEach(k -> {
        if (k.getKey().equals(Property.TABLE_BLOOM_ENABLED.getKey())) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          assertEquals("true", k.getValue());
        }
      });

      log.info(">>>> remove property...");
      context.tablePropUtil().removeProperties(TableId.of(TID),
          List.of(Property.TABLE_BLOOM_ENABLED.getKey()));

      Iterable<Map.Entry<String,String>> prop3 = client.tableOperations().getProperties(TABLENAME);
      prop3.forEach(k -> {
        if (k.getKey().equals(Property.TABLE_BLOOM_ENABLED.getKey())) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          assertEquals("false", k.getValue());
        }
      });

      // Add invalid property
      assertThrows(IllegalArgumentException.class, () -> {
        context.tablePropUtil().setProperties(TableId.of(TID),
            Map.of("NOT_A_PROPERTY", "not_a_value"));
      }, "Expected IllegalArgumentException to be thrown.");
    } // try-w/resources end
  }

  @Test
  public void testNamespacePropUtils() throws AccumuloException, AccumuloSecurityException,
      NamespaceExistsException, NamespaceNotFoundException {
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

      Iterable<Map.Entry<String,String>> prop4 =
          client.namespaceOperations().getProperties(NAMESPACE);
      log.info(">>>> Retrieve existing property value...");
      // prop4.forEach(entry -> log.info(">>>> \t{} - {}", entry.getKey(), entry.getValue()));
      prop4.forEach(k -> {
        if (k.getKey().equals(Property.TABLE_FILE_MAX.getKey())) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          assertEquals("15", k.getValue());
        }
      });

      log.info(">>>> Set property to 31...");
      context.namespacePropUtil().setProperties(NamespaceId.of(NID),
          Map.of(Property.TABLE_FILE_MAX.getKey(), "31"));

      log.info(">>>> verify property set...");
      Iterable<Map.Entry<String,String>> prop5 =
          client.namespaceOperations().getProperties(NAMESPACE);
      prop5.forEach(k -> {
        if (k.getKey().equals(Property.TABLE_FILE_MAX.getKey())) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          assertEquals("31", k.getValue());
        }
      });

      log.info(">>>> remove property...");
      context.namespacePropUtil().removeProperties(NamespaceId.of(NID),
          List.of(Property.TABLE_FILE_MAX.getKey()));
      Iterable<Map.Entry<String,String>> prop6 =
          client.namespaceOperations().getProperties(NAMESPACE);
      prop6.forEach(k -> {
        if (k.getKey().equals(Property.TABLE_FILE_MAX.getKey())) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
        }
      });

      // Add invalid property
      assertThrows(IllegalArgumentException.class, () -> {
        context.namespacePropUtil().setProperties(NamespaceId.of(NID),
            Map.of("NOT_A_PROPERTY", "not_a_value"));
      }, "Expected IllegalArgumentException to be thrown.");
    }
  }

}

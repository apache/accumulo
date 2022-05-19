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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.server.ServerContext;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperPropertiesIT extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(ZooKeeperPropertiesIT.class);

  // @Test
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
      TableNotFoundException, NamespaceExistsException, NamespaceNotFoundException {
    ServerContext context = getServerContext();

    log.info(">>>> ");
    log.info(">>>> TABLE TESTING...");
    log.info(">>>> ");

    String TABLENAME = "propUtilTable";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(TABLENAME);
      client.tableOperations().tableIdMap().forEach((k, v) -> log.info(">>>> {} -> {}", k, v));
      Map<String,String> idMap = client.tableOperations().tableIdMap();
      String TID = idMap.get(TABLENAME);

      Iterable<Map.Entry<String,String>> prop1 = client.tableOperations().getProperties(TABLENAME);
      prop1.forEach(k -> {
        if (k.getKey().equals(Property.TABLE_BLOOM_ENABLED.getKey())) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          assertTrue(k.getValue().equals("false"));
        }
      });

      log.info(">>>> -----------------");
      context.tablePropUtil().setProperties(TableId.of(TID),
          Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

      Iterable<Map.Entry<String,String>> prop2 = client.tableOperations().getProperties(TABLENAME);
      prop2.forEach(k -> {
        if (k.getKey().equals(Property.TABLE_BLOOM_ENABLED.getKey())) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          assertTrue(k.getValue().equals("true"));
        }
      });

      context.tablePropUtil().removeProperties(TableId.of(TID),
          List.of(Property.TABLE_BLOOM_ENABLED.getKey()));
      Iterable<Map.Entry<String,String>> prop3 = client.tableOperations().getProperties(TABLENAME);
      prop3.forEach(k -> {
        if (k.getKey().equals(Property.TABLE_BLOOM_ENABLED.getKey())) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          assertTrue(k.getValue().equals("false"));
        }
      });

      // Add invalid property
      assertThrows(IllegalArgumentException.class, () -> {
        context.tablePropUtil().setProperties(TableId.of(TID),
            Map.of("NOT_A_PROPERTY", "not_a_value"));
      }, "Expected IllegalArgumentException to be thrown.");

      //////////////////////////////////////////////////////////
      // Namespace tests
      //////////////////////////////////////////////////////////

      log.info(">>>> ");
      log.info(">>>> NAMESPACE TESTING...");
      log.info(">>>> ");

      String NAMESPACE = "testNamespace";
      client.namespaceOperations().create(NAMESPACE);
      client.namespaceOperations().namespaceIdMap()
          .forEach((k, v) -> log.info(">>>> {} -> {}", k, v));
      Map<String,String> nsMap = client.namespaceOperations().namespaceIdMap();
      nsMap.forEach((k, v) -> log.info(">>>> {}:{}", k, v));
      String NID = nsMap.get(NAMESPACE);
      log.info(">>>> NID: {}", NID);

      Iterable<Map.Entry<String,String>> prop4 =
          client.namespaceOperations().getProperties(NAMESPACE);
      // prop4.forEach(entry -> log.info(">>>> \t{} - {}", entry.getKey(), entry.getValue()));
      // prop4.forEach(k -> {
      // if (k.getKey().equals(Property.TABLE_FILE_MAX.getKey())) {
      // log.info(">>>> {} --- {}", k.getKey(), k.getValue());
      // assertTrue(k.getValue().equals("15"));
      // }
      // });

      log.info(">>>> -----------------");
      String myProp = Property.TABLE_ARBITRARY_PROP_PREFIX + "special";
      log.info(">>>> myProp: {}", myProp);
      context.namespacePropUtil().setProperties(NamespaceId.of(NID), Map.of(myProp, "im_special"));

      Iterable<Map.Entry<String,String>> prop5 =
          client.namespaceOperations().getProperties(NAMESPACE);
      prop5.forEach(k -> {
        if (k.getKey().equals(myProp)) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          assertTrue(k.getValue().equals("im_special"));
        }
      });

      context.namespacePropUtil().removeProperties(NamespaceId.of(TID), List.of(myProp));
      Iterable<Map.Entry<String,String>> prop6 =
          client.namespaceOperations().getProperties(NAMESPACE);
      prop6.forEach(k -> {
        if (k.getKey().equals(myProp)) {
          log.info(">>>> {} --- {}", k.getKey(), k.getValue());
          log.info(">>>> Should not have found me!");
        }
      });

      // Add invalid property
      assertThrows(IllegalArgumentException.class, () -> {
        context.namespacePropUtil().setProperties(NamespaceId.of(NID),
            Map.of("NOT_A_PROPERTY", "not_a_value"));
      }, "Expected IllegalArgumentException to be thrown.");

    } // try-w/resources end

  }

}

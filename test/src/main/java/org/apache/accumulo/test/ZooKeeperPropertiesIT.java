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
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
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
  public void testTablePropUtils() throws AccumuloException, TableExistsException,
      AccumuloSecurityException, TableNotFoundException {
    ServerContext context = getServerContext();
    String TABLENAME = "propUtilTable";
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(TABLENAME);
      client.tableOperations().tableIdMap().forEach((k, v) -> log.info(">>>> {} -> {}", k, v));
      Map<String,String> idMap = client.tableOperations().tableIdMap();
      String TID = idMap.get(TABLENAME);
      log.info(">>>> TID: {}", TID);

      Iterable<Map.Entry<String,String>> prop1 = client.tableOperations().getProperties(TABLENAME);
      log.info(">>>> Property: {}", Property.TABLE_BLOOM_ENABLED.getKey());
      prop1.forEach(k -> {
        if (k.getKey().startsWith("table")) {
          log.info(">>>> {} : {}", k.getKey(), k.getValue());
        }
        if (k.getKey() == Property.TABLE_BLOOM_ENABLED.getKey()) {
          log.info(">>>> {}:{}", k.getKey(), k.getValue());
          assertTrue(k.getValue() == "false");
        }
      });

      log.info(">>>> -----------------");
      context.tablePropUtil().setProperties(TableId.of(TID),
          Map.of(Property.TABLE_BLOOM_ENABLED.getKey(), "true"));

      Iterable<Map.Entry<String,String>> prop2 = client.tableOperations().getProperties(TABLENAME);
      prop2.forEach(k -> {
        if (k.getKey() == Property.TABLE_BLOOM_ENABLED.getKey()) {
          log.info(">>>> {}:{}", k.getKey(), k.getValue());
          assertTrue(k.getValue() == "true");
        }
      });

      context.tablePropUtil().removeProperties(TableId.of(TID),
          List.of(Property.TABLE_BLOOM_ENABLED.getKey()));
      Iterable<Map.Entry<String,String>> prop3 = client.tableOperations().getProperties(TABLENAME);
      log.info(">>>> does property exist now");
      prop3.forEach(k -> {
        if (k.getKey() == Property.TABLE_BLOOM_ENABLED.getKey()) {
          log.info(">>>> {}:{}", k.getKey(), k.getValue());
        }
      });
    }

  }

}

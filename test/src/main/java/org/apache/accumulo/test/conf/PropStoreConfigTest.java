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
package org.apache.accumulo.test.conf;

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.impl.ZooPropStore;
import org.apache.accumulo.server.conf.util.ConfigPropertyPrinter;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(MINI_CLUSTER_ONLY)
@Tag(SUNNY_DAY)
public class PropStoreConfigTest extends AccumuloClusterHarness {

  private static final Logger log = LoggerFactory.getLogger(PropStoreConfigTest.class);

  private AccumuloClient accumuloClient;

  @Before
  public void setup() {
    accumuloClient = Accumulo.newClient().from(getClientProps()).build();
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 180;
  }

  @Test
  public void initTest() throws Exception {

    ServerContext context = cluster.getServerContext();
    Thread.sleep(5_000);

    PropStore propStore = new ZooPropStore.Builder(context).build();

    ConfigPropertyPrinter propertyPrinter = new ConfigPropertyPrinter();

    log.info(" ****** Sys: {}", propStore.get(PropCacheKey.forSystem(context)));

    propertyPrinter.print(context, null, false);

    // TODO - need to add asserts
  }

  @Test
  public void setTablePropTest() throws Exception {
    String tableName = getUniqueNames(1)[0];
    accumuloClient.tableOperations().create(tableName);

    log.info("Tables: {}", accumuloClient.tableOperations().list());

    for (Map.Entry<String,String> e : accumuloClient.tableOperations().getProperties(tableName)) {
      if (e.getKey().contains("table.bloom.enabled")) {
        log.info("before bloom property: {}={}", e.getKey(), e.getValue());
      }
    }

    accumuloClient.instanceOperations().setProperty(Property.TABLE_BLOOM_ENABLED.getKey(), "true");

    accumuloClient.tableOperations().setProperty(tableName, Property.TABLE_BLOOM_ENABLED.getKey(),
        "true");

    try {
      Thread.sleep(10_000);
    } catch (InterruptedException ex) {
      // ignore
    }

    var props = accumuloClient.tableOperations().getProperties(tableName);
    log.info("Props: {}", props);
    for (Map.Entry<String,String> e : props) {
      if (e.getKey().contains("table.bloom.enabled")) {
        log.info("after bloom property: {}={}", e.getKey(), e.getValue());
        assertEquals("true", e.getValue());
      }
    }

  }
}

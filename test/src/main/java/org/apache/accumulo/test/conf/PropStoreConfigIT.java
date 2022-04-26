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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;
import static org.apache.accumulo.harness.AccumuloITBase.SUNNY_DAY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
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
}

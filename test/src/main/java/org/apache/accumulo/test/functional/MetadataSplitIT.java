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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class MetadataSplitIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setSiteConfig(Collections.singletonMap(Property.TSERV_MAJC_DELAY.getKey(), "100ms"));
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      assertEquals(1, c.tableOperations().listSplits(MetadataTable.NAME).size());
      c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(),
          "500");
      for (int i = 0; i < 10; i++) {
        c.tableOperations().create("table" + i);
        c.tableOperations().flush(MetadataTable.NAME, null, null, true);
      }
      sleepUninterruptibly(10, TimeUnit.SECONDS);
      assertTrue(c.tableOperations().listSplits(MetadataTable.NAME).size() > 2);
    }
  }
}

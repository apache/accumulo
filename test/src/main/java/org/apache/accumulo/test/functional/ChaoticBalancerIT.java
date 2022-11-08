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

import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.ChaoticLoadBalancer;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class ChaoticBalancerIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "10K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "50ms");
    // ChaoticLoadBalancer balances across all tables
    siteConfig.put(Property.TABLE_LOAD_BALANCER.getKey(), ChaoticLoadBalancer.class.getName());
    cfg.setSiteConfig(siteConfig);
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] names = getUniqueNames(1);
      String tableName = names[0];
      NewTableConfiguration ntc = new NewTableConfiguration();
      ntc.setProperties(Stream
          .of(new Pair<>(Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K"),
              new Pair<>(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K"))
          .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
      c.tableOperations().create(tableName, ntc);

      VerifyParams params = new VerifyParams(getClientProps(), tableName, 20_000);
      TestIngest.ingest(c, params);
      c.tableOperations().flush(tableName, null, null, true);
      VerifyIngest.verifyIngest(c, params);
    }
  }
}

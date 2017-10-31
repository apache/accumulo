/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.master.balancer.ChaoticLoadBalancer;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class ChaoticBalancerIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_MAXMEM.getKey(), "10K");
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "0");
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String[] names = getUniqueNames(1);
    String tableName = names[0];
    NewTableConfiguration ntc = new NewTableConfiguration();
    ntc.setProperties(Stream.of(new Pair<>(Property.TABLE_LOAD_BALANCER.getKey(), ChaoticLoadBalancer.class.getName()),
        new Pair<>(Property.TABLE_SPLIT_THRESHOLD.getKey(), "10K"), new Pair<>(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE.getKey(), "1K")).collect(
        Collectors.toMap(k -> k.getFirst(), v -> v.getSecond())));
    c.tableOperations().create(tableName, ntc);

    TestIngest.Opts opts = new TestIngest.Opts();
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.rows = opts.rows = 20000;
    opts.setTableName(tableName);
    vopts.setTableName(tableName);
    ClientConfiguration clientConfig = getCluster().getClientConfig();
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      opts.updateKerberosCredentials(clientConfig);
      vopts.updateKerberosCredentials(clientConfig);
    } else {
      opts.setPrincipal(getAdminPrincipal());
      vopts.setPrincipal(getAdminPrincipal());
    }
    TestIngest.ingest(c, opts, new BatchWriterOpts());
    c.tableOperations().flush(tableName, null, null, true);
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
  }
}

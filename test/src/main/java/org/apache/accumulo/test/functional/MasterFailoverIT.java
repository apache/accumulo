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

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class MasterFailoverIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "15s");
    cfg.setSiteConfig(siteConfig);
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 90;
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = createAccumuloClient()) {
      String[] names = getUniqueNames(2);
      c.tableOperations().create(names[0]);
      TestIngest.Opts opts = new TestIngest.Opts();
      opts.setTableName(names[0]);
      opts.setClientProperties(getClientProperties());
      TestIngest.ingest(c, opts, new BatchWriterOpts());

      ClusterControl control = cluster.getClusterControl();
      control.stopAllServers(ServerType.MASTER);
      // start up a new one
      control.startAllServers(ServerType.MASTER);
      // talk to it
      c.tableOperations().rename(names[0], names[1]);
      VerifyIngest.Opts vopts = new VerifyIngest.Opts();
      vopts.setTableName(names[1]);
      vopts.setClientProperties(getClientProperties());
      VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
    }
  }
}

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

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class ManagerFailoverIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(90);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "15s");
    cfg.setSiteConfig(siteConfig);
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String[] names = getUniqueNames(2);
      c.tableOperations().create(names[0]);
      VerifyParams params = new VerifyParams(getClientProps(), names[0]);
      TestIngest.ingest(c, params);

      ClusterControl control = cluster.getClusterControl();
      control.stopAllServers(ServerType.MANAGER);
      // start up a new one
      control.startAllServers(ServerType.MANAGER);
      // talk to it
      c.tableOperations().rename(names[0], names[1]);
      params.tableName = names[1];
      VerifyIngest.verifyIngest(c, params);
    }
  }
}

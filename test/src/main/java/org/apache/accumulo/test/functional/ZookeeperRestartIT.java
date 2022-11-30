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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class ZookeeperRestartIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<>();
    siteConfig.put(Property.INSTANCE_ZK_TIMEOUT.getKey(), "15s");
    cfg.setSiteConfig(siteConfig);
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT, "15s");
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      c.tableOperations().create("test_ingest");
      try (BatchWriter bw = c.createBatchWriter("test_ingest")) {
        Mutation m = new Mutation("row");
        m.put("cf", "cq", "value");
        bw.addMutation(m);
      }

      // kill zookeeper
      for (ProcessReference proc : cluster.getProcesses().get(ServerType.ZOOKEEPER)) {
        cluster.killProcess(ServerType.ZOOKEEPER, proc);
      }

      // give the servers time to react
      sleepUninterruptibly(1, TimeUnit.SECONDS);

      // start zookeeper back up
      cluster.start();

      // use the tservers
      try (Scanner s = c.createScanner("test_ingest", Authorizations.EMPTY)) {
        String actual = getOnlyElement(s).getKey().getRow().toString();
        assertEquals("row", actual);
        // use the manager
        c.tableOperations().delete("test_ingest");
      }
    }
  }

}

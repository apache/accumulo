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
package org.apache.accumulo.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

public class TabletServerHdfsRestartIT extends ConfigurableMacBase {

  private static final int N = 1000;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
    cfg.setNumTservers(1);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // wait until a tablet server is up
      while (client.instanceOperations().getTabletServers().isEmpty()) {
        Thread.sleep(50);
      }
      final String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);
      try (BatchWriter bw = client.createBatchWriter(tableName)) {
        for (int i = 0; i < N; i++) {
          Mutation m = new Mutation("" + i);
          m.put("", "", "");
          bw.addMutation(m);
        }
      }
      client.tableOperations().flush(tableName, null, null, true);

      // Kill dfs
      cluster.getMiniDfs().restartNameNode(false);

      assertEquals(N,
          Iterators.size(client.createScanner(tableName, Authorizations.EMPTY).iterator()));
    }
  }

}

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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

// ACCUMULO-2480
public class TabletServerGivesUpIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
    cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
    cfg.setProperty(Property.TSERV_WAL_TOLERATED_CREATION_FAILURES, "10");
    cfg.setProperty(Property.TSERV_WAL_TOLERATED_WAIT_INCREMENT, "0s");
    cfg.setProperty(Property.TSERV_WAL_TOLERATED_MAXIMUM_WAIT_DURATION, "0s");
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      while (client.instanceOperations().getTabletServers().isEmpty()) {
        // Wait until at least one tablet server is up
        Thread.sleep(100);
      }
      final String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName);

      // do an initial write to host the tablet and get its location in cache as we may not be able
      // to read the metadata table later
      try (var writer = client.createBatchWriter(tableName)) {
        Mutation m = new Mutation("001");
        m.put("a", "b", "c");
        writer.addMutation(m);
      }

      // Kill dfs
      cluster.getMiniDfs().shutdown();
      // ask the tserver to do something
      final AtomicReference<Exception> ex = new AtomicReference<>();
      Thread backgroundWriter = new Thread(() -> {
        try {
          for (int i = 0; i < 100; i++) {
            // These writes should cause the tserver to attempt to write to walog, which should
            // repeatedly fail. Relying on the client side cache to have the tablet location so the
            // writes make it to the tserver where the wal write fails.
            try (var writer = client.createBatchWriter(tableName)) {
              Mutation m = new Mutation("001");
              m.put("a", "b", "c");
              writer.addMutation(m);
            }
          }
        } catch (Exception e) {
          ex.set(e);
        }
      });
      backgroundWriter.start();
      // wait for the tserver to give up on writing to the WAL
      while (client.instanceOperations().getTabletServers().size() == 1) {
        Thread.sleep(SECONDS.toMillis(1));
      }
    }
  }
}

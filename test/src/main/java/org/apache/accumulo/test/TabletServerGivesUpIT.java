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

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.time.Duration;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
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
    cfg.setNumTservers(1);
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
      // Kill dfs
      cluster.getMiniDfs().shutdown();
      // ask the tserver to do something
      final AtomicReference<Exception> ex = new AtomicReference<>();
      Thread splitter = new Thread(() -> {
        try {
          TreeSet<Text> splits = new TreeSet<>();
          splits.add(new Text("X"));
          client.tableOperations().addSplits(tableName, splits);
        } catch (Exception e) {
          ex.set(e);
        }
      });
      splitter.start();
      // wait for the tserver to give up on writing to the WAL
      while (client.instanceOperations().getTabletServers().size() == 1) {
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    }
  }

}

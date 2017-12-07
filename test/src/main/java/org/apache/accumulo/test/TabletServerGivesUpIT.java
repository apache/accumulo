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
package org.apache.accumulo.test;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

// ACCUMULO-2480
public class TabletServerGivesUpIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
    cfg.setNumTservers(1);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.TSERV_WALOG_TOLERATED_CREATION_FAILURES, "15");
    cfg.setProperty(Property.TSERV_WALOG_TOLERATED_MAXIMUM_WAIT_DURATION, "0s");
  }

  @Test(timeout = 30 * 1000)
  public void test() throws Exception {
    final Connector conn = this.getConnector();
    // Yes, there's a tabletserver
    assertEquals(1, conn.instanceOperations().getTabletServers().size());
    final String tableName = getUniqueNames(1)[0];
    conn.tableOperations().create(tableName);
    // Kill dfs
    cluster.getMiniDfs().shutdown();
    // ask the tserver to do something
    final AtomicReference<Exception> ex = new AtomicReference<>();
    Thread splitter = new Thread() {
      @Override
      public void run() {
        try {
          TreeSet<Text> splits = new TreeSet<>();
          splits.add(new Text("X"));
          conn.tableOperations().addSplits(tableName, splits);
        } catch (Exception e) {
          ex.set(e);
        }
      }
    };
    splitter.start();
    // wait for the tserver to give up on writing to the WAL
    while (conn.instanceOperations().getTabletServers().size() == 1) {
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }

}

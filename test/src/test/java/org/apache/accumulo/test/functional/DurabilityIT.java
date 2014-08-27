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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class DurabilityIT extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
    cfg.setNumTservers(1);
  }

  static final long N = 100000;

  String tableNames[] = null;

  void init() throws Exception {
    synchronized (this) {
      if (tableNames == null) {
        tableNames = getUniqueNames(4);
        Connector c = getConnector();
        TableOperations tableOps = c.tableOperations();
        tableOps.create(tableNames[0]);
        tableOps.create(tableNames[1]);
        tableOps.create(tableNames[2]);
        tableOps.create(tableNames[3]);
        // default is sync
        tableOps.setProperty(tableNames[1], Property.TABLE_DURABILITY.getKey(), "flush");
        tableOps.setProperty(tableNames[2], Property.TABLE_DURABILITY.getKey(), "log");
        tableOps.setProperty(tableNames[3], Property.TABLE_DURABILITY.getKey(), "none");
        // zookeeper propagation
        UtilWaitThread.sleep(2 * 1000);
      }
    }
  }

  @Test(timeout = 2 * 60 * 1000)
  public void testWriteSpeed() throws Exception {
    init();
    // write some gunk
    long t0 = writeSome(tableNames[0], N); flush(tableNames[0]);
    long t1 = writeSome(tableNames[1], N); flush(tableNames[1]);
    long t2 = writeSome(tableNames[2], N); flush(tableNames[2]);
    long t3 = writeSome(tableNames[3], N); flush(tableNames[3]);
    System.out.println(String.format("t0 %d t1 %d t2 %d t3 %d", t0, t1, t2, t3));
    assertTrue(t0 > t1);
    assertTrue(t1 > t2);
    assertTrue(t2 > t3);
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testSync() throws Exception {
    init();
    // sync table should lose nothing
    getConnector().tableOperations().deleteRows(tableNames[0], null, null);
    writeSome(tableNames[0], N);
    restartTServer();
    assertEquals(N, readSome(tableNames[0], N));
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testFlush() throws Exception {
    init();
    // flush table won't lose anything since we're not losing power/dfs
    getConnector().tableOperations().deleteRows(tableNames[1], null, null);
    writeSome(tableNames[1], N);
    restartTServer();
    assertEquals(N, readSome(tableNames[1], N));
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testLog() throws Exception {
    init();
    // we're probably going to lose something the the log setting
    getConnector().tableOperations().deleteRows(tableNames[2], null, null);
    writeSome(tableNames[2], N);
    restartTServer();
    assertTrue(N > readSome(tableNames[2], N));
  }

  @Test(timeout = 4 * 60 * 1000)
  public void testNone() throws Exception {
    init();
    // probably won't get any data back without logging
    getConnector().tableOperations().deleteRows(tableNames[3], null, null);
    writeSome(tableNames[3], N);
    restartTServer();
    assertTrue(N > readSome(tableNames[3], N));
  }

  private long readSome(String table, long n) throws Exception {
    long count = 0;
    for (@SuppressWarnings("unused") Entry<Key,Value> entry : getConnector().createScanner(table, Authorizations.EMPTY)) {
      count++;
    }
    return count;
  }

  private void restartTServer() throws Exception {
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }
    cluster.start();
  }

  private void flush(String table) throws Exception {
    getConnector().tableOperations().flush(table, null, null, true);
  }

  private long writeSome(String table, long count) throws Exception {
    long now = System.currentTimeMillis();
    Connector c = getConnector();
    BatchWriter bw = c.createBatchWriter(table, null);
    for (int i = 1; i < count + 1; i++) {
      String data = "" + i;
      Mutation m = new Mutation("" + i);
      m.put(data, data, data);
      bw.addMutation(m);
      if (i % (count/100) == 0) {
        bw.flush();
      }
    }
    bw.close();
    long result = System.currentTimeMillis() - now;
    c.tableOperations().flush(table, null, null, true);
    return result;
  }

}

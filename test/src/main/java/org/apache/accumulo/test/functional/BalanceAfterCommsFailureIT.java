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

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Iterables;

public class BalanceAfterCommsFailureIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.GENERAL_RPC_TIMEOUT, "2s");
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = this.getConnector();
    c.tableOperations().create("test");
    Collection<ProcessReference> tservers = getCluster().getProcesses().get(ServerType.TABLET_SERVER);
    ArrayList<Integer> tserverPids = new ArrayList<>(tservers.size());
    for (ProcessReference tserver : tservers) {
      Process p = tserver.getProcess();
      if (!p.getClass().getName().equals("java.lang.UNIXProcess")) {
        log.info("Found process that was not UNIXProcess, exiting test");
        return;
      }

      Field f = p.getClass().getDeclaredField("pid");
      f.setAccessible(true);
      tserverPids.add(f.getInt(p));
    }

    for (int pid : tserverPids) {
      assertEquals(0, Runtime.getRuntime().exec(new String[] {"kill", "-SIGSTOP", Integer.toString(pid)}).waitFor());
    }
    UtilWaitThread.sleep(20 * 1000);
    for (int pid : tserverPids) {
      assertEquals(0, Runtime.getRuntime().exec(new String[] {"kill", "-SIGCONT", Integer.toString(pid)}).waitFor());
    }
    SortedSet<Text> splits = new TreeSet<>();
    for (String split : "a b c d e f g h i j k l m n o p q r s t u v w x y z".split(" ")) {
      splits.add(new Text(split));
    }
    c.tableOperations().addSplits("test", splits);
    // Ensure all of the tablets are actually assigned
    assertEquals(0, Iterables.size(c.createScanner("test", Authorizations.EMPTY)));
    UtilWaitThread.sleep(30 * 1000);
    checkBalance(c);
  }

  private void checkBalance(Connector c) throws Exception {
    Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));
    ClientContext context = new ClientContext(c.getInstance(), creds, getClientConfig());

    MasterMonitorInfo stats = null;
    int unassignedTablets = 1;
    for (int i = 0; unassignedTablets > 0 && i < 10; i++) {
      MasterClientService.Iface client = null;
      while (true) {
        try {
          client = MasterClient.getConnectionWithRetry(context);
          stats = client.getMasterStats(Tracer.traceInfo(), context.rpcCreds());
          break;
        } catch (ThriftNotActiveServiceException e) {
          // Let it loop, fetching a new location
          log.debug("Contacted a Master which is no longer active, retrying");
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } finally {
          if (client != null)
            MasterClient.close(client);
        }
      }
      unassignedTablets = stats.getUnassignedTablets();
      if (unassignedTablets > 0) {
        log.info("Found {} unassigned tablets, sleeping 3 seconds for tablet assignment", unassignedTablets);
        Thread.sleep(3000);
      }
    }

    assertEquals("Unassigned tablets were not assigned within 30 seconds", 0, unassignedTablets);

    List<Integer> counts = new ArrayList<>();
    for (TabletServerStatus server : stats.tServerInfo) {
      int count = 0;
      for (TableInfo table : server.tableMap.values()) {
        count += table.onlineTablets;
      }
      counts.add(count);
    }
    assertTrue("Expected to have at least two TabletServers", counts.size() > 1);
    for (int i = 1; i < counts.size(); i++) {
      int diff = Math.abs(counts.get(0) - counts.get(i));
      assertTrue("Expected difference in tablets to be less than or equal to " + counts.size() + " but was " + diff + ". Counts " + counts,
          diff <= counts.size());
    }
  }
}

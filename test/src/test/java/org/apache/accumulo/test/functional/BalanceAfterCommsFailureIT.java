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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BalanceAfterCommsFailureIT extends ConfigurableMacIT {
  
  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setSiteConfig(Collections.singletonMap(Property.GENERAL_RPC_TIMEOUT.getKey(), "2s"));
  }
  
  @Override
  protected int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = this.getConnector();
    c.tableOperations().create("test");
    assertEquals(0, Runtime.getRuntime().exec(new String[]{"pkill", "-SIGSTOP", "-f", "TabletServer"}).waitFor());
    UtilWaitThread.sleep(20 * 1000);
    assertEquals(0, Runtime.getRuntime().exec(new String[]{"pkill", "-SIGCONT", "-f", "TabletServer"}).waitFor());
    SortedSet<Text> splits = new TreeSet<Text>();
    for (String split : "a b c d e f g h i j k l m n o p q r s t u v w x y z".split(" ")) {
      splits.add(new Text(split));
    }
    c.tableOperations().addSplits("test", splits);
    UtilWaitThread.sleep(10 * 1000);
    checkBalance(c);
  }

  private void checkBalance(Connector c) throws Exception {
    Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));
    
    MasterClientService.Iface client = null;
    MasterMonitorInfo stats = null;
    try {
      client = MasterClient.getConnectionWithRetry(c.getInstance());
      stats = client.getMasterStats(Tracer.traceInfo(), creds.toThrift(c.getInstance()));
    } finally {
      if (client != null)
        MasterClient.close(client);
    }
    List<Integer> counts = new ArrayList<Integer>();
    for (TabletServerStatus server : stats.tServerInfo) {
      int count = 0;
      for (TableInfo table : server.tableMap.values()) {
        count += table.onlineTablets;
      }
      counts.add(count);
    }
    assertTrue(counts.size() > 1);
    for (int i = 1; i < counts.size(); i++)
      assertTrue(Math.abs(counts.get(0) - counts.get(i)) <= counts.size());
  }
}

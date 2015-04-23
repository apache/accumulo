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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.MasterClientService.Client;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.Admin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MetadataMaxFilesIT extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = new HashMap<String,String>();
    siteConfig.put(Property.TSERV_MAJC_DELAY.getKey(), "1");
    siteConfig.put(Property.TSERV_SCAN_MAX_OPENFILES.getKey(), "10");
    cfg.setSiteConfig(siteConfig);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    SortedSet<Text> splits = new TreeSet<Text>();
    for (int i = 0; i < 1000; i++) {
      splits.add(new Text(String.format("%03d", i)));
    }
    c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10000");
    for (int i = 0; i < 5; i++) {
      String tableName = "table" + i;
      log.info("Creating {}", tableName);
      c.tableOperations().create(tableName);
      log.info("adding splits");
      c.tableOperations().addSplits(tableName, splits);
      log.info("flushing");
      c.tableOperations().flush(MetadataTable.NAME, null, null, true);
      c.tableOperations().flush(RootTable.NAME, null, null, true);
    }
    UtilWaitThread.sleep(20 * 1000);
    log.info("shutting down");
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
    cluster.stop();
    log.info("starting up");
    cluster.start();

    UtilWaitThread.sleep(30 * 1000);

    while (true) {
      MasterMonitorInfo stats = null;
      Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));
      Client client = null;
      try {
        ClientContext context = new ClientContext(c.getInstance(), creds, getClientConfig());
        client = MasterClient.getConnectionWithRetry(context);
        stats = client.getMasterStats(Tracer.traceInfo(), context.rpcCreds());
      } finally {
        if (client != null)
          MasterClient.close(client);
      }
      int tablets = 0;
      for (TabletServerStatus tserver : stats.tServerInfo) {
        for (Entry<String,TableInfo> entry : tserver.tableMap.entrySet()) {
          if (entry.getKey().startsWith("!") || entry.getKey().startsWith("+"))
            continue;
          tablets += entry.getValue().onlineTablets;
        }
      }
      if (tablets == 5005)
        break;
      UtilWaitThread.sleep(1000);
    }
  }
}

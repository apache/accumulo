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

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.Map.Entry;
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
import org.apache.accumulo.core.master.thrift.MasterClientService.Client;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.trace.Tracer;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.Admin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MetadataMaxFilesIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1");
    cfg.setProperty(Property.TSERV_SCAN_MAX_OPENFILES, "10");
    cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT, "100");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 0; i < 1000; i++) {
      splits.add(new Text(String.format("%03d", i)));
    }
    c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(), "10000");
    // propagation time
    sleepUninterruptibly(5, TimeUnit.SECONDS);
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
    log.info("shutting down");
    assertEquals(0, cluster.exec(Admin.class, "stopAll").waitFor());
    cluster.stop();
    log.info("starting up");
    cluster.start();

    Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));

    while (true) {
      MasterMonitorInfo stats = null;
      Client client = null;
      try {
        ClientContext context = new ClientContext(c.getInstance(), creds, getClientConfig());
        client = MasterClient.getConnectionWithRetry(context);
        log.info("Fetching stats");
        stats = client.getMasterStats(Tracer.traceInfo(), context.rpcCreds());
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        continue;
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
      log.info("Online tablets " + tablets);
      if (tablets == 5005)
        break;
      sleepUninterruptibly(1, TimeUnit.SECONDS);
    }
  }
}

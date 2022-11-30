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

import java.time.Duration;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MetadataMaxFilesIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_MAJC_DELAY, "1");
    cfg.setProperty(Property.TSERV_SCAN_MAX_OPENFILES, "10");
    cfg.setProperty(Property.TSERV_ASSIGNMENT_MAXCONCURRENT, "100");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 0; i < 1000; i++) {
        splits.add(new Text(String.format("%03d", i)));
      }
      c.tableOperations().setProperty(MetadataTable.NAME, Property.TABLE_SPLIT_THRESHOLD.getKey(),
          "10000");
      // propagation time
      sleepUninterruptibly(5, TimeUnit.SECONDS);
      for (int i = 0; i < 2; i++) {
        String tableName = "table" + i;
        log.info("Creating {} with splits", tableName);
        NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
        c.tableOperations().create(tableName, ntc);
        log.info("flushing");
        c.tableOperations().flush(MetadataTable.NAME, null, null, true);
        c.tableOperations().flush(RootTable.NAME, null, null, true);
      }

      while (true) {
        ClientContext context = (ClientContext) c;
        ManagerMonitorInfo stats = ThriftClientTypes.MANAGER.execute(context,
            client -> client.getManagerStats(TraceUtil.traceInfo(), context.rpcCreds()));
        int tablets = 0;
        for (TabletServerStatus tserver : stats.tServerInfo) {
          for (Entry<String,TableInfo> entry : tserver.tableMap.entrySet()) {
            if (entry.getKey().startsWith("!") || entry.getKey().startsWith("+")) {
              continue;
            }
            tablets += entry.getValue().onlineTablets;
          }
        }
        log.info("Online tablets " + tablets);
        if (tablets == 2002) {
          break;
        }
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
    }
  }
}

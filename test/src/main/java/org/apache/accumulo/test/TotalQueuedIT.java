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
import static org.junit.Assert.assertTrue;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

// see ACCUMULO-1950
public class TotalQueuedIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setDefaultMemory(cfg.getDefaultMemory() * 2, MemoryUnit.BYTE);
    cfg.useMiniDFS();
  }

  int SMALL_QUEUE_SIZE = 100000;
  int LARGE_QUEUE_SIZE = SMALL_QUEUE_SIZE * 10;
  static final long N = 1000000;

  @Test(timeout = 4 * 60 * 1000)
  public void test() throws Exception {
    Random random = new Random();
    Connector c = getConnector();
    c.instanceOperations().setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey(), "" + SMALL_QUEUE_SIZE);
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "9999");
    c.tableOperations().setProperty(tableName, Property.TABLE_FILE_MAX.getKey(), "999");
    sleepUninterruptibly(1, TimeUnit.SECONDS);
    // get an idea of how fast the syncs occur
    byte row[] = new byte[250];
    BatchWriterConfig cfg = new BatchWriterConfig();
    cfg.setMaxWriteThreads(10);
    cfg.setMaxLatency(1, TimeUnit.SECONDS);
    cfg.setMaxMemory(1024 * 1024);
    long realSyncs = getSyncs();
    BatchWriter bw = c.createBatchWriter(tableName, cfg);
    long now = System.currentTimeMillis();
    long bytesSent = 0;
    for (int i = 0; i < N; i++) {
      random.nextBytes(row);
      Mutation m = new Mutation(row);
      m.put("", "", "");
      bw.addMutation(m);
      bytesSent += m.estimatedMemoryUsed();
    }
    bw.close();
    long diff = System.currentTimeMillis() - now;
    double secs = diff / 1000.;
    double syncs = bytesSent / SMALL_QUEUE_SIZE;
    double syncsPerSec = syncs / secs;
    System.out.println(String.format("Sent %d bytes in %f secs approximately %d syncs (%f syncs per sec)", bytesSent, secs, ((long) syncs), syncsPerSec));
    long update = getSyncs();
    System.out.println("Syncs " + (update - realSyncs));
    realSyncs = update;

    // Now with a much bigger total queue
    c.instanceOperations().setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX.getKey(), "" + LARGE_QUEUE_SIZE);
    c.tableOperations().flush(tableName, null, null, true);
    sleepUninterruptibly(1, TimeUnit.SECONDS);
    bw = c.createBatchWriter(tableName, cfg);
    now = System.currentTimeMillis();
    bytesSent = 0;
    for (int i = 0; i < N; i++) {
      random.nextBytes(row);
      Mutation m = new Mutation(row);
      m.put("", "", "");
      bw.addMutation(m);
      bytesSent += m.estimatedMemoryUsed();
    }
    bw.close();
    diff = System.currentTimeMillis() - now;
    secs = diff / 1000.;
    syncs = bytesSent / LARGE_QUEUE_SIZE;
    syncsPerSec = syncs / secs;
    System.out.println(String.format("Sent %d bytes in %f secs approximately %d syncs (%f syncs per sec)", bytesSent, secs, ((long) syncs), syncsPerSec));
    update = getSyncs();
    System.out.println("Syncs " + (update - realSyncs));
    assertTrue(update - realSyncs < realSyncs);
  }

  private long getSyncs() throws Exception {
    Connector c = getConnector();
    ServerConfigurationFactory confFactory = new ServerConfigurationFactory(c.getInstance());
    AccumuloServerContext context = new AccumuloServerContext(c.getInstance(), confFactory);
    for (String address : c.instanceOperations().getTabletServers()) {
      TabletClientService.Client client = ThriftUtil.getTServerClient(HostAndPort.fromString(address), context);
      TabletServerStatus status = client.getTabletServerStatus(null, context.rpcCreds());
      return status.syncs;
    }
    return 0;
  }

}

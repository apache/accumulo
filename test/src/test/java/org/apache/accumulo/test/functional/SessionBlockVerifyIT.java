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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify that we have resolved blocking issue by ensuring that we have not lost scan sessions which we know to currently be running
 */
public class SessionBlockVerifyIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(SessionBlockVerifyIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    cfg.setNumTservers(1);
    siteConfig.put(Property.TSERV_SESSION_MAXIDLE.getKey(), "1s");
    siteConfig.put(Property.TSERV_READ_AHEAD_MAXCONCURRENT.getKey(), "11");
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 60;
  }

  private String sessionIdle = null;

  @Before
  public void reduceSessionIdle() throws Exception {

    InstanceOperations ops = getConnector().instanceOperations();
    sessionIdle = ops.getSystemConfiguration().get(Property.TSERV_SESSION_MAXIDLE.getKey());
    ops.setProperty(Property.TSERV_SESSION_MAXIDLE.getKey(), "1s");
    log.info("Waiting for existing session idle time to expire");
    Thread.sleep(AccumuloConfiguration.getTimeInMillis(sessionIdle));
    log.info("Finished waiting");
  }

  ExecutorService service = Executors.newFixedThreadPool(10);

  @Test
  public void run() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());

    for (int i = 0; i < 1000; i++) {
      Mutation m = new Mutation(new Text(String.format("%08d", i)));
      for (int j = 0; j < 3; j++)
        m.put(new Text("cf1"), new Text("cq" + j), new Value((i + "_" + j).getBytes(UTF_8)));

      bw.addMutation(m);
    }

    bw.close();

    Scanner scanner = c.createScanner(tableName, new Authorizations());
    scanner.setReadaheadThreshold(20000);
    scanner.setRange(new Range(String.format("%08d", 0), String.format("%08d", 1000)));

    // test by making a slow iterator and then a couple of fast ones.
    // when then checking we shouldn't have any running except the slow iterator
    IteratorSetting setting = new IteratorSetting(21, SlowIterator.class);
    SlowIterator.setSeekSleepTime(setting, Long.MAX_VALUE);
    SlowIterator.setSleepTime(setting, Long.MAX_VALUE);
    scanner.addScanIterator(setting);

    final Iterator<Entry<Key,Value>> slow = scanner.iterator();

    final List<Future<Boolean>> callables = new ArrayList<Future<Boolean>>();
    for (int i = 0; i < 10; i++) {
      Future<Boolean> callable = service.submit(new Callable<Boolean>() {
        public Boolean call() {
          while (slow.hasNext()) {

            slow.next();
          }
          return slow.hasNext();
        }
      });
      callables.add(callable);
    }

    Thread.sleep(10000);
    log.info("Starting");
    // let's add more for good measure.
    for (int i = 0; i < 2; i++) {
      Scanner scanner2 = c.createScanner(tableName, new Authorizations());

      scanner2.setRange(new Range(String.format("%08d", 0), String.format("%08d", 1000)));

      scanner2.setBatchSize(1);
      Iterator<Entry<Key,Value>> iter = scanner2.iterator();
      sinkKeyValues(iter);

    }

    int sessionsFound = 0;
    // we have configured 1 tserver, so we can grab the one and only
    String tserver = c.instanceOperations().getTabletServers().iterator().next();

    final List<ActiveScan> scans = c.instanceOperations().getActiveScans(tserver);

    for (ActiveScan scan : scans) {
      // only here to minimize chance of seeing meta extent scans

      if (tableName.equals(scan.getTable()) && scan.getSsiList().size() > 0) {
        assertEquals("Not the expected iterator", 1, scan.getSsiList().size());
        assertTrue("Not the expected iterator", scan.getSsiList().iterator().next().contains("SlowIterator"));
        sessionsFound++;
      }

    }

    assertEquals("Must have ten sessions to ensure 3509 is resolved", 10, sessionsFound);
    for (Future<Boolean> callable : callables) {
      callable.cancel(true);
    }
    service.shutdown();
  }

  private void sinkKeyValues(Iterator<Entry<Key,Value>> iter) throws Exception {
    while (iter.hasNext()) {

      iter.next();
    }
  }

}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify that we have resolved blocking issue by ensuring that we have not lost scan sessions which
 * we know to currently be running
 */
public class SessionBlockVerifyIT extends ScanSessionTimeOutIT {
  private static final Logger log = LoggerFactory.getLogger(SessionBlockVerifyIT.class);

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public boolean canRunTest(ClusterType type) {
    return type == ClusterType.MINI;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    cfg.setNumTservers(1);
    siteConfig.put(Property.TSERV_SESSION_MAXIDLE.getKey(), getMaxIdleTimeString());
    siteConfig.put(Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS.getKey(), "11");
    cfg.setSiteConfig(siteConfig);
  }

  @Override
  protected String getMaxIdleTimeString() {
    return "1s";
  }

  ExecutorService service = Executors.newFixedThreadPool(10);

  @Test
  @Override
  public void run() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);

      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 0; i < 1000; i++) {
          Mutation m = new Mutation(new Text(String.format("%08d", i)));
          for (int j = 0; j < 3; j++) {
            m.put("cf1", "cq" + j, i + "_" + j);
          }

          bw.addMutation(m);
        }
      }

      try (Scanner scanner = c.createScanner(tableName, new Authorizations())) {
        scanner.setReadaheadThreshold(20000);
        scanner.setRange(new Range(String.format("%08d", 0), String.format("%08d", 1000)));

        // test by making a slow iterator and then a couple of fast ones.
        // when then checking we shouldn't have any running except the slow iterator
        IteratorSetting setting = new IteratorSetting(21, SlowIterator.class);
        SlowIterator.setSeekSleepTime(setting, Long.MAX_VALUE);
        SlowIterator.setSleepTime(setting, Long.MAX_VALUE);
        scanner.addScanIterator(setting);

        final List<Future<Boolean>> callables = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
          Future<Boolean> callable = service.submit(() -> {
            Iterator<Entry<Key,Value>> slow = scanner.iterator();
            latch.countDown();
            while (slow.hasNext()) {

              slow.next();
            }
            return slow.hasNext();
          });
          callables.add(callable);
        }

        latch.await();

        log.info("Starting SessionBlockVerifyIT");

        // let's add more for good measure.
        for (int i = 0; i < 2; i++) {
          // do not close scanner, since all data is consumed it should close the sessions
          Scanner scanner2 = c.createScanner(tableName, new Authorizations());
          scanner2.setRange(new Range(String.format("%08d", 0), String.format("%08d", 1000)));
          scanner2.setBatchSize(1);
          Iterator<Entry<Key,Value>> iter = scanner2.iterator();
          // call super's verify mechanism
          verify(iter, 0, 1000);
        }

        int sessionsFound = 0;
        // we have configured 1 tserver, so we can grab the one and only
        String tserver = getOnlyElement(c.instanceOperations().getTabletServers());

        final List<ActiveScan> scans = c.instanceOperations().getActiveScans(tserver);

        for (ActiveScan scan : scans) {
          // only here to minimize chance of seeing meta extent scans

          if (tableName.equals(scan.getTable()) && !scan.getSsiList().isEmpty()) {
            assertEquals(1, scan.getSsiList().size(), "Not the expected iterator");
            assertTrue(scan.getSsiList().iterator().next().contains("SlowIterator"),
                "Not the expected iterator");
            sessionsFound++;
          }

        }

        /**
         * The message below indicates the problem that we experience within ACCUMULO-3509. The
         * issue manifests as a blockage in the Scanner synchronization that prevent us from making
         * the close call against it. Since the close blocks until a read is finished, we ultimately
         * have a block within the sweep of SessionManager. As a result never reap subsequent idle
         * sessions AND we will orphan the sessionsToCleanup in the sweep, leading to an inaccurate
         * count within sessionsFound.
         */
        assertEquals(10, sessionsFound,
            "Must have ten sessions. Failure indicates a synchronization block within the sweep mechanism");
        for (Future<Boolean> callable : callables) {
          callable.cancel(true);
        }
      }
      service.shutdown();
    }
  }

}

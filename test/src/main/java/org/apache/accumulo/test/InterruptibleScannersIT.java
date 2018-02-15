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

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterators;

// ACCUMULO-3030
public class InterruptibleScannersIT extends AccumuloClusterHarness {

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Test
  public void test() throws Exception {
    // make a table
    final String tableName = getUniqueNames(1)[0];
    final Connector conn = getConnector();
    conn.tableOperations().create(tableName);

    // make the world's slowest scanner
    try (Scanner scanner = conn.createScanner(tableName, Authorizations.EMPTY)) {
      final IteratorSetting cfg = new IteratorSetting(100, SlowIterator.class);
      // Wait long enough to be sure we can catch it, but not indefinitely.
      SlowIterator.setSeekSleepTime(cfg, 60 * 1000);
      scanner.addScanIterator(cfg);
      // create a thread to interrupt the slow scan
      final Thread scanThread = Thread.currentThread();
      Thread thread = new Thread() {
        @Override
        public void run() {
          try {
            // ensure the scan is running: not perfect, the metadata tables could be scanned, too.
            String tserver = conn.instanceOperations().getTabletServers().iterator().next();
            do {
              ArrayList<ActiveScan> scans = new ArrayList<>(conn.instanceOperations().getActiveScans(tserver));
              Iterator<ActiveScan> iter = scans.iterator();
              while (iter.hasNext()) {
                ActiveScan scan = iter.next();
                // Remove scans not against our table and not owned by us
                if (!getAdminPrincipal().equals(scan.getUser()) || !tableName.equals(scan.getTable())) {
                  iter.remove();
                }
              }

              if (!scans.isEmpty()) {
                // We found our scan
                break;
              }
            } while (true);
          } catch (Exception e) {
            e.printStackTrace();
          }
          // BAM!
          scanThread.interrupt();
        }
      };
      thread.start();
      try {
        // Use the scanner, expect problems
        Iterators.size(scanner.iterator());
        Assert.fail("Scan should not succeed");
      } catch (Exception ex) {} finally {
        thread.join();
      }
    }
  }
}

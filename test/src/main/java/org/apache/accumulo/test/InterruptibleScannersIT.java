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
package org.apache.accumulo.test;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Iterators;

// ACCUMULO-3030
public class InterruptibleScannersIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Test
  public void test() throws Exception {
    // make a table
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      client.tableOperations().create(tableName);

      // make the world's slowest scanner
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        final IteratorSetting cfg = new IteratorSetting(100, SlowIterator.class);
        // Wait long enough to be sure we can catch it, but not indefinitely.
        SlowIterator.setSeekSleepTime(cfg, MINUTES.toMillis(1));
        scanner.addScanIterator(cfg);
        // create a thread to interrupt the slow scan
        final Thread scanThread = Thread.currentThread();
        Thread thread = new Thread(() -> {
          try {
            // ensure the scan is running: not perfect, the metadata tables could be scanned, too.
            String tserver = client.instanceOperations().getTabletServers().iterator().next();
            do {
              ArrayList<ActiveScan> scans =
                  new ArrayList<>(client.instanceOperations().getActiveScans(tserver));
              // Remove scans not against our table and not owned by us
              scans.removeIf(scan -> !getAdminPrincipal().equals(scan.getUser())
                  || !tableName.equals(scan.getTable()));

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
        });
        thread.start();
        try {
          // Use the scanner, expect problems
          assertThrows(RuntimeException.class, () -> Iterators.size(scanner.iterator()),
              "Scan should not succeed");
        } finally {
          thread.join();
        }
      }
    }
  }
}

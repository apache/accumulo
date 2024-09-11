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

import static org.apache.accumulo.test.functional.ScannerIT.countActiveScans;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseScannerIT extends AccumuloClusterHarness {

  static final int ROWS = 1000;
  static final int COLS = 1000;

  private static final Logger log = LoggerFactory.getLogger(CloseScannerIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteConfig = cfg.getSiteConfig();
    siteConfig.put(Property.TSERV_SESSION_MAXIDLE.getKey(), "20s");
    cfg.setSiteConfig(siteConfig);
  }

  /**
   * {@link org.apache.accumulo.test.functional.ScannerIT#testSessionCleanup()} is a similar test.
   */
  @Test
  public void testManyScans() throws Exception {

    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName);

      ReadWriteIT.ingest(client, ROWS, COLS, 50, 0, tableName);

      client.tableOperations().flush(tableName, null, null, true);

      Timer timer = Timer.startNew();

      int count = 0;
      while (count < 200 && timer.elapsed(TimeUnit.MILLISECONDS) < 3000) {
        try (Scanner scanner = createScanner(client, tableName, count)) {
          scanner.setRange(new Range());
          scanner.setReadaheadThreshold(count % 2 == 0 ? 0 : 3);

          for (int j = 0; j < count % 7 + 1; j++) {
            // only read a little data and quit, this should leave a session open on the tserver
            scanner.stream().limit(10).forEach(e -> {});
          }
        } // when the scanner is closed, all open sessions should be closed
        count++;
      }

      log.debug("Ran {} scans in {} ms", count, timer.elapsed(TimeUnit.MILLISECONDS));

      // The goal of this test it to ensure the scanner client object closes server side scan
      // sessions and not idle session cleanup. To do this the test is making the following
      // assumptions about how Accumulo works to set the timings in this test :
      // 1. Sessions not closed by the scanner will be cleaned up in 20s based on config set before
      // starting test
      // 2. This test creates readahead threads for some scans. The presence of a thread will
      // prevent immediate cleanup of the server side scan session. So when the scanner sends the
      // RPC to close the session if a thread is present, then cleanup will be deferred. A scheduled
      // task in the tserver runs deferred cleanup every TSERV_SESSION_MAXIDLE/2 which is 10s.
      //
      // Putting the assumptions above together we know that if sessions are closed in less than
      // 20s, then they were closed as result of the scanner.close() method initiating an RPC to
      // remove the scan session. The 13s below allows time for the 10s deferred cleanup to run in
      // the case when a thread is present. The 3s cap the test puts on running scans sets the total
      // time the test will allow to 3s+13s=16s which is less than the 20s when idle session clean
      // starts.

      Wait.waitFor(() -> countActiveScans(client, tableName) < 1, 13000, 250,
          "Found active scans after closing all scanners. Expected to find no scans");

      var elasped = timer.elapsed(TimeUnit.MILLISECONDS);
      if (elasped > 20000) {
        log.warn(
            "Total time since first scan was run {}ms.  Unable to verify that scanner RPC closed "
                + "sessions, could have been closed by idle session cleanup.",
            elasped);
      } else {
        log.debug("Total time since first scan was run {}ms.", elasped);
      }
    }
  }

  private static Scanner createScanner(AccumuloClient client, String tableName, int i)
      throws Exception {
    Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY);
    if (i % 2 == 0) {
      scanner = new IsolatedScanner(scanner);
    }
    return scanner;
  }
}

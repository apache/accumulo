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

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel.EVENTUAL;
import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel.IMMEDIATE;
import static org.apache.accumulo.minicluster.ServerType.SCAN_SERVER;
import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.CloseScannerIT;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class ScannerIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Test
  public void testScannerReadaheadConfiguration() throws Exception {
    final String table = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      c.tableOperations().create(table);

      try (BatchWriter bw = c.createBatchWriter(table)) {
        Mutation m = new Mutation("a");
        for (int i = 0; i < 10; i++) {
          m.put(Integer.toString(i), "", "");
        }
        bw.addMutation(m);
      }

      IteratorSetting cfg;
      Iterator<Entry<Key,Value>> iterator;
      long nanosWithWait = 0;
      try (Scanner s = c.createScanner(table, new Authorizations())) {

        cfg = new IteratorSetting(100, SlowIterator.class);
        // A batch size of one will end up calling seek() for each element with no calls to next()
        SlowIterator.setSeekSleepTime(cfg, 100L);

        s.addScanIterator(cfg);
        // Never start readahead
        s.setReadaheadThreshold(Long.MAX_VALUE);
        s.setBatchSize(1);
        s.setRange(new Range());

        iterator = s.iterator();
        long startTime = System.nanoTime();
        while (iterator.hasNext()) {
          nanosWithWait += System.nanoTime() - startTime;

          // While we "do work" in the client, we should be fetching the next result
          Thread.sleep(100L);
          iterator.next();
          startTime = System.nanoTime();
        }
        nanosWithWait += System.nanoTime() - startTime;
      }

      long nanosWithNoWait = 0;
      try (Scanner s = c.createScanner(table, new Authorizations())) {
        s.addScanIterator(cfg);
        s.setRange(new Range());
        s.setBatchSize(1);
        s.setReadaheadThreshold(0L);

        iterator = s.iterator();
        long startTime = System.nanoTime();
        while (iterator.hasNext()) {
          nanosWithNoWait += System.nanoTime() - startTime;

          // While we "do work" in the client, we should be fetching the next result
          Thread.sleep(100L);
          iterator.next();
          startTime = System.nanoTime();
        }
        nanosWithNoWait += System.nanoTime() - startTime;

        // The "no-wait" time should be much less than the "wait-time"
        assertTrue(nanosWithNoWait < nanosWithWait,
            "Expected less time to be taken with immediate readahead (" + nanosWithNoWait
                + ") than without immediate readahead (" + nanosWithWait + ")");
      }
    }
  }

  /**
   * {@link CloseScannerIT#testManyScans()} is a similar test.
   */
  @ParameterizedTest
  @EnumSource
  public void testSessionCleanup(ConsistencyLevel consistency) throws Exception {
    final String tableName = getUniqueNames(1)[0];
    final ServerType serverType = consistency == IMMEDIATE ? TABLET_SERVER : SCAN_SERVER;
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProperties()).build()) {

      if (serverType == SCAN_SERVER) {
        // Scans will fall back to tablet servers when no scan servers are present. So wait for scan
        // servers to show up in zookeeper. Can remove this in 3.1.
        Wait.waitFor(() -> !accumuloClient.instanceOperations()
            .getServers(ServerId.Type.SCAN_SERVER).isEmpty());
      }

      accumuloClient.tableOperations().create(tableName);

      try (var writer = accumuloClient.createBatchWriter(tableName)) {
        for (int i = 0; i < 100000; i++) {
          var m = new Mutation(String.format("%09d", i));
          m.put("1", "1", "" + i);
          writer.addMutation(m);
        }
      }

      if (consistency == EVENTUAL) {
        accumuloClient.tableOperations().flush(tableName, null, null, true);
      }

      // The test assumes the session timeout is configured to 1 minute, validate this. Later in the
      // test 10s is given for session to disappear and we want this 10s to be much smaller than the
      // configured session timeout.
      assertEquals("1m", accumuloClient.instanceOperations().getSystemConfiguration()
          .get(Property.TSERV_SESSION_MAXIDLE.getKey()));

      // The following test that when not all data is read from scanner that when the scanner is
      // closed that any open sessions will be closed.
      for (int i = 0; i < 3; i++) {
        try (var scanner = accumuloClient.createScanner(tableName)) {
          scanner.setConsistencyLevel(consistency);
          assertEquals(10, scanner.stream().limit(10).count());
          assertEquals(10000, scanner.stream().limit(10000).count());
          // since not all data in the range was read from the scanner it should leave an active
          // scan session per scanner iterator created
          assertEquals(2, countActiveScans(accumuloClient, serverType, tableName));
        }
        // When close is called on on the scanner it should close the scan session. The session
        // cleanup is async on the server because task may still be running server side, but it
        // should happen in less than the session timeout. Also the server should start working on
        // it immediately.
        Wait.waitFor(() -> countActiveScans(accumuloClient, serverType, tableName) == 0, 10000);

        try (var scanner = accumuloClient.createBatchScanner(tableName)) {
          scanner.setConsistencyLevel(consistency);
          scanner.setRanges(List.of(new Range()));
          assertEquals(10, scanner.stream().limit(10).count());
          assertEquals(10000, scanner.stream().limit(10000).count());
          assertEquals(2, countActiveScans(accumuloClient, serverType, tableName));
        }
        Wait.waitFor(() -> countActiveScans(accumuloClient, serverType, tableName) == 0, 10000);
      }

      // Test the case where all data is read from a scanner. In this case the scanner should close
      // the scan session at the end of the range even before the scanner itself is closed.
      for (int i = 0; i < 3; i++) {
        try (var scanner = accumuloClient.createScanner(tableName)) {
          scanner.setConsistencyLevel(consistency);
          assertEquals(100000, scanner.stream().count());
          assertEquals(100000, scanner.stream().count());
          // The server side cleanup of the session should be able to happen immediately in this
          // case because nothing should be running on the server side to fetch data because all
          // data in the range was fetched.
          assertEquals(0, countActiveScans(accumuloClient, serverType, tableName));
        }

        try (var scanner = accumuloClient.createBatchScanner(tableName)) {
          scanner.setConsistencyLevel(consistency);
          scanner.setRanges(List.of(new Range()));
          assertEquals(100000, scanner.stream().count());
          assertEquals(100000, scanner.stream().count());
          assertEquals(0, countActiveScans(accumuloClient, serverType, tableName));
        }
      }
    }
  }

  public static long countActiveScans(AccumuloClient c, ServerType serverType, String tableName)
      throws Exception {
    final Collection<ServerId> servers;
    if (serverType == TABLET_SERVER) {
      servers = c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
    } else if (serverType == SCAN_SERVER) {
      servers = c.instanceOperations().getServers(ServerId.Type.SCAN_SERVER);
    } else {
      throw new IllegalArgumentException("Unsupported server type " + serverType);
    }

    return c.instanceOperations().getActiveScans(servers).stream()
        .filter(activeScan -> activeScan.getTable().equals(tableName)).count();
  }
}

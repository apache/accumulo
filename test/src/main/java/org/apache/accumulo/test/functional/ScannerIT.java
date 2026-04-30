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
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.minicluster.ServerType.SCAN_SERVER;
import static org.apache.accumulo.minicluster.ServerType.TABLET_SERVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.clientImpl.ThriftScanner;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.CloseScannerIT;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import com.google.common.collect.MoreCollectors;

public class ScannerIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(1);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumScanServers(1);
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
          UtilWaitThread.sleep(100L);
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
          UtilWaitThread.sleep(100L);
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
    final String tableName = getUniqueNames(1)[0] + "_" + consistency;
    final ServerType serverType = consistency == IMMEDIATE ? TABLET_SERVER : SCAN_SERVER;
    try (AccumuloClient accumuloClient = Accumulo.newClient().from(getClientProperties()).build()) {

      if (serverType == SCAN_SERVER) {
        getCluster().getConfig().setNumScanServers(1);
        getCluster().getClusterControl().startAllServers(SCAN_SERVER);
        // Scans will fall back to tablet servers when no scan servers are present. So wait for scan
        // servers to show up in zookeeper. Can remove this in 3.1.
        Wait.waitFor(() -> !accumuloClient.instanceOperations().getScanServers().isEmpty());
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
    } finally {
      if (serverType == SCAN_SERVER) {
        getCluster().getConfig().setNumScanServers(0);
        getCluster().getClusterControl().stopAllServers(SCAN_SERVER);
      }
    }
  }

  public static long countActiveScans(AccumuloClient c, ServerType serverType, String tableName)
      throws Exception {
    final Collection<String> servers;
    if (serverType == TABLET_SERVER) {
      servers = c.instanceOperations().getTabletServers();
    } else if (serverType == SCAN_SERVER) {
      servers = c.instanceOperations().getScanServers();
    } else {
      throw new IllegalArgumentException("Unsupported server type " + serverType);
    }

    long count = 0;
    for (String server : servers) {
      count += c.instanceOperations().getActiveScans(server).stream()
          .filter(activeScan -> activeScan.getTable().equals(tableName)).count();
    }
    return count;
  }

  @Test
  public void testIOExceptionDuringScanIterator() throws Exception {

    getCluster().getClusterControl().startAllServers(SCAN_SERVER);
    var random = new SecureRandom();

    Properties props = getClientProperties();
    // configure scan server not to fallback to tablet servers
    String profiles = "[{'isDefault':true,'maxBusyTimeout':'1s', 'busyTimeoutMultiplier':8,"
        + "'timeToWaitForScanServers':10h, "
        + "'attemptPlans':[{'servers':'3', 'busyTimeout':'100ms'}]}]";
    props.put(ClientProperty.SCAN_SERVER_SELECTOR_OPTS_PREFIX.getKey() + "profiles", profiles);

    final String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(props).build()) {
      client.tableOperations().create(table);

      try (var writer = client.createBatchWriter(table)) {
        for (int i = 0; i < 10; i++) {
          Mutation m = new Mutation("row" + i);
          m.put("", "", "");
          writer.addMutation(m);
        }
      }

      // need to flush data to disk so its visible to scan server
      client.tableOperations().flush(table, null, null, true);

      IteratorSetting iteratorSetting = new IteratorSetting(1000, ErrorThrowingIterator.class);
      iteratorSetting.addOption(ErrorThrowingIterator.TIMES, "3");
      // Set a single row to fail so that after splitting some tablets fail and some do not fail.
      iteratorSetting.addOption(ErrorThrowingIterator.ROW, "row5");

      // The batch scanner sends multiple extents in a single RPC. Need to try a mixture of failing
      // and non failing extents for this RPC, so test w/ single tablet and three tablets.
      for (List<String> splitsToAdd : List.of(List.<String>of(), List.of("row3", "row7"))) {
        if (!splitsToAdd.isEmpty()) {
          TreeSet<Text> splits =
              splitsToAdd.stream().map(Text::new).collect(Collectors.toCollection(TreeSet::new));
          client.tableOperations().addSplits(table, splits);
          // The scan server would not see these splits as it caches tablet info for a bit
          getCluster().getClusterControl().stopAllServers(SCAN_SERVER);
          getCluster().getClusterControl().startAllServers(SCAN_SERVER);
        }
        // try tablet and scan server to ensure both have same behavior
        for (var cl : ConsistencyLevel.values()) {
          log.debug("Starting scan {} {}", cl, splitsToAdd);
          try (var scanner = client.createScanner(table)) {
            iteratorSetting.addOption(ErrorThrowingIterator.NAME, random.nextLong() + "");
            scanner.addScanIterator(iteratorSetting);
            scanner.setConsistencyLevel(cl);
            assertEquals(10, scanner.stream().count());
          }

          log.debug("Starting batch scan {} {}", cl, splitsToAdd);
          iteratorSetting.addOption(ErrorThrowingIterator.NAME, random.nextLong() + "");
          try (var scanner = client.createBatchScanner(table)) {
            scanner.setRanges(List.of(new Range()));
            scanner.addScanIterator(iteratorSetting);
            scanner.setConsistencyLevel(cl);
            assertEquals(10, scanner.stream().count());
          }
        }
      }

      // ensure a repeating IOException in an iterator times out eventually
      iteratorSetting.addOption(ErrorThrowingIterator.TIMES, "1000000");
      var executor = Executors.newCachedThreadPool();
      try {
        List<Future<?>> futures = new ArrayList<>();
        for (var consistencyLevel : List.of(IMMEDIATE, EVENTUAL)) {
          iteratorSetting.addOption(ErrorThrowingIterator.NAME, random.nextLong() + "");
          futures.add(executor
              .submit(() -> expectScanTimeout(client, table, consistencyLevel, iteratorSetting)));
          iteratorSetting.addOption(ErrorThrowingIterator.NAME, random.nextLong() + "");
          futures.add(executor.submit(
              () -> expectBatchScanTimeout(client, table, consistencyLevel, iteratorSetting)));
        }

        for (var future : futures) {
          future.get();
        }
      } finally {
        executor.shutdownNow();
      }

    }
  }

  @Test
  public void testIOExceptionDuringScanFileOpen() throws Exception {

    getCluster().getClusterControl().startAllServers(SCAN_SERVER);

    final String table = getUniqueNames(1)[0];
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.tableOperations().create(table);

      try (var writer = client.createBatchWriter(table)) {
        for (int i = 0; i < 10; i++) {
          Mutation m = new Mutation("row" + i);
          m.put("", "", "");
          writer.addMutation(m);
        }
      }

      client.tableOperations().flush(table, null, null, true);

      var ctx = getCluster().getServerContext();
      var tableId = ctx.getTableId(table);

      // Delete the tablets file to cause an IOException during opening the file. By default
      // scanners will retry indefinitely when an IOException happens. Test setting a timeout on the
      // scans for this case.
      try (var tablets = ctx.getAmple().readTablets().forTable(tableId).fetch(FILES).build()) {
        var tabletList = tablets.stream().collect(Collectors.toList());
        assertEquals(1, tabletList.size());
        for (var tablet : tabletList) {
          var file = tablet.getFiles().stream().collect(MoreCollectors.onlyElement());
          assertTrue(getCluster().getFileSystem().delete(file.getPath(), false));
        }
      }

      // Run scans all concurrently to avoid waiting on each one to timeout sequentially.
      var executor = Executors.newCachedThreadPool();
      try {
        List<Future<?>> futures = new ArrayList<>();
        for (var consistencyLevel : List.of(IMMEDIATE, EVENTUAL)) {
          futures.add(executor.submit(() -> expectScanTimeout(client, table, consistencyLevel)));
          futures
              .add(executor.submit(() -> expectBatchScanTimeout(client, table, consistencyLevel)));
        }

        for (var future : futures) {
          future.get();
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private static void expectBatchScanTimeout(AccumuloClient client, String table,
      ConsistencyLevel consistencyLevel, IteratorSetting... iters) {
    try (var scanner = client.createBatchScanner(table)) {
      scanner.setRanges(List.of(new Range()));
      scanner.setTimeout(5, TimeUnit.SECONDS);
      scanner.setConsistencyLevel(consistencyLevel);
      for (var iter : iters) {
        scanner.addScanIterator(iter);
      }
      Timer timer = Timer.startNew();
      assertThrows(TimedOutException.class, () -> scanner.stream().count());
      long elapsed = timer.elapsed(TimeUnit.MILLISECONDS);
      assertTrue(elapsed >= 5000, () -> "elapsed : " + elapsed);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static void expectScanTimeout(AccumuloClient client, String table,
      ConsistencyLevel consistencyLevel, IteratorSetting... iters) {
    try (var scanner = client.createScanner(table)) {
      scanner.setTimeout(5, TimeUnit.SECONDS);
      scanner.setConsistencyLevel(consistencyLevel);
      for (var iter : iters) {
        scanner.addScanIterator(iter);
      }
      Timer timer = Timer.startNew();
      var exception = assertThrows(RuntimeException.class, () -> scanner.stream().count());
      assertEquals(ThriftScanner.ScanTimedOutException.class, exception.getCause().getClass());
      long elapsed = timer.elapsed(TimeUnit.MILLISECONDS);
      assertTrue(elapsed >= 5000, () -> "elapsed : " + elapsed);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}

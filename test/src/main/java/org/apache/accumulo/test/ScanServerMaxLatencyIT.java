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

import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel.EVENTUAL;
import static org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel.IMMEDIATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class ScanServerMaxLatencyIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.SSERV_CACHED_TABLET_METADATA_EXPIRATION, "2s");
  }

  @SuppressWarnings("deprecation")
  private static Property IDLE_MINC_PROP = Property.TABLE_MINC_COMPACT_IDLETIME;

  @Test
  public void testMaxLatency() throws Exception {
    final String[] tables = this.getUniqueNames(4);
    final String table1 = tables[0];
    final String table2 = tables[1];
    final String table3 = tables[2];
    final String table4 = tables[3];

    getCluster().getConfig().setNumScanServers(1);
    getCluster().getClusterControl().startAllServers(ServerType.SCAN_SERVER);

    ExecutorService executor = Executors.newCachedThreadPool();
    try (var client = Accumulo.newClient().from(getClientProperties()).build()) {

      Wait.waitFor(() -> !client.instanceOperations().getScanServers().isEmpty());

      var ntc = new NewTableConfiguration();
      ntc.setProperties(Map.of(Property.TABLE_MINC_COMPACT_MAXAGE.getKey(), "2s"));
      client.tableOperations().create(table1, ntc);
      client.tableOperations().create(table2);
      ntc = new NewTableConfiguration();
      ntc.setProperties(Map.of(IDLE_MINC_PROP.getKey(), "2s"));
      client.tableOperations().create(table3, ntc);
      ntc = new NewTableConfiguration();
      ntc.setProperties(Map.of(Property.TABLE_MINC_COMPACT_MAXAGE.getKey(), "3s"));
      client.tableOperations().create(table4, ntc);

      Timer timer = Timer.startNew();

      // Write to table4 once, this is different than the other tables that are continually being
      // written to. table4 should minor compact 3 seconds after this write.
      writeElapsed(new SecureRandom(), client, table4, timer);
      boolean sawDataInTable4 = false;

      List<Future<Void>> futures = new ArrayList<>();
      futures.add(executor.submit(createWriterTask(client, table1, timer)));
      futures.add(executor.submit(createWriterTask(client, table2, timer)));
      futures.add(executor.submit(createWriterTask(client, table3, timer)));

      // wait for some data to be written
      Wait.waitFor(() -> readMaxElapsed(client, IMMEDIATE, table1) > 0
          && readMaxElapsed(client, IMMEDIATE, table2) > 0
          && readMaxElapsed(client, IMMEDIATE, table3) > 0);

      long lastMaxSeen = -1;
      int changes = 0;

      List<Long> deltas = new ArrayList<>();

      while (changes < 4) {
        Thread.sleep(250);
        var currElapsed = timer.elapsed(TimeUnit.MILLISECONDS);
        var maxElapsedInTable = readMaxElapsed(client, EVENTUAL, table1);

        if (maxElapsedInTable > 0 && maxElapsedInTable != lastMaxSeen) {
          log.info("new max elapsed seen {} {}", lastMaxSeen, maxElapsedInTable);
          changes++;
          lastMaxSeen = maxElapsedInTable;
        }

        if (maxElapsedInTable > 0) {
          // This is difference in elapsed time written to the table vs the most recent elapsed
          // time.
          deltas.add(currElapsed - maxElapsedInTable);
        }

        // The other table does not have the setting to minor compact based on age, so should never
        // see any data for it from the scan server.
        assertEquals(-1, readMaxElapsed(client, EVENTUAL, table2));
        // The background thread is writing to this table every 100ms so it should not be considered
        // idle and therefor should not minor compact.
        assertEquals(-1, readMaxElapsed(client, EVENTUAL, table3));

        if (!sawDataInTable4 && readMaxElapsed(client, EVENTUAL, table4) != -1) {
          assertTrue(timer.elapsed(TimeUnit.MILLISECONDS) > 3000
              && timer.elapsed(TimeUnit.MILLISECONDS) < 6000);
          sawDataInTable4 = true;
        }
      }

      assertTrue(sawDataInTable4);

      var stats = deltas.stream().mapToLong(l -> l).summaryStatistics();
      log.info("Delta stats : {}", stats);
      // Should usually see data within 4 seconds, but not always because the timings config are
      // when things should start to happen and not when they are guaranteed to finish. Would expect
      // the average to be less than 4 seconds and the max less than 8 seconds. These numbers may
      // not hold if running test on a heavily loaded machine.
      assertTrue(stats.getAverage() > 500 && stats.getAverage() < 4000);
      assertTrue(stats.getMax() < 8000);
      assertTrue(stats.getCount() > 9);

      // The write task should still be running unless they experienced an exception.
      assertTrue(futures.stream().noneMatch(Future::isDone));

      executor.shutdownNow();
      executor.awaitTermination(600, TimeUnit.SECONDS);

      assertEquals(-1, readMaxElapsed(client, EVENTUAL, table2));
      // Now that nothing is writing its expected that max read by an immediate scan will see any
      // data an eventual scan would see.
      assertTrue(
          readMaxElapsed(client, IMMEDIATE, table1) >= readMaxElapsed(client, EVENTUAL, table1));
    }
  }

  private long readMaxElapsed(AccumuloClient client, ScannerBase.ConsistencyLevel consistency,
      String table) throws Exception {
    try (var scanner = client.createScanner(table)) {
      scanner.setConsistencyLevel(consistency);
      scanner.fetchColumn(new Text("elapsed"), new Text("nanos"));
      return scanner.stream().mapToLong(e -> Long.parseLong(e.getValue().toString(), 10)).max()
          .orElse(-1);
    }
  }

  private static void writeElapsed(SecureRandom random, AccumuloClient client, String table,
      Timer timer) throws Exception {
    try (var writer = client.createBatchWriter(table)) {
      writeElapsed(random, timer, writer);
    }
  }

  private static Callable<Void> createWriterTask(AccumuloClient client, String table, Timer timer) {
    SecureRandom random = new SecureRandom();
    return () -> {
      try (var writer = client.createBatchWriter(table)) {
        while (true) {
          writeElapsed(random, timer, writer);
          writer.flush();
          Thread.sleep(100);
        }
      }
    };
  }

  private static void writeElapsed(SecureRandom random, Timer timer, BatchWriter writer)
      throws MutationsRejectedException {
    var elapsed = timer.elapsed(TimeUnit.MILLISECONDS);
    Mutation m = new Mutation(Long.toHexString(random.nextLong()));
    m.put("elapsed", "nanos", "" + elapsed);
    writer.addMutation(m);
  }
}

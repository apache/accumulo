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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class ZombieScanIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    super.configure(cfg, hadoopCoreSite);

    cfg.setNumTservers(1);
  }

  /**
   * An iterator that should get stuck forever when used
   */
  public static class ZombieIterator extends WrappingIterator {
    @Override
    public boolean hasTop() {
      // must call super.hasTop() before blocking as that will run accumulo code to setup iterator
      boolean ht = super.hasTop();
      Semaphore semaphore = new Semaphore(10);
      semaphore.acquireUninterruptibly(5);
      // this should block forever
      semaphore.acquireUninterruptibly(6);
      return ht;
    }
  }

  /**
   * This test ensure that scans threads that run forever do not prevent tablets from unloading.
   */
  @Test
  public void testZombieScan() throws Exception {

    String table = getUniqueNames(1)[0];

    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {

      var splits = new TreeSet<Text>();
      splits.add(new Text("3"));
      splits.add(new Text("5"));
      splits.add(new Text("7"));
      var ntc = new NewTableConfiguration().withSplits(splits);
      c.tableOperations().create(table, ntc);

      try (var writer = c.createBatchWriter(table)) {
        for (var row : List.of("2", "4", "6", "8")) {
          Mutation m = new Mutation(row);
          m.put("f", "q", "v");
          writer.addMutation(m);
        }
      }

      // Flush the data otherwise when the tablet attempts to close with an active scan reading from
      // the in memory map it will wait for 15 seconds for the scan
      c.tableOperations().flush(table, null, null, true);

      var executor = Executors.newCachedThreadPool();

      // start two zombie scans that should never return using a normal scanner
      List<Future<String>> futures = new ArrayList<>();
      for (var row : List.of("2", "4")) {
        var future = executor.submit(() -> {
          try (var scanner = c.createScanner(table)) {
            IteratorSetting iter = new IteratorSetting(100, "Z", ZombieIterator.class);
            scanner.addScanIterator(iter);
            scanner.setRange(new Range(row));
            return scanner.stream().findFirst().map(e -> e.getKey().getRowData().toString())
                .orElse("none");
          }
        });
        futures.add(future);
      }

      // start two zombie scans that should never return using a batch scanner
      for (var row : List.of("6", "8")) {
        var future = executor.submit(() -> {
          try (var scanner = c.createBatchScanner(table)) {
            IteratorSetting iter = new IteratorSetting(100, "Z", ZombieIterator.class);
            scanner.addScanIterator(iter);
            scanner.setRanges(List.of(new Range(row)));
            return scanner.stream().findFirst().map(e -> e.getKey().getRowData().toString())
                .orElse("none");
          }
        });
        futures.add(future);
      }

      // should eventually see the four zombie scans running against four tablets
      Wait.waitFor(() -> countDistinctTabletsScans(table, c) == 4);

      assertEquals(1, c.instanceOperations().getTabletServers().size());

      // Start 3 new tablet servers, this should cause the table to balance and the tablets with
      // zombie scans to unload. The Zombie scans should not prevent the table from unloading. The
      // scan threads will still be running on the old tablet servers.
      getCluster().getConfig().setNumTservers(4);
      getCluster().getClusterControl().startAllServers(ServerType.TABLET_SERVER);

      // Wait for all tablets servers
      Wait.waitFor(() -> c.instanceOperations().getTabletServers().size() == 4);

      // The table should eventually balance across the 4 tablet servers
      Wait.waitFor(() -> countLocations(table, c) == 4);

      // The zombie scans should still be running
      assertTrue(futures.stream().noneMatch(Future::isDone));

      // Should be able to scan all the tablets at the new locations.
      try (var scanner = c.createScanner(table)) {
        var rows = scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(Collectors.toSet());
        assertEquals(Set.of("2", "4", "6", "8"), rows);
      }

      try (var scanner = c.createBatchScanner(table)) {
        scanner.setRanges(List.of(new Range()));
        var rows = scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(Collectors.toSet());
        assertEquals(Set.of("2", "4", "6", "8"), rows);
      }

      // The zombie scans should migrate with the tablets, taking up more scan threads in the
      // system.
      Set<String> tabletSeversWithZombieScans = new HashSet<>();
      for (String tserver : c.instanceOperations().getTabletServers()) {
        if (c.instanceOperations().getActiveScans(tserver).stream()
            .flatMap(activeScan -> activeScan.getSsiList().stream())
            .anyMatch(scanIters -> scanIters.contains(ZombieIterator.class.getName()))) {
          tabletSeversWithZombieScans.add(tserver);
        }
      }
      assertEquals(4, tabletSeversWithZombieScans.size());

      executor.shutdownNow();
    }

  }

  private static long countLocations(String table, AccumuloClient client) throws Exception {
    var ctx = (ClientContext) client;
    var tableId = ctx.getTableId(table);
    return ctx.getAmple().readTablets().forTable(tableId).build().stream()
        .map(TabletMetadata::getLocation).filter(Objects::nonNull).distinct().count();
  }

  private static long countDistinctTabletsScans(String table, AccumuloClient client)
      throws Exception {
    var tservers = client.instanceOperations().getTabletServers();
    long count = 0;
    for (String tserver : tservers) {
      count += client.instanceOperations().getActiveScans(tserver).stream()
          .filter(activeScan -> activeScan.getTable().equals(table))
          .map(activeScan -> activeScan.getTablet()).distinct().count();
    }
    return count;
  }

}

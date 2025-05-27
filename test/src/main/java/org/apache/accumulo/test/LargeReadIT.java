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

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

/**
 * Accumulo had a bug where the native map code would always read the first key in a tablet even if
 * the scan did not need it. This test is structured so that if the native map does this the tablet
 * server will fail w/ an out of memory exception.
 */
public class LargeReadIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(6);
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.TSERV_SCAN_EXECUTORS_DEFAULT_THREADS, "64");
    cfg.setProperty(Property.TSERV_MINTHREADS, "64");
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, "true");
    cfg.setProperty(Property.TSERV_MAXMEM, "100M");
    cfg.setMemory(ServerType.TABLET_SERVER, 128, MemoryUnit.MEGABYTE);
    cfg.setNumTservers(1);
  }

  @Test
  public void testLargeMemoryLG() throws Exception {
    // if locality groups are not set then this test will fail because all the scans will read the
    // big key value in to memory and then filter it
    Map<String,Set<Text>> groups =
        Map.of("big", Set.of(new Text("big")), "small", Set.of(new Text("small")));
    NewTableConfiguration config = new NewTableConfiguration().setLocalityGroups(groups);

    Consumer<Scanner> scanConfigurer = scanner -> {
      // setting this column family plus the locality group settings should exclude the large value
      // at row=001 family=big
      scanner.fetchColumnFamily("small");
    };

    testLargeMemory(config, scanConfigurer);
  }

  @Test
  public void testLargeMemoryRange() throws Exception {
    NewTableConfiguration config = new NewTableConfiguration();

    Consumer<Scanner> scanConfigurer = scanner -> {
      // This range should exclude the large value at row=001 family=big from ever being read
      scanner.setRange(new Range(new Key("000", "small", ""), false, null, true));
    };

    testLargeMemory(config, scanConfigurer);
  }

  private void testLargeMemory(NewTableConfiguration config, Consumer<Scanner> scannerConfigurer)
      throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];

      client.tableOperations().create(tableName, config);

      try (var writer = client.createBatchWriter(tableName)) {
        var bigValue = new Mutation("000");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10_000_000; i++) {
          sb.append((i % 10));
        }
        // This is the first key in the tablet, it sorts before everything else.
        bigValue.put("big", "number", sb.toString());
        writer.addMutation(bigValue);

        for (int i = 0; i < 100; i++) {
          String row = String.format("%03d", i);
          var smallValue = new Mutation(row);
          smallValue.put("small", "number", i + "");
          writer.addMutation(smallValue);
        }
      }

      final int numThreads = 64;
      var executor = Executors.newFixedThreadPool(numThreads);
      Callable<Long> scanTask = () -> {
        try (var scanner = client.createScanner(tableName)) {
          scannerConfigurer.accept(scanner);
          return scanner.stream().count();
        }
      };

      // Run lots of concurrent task that should only read the small data, if they read the big
      // column family then it will exceed the tablet server memory and cause it to die and the test
      // to timeout.
      var tasks =
          Stream.iterate(scanTask, t -> t).limit(numThreads * 5).collect(Collectors.toList());
      assertEquals(numThreads * 5, tasks.size());
      for (var future : executor.invokeAll(tasks)) {
        assertEquals(100, future.get());
      }

      // expected data to be in memory for this part of the test, so verify that
      var ctx = getCluster().getServerContext();
      try (var tablets = ctx.getAmple().readTablets().forTable(ctx.getTableId(tableName))
          .fetch(TabletMetadata.ColumnType.FILES).build()) {
        assertEquals(0, tablets.stream().count());
      }

      // flush data and verify
      client.tableOperations().flush(tableName, null, null, true);
      try (var tablets = ctx.getAmple().readTablets().forTable(ctx.getTableId(tableName))
          .fetch(TabletMetadata.ColumnType.FILES).build()) {
        assertEquals(1, tablets.stream().count());
      }

      // Run the scans again, reading from files instead of in memory map... verify the large data
      // is not brought into memory from the file, which would kill the tablet server. The test was
      // created because of a bug in the native map code, but can also check the rfile code for a
      // similar problem.
      for (var future : executor.invokeAll(tasks)) {
        assertEquals(100, future.get());
      }

      // this test assumes the first key in the tablet is the big one, verify this assumption
      try (var scanner = client.createScanner(tableName)) {
        assertEquals(10_000_000, scanner.iterator().next().getValue().getSize());
      }
    }
  }
}

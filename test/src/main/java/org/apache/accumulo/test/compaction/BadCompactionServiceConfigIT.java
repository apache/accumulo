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
package org.apache.accumulo.test.compaction;

import static org.apache.accumulo.core.Constants.DEFAULT_COMPACTION_SERVICE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.spi.compaction.DefaultCompactionPlanner;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.MoreCollectors;

public class BadCompactionServiceConfigIT extends AccumuloClusterHarness {

  private static final String CSP = Property.TSERV_COMPACTION_SERVICE_PREFIX.getKey();

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    Map<String,String> siteCfg = new HashMap<>();
    siteCfg.put(CSP + "cs1.planner", DefaultCompactionPlanner.class.getName());
    // place invalid json in the planners config
    siteCfg.put(CSP + "cs1.planner.opts.executors", "{{'name]");
    cfg.setSiteConfig(siteCfg);
  }

  public static class EverythingFilter extends Filter {
    @Override
    public boolean accept(Key k, Value v) {
      return false;
    }
  }

  private ExecutorService executorService;

  @BeforeEach
  public void setup() {
    executorService = Executors.newCachedThreadPool();
  }

  @AfterEach
  public void teardown() {
    executorService.shutdownNow();
  }

  @Test
  public void testUsingMisconfiguredService() throws Exception {
    String table = getUniqueNames(1)[0];

    // Create a table that is configured to use a compaction service with bad configuration.
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(
          Map.of(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "cs1"));
      client.tableOperations().create(table, ntc);

      try (var writer = client.createBatchWriter(table)) {
        writer.addMutation(new Mutation("0").at().family("f").qualifier("q").put("v"));
      }

      client.tableOperations().flush(table, null, null, true);

      try (var scanner = client.createScanner(table)) {
        assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(MoreCollectors.onlyElement()));
      }

      Future<?> fixerFuture = executorService.submit(() -> {
        try {
          Thread.sleep(2000);

          // Verify the compaction has not run yet, it should not be able to with the bad config.
          try (var scanner = client.createScanner(table)) {
            assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
                .collect(MoreCollectors.onlyElement()));
          }

          var value =
              "[{'name':'small', 'type': 'internal', 'numThreads':1}]".replaceAll("'", "\"");
          client.instanceOperations().setProperty(CSP + "cs1.planner.opts.executors", value);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      List<IteratorSetting> iterators =
          Collections.singletonList(new IteratorSetting(100, EverythingFilter.class));
      client.tableOperations().compact(table,
          new CompactionConfig().setIterators(iterators).setWait(true));

      // Verify compaction ran.
      try (var scanner = client.createScanner(table)) {
        assertEquals(0, scanner.stream().count());
      }

      fixerFuture.get();

      // misconfigure the service, test how going from good config to bad config works. The test
      // started with an initial state of bad config.
      client.instanceOperations().setProperty(CSP + "cs1.planner.opts.executors", "]o.o[");
      try (var writer = client.createBatchWriter(table)) {
        writer.addMutation(new Mutation("0").at().family("f").qualifier("q").put("v"));
      }
      client.tableOperations().flush(table, null, null, true);
      try (var scanner = client.createScanner(table)) {
        assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(MoreCollectors.onlyElement()));
      }
      fixerFuture = executorService.submit(() -> {
        try {
          Thread.sleep(2000);
          var value =
              "[{'name':'small', 'type': 'internal', 'numThreads':1}]".replaceAll("'", "\"");
          client.instanceOperations().setProperty(CSP + "cs1.planner.opts.executors", value);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      client.tableOperations().compact(table,
          new CompactionConfig().setIterators(iterators).setWait(true));

      // Verify compaction ran.
      try (var scanner = client.createScanner(table)) {
        assertEquals(0, scanner.stream().count());
      }

      fixerFuture.get();

    }
  }

  @Test
  public void testUsingNonExistentService() throws Exception {
    String table = getUniqueNames(1)[0];

    // Create a table that is configured to use a compaction service that does not exist
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      NewTableConfiguration ntc = new NewTableConfiguration().setProperties(
          Map.of(Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service", "cs5"));
      client.tableOperations().create(table, ntc);

      // Add splits so that the tserver logs can manually be inspected to ensure they are not
      // spammed. Not sure how to check this automatically.
      var splits = IntStream.range(1, 10).mapToObj(i -> new Text(i + ""))
          .collect(Collectors.toCollection(TreeSet::new));
      client.tableOperations().addSplits(table, splits);

      try (var writer = client.createBatchWriter(table)) {
        writer.addMutation(new Mutation("0").at().family("f").qualifier("q").put("v"));
      }

      client.tableOperations().flush(table, null, null, true);

      try (var scanner = client.createScanner(table)) {
        assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(MoreCollectors.onlyElement()));
      }

      // Create a thread to fix the compaction config after a bit.
      Future<?> fixerFuture = executorService.submit(() -> {
        try {
          Thread.sleep(2000);

          // Verify the compaction has not run yet, it should not be able to with the bad config.
          try (var scanner = client.createScanner(table)) {
            assertEquals("0", scanner.stream().map(e -> e.getKey().getRowData().toString())
                .collect(MoreCollectors.onlyElement()));
          }

          // fix the compaction dispatcher config
          client.tableOperations().setProperty(table,
              Property.TABLE_COMPACTION_DISPATCHER_OPTS.getKey() + "service",
              DEFAULT_COMPACTION_SERVICE_NAME);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      List<IteratorSetting> iterators =
          Collections.singletonList(new IteratorSetting(100, EverythingFilter.class));
      client.tableOperations().compact(table,
          new CompactionConfig().setIterators(iterators).setWait(true));

      // Verify compaction ran.
      try (var scanner = client.createScanner(table)) {
        assertEquals(0, scanner.stream().count());
      }

      fixerFuture.get();

    }
  }
}

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.manager.ManagerWorker;
import org.apache.accumulo.manager.fate.FateManager;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class MultipleManagerIT extends ConfigurableMacBase {
  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // TODO add a way to start multiple managers to mini
    super.configure(cfg, hadoopCoreSite);
  }

  @Test
  public void test() throws Exception {

    List<Process> managerWorkers = new ArrayList<>();
    // start two fate workers initially
    for (int i = 0; i < 2; i++) {
      managerWorkers.add(exec(ManagerWorker.class));
    }

    var executor = Executors.newCachedThreadPool();

    // This assigns fate partitions to fate worker processes, run it in a background thread.
    var fateMgr = new FateManager(getServerContext());
    var future = executor.submit(() -> {
      fateMgr.managerWorkers();
      return null;
    });

    Thread.sleep(30_000);
    // start more fate workers, should see the partitions be shuffled eventually
    for (int i = 0; i < 3; i++) {
      managerWorkers.add(exec(ManagerWorker.class));
    }

    try (var client = Accumulo.newClient().from(getClientProperties()).build()) {
      var splits = IntStream.range(1, 10).mapToObj(i -> String.format("%03d", i)).map(Text::new)
          .collect(Collectors.toCollection(TreeSet::new));
      var tableOpFutures = new ArrayList<Future<?>>();
      for (int i = 0; i < 30; i++) {
        var table = "t" + i;
        // TODO seeing in the logs that fate operations for the same table are running on different
        // processes, however there is a 5 second delay because there is no notification mechanism
        // currently.

        // TODO its hard to find everything related to a table id in the logs, especially when the
        // table id is like "b". Was tring to follow a single table across multiple manager workers
        // processes.
        var tableOpsFuture = executor.submit(() -> {
          client.tableOperations().create(table);
          log.info("Created table {}", table);
          var expectedRows = new HashSet<String>();
          try (var writer = client.createBatchWriter(table)) {
            for (int r = 0; r < 10; r++) {
              var row = String.format("%03d", r);
              expectedRows.add(row);
              Mutation m = new Mutation(row);
              m.put("f", "q", "v");
              writer.addMutation(m);
            }
          }
          log.info("Wrote data to table {}", table);
          client.tableOperations().addSplits(table, splits);
          log.info("Split table {}", table); // TODO split operation does not log table id and fate
                                             // opid anywhere
          client.tableOperations().compact(table, new CompactionConfig().setWait(true));
          log.info("Compacted table {}", table);
          client.tableOperations().merge(table, null, null);
          log.info("Merged table {}", table);
          try (var scanner = client.createScanner(table)) {
            var rowsSeen = scanner.stream().map(e -> e.getKey().getRowData().toString())
                .collect(Collectors.toSet());
            assertEquals(expectedRows, rowsSeen);
            log.info("verified table {}", table);
          }
          return null;
        });
        tableOpFutures.add(tableOpsFuture);
      }

      for (var tof : tableOpFutures) {
        tof.get();
      }
    }

    FateManager.stop.set(true);

    future.get();

    executor.shutdown();

    System.out.println("DONE");
    // TODO kill processes
  }
}

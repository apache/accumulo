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

import java.time.Duration;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test that verifies the behavior of {@link Property#TSERV_WAL_MAX_REFERENCED}.
 * <p>
 * This test creates a table with splits and writes data in batches until the number of WALs in use
 * exceeds the configured limit. It then waits for minor compactions to reduce the WAL count.
 */
public class MaxWalReferencedIT extends ConfigurableMacBase {
  private static final Logger log = LoggerFactory.getLogger(MaxWalReferencedIT.class);

  final int WAL_MAX_REFERENCED = 3;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    final String hdfsMinBlockSize = "1048576";

    // Set a small WAL size so we roll frequently
    cfg.setProperty(Property.TSERV_WAL_MAX_SIZE, hdfsMinBlockSize);
    // Set the max number of WALs that can be referenced
    cfg.setProperty(Property.TSERV_WAL_MAX_REFERENCED, Integer.toString(WAL_MAX_REFERENCED));
    cfg.setNumTservers(1);

    // Use raw local file system so WAL syncs and flushes work as expected
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void testWALMaxReferenced() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];

      SortedSet<Text> splits = new TreeSet<>();
      for (int i = 1; i <= 4; i++) {
        splits.add(new Text(Integer.toString(i)));
      }
      client.tableOperations().create(tableName, new NewTableConfiguration().withSplits(splits));

      log.info("Created table {} with splits. Now writing data.", tableName);

      // Write data multiple times until we see the WAL count exceed WAL_MAX_REFERENCED
      final int rowsPerIteration = 30000;
      AtomicInteger iteration = new AtomicInteger(0);
      Wait.waitFor(() -> {
        int startRow = iteration.get() * rowsPerIteration;
        int endRow = (iteration.get() + 1) * rowsPerIteration;

        // Write data that should fill or partially fill the WAL
        writeData(client, tableName, startRow, endRow);

        // Check the current number of WALs in use
        int walCount = getWalCount(getServerContext());
        log.info("After iteration {}, wrote rows [{}..{}), WAL count is {}", iteration, startRow,
            endRow, walCount);
        iteration.getAndIncrement();

        if (walCount > WAL_MAX_REFERENCED) {
          log.info("Reached WAL count of {}, now wait for minor compactions to reduce WAL count",
              walCount);
          return true;
        } else {
          return false;
        }
      }, 60000, 250, "Expected to see WAL count exceed " + WAL_MAX_REFERENCED);

      // wait for minor compactions to reduce the WAL count
      Wait.waitFor(() -> getWalCount(getServerContext()) <= WAL_MAX_REFERENCED, 30000, 1000,
          "WAL count never dropped within 30 seconds");
    }
  }

  private void writeData(AccumuloClient client, String table, int startRow, int endRow)
      throws Exception {
    try (BatchWriter bw = client.createBatchWriter(table, new BatchWriterConfig())) {
      for (int r = startRow; r < endRow; r++) {
        Mutation m = new Mutation(String.format("row_%07d", r));
        m.put("cf", "cq", String.format("val_%d", r));
        bw.addMutation(m);
      }
    }
  }

  private int getWalCount(ServerContext context) throws Exception {
    WalStateManager wals = new WalStateManager(context);
    int total = 0;
    for (Entry<TServerInstance,List<UUID>> entry : wals.getAllMarkers().entrySet()) {
      total += entry.getValue().size();
    }
    return total;
  }

}

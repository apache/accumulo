/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.conf.Property.TSERV_WRITE_THREADS_MAX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteThreadsIT extends AccumuloClusterHarness {

  ThreadPoolExecutor tpe;
  private static final Logger log = LoggerFactory.getLogger(WriteThreadsIT.class);

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // sets the thread limit on the SERVER SIDE
    // default value is 0. when set to 0, there is no limit
    cfg.setProperty(TSERV_WRITE_THREADS_MAX.getKey(), "10");
  }

  @Test
  public void test() throws Exception {
    write();
  }

  public void write() throws Exception {
    // each thread creates a batch writer, adds mutations, and then flushes.
    final int threadCount = 100;

    // Reads and writes from Accumulo
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxWriteThreads(1_000); // this is the max write threads on the CLIENT SIDE

    try (AccumuloClient client =
        Accumulo.newClient().from(getClientProps()).batchWriterConfig(config).build()) {

      tpe = new ThreadPoolExecutor(threadCount, threadCount, 0, TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(threadCount));

      final String[] tables = getUniqueNames(threadCount);

      for (int i = 0; i < threadCount; i++) {
        if (i % 10 == 0)
          log.debug("iteration: {}", i);

        final String tableName = tables[i];
        client.tableOperations().create(tableName);

        Runnable r = () -> {
          try (BatchWriter writer = client.createBatchWriter(tableName)) {

            // Data is written to mutation objects
            List<Mutation> mutations = Stream.of("row1", "row2", "row3", "row4", "row5")
                .map(Mutation::new).collect(Collectors.toList());

            for (Mutation mutation : mutations) {
              mutation.at().family("myColFam").qualifier("myColQual").put("myValue1");
              mutation.at().family("myColFam").qualifier("myColQual").put("myValue2");
              writer.addMutation(mutation);
            }

            // Sends buffered mutation to Accumulo immediately (write executed)
            writer.flush();
          } catch (Exception e) {
            log.error("ERROR WRITING MUTATION", e);
          }
        };
        // Runnable above is executed
        tpe.execute(r);
      }

      tpe.shutdown();
      assertTrue(tpe.awaitTermination(90, TimeUnit.SECONDS));

      validateData(client, threadCount, tables);
    }
  }

  private void validateData(AccumuloClient client, int threadCount, String[] tables)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    for (int i = 0; i < threadCount; i++) {
      try (var batchScanner = client.createBatchScanner(tables[i])) {
        batchScanner.setRanges(List.of(new Range()));

        List<String> rows = new ArrayList<>();
        for (var e : batchScanner) {
          rows.add(e.getKey().getRow().toString());
        }
        assertEquals(5, rows.size(), "Wrong number of rows returned.");
        assertTrue(rows.containsAll(List.of("row1", "row2", "row3", "row4", "row5")));
      }
    }
  }

}

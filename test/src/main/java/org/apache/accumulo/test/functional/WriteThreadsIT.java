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

import static org.apache.accumulo.core.conf.Property.TSERV_MAX_WRITETHREADS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
    cfg.setProperty(TSERV_MAX_WRITETHREADS.getKey(), "10");
  }

  @Test
  public void test() throws Exception {
    write();
  }

  public void write() throws Exception {
    // each thread create a batch writer, add a mutation, and then flush.
    int threads = 100;
    int max = 1000;

    // Reads and writes from Accumulo
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxWriteThreads(max); // this is the max write threads on the CLIENT SIDE

    try (AccumuloClient client =
        Accumulo.newClient().from(getClientProps()).batchWriterConfig(config).build()) {

      tpe = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(threads));

      for (int i = 0; i < threads; i++) {
        if (i % 10 == 0)
          log.debug("iteration: " + i);
        String tableName = "table" + i;
        client.tableOperations().create(tableName);

        Runnable r = () -> {
          try (BatchWriter writer = client.createBatchWriter(tableName)) {
            // Data is written to a mutation object
            Mutation mutation1 = new Mutation("row1");
            Mutation mutation2 = new Mutation("row2");
            Mutation mutation3 = new Mutation("row3");
            Mutation mutation4 = new Mutation("row4");
            Mutation mutation5 = new Mutation("row5");

            mutation1.at().family("myColFam").qualifier("myColQual").put("myValue1");
            mutation1.at().family("myColFam").qualifier("myColQual").put("myValue2");
            mutation2.at().family("myColFam").qualifier("myColQual").put("myValue1");
            mutation2.at().family("myColFam").qualifier("myColQual").put("myValue2");
            mutation3.at().family("myColFam").qualifier("myColQual").put("myValue1");
            mutation3.at().family("myColFam").qualifier("myColQual").put("myValue2");
            mutation4.at().family("myColFam").qualifier("myColQual").put("myValue1");
            mutation4.at().family("myColFam").qualifier("myColQual").put("myValue2");
            mutation5.at().family("myColFam").qualifier("myColQual").put("myValue1");
            mutation5.at().family("myColFam").qualifier("myColQual").put("myValue2");

            // Queues the mutation to write (adds to batch)
            writer.addMutation(mutation1);
            writer.addMutation(mutation2);
            writer.addMutation(mutation3);
            writer.addMutation(mutation4);
            writer.addMutation(mutation5);

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

      validateData(client, threads);
    }
  }

  private void validateData(AccumuloClient client, int threads)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    for (int i = 0; i < threads; i++) {
      var batchScanner = client.createBatchScanner("table" + i);
      batchScanner.setRanges(List.of(new Range()));

      // batchScanner.fetchColumn(new IteratorSetting.Column("myColFam", "myColQual"));
      List<String> rows = new ArrayList<>();
      for (var e : batchScanner) {
        rows.add(e.getKey().getRow().toString());
      }
      assertEquals(5, rows.size(), "Wrong number of rows returned.");
      assertTrue(rows.contains("row1"));
      assertTrue(rows.contains("row2"));
      assertTrue(rows.contains("row3"));
      assertTrue(rows.contains("row4"));
      assertTrue(rows.contains("row5"));
    }
  }

}

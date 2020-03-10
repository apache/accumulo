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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.clientImpl.TabletServerBatchWriter;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteThreadsIT extends AccumuloClusterHarness {

  ThreadPoolExecutor tpe;
  private static final Logger log = LoggerFactory.getLogger(TabletServerBatchWriter.class);
  private int i;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // this function runs before test()

    // sets the thread limit on the SERVER SIDE
    // default value is 0. when set to 0, there is no limit
    cfg.setProperty(TSERV_MAX_WRITETHREADS.getKey(), "0");
  }

  @Test
  public void test() throws Exception {
    write();
  }

  public void write() throws Exception {
    // each thread create a batch writer, add a mutation, and then flush.
    int threads = 100;
    int max = 100;

    // Reads and writes from Accumulo
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxWriteThreads(max); // this is the max write threads on the CLIENT SIDE

    try (AccumuloClient client =
        Accumulo.newClient().from(getClientProps()).batchWriterConfig(config).build()) {

      tpe = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(threads));

      for (i = 0; i < threads; i++) {

        log.info("iteration: " + i);
        String tableName = "table" + i;
        client.tableOperations().create(tableName);

        Runnable r = () -> {
          try (BatchWriter writer = client.createBatchWriter(tableName)) {

            client.tableOperations().addConstraint(tableName, SlowConstraint.class.getName());


            log.info("START OF RUNNABLE");

            // Data is written to a mutation object
            Mutation mutation1 = new Mutation("row1");
            Mutation mutation2 = new Mutation("row2");
            Mutation mutation3 = new Mutation("row3");
            Mutation mutation4 = new Mutation("row4");
            Mutation mutation5 = new Mutation("row5");


            mutation1.at().family("myColFam").qualifier("myColQual").visibility("public")
                .put("myValue1");
            mutation1.at().family("myColFam").qualifier("myColQual").visibility("public")
                .put("myValue2");
            mutation2.at().family("myColFam").qualifier("myColQual").visibility("public")
                .put("myValue1");
            mutation2.at().family("myColFam").qualifier("myColQual").visibility("public")
                .put("myValue2");
            mutation3.at().family("myColFam").qualifier("myColQual").visibility("public")
                    .put("myValue1");
            mutation3.at().family("myColFam").qualifier("myColQual").visibility("public")
                    .put("myValue2");
            mutation4.at().family("myColFam").qualifier("myColQual").visibility("public")
                    .put("myValue1");
            mutation4.at().family("myColFam").qualifier("myColQual").visibility("public")
                    .put("myValue2");
            mutation5.at().family("myColFam").qualifier("myColQual").visibility("public")
                    .put("myValue1");
            mutation5.at().family("myColFam").qualifier("myColQual").visibility("public")
                    .put("myValue2");


            log.info("AFTER MUTATIONS PUT TO TABLE");

            // Queues the mutation to write (adds to batch)
            writer.addMutation(mutation1);
            writer.addMutation(mutation2);
            writer.addMutation(mutation3);
            writer.addMutation(mutation4);
            writer.addMutation(mutation5);

            log.info("AFTER ADD");

            // Sends buffered mutation to Accumulo immediately (write executed)
            writer.flush();

            log.info("AFTER FLUSH");
            log.info("END OF RUNNABLE");

          } catch (Exception e) {
            log.error("ERROR WRITING MUTATION");
          }
        };
        // Runnable above is executed
        tpe.execute(r);
      }

      tpe.shutdown();
      tpe.awaitTermination(90, TimeUnit.SECONDS);

    }
  }

}

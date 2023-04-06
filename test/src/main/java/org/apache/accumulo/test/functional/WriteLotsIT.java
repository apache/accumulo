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

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestIngest.IngestParams;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.VerifyIngest.VerifyParams;
import org.junit.jupiter.api.Test;

public class WriteLotsIT extends AccumuloClusterHarness {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofSeconds(90);
  }

  @Test
  public void writeLots() throws Exception {
    BatchWriterConfig bwConfig = new BatchWriterConfig();
    bwConfig.setMaxMemory(1024L * 1024);
    bwConfig.setMaxWriteThreads(2);
    try (AccumuloClient c =
        Accumulo.newClient().from(getClientProps()).batchWriterConfig(bwConfig).build()) {
      final String tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      final AtomicReference<Exception> ref = new AtomicReference<>();
      final int THREADS = 5;
      ThreadPoolExecutor tpe = new ThreadPoolExecutor(0, THREADS, 0, TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(THREADS));
      for (int i = 0; i < THREADS; i++) {
        final int index = i;
        Runnable r = () -> {
          try {
            IngestParams ingestParams = new IngestParams(getClientProps(), tableName, 10_000);
            ingestParams.startRow = index * 10000;
            TestIngest.ingest(c, ingestParams);
          } catch (Exception ex) {
            ref.set(ex);
          }
        };
        tpe.execute(r);
      }
      tpe.shutdown();
      tpe.awaitTermination(90, TimeUnit.SECONDS);
      if (ref.get() != null) {
        throw ref.get();
      }
      VerifyParams params = new VerifyParams(getClientProps(), tableName, 10_000 * THREADS);
      VerifyIngest.verifyIngest(c, params);
    }
  }
}

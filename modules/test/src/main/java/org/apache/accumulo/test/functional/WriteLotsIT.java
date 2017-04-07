/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test.functional;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.cli.BatchWriterOpts;
import org.apache.accumulo.core.cli.ScannerOpts;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.VerifyIngest;
import org.junit.Test;

public class WriteLotsIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 90;
  }

  @Test
  public void writeLots() throws Exception {
    final Connector c = getConnector();
    final String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    final AtomicReference<Exception> ref = new AtomicReference<>();
    final ClientConfiguration clientConfig = getCluster().getClientConfig();
    final int THREADS = 5;
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(0, THREADS, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(THREADS));
    for (int i = 0; i < THREADS; i++) {
      final int index = i;
      Runnable r = new Runnable() {
        @Override
        public void run() {
          try {
            TestIngest.Opts opts = new TestIngest.Opts();
            opts.startRow = index * 10000;
            opts.rows = 10000;
            opts.setTableName(tableName);
            if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
              opts.updateKerberosCredentials(clientConfig);
            } else {
              opts.setPrincipal(getAdminPrincipal());
            }
            BatchWriterOpts bwOpts = new BatchWriterOpts();
            bwOpts.batchMemory = 1024L * 1024;
            bwOpts.batchThreads = 2;
            TestIngest.ingest(c, opts, new BatchWriterOpts());
          } catch (Exception ex) {
            ref.set(ex);
          }
        }
      };
      tpe.execute(r);
    }
    tpe.shutdown();
    tpe.awaitTermination(90, TimeUnit.SECONDS);
    if (ref.get() != null) {
      throw ref.get();
    }
    VerifyIngest.Opts vopts = new VerifyIngest.Opts();
    vopts.rows = 10000 * THREADS;
    vopts.setTableName(tableName);
    if (clientConfig.getBoolean(ClientProperty.INSTANCE_RPC_SASL_ENABLED.getKey(), false)) {
      vopts.updateKerberosCredentials(clientConfig);
    } else {
      vopts.setPrincipal(getAdminPrincipal());
    }
    VerifyIngest.verifyIngest(c, vopts, new ScannerOpts());
  }

}

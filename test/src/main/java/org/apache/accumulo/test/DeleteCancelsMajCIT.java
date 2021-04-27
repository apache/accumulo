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
package org.apache.accumulo.test;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.functional.SlowIterator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteCancelsMajCIT extends SharedMiniClusterBase {
  private static final Logger log = LoggerFactory.getLogger(DeleteCancelsMajCIT.class);

  AtomicBoolean started = new AtomicBoolean(false);
  AtomicBoolean finished = new AtomicBoolean(false);

  @Override
  public int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void test() throws Exception {
    final String tableName = getUniqueNames(1)[0];
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().create(tableName);
      // majc should take 100 * 5 secs
      IteratorSetting it = new IteratorSetting(100, SlowIterator.class);
      SlowIterator.setSleepTime(it, 5000);
      c.tableOperations().attachIterator(tableName, it,
          EnumSet.of(IteratorUtil.IteratorScope.majc));
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        for (int i = 0; i < 100; i++) {
          Mutation m = new Mutation("" + i);
          m.put("cf", "cq", new Value());
          bw.addMutation(m);
        }
        bw.flush();
      }
      // start majc
      final AtomicReference<Exception> ex = new AtomicReference<>();
      Thread thread = new Thread(() -> {
        try {
          log.info("Compact " + tableName);
          started.set(true);
          c.tableOperations().compact(tableName, null, null, true, true);
          log.info("Finished Compact " + tableName);
          finished.set(true);
        } catch (Exception e) {
          ex.set(e);
        }
      });
      thread.start();

      while (!started.get()) {
        sleepUninterruptibly(1, TimeUnit.SECONDS);
      }
      // delete the table, interrupts the compaction
      log.info("Delete table: " + tableName);
      c.tableOperations().delete(tableName);

      while (!finished.get()) {
        log.info("Wait for compaction to be cancelled");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      assertTrue(finished.get());
    }
  }
}

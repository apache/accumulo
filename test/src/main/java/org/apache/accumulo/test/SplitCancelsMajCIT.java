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
package org.apache.accumulo.test;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertTrue;

import java.util.EnumSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// ACCUMULO-2862
public class SplitCancelsMajCIT extends SharedMiniClusterBase {

  @Override
  public int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void test() throws Exception {
    final String tableName = getUniqueNames(1)[0];
    final Connector c = getConnector();
    c.tableOperations().create(tableName);
    // majc should take 100 * .5 secs
    IteratorSetting it = new IteratorSetting(100, SlowIterator.class);
    SlowIterator.setSleepTime(it, 500);
    c.tableOperations().attachIterator(tableName, it, EnumSet.of(IteratorScope.majc));
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    for (int i = 0; i < 100; i++) {
      Mutation m = new Mutation("" + i);
      m.put("", "", new Value());
      bw.addMutation(m);
    }
    bw.flush();
    // start majc
    final AtomicReference<Exception> ex = new AtomicReference<>();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          c.tableOperations().compact(tableName, null, null, true, true);
        } catch (Exception e) {
          ex.set(e);
        }
      }
    };
    thread.start();

    long now = System.currentTimeMillis();
    sleepUninterruptibly(10, TimeUnit.SECONDS);
    // split the table, interrupts the compaction
    SortedSet<Text> partitionKeys = new TreeSet<>();
    partitionKeys.add(new Text("10"));
    c.tableOperations().addSplits(tableName, partitionKeys);
    thread.join();
    // wait for the restarted compaction
    assertTrue(System.currentTimeMillis() - now > 59 * 1000);
    if (ex.get() != null)
      throw ex.get();
  }
}

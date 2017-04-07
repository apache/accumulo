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

import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.categories.PerformanceTests;
import org.apache.accumulo.test.categories.StandaloneCapableClusterTests;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// ACCUMULO-2361
@Category({StandaloneCapableClusterTests.class, PerformanceTests.class})
public class DeleteTableDuringSplitIT extends AccumuloClusterHarness {

  @Override
  protected int defaultTimeoutSeconds() {
    return 15 * 60;
  }

  @Test
  public void test() throws Exception {
    // 96 invocations, 8 at a time
    int batches = 12, batchSize = 8;
    String[] tableNames = getUniqueNames(batches * batchSize);
    // make a bunch of tables
    for (String tableName : tableNames) {
      getConnector().tableOperations().create(tableName);
    }
    final SortedSet<Text> splits = new TreeSet<>();
    for (byte i = 0; i < 100; i++) {
      splits.add(new Text(new byte[] {0, 0, i}));
    }

    List<Future<?>> results = new ArrayList<>();
    List<Runnable> tasks = new ArrayList<>();
    SimpleThreadPool es = new SimpleThreadPool(batchSize * 2, "concurrent-api-requests");
    for (String tableName : tableNames) {
      final String finalName = tableName;
      tasks.add(new Runnable() {
        @Override
        public void run() {
          try {
            getConnector().tableOperations().addSplits(finalName, splits);
          } catch (TableNotFoundException ex) {
            // expected, ignore
          } catch (Exception ex) {
            throw new RuntimeException(finalName, ex);
          }
        }
      });
      tasks.add(new Runnable() {
        @Override
        public void run() {
          try {
            UtilWaitThread.sleep(500);
            getConnector().tableOperations().delete(finalName);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }
      });
    }
    Iterator<Runnable> itr = tasks.iterator();
    for (int batch = 0; batch < batches; batch++) {
      for (int i = 0; i < batchSize; i++) {
        Future<?> f = es.submit(itr.next());
        results.add(f);
        f = es.submit(itr.next());
        results.add(f);
      }
      for (Future<?> f : results) {
        f.get();
      }
      results.clear();
    }
    // Shut down the ES
    List<Runnable> queued = es.shutdownNow();
    Assert.assertTrue("Had more tasks to run", queued.isEmpty());
    Assert.assertFalse("Had more tasks that needed to be submitted", itr.hasNext());
    for (String tableName : tableNames) {
      assertFalse(getConnector().tableOperations().exists(tableName));
    }
  }

}

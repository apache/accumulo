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
package org.apache.accumulo.core.client.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.impl.BulkImport.ConcurrentKeyExtentCache;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

public class ConcurrentKeyExtentCacheTest {

  private static List<KeyExtent> extents = new ArrayList<>();

  @BeforeClass
  public static void setupSplits() {
    Text prev = null;
    for (int i = 1; i < 256; i++) {
      Text endRow = new Text(String.format("%02x", i));
      extents.add(new KeyExtent(Table.ID.of("1"), endRow, prev));
      prev = endRow;
    }

    extents.add(new KeyExtent(Table.ID.of("1"), null, prev));
  }

  private static class TestCache extends ConcurrentKeyExtentCache {

    volatile int updates = 0;

    TestCache() {
      super(null, null);
    }

    @Override
    protected void updateCache(KeyExtent e) {
      super.updateCache(e);
      updates++;
    }

    @Override
    protected Stream<KeyExtent> lookupExtents(Text row) {
      int index = -1;
      for (int i = 0; i < extents.size(); i++) {
        if (extents.get(i).contains(row)) {
          index = i;
          break;
        }
      }

      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);

      return extents.subList(index, extents.size()).stream().limit(100);
    }
  }

  @Test
  public void testBasic() throws Exception {
    TestCache tc = new TestCache();

    Random rand = new Random(42);

    for (int i = 0; i < 10000; i++) {
      Text lookupRow = new Text(String.format("%08x", rand.nextInt()));
      KeyExtent extent = tc.lookup(lookupRow);
      Assert.assertTrue(extent.contains(lookupRow));
      Assert.assertTrue(extents.contains(extent));
    }

    Assert.assertEquals(256, tc.updates);
  }

  @Test
  public void testConcurrent() throws Exception {
    TestCache tc = new TestCache();

    ExecutorService es = Executors.newFixedThreadPool(30);

    Random rand = new Random(42);

    List<Future<KeyExtent>> futures = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      Text lookupRow = new Text(String.format("%08x", rand.nextInt()));
      futures.add(es.submit(() -> {
        KeyExtent extent = tc.lookup(lookupRow);
        Assert.assertTrue(extent.contains(lookupRow));
        return extent;
      }));
    }

    for (Future<KeyExtent> future : futures) {
      KeyExtent extent = future.get();
      Assert.assertTrue(extents.contains(extent));
    }

    es.shutdown();

    Assert.assertEquals(256, tc.updates);
  }
}

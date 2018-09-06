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

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

public class ConcurrentKeyExtentCacheTest {

  private static List<KeyExtent> extents = new ArrayList<>();
  private static Set<KeyExtent> extentsSet = new HashSet<>();

  @BeforeClass
  public static void setupSplits() {
    Text prev = null;
    for (int i = 1; i < 256; i++) {
      Text endRow = new Text(String.format("%02x", i));
      extents.add(new KeyExtent(Table.ID.of("1"), endRow, prev));
      prev = endRow;
    }

    extents.add(new KeyExtent(Table.ID.of("1"), null, prev));

    extentsSet.addAll(extents);
  }

  private static class TestCache extends ConcurrentKeyExtentCache {

    AtomicInteger updates = new AtomicInteger(0);

    TestCache() {
      super(null, null);
    }

    @Override
    protected void updateCache(KeyExtent e) {
      super.updateCache(e);
      updates.incrementAndGet();
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

      Uninterruptibles.sleepUninterruptibly(3, TimeUnit.MILLISECONDS);

      return extents.subList(index, extents.size()).stream().limit(73);
    }
  }

  private void testLookup(TestCache tc, Text lookupRow) {
    try {
      KeyExtent extent = tc.lookup(lookupRow);
      Assert.assertTrue(extent.contains(lookupRow));
      Assert.assertTrue(extentsSet.contains(extent));
    } catch (IOException | AccumuloException | AccumuloSecurityException
        | TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testExactEndRows() {
    Random rand = new SecureRandom();
    TestCache tc = new TestCache();
    rand.ints(10000, 0, 256).mapToObj(i -> new Text(String.format("%02x", i))).sequential()
        .forEach(lookupRow -> testLookup(tc, lookupRow));
    Assert.assertEquals(256, tc.updates.get());

    // try parallel
    TestCache tc2 = new TestCache();
    rand.ints(10000, 0, 256).mapToObj(i -> new Text(String.format("%02x", i))).parallel()
        .forEach(lookupRow -> testLookup(tc2, lookupRow));
    Assert.assertEquals(256, tc2.updates.get());
  }

  @Test
  public void testRandom() throws Exception {
    TestCache tc = new TestCache();

    Random rand = new SecureRandom();
    rand.ints(10000).mapToObj(i -> new Text(String.format("%08x", i))).sequential()
        .forEach(lookupRow -> testLookup(tc, lookupRow));
    Assert.assertEquals(256, tc.updates.get());

    // try parallel
    TestCache tc2 = new TestCache();
    rand.ints(10000).mapToObj(i -> new Text(String.format("%08x", i))).parallel()
        .forEach(lookupRow -> testLookup(tc2, lookupRow));
    Assert.assertEquals(256, tc2.updates.get());
  }
}

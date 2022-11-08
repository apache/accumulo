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
package org.apache.accumulo.core.clientImpl.bulk;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.Uninterruptibles;

public class ConcurrentKeyExtentCacheTest {

  private static final SecureRandom random = new SecureRandom();

  private static List<KeyExtent> extents = new ArrayList<>();
  private static Set<KeyExtent> extentsSet = new HashSet<>();

  @BeforeAll
  public static void setupSplits() {
    Text prev = null;
    for (int i = 1; i < 255; i++) {
      Text endRow = new Text(String.format("%02x", i));
      extents.add(new KeyExtent(TableId.of("1"), endRow, prev));
      prev = endRow;
    }

    extents.add(new KeyExtent(TableId.of("1"), null, prev));

    extentsSet.addAll(extents);
  }

  private static class TestCache extends ConcurrentKeyExtentCache {

    ConcurrentSkipListSet<KeyExtent> seen = new ConcurrentSkipListSet<>();

    TestCache() {
      super(null, null);
    }

    @Override
    protected void updateCache(KeyExtent e) {
      super.updateCache(e);
      assertTrue(seen.add(e));
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

      Uninterruptibles.sleepUninterruptibly(3, MILLISECONDS);

      return extents.subList(index, extents.size()).stream().limit(73);
    }
  }

  private void testLookup(TestCache tc, Text lookupRow) {
    KeyExtent extent = tc.lookup(lookupRow);
    assertTrue(extent.contains(lookupRow));
    assertTrue(extentsSet.contains(extent));
  }

  @Test
  public void testExactEndRows() {

    TestCache tc = new TestCache();

    random.ints(20000, 0, 256).mapToObj(i -> new Text(String.format("%02x", i))).sequential()
        .forEach(lookupRow -> testLookup(tc, lookupRow));
    assertEquals(extentsSet, tc.seen);

    // try parallel
    TestCache tc2 = new TestCache();
    random.ints(20000, 0, 256).mapToObj(i -> new Text(String.format("%02x", i))).parallel()
        .forEach(lookupRow -> testLookup(tc2, lookupRow));
    assertEquals(extentsSet, tc.seen);
  }

  @Test
  public void testRandom() {
    TestCache tc = new TestCache();

    random.ints(20000).mapToObj(i -> new Text(String.format("%08x", i))).sequential()
        .forEach(lookupRow -> testLookup(tc, lookupRow));
    assertEquals(extentsSet, tc.seen);

    // try parallel
    TestCache tc2 = new TestCache();
    random.ints(20000).mapToObj(i -> new Text(String.format("%08x", i))).parallel()
        .forEach(lookupRow -> testLookup(tc2, lookupRow));
    assertEquals(extentsSet, tc2.seen);
  }
}

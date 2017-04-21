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
package org.apache.accumulo.tserver.tablet;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.system.ColumnFamilySkippingIterator;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.MultiIterator;
import org.junit.Test;

import junit.framework.TestCase;

public class CompactorTest extends TestCase {
  private static class CloseTestIter extends SortedMapIterator {

    int closeCallCount = 0;

    public CloseTestIter(SortedMap<Key,Value> map) {
      super(map);
    }

    @Override
    public void close() {
      System.out.println("Closing inner CloseIterator.");
      closeCallCount++;
    }
  }

  private static class CloseCountingTestIter extends CountingIterator {

    int closeCallCount = 0;

    public CloseCountingTestIter(SortedKeyValueIterator<Key,Value> source, AtomicLong entriesRead) {
      super(source, entriesRead);
    }

    @Override
    public void close() {
      System.out.println("Closing inner CloseCountingTestIter.");
      closeCallCount++;
    }
  }

  /**
   * Test that the top level iterator will close upon exiting try-with-resources and that only the appropriate iterators below it will be closed. MultiIterator
   * should not call close on its source since it could be reused.
   */
  @Test
  public void testIteratorCloseStopsAtMultiIterator() {
    CloseTestIter lowestCloseIter = new CloseTestIter(new TreeMap<>());

    try {
      List<SortedKeyValueIterator<Key,Value>> iters = new ArrayList<>(1);
      iters.add(lowestCloseIter);

      MultiIterator mitr = new MultiIterator(iters, new Range());
      CloseCountingTestIter citr = new CloseCountingTestIter(mitr, new AtomicLong(1));
      DeletingIterator delIter = new DeletingIterator(citr, true);
      ColumnFamilySkippingIterator cfsi = new ColumnFamilySkippingIterator(delIter);

      assertEquals(0, lowestCloseIter.closeCallCount);
      assertEquals(0, citr.closeCallCount);
      try (SortedKeyValueIterator<Key,Value> itr = cfsi) {
        itr.seek(new Range(), new ArrayList<ByteSequence>(), false);
        while (itr.hasTop()) {
          itr.next();
        }
      } finally {
        System.out.println("Finished try with resources");
        assertEquals(0, lowestCloseIter.closeCallCount);
        assertEquals(1, citr.closeCallCount);
      }
    } catch (Exception e) {
      assertTrue("Exception occurred " + e.getMessage(), false);
    }
    assertEquals(0, lowestCloseIter.closeCallCount);
  }

}

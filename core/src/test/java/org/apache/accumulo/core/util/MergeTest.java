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
package org.apache.accumulo.core.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.util.Merge.Size;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class MergeTest {

  static class MergeTester extends Merge {
    public List<List<Size>> merges = new ArrayList<>();
    public List<Size> tablets = new ArrayList<>();

    MergeTester(Integer... sizes) {
      Text start = null;
      for (Integer size : sizes) {
        Text end;
        if (tablets.size() == sizes.length - 1)
          end = null;
        else
          end = new Text(String.format("%05d", tablets.size()));
        KeyExtent extent = new KeyExtent(Table.ID.of("table"), end, start);
        start = end;
        tablets.add(new Size(extent, size));
      }
    }

    @Override
    protected void message(String format, Object... args) {}

    @Override
    protected Iterator<Size> getSizeIterator(Connector conn, String tablename, final Text start, final Text end) throws MergeException {
      final Iterator<Size> impl = tablets.iterator();
      return new Iterator<Size>() {
        Size next = skip();

        @Override
        public boolean hasNext() {
          return next != null;
        }

        private Size skip() {
          while (impl.hasNext()) {
            Size candidate = impl.next();
            if (start != null) {
              if (candidate.extent.getEndRow() != null && candidate.extent.getEndRow().compareTo(start) < 0)
                continue;
            }
            if (end != null) {
              if (candidate.extent.getPrevEndRow() != null && candidate.extent.getPrevEndRow().compareTo(end) >= 0)
                continue;
            }
            return candidate;
          }
          return null;
        }

        @Override
        public Size next() {
          Size result = next;
          next = skip();
          return result;
        }

        @Override
        public void remove() {
          impl.remove();
        }
      };
    }

    @Override
    protected void merge(Connector conn, String table, List<Size> sizes, int numToMerge) throws MergeException {
      List<Size> merge = new ArrayList<>();
      for (int i = 0; i < numToMerge; i++) {
        merge.add(sizes.get(i));
      }
      merges.add(merge);
    }
  }

  static private int[] sizes(List<Size> sizes) {
    int result[] = new int[sizes.size()];
    int i = 0;
    for (Size s : sizes) {
      result[i++] = (int) s.size;
    }
    return result;
  }

  @Test
  public void testMergomatic() throws Exception {
    // Merge everything to the last tablet
    int i;
    MergeTester test = new MergeTester(10, 20, 30);
    test.mergomatic(null, "table", null, null, 1000, false);
    assertEquals(1, test.merges.size());
    assertArrayEquals(new int[] {10, 20, 30}, sizes(test.merges.get(i = 0)));

    // Merge ranges around tablets that are big enough
    test = new MergeTester(1, 2, 100, 1000, 17, 1000, 4, 5, 6, 900);
    test.mergomatic(null, "table", null, null, 1000, false);
    assertEquals(2, test.merges.size());
    assertArrayEquals(new int[] {1, 2, 100}, sizes(test.merges.get(i = 0)));
    assertArrayEquals(new int[] {4, 5, 6, 900}, sizes(test.merges.get(++i)));

    // Test the force option
    test = new MergeTester(1, 2, 100, 1000, 17, 1000, 4, 5, 6, 900);
    test.mergomatic(null, "table", null, null, 1000, true);
    assertEquals(3, test.merges.size());
    assertArrayEquals(new int[] {1, 2, 100}, sizes(test.merges.get(i = 0)));
    assertArrayEquals(new int[] {17, 1000}, sizes(test.merges.get(++i)));
    assertArrayEquals(new int[] {4, 5, 6, 900}, sizes(test.merges.get(++i)));

    // Limit the low-end of the merges
    test = new MergeTester(1, 2, 1000, 17, 1000, 4, 5, 6, 900);
    test.mergomatic(null, "table", new Text("00004"), null, 1000, false);
    assertEquals(1, test.merges.size());
    assertArrayEquals(new int[] {4, 5, 6, 900}, sizes(test.merges.get(i = 0)));

    // Limit the upper end of the merges
    test = new MergeTester(1, 2, 1000, 17, 1000, 4, 5, 6, 900);
    test.mergomatic(null, "table", null, new Text("00004"), 1000, false);
    assertEquals(1, test.merges.size());
    assertArrayEquals(new int[] {1, 2}, sizes(test.merges.get(i = 0)));

    // Limit both ends
    test = new MergeTester(1, 2, 1000, 17, 1000, 4, 5, 6, 900);
    test.mergomatic(null, "table", new Text("00002"), new Text("00004"), 1000, true);
    assertEquals(1, test.merges.size());
    assertArrayEquals(new int[] {17, 1000}, sizes(test.merges.get(i = 0)));

    // Clump up tablets into larger values
    test = new MergeTester(100, 250, 500, 600, 100, 200, 500, 200);
    test.mergomatic(null, "table", null, null, 1000, false);
    assertEquals(3, test.merges.size());
    assertArrayEquals(new int[] {100, 250, 500}, sizes(test.merges.get(i = 0)));
    assertArrayEquals(new int[] {600, 100, 200}, sizes(test.merges.get(++i)));
    assertArrayEquals(new int[] {500, 200}, sizes(test.merges.get(++i)));
  }

}

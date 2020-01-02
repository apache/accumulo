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
package org.apache.accumulo.server.util;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.server.util.TabletIterator.TabletDeletedException;
import org.apache.hadoop.io.Text;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Iterators;

public class TabletIteratorTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSplits() {
    TreeMap<Key,Value> data1 = new TreeMap<Key,Value>();

    createTabletData(data1, "4", "j", null);
    createTabletData(data1, "4", "m", "j");
    createTabletData(data1, "4", null, "x");

    TreeMap<Key,Value> data2 = new TreeMap<Key,Value>(data1);

    createTabletData(data2, "4", "s", "m");
    createTabletData(data2, "4", "x", "s");

    runTest(Arrays.asList(data1, data2), Arrays.asList("4;j", "4;m", "4;s", "4;x", "4<"));
  }

  @Test
  public void testTableTransition1() {
    TreeMap<Key,Value> data1 = new TreeMap<Key,Value>();

    createTabletData(data1, "3", "c", null);
    createTabletData(data1, "3", "n", "c");
    createTabletData(data1, "4", "f", null);
    createTabletData(data1, "4", null, "f");

    TreeMap<Key,Value> data2 = new TreeMap<Key,Value>(data1);

    createTabletData(data2, "3", null, "n");

    runTest(Arrays.asList(data1, data2), Arrays.asList("3;c", "3;n", "3<", "4;f", "4<"));
  }

  @Test
  public void testTableTransition2() {
    TreeMap<Key,Value> data1 = new TreeMap<Key,Value>();

    createTabletData(data1, "3", "c", null);
    createTabletData(data1, "3", "n", "c");
    createTabletData(data1, "3", null, "n");
    createTabletData(data1, "4", null, "f");

    TreeMap<Key,Value> data2 = new TreeMap<Key,Value>(data1);

    createTabletData(data2, "4", "f", null);

    runTest(Arrays.asList(data1, data2), Arrays.asList("3;c", "3;n", "3<", "4;f", "4<"));
  }

  @Test
  public void testMissingFirstTablet() {
    TreeMap<Key,Value> data1 = new TreeMap<Key,Value>();

    createTabletData(data1, "3", "n", "c");
    createTabletData(data1, "3", null, "n");

    TreeMap<Key,Value> data2 = new TreeMap<Key,Value>(data1);

    createTabletData(data2, "3", "c", null);

    runTest(Arrays.asList(data1, data2), Arrays.asList("3;c", "3;n", "3<"));
  }

  @Test
  public void testMissingLastTablet() {
    TreeMap<Key,Value> data1 = new TreeMap<Key,Value>();

    createTabletData(data1, "3", "c", null);
    createTabletData(data1, "3", "n", "c");

    TreeMap<Key,Value> data2 = new TreeMap<Key,Value>(data1);

    createTabletData(data2, "3", null, "n");

    thrown.expect(IllegalStateException.class);
    runTest(Arrays.asList(data1, data2), Arrays.asList("3;c", "3;n", "3<"));
  }

  @Test
  public void testDeletedTable() {
    TreeMap<Key,Value> data1 = new TreeMap<Key,Value>();

    createTabletData(data1, "3", "c", null);
    createTabletData(data1, "3", "n", "c");
    createTabletData(data1, "4", null, "f");
    createTabletData(data1, "4", "f", null);

    TreeMap<Key,Value> data2 = new TreeMap<Key,Value>();
    createTabletData(data1, "4", null, "f");
    createTabletData(data1, "4", "f", null);

    thrown.expect(TabletDeletedException.class);
    runTest(Arrays.asList(data1, data2), Arrays.asList("3;c", "3;n"));
  }

  private static class ScannerState {

    public Capture<Range> rangeCapture;

    private Range getRange() {
      if (rangeCapture.hasCaptured())
        return rangeCapture.getValue();
      return null;
    }

    Predicate<Entry<Key,Value>> getScanPredicate() {
      final Range range = getRange();

      return input -> range == null || range.contains(input.getKey());
    }
  }

  private Scanner createMockScanner(final TreeMap<Key,Value> data) {

    final ScannerState state = new ScannerState();

    Scanner scanner = createMock(Scanner.class);

    expect(scanner.iterator()).andAnswer(new IAnswer<Iterator<Entry<Key,Value>>>() {
      @Override
      public Iterator<Entry<Key,Value>> answer() throws Throwable {
        Iterator<Entry<Key,Value>> iter = data.entrySet().iterator();
        iter = Iterators.filter(iter, state.getScanPredicate()::test);
        return iter;
      }
    }).anyTimes();

    state.rangeCapture = Capture.newInstance();

    scanner.setRange(capture(state.rangeCapture));
    expectLastCall().anyTimes();

    scanner.fetchColumn(anyObject(Text.class), anyObject(Text.class));
    expectLastCall().anyTimes();

    scanner.fetchColumnFamily(anyObject(Text.class));
    expectLastCall().anyTimes();

    replay(scanner);

    return scanner;
  }

  private void createTabletData(TreeMap<Key,Value> data, String tableId, String endRow,
      String prevEndRow) {
    KeyExtent ke = new KeyExtent(tableId, endRow == null ? null : new Text(endRow),
        prevEndRow == null ? null : new Text(prevEndRow));

    Mutation m = ke.getPrevRowUpdateMutation();
    TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.put(m, new Value("/d1"));

    for (ColumnUpdate cu : m.getUpdates()) {
      Key k = new Key(m.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
          cu.getColumnVisibility(), cu.getTimestamp());
      Value v = new Value(cu.getValue());

      data.put(k, v);
    }
  }

  public void runTest(List<TreeMap<Key,Value>> dataSets, List<String> expectedEndRows) {
    final Iterator<TreeMap<Key,Value>> dataSetIter = dataSets.iterator();

    final TreeMap<Key,Value> data = new TreeMap<Key,Value>(dataSetIter.next());

    Scanner scanner = createMockScanner(data);

    TabletIterator tabIter = new TabletIterator(scanner, TabletsSection.getRange(), true, false) {
      protected void resetScanner() {
        data.clear();
        data.putAll(dataSetIter.next());
        super.resetScanner();
      }
    };

    Iterator<String> expEndRowIter = expectedEndRows.iterator();

    while (tabIter.hasNext()) {
      String expextedEndRow = expEndRowIter.next();
      Map<Key,Value> tabEntries = tabIter.next();

      assertFalse(tabEntries.isEmpty());

      for (Key k : tabEntries.keySet()) {
        assertEquals(expextedEndRow, k.getRowData().toString());
      }
    }

    assertFalse(expEndRowIter.hasNext());
    assertFalse(dataSetIter.hasNext());
  }
}

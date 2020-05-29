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
package org.apache.accumulo.master.tableOps.bulkVer2;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.master.tableOps.bulkVer2.PrepBulkImport.TabletIterFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class PrepBulkImportTest {

  KeyExtent nke(String prev, String end) {
    Text per = prev == null ? null : new Text(prev);
    Text er = end == null ? null : new Text(end);

    return new KeyExtent(TableId.of("1"), er, per);
  }

  List<KeyExtent> createExtents(Iterable<String> rowsIter) {
    List<KeyExtent> extents = new ArrayList<>();

    List<String> rows = new ArrayList<>();
    rowsIter.forEach(rows::add);

    Collections.sort(rows);
    for (int i = 0; i < rows.size(); i++) {
      extents.add(nke(i == 0 ? null : rows.get(i - 1), rows.get(i)));

    }

    extents.add(nke(rows.isEmpty() ? null : rows.get(rows.size() - 1), null));

    return extents;
  }

  Iterable<List<KeyExtent>> powerSet(KeyExtent... extents) {
    Set<Set<KeyExtent>> powerSet = Sets.powerSet(Set.copyOf(Arrays.asList(extents)));

    return Iterables.transform(powerSet, set -> {
      List<KeyExtent> list = new ArrayList<>(set);

      Collections.sort(list);

      return list;
    });
  }

  public void runTest(List<KeyExtent> loadRanges, List<KeyExtent> tabletRanges) throws Exception {
    TabletIterFactory tabletIterFactory = startRow -> {
      int start = -1;

      if (startRow == null) {
        start = 0;
      } else {
        for (int i = 0; i < tabletRanges.size(); i++) {
          if (tabletRanges.get(i).contains(startRow)) {
            start = i;
            break;
          }
        }
      }

      return tabletRanges.subList(start, tabletRanges.size()).iterator();
    };

    PrepBulkImport.checkForMerge("1", loadRanges.iterator(), tabletIterFactory);
  }

  static String toRangeStrings(Collection<KeyExtent> extents) {
    return extents.stream().map(ke -> "(" + ke.getPrevEndRow() + "," + ke.getEndRow() + "]")
        .collect(Collectors.joining(","));
  }

  public void runExceptionTest(List<KeyExtent> loadRanges, List<KeyExtent> tabletRanges)
      throws AccumuloException {
    try {
      runTest(loadRanges, tabletRanges);
      fail("expected " + toRangeStrings(loadRanges) + " to fail against "
          + toRangeStrings(tabletRanges));
    } catch (Exception e) {
      assertTrue(e instanceof AcceptableThriftTableOperationException);
    }
  }

  @Test
  public void testSingleTablet() throws Exception {
    runTest(Arrays.asList(nke(null, null)), Arrays.asList(nke(null, null)));

    for (List<KeyExtent> loadRanges : powerSet(nke(null, "b"), nke("b", "k"), nke("k", "r"),
        nke("r", null))) {
      if (loadRanges.isEmpty()) {
        continue;
      }
      runExceptionTest(loadRanges, Arrays.asList(nke(null, null)));
    }
  }

  @Test
  public void testNominal() throws Exception {

    for (List<KeyExtent> loadRanges : powerSet(nke(null, "b"), nke("b", "m"), nke("m", "r"),
        nke("r", "v"), nke("v", null))) {

      if (loadRanges.isEmpty()) {
        loadRanges = Arrays.asList(nke(null, null));
      }

      List<String> requiredRows = Arrays.asList("b", "m", "r", "v");
      for (Set<String> otherRows : Sets.powerSet(Set.of("a", "c", "q", "t", "x"))) {
        runTest(loadRanges, createExtents(Iterables.concat(requiredRows, otherRows)));
      }
    }
  }

  @Test
  public void testException() throws Exception {
    for (List<KeyExtent> loadRanges : powerSet(nke(null, "b"), nke("b", "m"), nke("m", "r"),
        nke("r", "v"), nke("v", null))) {

      if (loadRanges.isEmpty()) {
        continue;
      }

      Set<String> rows = new HashSet<>();
      for (KeyExtent ke : loadRanges) {
        if (ke.getPrevEndRow() != null) {
          rows.add(ke.getPrevEndRow().toString());
        }
        if (ke.getEndRow() != null) {
          rows.add(ke.getEndRow().toString());
        }
      }

      for (String row : rows) {
        Set<String> rows2 = new HashSet<>(rows);
        rows2.remove(row);
        // test will all but one of the rows in the load mapping
        for (Set<String> otherRows : Sets.powerSet(Set.of("a", "c", "q", "t", "x"))) {
          runExceptionTest(loadRanges, createExtents(Iterables.concat(rows2, otherRows)));
        }
      }

      if (rows.size() > 1) {
        // test with none of the rows in the load mapping
        for (Set<String> otherRows : Sets.powerSet(Set.of("a", "c", "q", "t", "x"))) {
          runExceptionTest(loadRanges, createExtents(otherRows));
        }
      }
    }

  }
}

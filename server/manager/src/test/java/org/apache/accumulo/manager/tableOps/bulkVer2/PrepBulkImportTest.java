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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.bulk.Bulk;
import org.apache.accumulo.core.clientImpl.bulk.BulkSerialize;
import org.apache.accumulo.core.clientImpl.bulk.LoadMappingIterator;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.manager.tableOps.bulkVer2.PrepBulkImport.TabletIterFactory;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class PrepBulkImportTest {

  KeyExtent nke(String prev, String end) {
    Text per = prev == null ? null : new Text(prev);
    Text er = end == null ? null : new Text(end);

    return new KeyExtent(TableId.of("1"), er, per);
  }

  List<KeyExtent> createExtents(Stream<String> rowsIter) {
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
    Set<Set<KeyExtent>> powerSet = Sets.powerSet(Set.of(extents));
    return () -> powerSet.stream().map(set -> {
      List<KeyExtent> list = new ArrayList<>(set);
      Collections.sort(list);
      return list;
    }).iterator();
  }

  private void runTest(List<KeyExtent> loadRanges, List<KeyExtent> tabletRanges) throws Exception {
    Map<KeyExtent,String> lrm = new HashMap<>();
    loadRanges.forEach(e -> lrm.put(e, "f1 f2 f3"));
    runTest(lrm, tabletRanges, 100);
  }

  public void runTest(Map<KeyExtent,String> loadRanges, List<KeyExtent> tabletRanges,
      int maxTablets) throws Exception {
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

    try (LoadMappingIterator lmi = createLoadMappingIter(loadRanges)) {
      PrepBulkImport.sanityCheckLoadMapping("1", lmi, tabletIterFactory, maxTablets, 10001);
    }
  }

  private LoadMappingIterator createLoadMappingIter(Map<KeyExtent,String> loadRanges)
      throws IOException {
    SortedMap<KeyExtent,Bulk.Files> mapping = new TreeMap<>();

    loadRanges.forEach((extent, files) -> {
      Bulk.Files testFiles = new Bulk.Files();
      long c = 0L;
      for (String f : files.split(" ")) {
        c++;
        testFiles.add(new Bulk.FileInfo(f, c, c));
      }

      mapping.put(extent, testFiles);
    });

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BulkSerialize.writeLoadMapping(mapping, "/some/dir", p -> baos);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    LoadMappingIterator lmi =
        BulkSerialize.readLoadMapping("/some/dir", TableId.of("1"), p -> bais);
    return lmi;
  }

  static String toRangeStrings(Collection<KeyExtent> extents) {
    return extents.stream().map(ke -> "(" + ke.prevEndRow() + "," + ke.endRow() + "]")
        .collect(Collectors.joining(","));
  }

  public void runExceptionTest(List<KeyExtent> loadRanges, List<KeyExtent> tabletRanges) {
    String message = "expected " + toRangeStrings(loadRanges) + " to fail against "
        + toRangeStrings(tabletRanges);
    assertThrows(AcceptableThriftTableOperationException.class,
        () -> runTest(loadRanges, tabletRanges), message);
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

      List<String> requiredRows = List.of("b", "m", "r", "v");
      for (Set<String> otherRows : Sets.powerSet(Set.of("a", "c", "q", "t", "x"))) {
        runTest(loadRanges,
            createExtents(Stream.concat(requiredRows.stream(), otherRows.stream())));
      }
    }
  }

  @Test
  public void testException() {
    for (List<KeyExtent> loadRanges : powerSet(nke(null, "b"), nke("b", "m"), nke("m", "r"),
        nke("r", "v"), nke("v", null))) {

      if (loadRanges.isEmpty()) {
        continue;
      }

      Set<String> rows = new HashSet<>();
      for (KeyExtent ke : loadRanges) {
        if (ke.prevEndRow() != null) {
          rows.add(ke.prevEndRow().toString());
        }
        if (ke.endRow() != null) {
          rows.add(ke.endRow().toString());
        }
      }

      for (String row : rows) {
        Set<String> rows2 = new HashSet<>(rows);
        rows2.remove(row);
        // test will all but one of the rows in the load mapping
        for (Set<String> otherRows : Sets.powerSet(Set.of("a", "c", "q", "t", "x"))) {
          runExceptionTest(loadRanges,
              createExtents(Stream.concat(rows2.stream(), otherRows.stream())));
        }
      }

      if (rows.size() > 1) {
        // test with none of the rows in the load mapping
        for (Set<String> otherRows : Sets.powerSet(Set.of("a", "c", "q", "t", "x"))) {
          runExceptionTest(loadRanges, createExtents(otherRows.stream()));
        }
      }
    }

  }

  @Test
  public void testTooManyTablets() throws Exception {
    Map<KeyExtent,String> loadRanges = new HashMap<>();

    loadRanges.put(nke(null, "c"), "f1 f2");
    loadRanges.put(nke("c", "g"), "f2 f3");
    loadRanges.put(nke("g", "r"), "f2 f4");
    loadRanges.put(nke("r", "w"), "f2 f5");
    loadRanges.put(nke("w", null), "f2 f6");

    List<String> requiredRows = Arrays.asList("c", "g", "r", "w");
    for (Set<String> otherRows : Sets.powerSet(Set.of("a", "e", "i", "s", "x"))) {

      for (int maxTablets = 3; maxTablets < 10; maxTablets++) {
        var tablets = Stream.concat(requiredRows.stream(), otherRows.stream());

        int totalTablets = requiredRows.size() + otherRows.size() + 1;

        if (totalTablets > maxTablets) {
          runTooManyTest(loadRanges, tablets, "{f2=" + totalTablets + "}", maxTablets);
        } else {
          runTest(loadRanges, createExtents(tablets), maxTablets);
        }
      }

      runTest(loadRanges, createExtents(Stream.concat(requiredRows.stream(), otherRows.stream())),
          0);
    }

    loadRanges.clear();

    loadRanges.put(nke("ca", "cz"), "f3");
    loadRanges.put(nke("ma", "mm"), "f3");
    loadRanges.put(nke("re", "rz"), "f4");

    runTooManyTest(loadRanges, Stream.of("ca", "cd", "cz", "e", "ma", "md", "mm", "re", "rz"),
        "{f3=4}", 3);
    runTooManyTest(loadRanges, Stream.of("b", "ca", "cd", "cz", "e", "ma", "md", "mm", "re", "rz"),
        "{f3=4}", 3);
    runTooManyTest(loadRanges,
        Stream.of("ca", "cd", "cz", "e", "ma", "md", "mm", "re", "rf", "rh", "rm", "rz"),
        "{f3=4, f4=4}", 3);
    runTooManyTest(loadRanges,
        Stream.of("ca", "cd", "cz", "e", "ma", "mm", "re", "rf", "rh", "rm", "rz"), "{f4=4}", 3);
  }

  private void runTooManyTest(Map<KeyExtent,String> loadRanges, Stream<String> tablets,
      String expectedMessage, int maxTablets) {
    var exception = assertThrows(ThriftTableOperationException.class,
        () -> runTest(loadRanges, createExtents(tablets), maxTablets));
    String message = exception.toString();
    assertTrue(exception.toString().contains(expectedMessage), expectedMessage + " -- " + message);
  }
}

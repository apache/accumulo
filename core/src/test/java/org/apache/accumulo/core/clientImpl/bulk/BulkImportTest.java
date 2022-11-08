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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.clientImpl.bulk.Bulk.FileInfo;
import org.apache.accumulo.core.clientImpl.bulk.Bulk.Files;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class BulkImportTest {

  @Test
  public void testMergeOverlappingSingleSplit() {
    SortedMap<KeyExtent,Files> mappings = new TreeMap<>();

    // simulate the tablet (m,s] splitting into (m,p] and (p,s] while files are being examined
    addMapping(mappings, null, "m", "f0");
    addMapping(mappings, "m", "s", "f1", "f2");
    addMapping(mappings, "p", "s", "f3");
    addMapping(mappings, "m", "p", "f4");
    addMapping(mappings, "s", null, "f5");

    var actual = BulkImport.mergeOverlapping(mappings);

    SortedMap<KeyExtent,Files> expected = new TreeMap<>();
    addMapping(expected, null, "m", "f0");
    addMapping(expected, "m", "s", "f1", "f2", "f3", "f4");
    addMapping(expected, "s", null, "f5");

    assertEquals(expected, actual);
  }

  @Test
  public void testMergeOverlappingMultipleSplit() {
    SortedMap<KeyExtent,Files> mappings = new TreeMap<>();

    // simulate the tablet (m,s] splitting into (m,o],(o,p],(p,s] while files are being examined
    addMapping(mappings, null, "m", "f0");
    addMapping(mappings, "m", "s", "f1");
    addMapping(mappings, "m", "o", "f2");
    addMapping(mappings, "o", "p", "f3");
    addMapping(mappings, "p", "s", "f4");
    addMapping(mappings, "s", null, "f5");

    var actual = BulkImport.mergeOverlapping(mappings);

    SortedMap<KeyExtent,Files> expected = new TreeMap<>();
    addMapping(expected, null, "m", "f0");
    addMapping(expected, "m", "s", "f1", "f2", "f3", "f4");
    addMapping(expected, "s", null, "f5");

    assertEquals(expected, actual);
  }

  @Test
  public void testMergeOverlappingTabletsMergedAway() {
    // simulate the tablets (m,p] and (p,s] being merged into (m,s] and that splitting into
    // (m,q],(q,s] while files are being examined

    SortedMap<KeyExtent,Files> mappings = new TreeMap<>();

    addMapping(mappings, null, "m", "f0");
    addMapping(mappings, "p", "s", "f1");
    addMapping(mappings, "m", "p", "f2");
    addMapping(mappings, "m", "s", "f3");
    addMapping(mappings, "q", "s", "f4");
    addMapping(mappings, "m", "q", "f5");
    addMapping(mappings, "s", null, "f6");
    assertThrows(RuntimeException.class, () -> BulkImport.mergeOverlapping(mappings));
  }

  private void addMapping(SortedMap<KeyExtent,Files> mappings, String prevRow, String endRow,
      String... fileNames) {
    KeyExtent ke = new KeyExtent(TableId.of("42"), endRow == null ? null : new Text(endRow),
        prevRow == null ? null : new Text(prevRow));
    Files files = new Files();
    for (String name : fileNames) {
      files.add(new FileInfo(name, 2, 2));
    }
    mappings.put(ke, files);
  }
}

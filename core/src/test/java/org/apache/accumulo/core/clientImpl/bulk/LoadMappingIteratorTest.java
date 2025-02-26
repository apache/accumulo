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

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class LoadMappingIteratorTest {
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

  KeyExtent nke(String prev, String end) {
    Text per = prev == null ? null : new Text(prev);
    Text er = end == null ? null : new Text(end);

    return new KeyExtent(TableId.of("1"), er, per);
  }

  @Test
  void testValidOrderedInput() throws IOException {
    Map<KeyExtent,String> loadRanges = new LinkedHashMap<>();
    loadRanges.put(nke(null, "c"), "f1 f2");
    loadRanges.put(nke("c", "g"), "f2 f3");
    loadRanges.put(nke("g", "r"), "f2 f4");
    loadRanges.put(nke("r", "w"), "f2 f5");
    loadRanges.put(nke("w", null), "f2 f6");

    try (LoadMappingIterator iterator = createLoadMappingIter(loadRanges)) {
      List<KeyExtent> result = new ArrayList<>();
      while (iterator.hasNext()) {
        result.add(iterator.next().getKey());
      }
      assertEquals(new ArrayList<>(loadRanges.keySet()), result);
    }
  }

  @Test
  void testInvalidOutOfOrderInput() throws IOException {
    Map<KeyExtent,String> loadRanges = new LinkedHashMap<>();
    loadRanges.put(nke("c", "g"), "f2 f3");
    loadRanges.put(nke(null, "c"), "f1 f2"); // Out of order!
    loadRanges.put(nke("g", "r"), "f2 f4");
    loadRanges.put(nke("r", "w"), "f2 f5");
    loadRanges.put(nke("w", null), "f2 f6");

    try (LoadMappingIterator iterator = createLoadMappingIter(loadRanges)) {
      KeyExtent previous = null;
      while (iterator.hasNext()) {
        KeyExtent current = iterator.next().getKey();
        System.out.println("Comparing " + current + " with previous: " + previous);
        assertFalse(previous != null && current.compareTo(previous) < 0, "Input is out of order!");

        previous = current;
      }
    } catch (IllegalStateException e) {
      assertThrows(IllegalStateException.class, () -> {
        throw e;
      }, "Expected an IllegalStateException due to out-of-order KeyExtents");
    }
  }
}

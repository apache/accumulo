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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.clientImpl.bulk.BulkSerialize.createGson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;

public class LoadMappingIteratorTest {
  private LoadMappingIterator createLoadMappingIter(Map<KeyExtent,String> loadRanges)
      throws IOException {
    Map<KeyExtent,Bulk.Files> unorderedMapping = new LinkedHashMap<>();

    loadRanges.forEach((extent, files) -> {
      Bulk.Files testFiles = new Bulk.Files();
      long c = 0L;
      for (String f : files.split(" ")) {
        c++;
        testFiles.add(new Bulk.FileInfo(f, c, c));
      }

      unorderedMapping.put(extent, testFiles);
    });

    // Serialize unordered mapping directly
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    writeLoadMappingWithoutSorting(unorderedMapping, "/some/dir", p -> baos);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

    return BulkSerialize.readLoadMapping("/some/dir", TableId.of("1"), p -> bais);
  }

  KeyExtent nke(String prev, String end) {
    Text per = prev == null ? null : new Text(prev);
    Text er = end == null ? null : new Text(end);

    return new KeyExtent(TableId.of("1"), er, per);
  }

  /**
   * Serialize bulk load mapping without sorting.
   */
  public static void writeLoadMappingWithoutSorting(Map<KeyExtent,Bulk.Files> loadMapping,
      String sourceDir, BulkSerialize.Output output) throws IOException {
    final Path lmFile = new Path(sourceDir, Constants.BULK_LOAD_MAPPING);

    try (OutputStream fsOut = output.create(lmFile); JsonWriter writer =
        new JsonWriter(new BufferedWriter(new OutputStreamWriter(fsOut, UTF_8)))) {
      Gson gson = createGson();
      writer.setIndent("  ");
      writer.beginArray();
      // Iterate over entries in the order they are inserted
      for (Map.Entry<KeyExtent,Bulk.Files> entry : loadMapping.entrySet()) {
        Bulk.Mapping mapping = new Bulk.Mapping(entry.getKey(), entry.getValue());
        gson.toJson(mapping, Bulk.Mapping.class, writer);
      }
      writer.endArray();
    }
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
      var loadRangesIter = loadRanges.keySet().iterator();

      while (iterator.hasNext()) {
        assertEquals(loadRangesIter.next(), iterator.next().getKey());
      }
      assertFalse(loadRangesIter.hasNext(), "Iterator should consume all expected entries");
    }
  }

  @Test
  void testInvalidOutOfOrderInput() throws IOException {
    Map<KeyExtent,String> loadRanges = new LinkedHashMap<>();
    loadRanges.put(nke("c", "g"), "f2 f3");
    loadRanges.put(nke(null, "c"), "f1 f2");
    loadRanges.put(nke("g", "r"), "f2 f4");
    loadRanges.put(nke("r", "w"), "f2 f5");
    loadRanges.put(nke("w", null), "f2 f6");

    try (LoadMappingIterator iterator = createLoadMappingIter(loadRanges)) {
      assertEquals(nke("c", "g"), iterator.next().getKey());
      assertThrows(IllegalStateException.class, iterator::next,
          "Expected an IllegalStateException due to out-of-order KeyExtents");
    }
  }
}

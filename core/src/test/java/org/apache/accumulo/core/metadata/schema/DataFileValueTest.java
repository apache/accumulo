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
package org.apache.accumulo.core.metadata.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Test;

public class DataFileValueTest {

  @Test
  public void testNoTimeOrRanges() {
    final String json = new DataFileValue(555, 23).encodeAsString();
    final DataFileValue dfv = DataFileValue.decode(json);

    assertEquals(555, dfv.getSize());
    assertEquals(23, dfv.getNumEntries());
    assertEquals(-1, dfv.getTime());
    assertEquals(0, dfv.getRanges().size());
  }

  @Test
  public void testTimeNoRanges() {
    long time = System.currentTimeMillis();
    final String json = new DataFileValue(555, 23, time).encodeAsString();
    final DataFileValue dfv = DataFileValue.decode(json);

    assertEquals(555, dfv.getSize());
    assertEquals(23, dfv.getNumEntries());
    assertEquals(time, dfv.getTime());
    assertEquals(0, dfv.getRanges().size());
  }

  @Test
  public void testInfiniteRange() {
    long time = System.currentTimeMillis();
    final DataFileValue dfv = new DataFileValue(555, 23, time, List.of(new Range()));
    assertEquals(1, dfv.getRanges().size());
    assertEquals(new Range(), dfv.getRanges().get(0));

    // Verify serialization
    final String json = new DataFileValue(555, 23, time, List.of(new Range())).encodeAsString();
    final DataFileValue decoded = DataFileValue.decode(json);

    assertEquals(555, decoded.getSize());
    assertEquals(23, decoded.getNumEntries());
    assertEquals(time, decoded.getTime());
    assertEquals(1, decoded.getRanges().size());
    assertEquals(new Range(), decoded.getRanges().get(0));
  }

  @Test
  public void testComplexRange() {
    final Key key1 = new Key("row1", "cf1", "cq1", 50);
    final Key key2 = new Key("row2", "cf2", "cq2", 100);
    final String json = new DataFileValue(555, 23, List.of(new Range(key1, key2))).encodeAsString();
    final DataFileValue dfv = DataFileValue.decode(json);

    assertEquals(555, dfv.getSize());
    assertEquals(23, dfv.getNumEntries());
    assertEquals(-1, dfv.getTime());
    assertEquals(1, dfv.getRanges().size());
    assertEquals(new Range(key1, key2), dfv.getRanges().get(0));
  }

  @Test
  public void testOverlappingRanges() {
    long time = System.currentTimeMillis();
    final DataFileValue dfv =
        new DataFileValue(555, 23, time, List.of(new Range("abc", "xyz"), new Range("def")));
    // Second range is part of first range so should collapse into 1
    assertEquals(1, dfv.getRanges().size());
    assertEquals(new Range("abc", "xyz"), dfv.getRanges().get(0));
  }

  @Test
  public void testMultipleRanges() {
    long time = System.currentTimeMillis();
    final DataFileValue dfv =
        new DataFileValue(555, 23, time, List.of(new Range("abc", "xyz"), new Range("123", "789")));

    // Ranges will be sorted
    assertEquals(2, dfv.getRanges().size());
    assertEquals(new Range("123", "789"), dfv.getRanges().get(0));
    assertEquals(new Range("abc", "xyz"), dfv.getRanges().get(1));

    // Test encoding
    final String json =
        new DataFileValue(700, 25, time, List.of(new Range("abc", "xyz"), new Range("123", "789")))
            .encodeAsString();
    final DataFileValue decoded = DataFileValue.decode(json);

    assertEquals(700, decoded.getSize());
    assertEquals(25, decoded.getNumEntries());
    assertEquals(time, decoded.getTime());
    assertEquals(2, decoded.getRanges().size());
    assertEquals(2, decoded.getRanges().size());
    assertEquals(new Range("123", "789"), decoded.getRanges().get(0));
    assertEquals(new Range("abc", "xyz"), decoded.getRanges().get(1));
  }

  @Test
  public void testLegacyNoTime() {
    String legacyFormat = "500,25";
    DataFileValue dfv = DataFileValue.decode(legacyFormat);

    assertEquals(500, dfv.getSize());
    assertEquals(25, dfv.getNumEntries());
    assertEquals(-1, dfv.getTime());
  }

  @Test
  public void testLegacy() {
    long time = System.currentTimeMillis();
    String legacyFormat = "500,25," + time;
    DataFileValue dfv = DataFileValue.decode(legacyFormat);

    assertEquals(500, dfv.getSize());
    assertEquals(25, dfv.getNumEntries());
    assertEquals(time, dfv.getTime());
  }

}

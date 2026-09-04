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

package org.apache.accumulo.server.util.adminCommand;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

public class PreSplitUtilTest {
  @Test
  public void testSplitCount() {
    assertEquals(1, PreSplitUtil.generateSplits(1).size());
    assertEquals(4, PreSplitUtil.generateSplits(4).size());
    assertEquals(10, PreSplitUtil.generateSplits(10).size());
  }

  @Test
  public void testAllValidUUIDs() {
    List<String> splits = PreSplitUtil.generateSplits(8);
    for (String s : splits) {
      assertEquals(s, UUID.fromString(s).toString(),
          "Split point is not a valid UUID string: " + s);
    }
  }

  @Test
  public void testSplitsAreAscending() {
    List<String> splits = PreSplitUtil.generateSplits(16);
    for (int i = 0; i < splits.size() - 1; i++) {
      assertTrue(splits.get(i).compareTo(splits.get(i + 1)) < 0,
          "Splits are not in ascending lexicographic order at index " + i);
    }
  }

  @Test
  public void testSingleSplitIsMidpoint() {
    String expected = new UUID(Long.MIN_VALUE, 0).toString();
    assertEquals(expected, PreSplitUtil.generateSplits(1).get(0));
  }

  @Test
  public void testZeroThrows() {
    assertThrows(IllegalArgumentException.class, () -> PreSplitUtil.generateSplits(0));
  }

  @Test
  public void testNegativeThrows() {
    assertThrows(IllegalArgumentException.class, () -> PreSplitUtil.generateSplits(-1));
    assertThrows(IllegalArgumentException.class, () -> PreSplitUtil.generateSplits(-100));
  }

  @Test
  public void testResultIsImmutable() {
    List<String> splits = PreSplitUtil.generateSplits(4);
    assertThrows(UnsupportedOperationException.class, () -> splits.add("extra"));
  }
}

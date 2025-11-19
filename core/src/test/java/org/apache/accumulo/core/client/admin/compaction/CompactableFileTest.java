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
package org.apache.accumulo.core.client.admin.compaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.net.URI;

import org.apache.accumulo.core.data.RowRange;
import org.junit.jupiter.api.Test;

class CompactableFileTest {
  @Test
  public void testEqualsHashcode() throws Exception {
    String prefix = "hdfs://fake/accumulo/tables/1/t-0000000z/";
    var cf1 = CompactableFile.create(new URI(prefix + "F1.rf"), 100, 10);
    assertEquals(new URI(prefix + "F1.rf"), cf1.getUri());
    assertEquals(100, cf1.getEstimatedSize());
    assertEquals(10, cf1.getEstimatedEntries());

    // create compactable files with one change
    var cf2 = CompactableFile.create(new URI(prefix + "F2.rf"), 100, 10);
    var cf3 = CompactableFile.create(new URI(prefix + "F1.rf"), 101, 10);
    var cf4 = CompactableFile.create(new URI(prefix + "F1.rf"), 100, 11);

    assertNotEquals(cf1, cf2);
    assertNotEquals(cf1.hashCode(), cf2.hashCode());
    assertEquals(cf1, cf3);
    assertEquals(cf1.hashCode(), cf3.hashCode());
    assertEquals(cf1, cf4);
    assertEquals(cf1.hashCode(), cf3.hashCode());

    var cf5 = CompactableFile.create(new URI(prefix + "F1.rf"), 100, 11);
    assertEquals(cf1, cf5);
    assertEquals(cf1.hashCode(), cf5.hashCode());

    // the creating with the same file as cf1 and an inf range
    var cf6 = CompactableFile.create(new URI(prefix + "F1.rf"), RowRange.all(), 100, 10);
    assertEquals(cf1, cf6);
    assertEquals(cf1.hashCode(), cf6.hashCode());

    // same file with different range should not be equals
    var cf7 =
        CompactableFile.create(new URI(prefix + "F1.rf"), RowRange.openClosed("c", "f"), 100, 10);
    assertNotEquals(cf1, cf7);
    assertNotEquals(cf1.hashCode(), cf7.hashCode());
    assertEquals(new URI(prefix + "F1.rf"), cf7.getUri());
    assertEquals(100, cf7.getEstimatedSize());
    assertEquals(10, cf7.getEstimatedEntries());
    assertEquals(RowRange.openClosed("c", "f"), cf7.getRange());

    var cf8 =
        CompactableFile.create(new URI(prefix + "F1.rf"), RowRange.openClosed("x", "z"), 100, 10);
    assertNotEquals(cf7, cf8);
    assertNotEquals(cf7.hashCode(), cf8.hashCode());

    // test different objects created with same data
    var cf9 =
        CompactableFile.create(new URI(prefix + "F1.rf"), RowRange.openClosed("x", "z"), 100, 10);
    assertEquals(cf8, cf9);
    assertEquals(cf8.hashCode(), cf9.hashCode());
  }
}

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
package org.apache.accumulo.core.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class StoredTabletFileTest {

  @Test
  public void fileConversionTest() {
    String s21 = "hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf";
    String s31 =
        "{\"path\":\"hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf\",\"startRow\":\"\",\"endRow\":\"\"}";
    String s31_untrimmed =
        "   {  \"path\":\"hdfs://localhost:8020/accumulo/tables/1/t-0000000/A000003v.rf\",\"startRow\":\"\",\"endRow\":\"\"  }   ";

    assertTrue(StoredTabletFile.fileNeedsConversion(s21));
    assertFalse(StoredTabletFile.fileNeedsConversion(s31));
    assertFalse(StoredTabletFile.fileNeedsConversion(s31_untrimmed));
  }

  @Test
  public void externalFileRoundTrip() {
    Path path = new Path("hdfs://external-nn/imports/F0001.rf");
    StoredTabletFile stored = StoredTabletFile.ofExternal(path);

    assertTrue(stored.isExternal());
    assertTrue(stored.getTabletFile() instanceof ExternalTabletFile);
    assertEquals(path, stored.getPath());
    assertTrue(stored.getMetadata().contains("\"type\":\"EXTERNAL\""));

    StoredTabletFile roundTripped = StoredTabletFile.of(stored.getMetadata());
    assertTrue(roundTripped.isExternal());
    assertEquals(stored.getMetadata(), roundTripped.getMetadata());
    assertEquals(stored.getPath(), roundTripped.getPath());
    assertEquals(stored.getRange(), roundTripped.getRange());
    assertThrows(IllegalStateException.class, roundTripped::getTableId);
  }

  @Test
  public void insertingExternalFilePreservesMarker() {
    ExternalTabletFile external =
        ExternalTabletFile.of(new Path("file:///external/files/F0002.rf"));

    StoredTabletFile stored = external.insert();

    assertTrue(stored.isExternal());
    assertTrue(StoredTabletFile.of(stored.getMetadata()).isExternal());
  }

  @Test
  public void changingRangePreservesExternalMarker() {
    Path path = new Path("hdfs://external-nn/imports/F0003.rf");
    StoredTabletFile external = StoredTabletFile.ofExternal(path);
    Range range = new Range("m", false, "z", true);

    StoredTabletFile ranged = external.withRange(range);

    assertTrue(ranged.isExternal());
    assertEquals(path, ranged.getPath());
    assertEquals(range, ranged.getRange());
    assertTrue(StoredTabletFile.of(ranged.getMetadata()).isExternal());
  }
}

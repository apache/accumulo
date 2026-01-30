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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class ReferencedTabletFileTest {

  private ReferencedTabletFile test(String metadataFile, String volume, String tableId,
      String tabletDir, String fileName) {
    String metadataPath = StoredTabletFile.serialize(metadataFile);
    StoredTabletFile storedTabletFile = new StoredTabletFile(metadataPath);
    ReferencedTabletFile tabletFile = storedTabletFile.getTabletFile();

    // Make sure original file name wasn't changed when serialized
    assertTrue(metadataPath.contains(metadataFile));
    assertEquals(volume, tabletFile.getVolume());
    assertEquals(metadataPath, storedTabletFile.getMetadata());
    assertEquals(TableId.of(tableId), tabletFile.getTableId());
    assertEquals(tabletDir, tabletFile.getTabletDir());
    assertEquals(fileName, tabletFile.getFileName());
    return tabletFile;
  }

  @Test
  public void testValidPaths() {
    test("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf",
        "hdfs://localhost:8020/accumulo", "2a", "default_tablet", "F0000070.rf");
    test("hdfs://nn1:9000/accumulo/tables/5a/t-0005/C0009.rf", "hdfs://nn1:9000/accumulo", "5a",
        "t-0005", "C0009.rf");
    test(
        "file:/home/dude/workspace/accumulo/test/target/mini-tests/org.apache.accumulo.test.VolumeIT_test/volumes/v1/tables/1/t-0000003/F0000006.rf",
        "file:/home/dude/workspace/accumulo/test/target/mini-tests/org.apache.accumulo.test.VolumeIT_test/volumes/v1",
        "1", "t-0000003", "F0000006.rf");
  }

  @Test
  public void testBadPaths() {
    // 2a< srv:dir
    final String message = "Failed to throw error on bad path";

    assertThrows(IllegalArgumentException.class,
        () -> test("C0004.rf", "", "2a", "t-0003", "C0004.rf"), message);
    assertThrows(IllegalArgumentException.class, () -> test("dir", "", "2a", "", ""), message);

    assertThrows(IllegalArgumentException.class,
        () -> test("hdfs://localhost:8020/accumulo/tablets/2a/default_tablet/F0000070.rf",
            "hdfs://localhost:8020/accumulo", "2a", "default_tablet", "F0000070.rf"),
        message);
    assertThrows(IllegalArgumentException.class,
        () -> test("hdfs://localhost:8020/accumulo/2a/default_tablet/F0000070.rf",
            " hdfs://localhost:8020/accumulo", "2a", "default_tablet", " F0000070.rf"),
        message);
    assertThrows(IllegalArgumentException.class,
        () -> test("/accumulo/tables/2a/default_tablet/F0000070.rf", "", "2a", "default_tablet",
            "F0000070.rf"),
        message);
    assertThrows(IllegalArgumentException.class,
        () -> test("hdfs://localhost:8020/accumulo/tables/2a/F0000070.rf",
            "hdfs://localhost:8020/accumulo", "2a", "", "F0000070.rf"),
        message);
    assertThrows(IllegalArgumentException.class,
        () -> test("hdfs://localhost:8020/accumulo/tables/F0000070.rf",
            "hdfs://localhost:8020/accumulo", null, "", "F0000070.rf"),
        message);

  }

  private final String id = "2a";
  private final String dir = "t-0003";
  private final String filename = "C0004.rf";

  @Test
  public void testFullPathWithVolume() {
    String volume = "hdfs://1.2.3.4/accumulo";
    String metadataEntry = volume + "/tables/" + id + "/" + dir + "/" + filename;
    test(metadataEntry, volume, id, dir, filename);
  }

  @Test
  public void testNormalizePath() {
    String uglyVolume = "hdfs://nn.somewhere.com:86753/accumulo/blah/.././/bad/bad2/../.././/////";
    String metadataEntry = uglyVolume + "/tables/" + id + "/" + dir + "/" + filename;
    ReferencedTabletFile uglyFile =
        test(metadataEntry, "hdfs://nn.somewhere.com:86753/accumulo", id, dir, filename);
    ReferencedTabletFile niceFile = StoredTabletFile
        .of(new Path(
            "hdfs://nn.somewhere.com:86753/accumulo/tables/" + id + "/" + dir + "/" + filename))
        .getTabletFile();
    assertEquals(niceFile, uglyFile);
    assertEquals(niceFile.hashCode(), uglyFile.hashCode());
  }

  @Test
  public void testNonRowRange() {
    Path testPath = new Path("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf");

    // range where start key is not a row key
    Range r1 = new Range(new Key("r1", "f1"), true, null, false);
    assertThrows(IllegalArgumentException.class, () -> new ReferencedTabletFile(testPath, r1));

    // range where end key is not a row key
    Range r2 = new Range(null, true, new Key("r1", "f1"), false);
    assertThrows(IllegalArgumentException.class, () -> new ReferencedTabletFile(testPath, r2));

    // range where the key looks like a row, but the start key inclusivity is not whats expected
    Range r3 = new Range(new Key("r1").followingKey(PartialKey.ROW), false, new Key("r2"), false);
    assertThrows(IllegalArgumentException.class, () -> new ReferencedTabletFile(testPath, r3));

    // range where the key looks like a row, but the end key inclusivity is not whats expected
    Range r4 = new Range(new Key("r1").followingKey(PartialKey.ROW), true, new Key("r2"), true);
    assertThrows(IllegalArgumentException.class, () -> new ReferencedTabletFile(testPath, r4));

    // range where end key does not end with correct byte and is marked exclusive false
    Range r5 = new Range(new Key("r1").followingKey(PartialKey.ROW), true, new Key("r2"), false);
    assertThrows(IllegalArgumentException.class, () -> new ReferencedTabletFile(testPath, r5));

    // This is valid as the end key is exclusive and ends in 0x00
    Range r6 = new Range(new Key("r1").followingKey(PartialKey.ROW), true,
        new Key("r2").followingKey(PartialKey.ROW), false);
    assertTrue(new ReferencedTabletFile(testPath, r6).hasRange());

    // This is valid as the start key will be converted to inclusive and 0x00 should be appended
    // and the end key will be converted to exclusive and 0x00 should also be appended
    Range r7 = new Range(new Text("r1"), false, new Text("r2"), true);
    assertTrue(new ReferencedTabletFile(testPath, r7).hasRange());

    // This is invalid as the start key is exclusive
    Range r8 = new Range(new Key("r1"), false, new Key("r2").followingKey(PartialKey.ROW), false);
    assertThrows(IllegalArgumentException.class, () -> new ReferencedTabletFile(testPath, r8));

    // This is invalid as the start key is missing the 0x00 byte
    Range r9 = new Range(new Key("r1"), true, new Key("r2").followingKey(PartialKey.ROW), false);
    assertThrows(IllegalArgumentException.class, () -> new ReferencedTabletFile(testPath, r9));
  }

}

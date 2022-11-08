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

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.junit.jupiter.api.Test;

public class TabletFileTest {

  private TabletFile test(String metadataEntry, String volume, String tableId, String tabletDir,
      String fileName) {
    StoredTabletFile tabletFile = new StoredTabletFile(metadataEntry);

    assertEquals(volume, tabletFile.getVolume());
    assertEquals(metadataEntry, tabletFile.getMetaUpdateDelete());
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

    assertThrows(NullPointerException.class, () -> test("C0004.rf", "", "2a", "t-0003", "C0004.rf"),
        message);
    assertThrows(NullPointerException.class, () -> test("dir", "", "2a", "", ""), message);

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
    TabletFile uglyFile =
        test(metadataEntry, "hdfs://nn.somewhere.com:86753/accumulo", id, dir, filename);
    TabletFile niceFile = new StoredTabletFile(
        "hdfs://nn.somewhere.com:86753/accumulo/tables/" + id + "/" + dir + "/" + filename);
    assertEquals(niceFile, uglyFile);
    assertEquals(niceFile.hashCode(), uglyFile.hashCode());
  }

}

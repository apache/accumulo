/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.data.TableId;
import org.junit.Test;

public class TabletFileTest {

  private void test(String metadataEntry, TableId tableId, String tabletDir,
      String fileName) {
    TabletFile tabletFile = new TabletFile(metadataEntry);

    assertEquals(metadataEntry, tabletFile.getMetadataEntry());
    assertEquals(tableId, tabletFile.getTableId());
    assertEquals(tabletDir, tabletFile.getTabletDir());
    assertEquals(fileName, tabletFile.getFileName());

  }

  @Test
  public void testValidPaths() {
    test("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf",
        TableId.of("2a"), "default_tablet", "F0000070.rf");
    test("hdfs://nn1:9000/accumulo/tables/5a/t-0005/C0009.rf",
            TableId.of("5a"), "t-0005", "C0009.rf");
  }

  @Test
  public void testBadPaths() {
    try {
      test("C0004.rf", TableId.of("2a"), "t-0003", "C0004.rf");
      fail("Failed to throw error on bad path");
    } catch (NullPointerException e) {}

    // 2a< srv:dir
    try {
      test("dir", TableId.of("2a"), "", "");
      fail("Failed to throw error on bad path");
    } catch (NullPointerException e) {}
  }

  @Test
  // metadataEntry = "hdfs://1.2.3.4/accumulo/tables/2a/t-0003/C0004.rf"
  public void testFullPathWithVolume() {
    String volume = "hdfs://1.2.3.4/accumulo";
    String id = "2a";
    String dir = "t-0003";
    String filename = "C0004.rf";
    String metadataEntry = volume + "/tables/" + id + "/" + dir + "/" + filename;

    TabletFile tabletFile = new TabletFile(metadataEntry);

    assertEquals(volume, tabletFile.getVolume());
    assertEquals(id, tabletFile.getTableId().canonical());
    assertEquals(dir, tabletFile.getTabletDir());
    assertEquals(filename, tabletFile.getFileName());
  }

}

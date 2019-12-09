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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyBuilder;
import org.apache.accumulo.core.data.TableId;
import org.junit.Test;

public class TabletFileTest {

  private void test(String metadataEntry, Key key, TableId tableId, String tabletDir,
      String fileName) {
    TabletFile parsedTabletFile = TabletFileUtil.newTabletFile(metadataEntry, key);
    TabletFile tabletFile = new TabletFile(metadataEntry, tableId, tabletDir, fileName);

    assertEquals(metadataEntry, tabletFile.getMetadataEntry());
    assertEquals(tableId, tabletFile.getTableId());
    assertEquals(tabletDir, tabletFile.getTabletDir());
    assertEquals(fileName, tabletFile.getFileName());
    assertTrue("Parsed tabletFile not equal to constructed one",
        tabletFile.equals(parsedTabletFile));
  }

  @Test
  public void testValidPaths() {
    KeyBuilder.ColumnQualifierStep rowFamily =
        Key.builder().row("2a<").family(DataFileColumnFamily.STR_NAME);
    test("../2a/t-0003/C0004.rf", rowFamily.qualifier("../2a/t-0003/C0004.rf").build(),
        TableId.of("2a"), "t-0003", "C0004.rf");
    test("/t-0003/C0004.rf", rowFamily.qualifier("/t-0003/C0004.rf").build(), TableId.of("2a"),
        "t-0003", "C0004.rf");
    test("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf", rowFamily
        .qualifier("hdfs://localhost:8020/accumulo/tables/2a/default_tablet/F0000070.rf").build(),
        TableId.of("2a"), "default_tablet", "F0000070.rf");
  }

  @Test
  public void testBadPaths() {
    try {
      test("C0004.rf", Key.builder().row("2a<").family(DataFileColumnFamily.STR_NAME)
          .qualifier("C0004.rf").build(), TableId.of("2a"), "t-0003", "C0004.rf");
      fail("Failed to throw error on bad path");
    } catch (NullPointerException e) {}

    // 2a< srv:dir
    try {
      test("dir", Key.builder().row("2a<").family(ServerColumnFamily.STR_NAME)
          .qualifier(ServerColumnFamily.DIRECTORY_QUAL).build(), TableId.of("2a"), "", "");
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

    TabletFile tabletFile = TabletFileUtil.newTabletFile(metadataEntry, null);

    assertTrue(tabletFile.getVolume().isPresent());
    assertEquals(volume, tabletFile.getVolume().get());
    assertEquals(id, tabletFile.getTableId().canonical());
    assertEquals(dir, tabletFile.getTabletDir());
    assertEquals(filename, tabletFile.getFileName());
  }

}

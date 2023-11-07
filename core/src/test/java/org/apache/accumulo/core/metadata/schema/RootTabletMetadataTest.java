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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RootTabletMetadataTest {
  private static final Logger LOG = LoggerFactory.getLogger(RootTabletMetadataTest.class);

  @Test
  public void convertRoot1File() {
    String root21ZkData =
        "{\"version\":1,\"columnValues\":{\"file\":{\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A000000v.rf\":\"1368,61\"},\"last\":{\"100025091780006\":\"localhost:9997\"},\"loc\":{\"100025091780006\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"3\",\"lock\":\"tservers/localhost:9997/zlock#9db8961a-4ee9-400e-8e80-3353148baadd#0000000000$100025091780006\",\"time\":\"L53\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}";

    RootTabletMetadata rtm = RootTabletMetadata.upgrade(root21ZkData);
    LOG.debug("converted column values: {}", rtm.toTabletMetadata().getFiles());

    var files = rtm.toTabletMetadata().getFiles();
    LOG.info("FILES: {}", rtm.toTabletMetadata().getFilesMap());

    assertEquals(1, files.size());
    assertTrue(files.contains(StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A000000v.rf"))));
  }

  @Test
  public void convertRoot2Files() {
    String root212ZkData2Files =
        "{\"version\":1,\"columnValues\":{\"file\":{\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/00000_00000.rf\":\"0,0\",\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/F000000c.rf\":\"926,18\"},\"last\":{\"10001a84d7d0005\":\"localhost:9997\"},\"loc\":{\"10001a84d7d0005\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"2\",\"lock\":\"tservers/localhost:9997/zlock#d21adaa4-0f97-4004-9ff8-cce9dbb6687f#0000000000$10001a84d7d0005\",\"time\":\"L6\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}\n";

    RootTabletMetadata rtm = RootTabletMetadata.upgrade(root212ZkData2Files);
    LOG.debug("converted column values: {}", rtm.toTabletMetadata());

    var files = rtm.toTabletMetadata().getFiles();
    LOG.info("FILES: {}", rtm.toTabletMetadata().getFilesMap());

    assertEquals(2, files.size());
    assertTrue(files.contains(StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/+r/root_tablet/00000_00000.rf"))));
    assertTrue(files.contains(StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/+r/root_tablet/F000000c.rf"))));
  }

  @Test
  public void needsUpgradeTest() {
    String root212ZkData2Files =
        "{\"version\":1,\"columnValues\":{\"file\":{\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/00000_00000.rf\":\"0,0\",\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/F000000c.rf\":\"926,18\"},\"last\":{\"10001a84d7d0005\":\"localhost:9997\"},\"loc\":{\"10001a84d7d0005\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"2\",\"lock\":\"tservers/localhost:9997/zlock#d21adaa4-0f97-4004-9ff8-cce9dbb6687f#0000000000$10001a84d7d0005\",\"time\":\"L6\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}\n";
    assertTrue(RootTabletMetadata.needsUpgrade(root212ZkData2Files));

    String converted =
        "{\"version\":2,\"columnValues\":{\"file\":{\"{\\\"path\\\":\\\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A0000013.rf\\\",\\\"startRow\\\":\\\"\\\",\\\"endRow\\\":\\\"\\\"}\":\"974,19\",\"{\\\"path\\\":\\\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/F0000014.rf\\\",\\\"startRow\\\":\\\"\\\",\\\"endRow\\\":\\\"\\\"}\":\"708,8\"},\"last\":{\"100024ec6110005\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"6\",\"lock\":\"tservers/localhost:9997/zlock#0f3000c9-ecf9-4bcd-8790-066c3f7a3818#0000000000$100024ec6110005\",\"time\":\"L43\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}";

    assertFalse(RootTabletMetadata.needsUpgrade(converted));
  }

  @Test
  public void ignoresConvertedTest() {
    String converted =
        "{\"version\":2,\"columnValues\":{\"file\":{\"{\\\"path\\\":\\\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A0000013.rf\\\",\\\"startRow\\\":\\\"\\\",\\\"endRow\\\":\\\"\\\"}\":\"974,19\",\"{\\\"path\\\":\\\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/F0000014.rf\\\",\\\"startRow\\\":\\\"\\\",\\\"endRow\\\":\\\"\\\"}\":\"708,8\"},\"last\":{\"100024ec6110005\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"6\",\"lock\":\"tservers/localhost:9997/zlock#0f3000c9-ecf9-4bcd-8790-066c3f7a3818#0000000000$100024ec6110005\",\"time\":\"L43\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}";

    assertFalse(RootTabletMetadata.needsUpgrade(converted));

    RootTabletMetadata rtm = RootTabletMetadata.upgrade(converted);
    var files = rtm.toTabletMetadata().getFiles();
    assertEquals(2, files.size());
    assertTrue(files.contains(StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A0000013.rf"))));
    assertTrue(files.contains(StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/+r/root_tablet/F0000014.rf"))));

  }

  @Test
  public void invalidVersionTest() {
    String valid =
        "{\"version\":1,\"columnValues\":{\"file\":{\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A000000v.rf\":\"1368,61\"},\"last\":{\"100025091780006\":\"localhost:9997\"},\"loc\":{\"100025091780006\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"3\",\"lock\":\"tservers/localhost:9997/zlock#9db8961a-4ee9-400e-8e80-3353148baadd#0000000000$100025091780006\",\"time\":\"L53\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}";
    // only version changed to invalid value
    String invalid =
        "{\"version\":-1,\"columnValues\":{\"file\":{\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A000000v.rf\":\"1368,61\"},\"last\":{\"100025091780006\":\"localhost:9997\"},\"loc\":{\"100025091780006\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"3\",\"lock\":\"tservers/localhost:9997/zlock#9db8961a-4ee9-400e-8e80-3353148baadd#0000000000$100025091780006\",\"time\":\"L53\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}";

    assertTrue(RootTabletMetadata.needsUpgrade(valid));

    RootTabletMetadata rtm = RootTabletMetadata.upgrade(valid);
    var files = rtm.toTabletMetadata().getFiles();
    assertEquals(1, files.size());
    assertTrue(files.contains(StoredTabletFile
        .of(new Path("hdfs://localhost:8020/accumulo/tables/+r/root_tablet/A000000v.rf"))));

    // valid json with files, so try conversion
    assertTrue(RootTabletMetadata.needsUpgrade(invalid));

    assertThrows(IllegalArgumentException.class, () -> RootTabletMetadata.upgrade(invalid));
  }
}

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
    assertEquals(1, rtm.toTabletMetadata().getFiles().size());

    LOG.info("FILES: {}", rtm.toTabletMetadata().getFilesMap());
  }

  @Test
  public void convertRoot2Files() {
    String root212ZkData2Files =
        "{\"version\":1,\"columnValues\":{\"file\":{\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/00000_00000.rf\":\"0,0\",\"hdfs://localhost:8020/accumulo/tables/+r/root_tablet/F000000c.rf\":\"926,18\"},\"last\":{\"10001a84d7d0005\":\"localhost:9997\"},\"loc\":{\"10001a84d7d0005\":\"localhost:9997\"},\"srv\":{\"dir\":\"root_tablet\",\"flush\":\"2\",\"lock\":\"tservers/localhost:9997/zlock#d21adaa4-0f97-4004-9ff8-cce9dbb6687f#0000000000$10001a84d7d0005\",\"time\":\"L6\"},\"~tab\":{\"~pr\":\"\\u0000\"}}}\n";

    RootTabletMetadata rtm = RootTabletMetadata.upgrade(root212ZkData2Files);
    LOG.debug("converted column values: {}", rtm.toTabletMetadata());
    assertEquals(2, rtm.toTabletMetadata().getFiles().size());
  }
}

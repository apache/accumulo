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
package org.apache.accumulo.manager.upgrade;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class UpgradeProgressTest {

  @Test
  void testInitialize() {
    var progress = new UpgradeProgress(42, 1042);
    assertEquals(42, progress.getZooKeeperVersion());
    assertEquals(42, progress.getRootVersion());
    assertEquals(42, progress.getMetadataVersion());

    var progress2 = new UpgradeProgress();
    assertEquals(0, progress2.getZooKeeperVersion());
    assertEquals(0, progress2.getRootVersion());
    assertEquals(0, progress2.getMetadataVersion());
  }

  @Test
  void testToFromJson() {
    var progress = new UpgradeProgress();
    progress.zooKeeperVersion = 5001;
    progress.rootVersion = 5002;
    progress.metadataVersion = 5003;
    progress.upgradeTargetVersion = 5004;

    // serialize and deserialize
    byte[] jsonBytes = progress.toJsonBytes();
    var progress2 = UpgradeProgress.fromJsonBytes(jsonBytes);
    assertEquals(5001, progress2.getZooKeeperVersion());
    assertEquals(5002, progress2.getRootVersion());
    assertEquals(5003, progress2.getMetadataVersion());
    assertEquals(5004, progress2.getUpgradeTargetVersion());

    // show original is unchanged
    assertEquals(5001, progress.getZooKeeperVersion());
    assertEquals(5002, progress.getRootVersion());
    assertEquals(5003, progress.getMetadataVersion());
    assertEquals(5004, progress.getUpgradeTargetVersion());

    // test deserialization with a test json string, so we know the deserialization/serialization is
    // actually using json, and not some other reversible format
    var json =
        "{\"zooKeeperVersion\":7001,\"rootVersion\":7002,\"metadataVersion\":7003,\"upgradeTargetVersion\":7004}";
    var progress3 = UpgradeProgress.fromJsonBytes(json.getBytes(UTF_8));
    assertEquals(7001, progress3.getZooKeeperVersion());
    assertEquals(7002, progress3.getRootVersion());
    assertEquals(7003, progress3.getMetadataVersion());
    assertEquals(7004, progress3.getUpgradeTargetVersion());
  }

}

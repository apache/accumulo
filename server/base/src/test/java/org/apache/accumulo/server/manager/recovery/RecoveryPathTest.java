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
package org.apache.accumulo.server.manager.recovery;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

class RecoveryPathTest {

  private static final String ID = "11111111-1111-1111-1111-111111111111";

  @Test
  void mapsNewWalLayout() {
    var log = LogEntry.fromPath("dfs", "hdfs://nn/accumulo/wal/" + ID);
    assertEquals(new Path("hdfs://nn/accumulo/recovery/" + ID), RecoveryPath.getRecoveryPath(log));
  }

  @Test
  void mapsLegacyWalLayout() {
    var log = LogEntry.fromPath("dfs", "hdfs://nn/accumulo/wal/host+1234/" + ID);
    assertEquals(new Path("hdfs://nn/accumulo/recovery/" + ID), RecoveryPath.getRecoveryPath(log));
  }

  @Test
  void requiresUuidAsBasenameAndWalDirectory() {
    var wrongWalDirectory = LogEntry.fromPath("dfs", "hdfs://nn/accumulo/other/" + ID);
    assertThrows(IllegalArgumentException.class,
        () -> RecoveryPath.getRecoveryPath(wrongWalDirectory));

    var original = LogEntry.fromPath("dfs", "hdfs://nn/accumulo/wal/" + ID);
    var wrongUuid = LogEntry.withSwitchedVolume(original,
        new Path("hdfs://nn/accumulo/wal/22222222-2222-2222-2222-222222222222"));
    assertThrows(IllegalArgumentException.class, () -> RecoveryPath.getRecoveryPath(wrongUuid));
  }

  @Test
  void distributesOpaqueWalRecoveryAcrossVolumesDeterministically() {
    List<String> recoveryDirectories = List.of("hdfs://nn1/accumulo/recovery",
        "hdfs://nn2/accumulo/recovery", "hdfs://nn3/accumulo/recovery");
    Set<String> selectedVolumes = new HashSet<>();

    for (int i = 0; i < 300; i++) {
      UUID uuid = UUID.nameUUIDFromBytes(("wal-" + i).getBytes(UTF_8));
      String reference = "custom://logs/wal?uuid=" + uuid + "&shard=" + i;
      LogEntry logEntry = LogEntry.fromReference("custom", reference, uuid);
      Path selected = RecoveryPath.getRecoveryPath(logEntry, recoveryDirectories);
      Path selectedWithDifferentOrder = RecoveryPath.getRecoveryPath(logEntry, List
          .of(recoveryDirectories.get(2), recoveryDirectories.get(0), recoveryDirectories.get(1)));
      assertEquals(selected, selectedWithDifferentOrder);
      assertEquals(uuid.toString(), selected.getName());
      selectedVolumes.add(selected.getParent().toString());
    }

    assertEquals(recoveryDirectories.size(), selectedVolumes.size());
  }
}

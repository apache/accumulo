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
package org.apache.accumulo.core.fate.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.junit.jupiter.api.Test;

public class FateLockTest {

  @Test
  public void testParsing() {
    var fateId = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    // ZooKeeper docs state that sequence numbers are formatted using %010d
    var lockNode = new FateLock.FateLockNode(
        FateLock.PREFIX + fateId.canonical() + "#" + String.format("%010d", 40));
    assertEquals(40, lockNode.sequence);
    assertEquals(fateId.canonical(), lockNode.lockData);

    assertThrows(IllegalArgumentException.class,
        () -> new FateLock.FateLockNode(fateId.canonical() + "#" + String.format("%010d", 40)));
    assertThrows(IllegalArgumentException.class, () -> new FateLock.FateLockNode(
        FateLock.PREFIX + fateId.canonical() + "#" + String.format("%d", 40)));
    assertThrows(IllegalArgumentException.class, () -> new FateLock.FateLockNode(
        FateLock.PREFIX + fateId.canonical() + "#" + String.format("%09d", 40)));
    assertThrows(IllegalArgumentException.class, () -> new FateLock.FateLockNode(
        FateLock.PREFIX + fateId.canonical() + "#" + String.format("%011d", 40)));
    assertThrows(IllegalArgumentException.class,
        () -> new FateLock.FateLockNode(FateLock.PREFIX + fateId.canonical() + "#abc"));
    assertThrows(IllegalArgumentException.class, () -> new FateLock.FateLockNode(
        FateLock.PREFIX + fateId.canonical() + String.format("%010d", 40)));

    // ZooKeeper docs state that sequence numbers can roll and become negative. The FateLock code
    // does not support this, so make sure it fails if this happens.
    for (int i : new int[] {Integer.MIN_VALUE, Integer.MIN_VALUE / 2, Integer.MIN_VALUE / 10,
        Integer.MIN_VALUE / 1000, -40}) {
      assertThrows(IllegalArgumentException.class, () -> new FateLock.FateLockNode(
          FateLock.PREFIX + fateId.canonical() + "#" + String.format("%010d", i)));
    }

    assertThrows(IllegalArgumentException.class, () -> new FateLock.FateLockNode(
        FateLock.PREFIX + fateId.canonical() + "#" + String.format("%d", -40)));
  }
}

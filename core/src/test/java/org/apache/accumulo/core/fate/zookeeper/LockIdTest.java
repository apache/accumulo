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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.accumulo.core.fate.zookeeper.ZooUtil.LockID;
import org.junit.jupiter.api.Test;

public class LockIdTest {

  @Test
  public void testNormalUse() {
    var lockId = new LockID("/p1", "n1", 42);
    assertEquals("/p1", lockId.path);
    assertEquals("n1", lockId.node);
    assertEquals(42, lockId.eid);
    assertEquals("/p1/n1$2a", lockId.serialize());
    assertEquals(lockId, lockId);

    var lockId2 = LockID.deserialize(lockId.serialize());
    assertEquals("/p1", lockId2.path);
    assertEquals("n1", lockId2.node);
    assertEquals(42, lockId2.eid);
    assertEquals("/p1/n1$2a", lockId2.serialize());

    assertEquals(lockId.hashCode(), lockId2.hashCode());
    assertEquals(lockId, lockId2);

    var lockId3 = new LockID("/p2", lockId.node, lockId.eid);
    assertNotEquals(lockId, lockId3);
    assertNotEquals(lockId.hashCode(), lockId3.hashCode());

    var lockId4 = new LockID(lockId.path, "n2", lockId.eid);
    assertNotEquals(lockId, lockId4);
    assertNotEquals(lockId.hashCode(), lockId4.hashCode());

    var lockId5 = new LockID(lockId.path, lockId.node, lockId.eid + 1);
    assertNotEquals(lockId, lockId5);
    assertNotEquals(lockId.hashCode(), lockId5.hashCode());

    var lockId6 = new LockID("/p1", "n1", -42);
    assertEquals(-42, lockId6.eid);
    assertEquals("/p1/n1$ffffffffffffffd6", lockId6.serialize());
    assertEquals(lockId6, LockID.deserialize(lockId6.serialize()));
    assertEquals(-42, LockID.deserialize(lockId6.serialize()).eid);
  }

  @Test
  public void testUnexpected() {
    // test illegal paths
    assertThrows(IllegalArgumentException.class, () -> new LockID("", "n1", 42));
    assertThrows(IllegalArgumentException.class, () -> new LockID("/p1/", "n1", 42));
    assertThrows(IllegalArgumentException.class, () -> new LockID("p1/", "n1", 42));
    assertThrows(IllegalArgumentException.class, () -> new LockID("/p$1", "n1", 42));
    assertThrows(IllegalArgumentException.class, () -> new LockID(null, "n1", 42));

    // test illegal node names
    assertThrows(IllegalArgumentException.class, () -> new LockID("/p1", null, 42));
    assertThrows(IllegalArgumentException.class, () -> new LockID("/p1", "", 42));
    assertThrows(IllegalArgumentException.class, () -> new LockID("/p1", "/n1", 42));
    assertThrows(IllegalArgumentException.class, () -> new LockID("/p1", "n$1", 42));

    // test illegal serialized data
    assertThrows(IllegalArgumentException.class, () -> LockID.deserialize("/p1/n1$"));
    assertThrows(IllegalArgumentException.class, () -> LockID.deserialize("/p1/n1$foo"));
    assertThrows(IllegalArgumentException.class, () -> LockID.deserialize(""));
    assertThrows(IllegalArgumentException.class, () -> LockID.deserialize("/p1//n1$2a"));
    assertThrows(IllegalArgumentException.class, () -> LockID.deserialize("/p1/n1/$2a"));
    assertThrows(IllegalArgumentException.class, () -> LockID.deserialize("p1/n1$2a"));
    assertThrows(IllegalArgumentException.class, () -> LockID.deserialize("/n1$2a"));
    assertThrows(IllegalArgumentException.class, () -> LockID.deserialize("/p1/n1#2a"));
    assertThrows(IllegalArgumentException.class, () -> LockID.deserialize("p1n1$2a"));

    // Test equals w/ a different type
    assertNotEquals(new LockID("/p1", "n1", 42), "/p1/n1$2a");
  }

}

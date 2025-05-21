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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;

import org.junit.jupiter.api.Test;

public class ExternalCopmactionIdTest {
  @Test
  public void test() {
    UUID uuid1 = UUID.randomUUID();
    UUID uuid2 = UUID.randomUUID();

    var ecid1 = ExternalCompactionId.generate(uuid1);
    var ecid1_1 = ExternalCompactionId.generate(uuid1);
    assertEquals(ecid1, ecid1_1);
    assertEquals(ecid1.hashCode(), ecid1_1.hashCode());
    assertEquals(ExternalCompactionId.of(ecid1.canonical()), ecid1_1);
    assertEquals(ExternalCompactionId.of(ecid1.canonical()).hashCode(), ecid1_1.hashCode());
    assertEquals(ExternalCompactionId.from(ecid1.canonical()), ecid1_1);
    assertEquals(ExternalCompactionId.from(ecid1.canonical()).hashCode(), ecid1_1.hashCode());
    assertEquals(ExternalCompactionId.from(uuid1.toString()), ecid1_1);

    var ecid2 = ExternalCompactionId.generate(uuid2);
    assertNotEquals(ecid1, ecid2);
    assertNotEquals(ecid1.hashCode(), ecid2.hashCode());

    assertEquals(uuid1.toString().charAt(0), ecid1.getFirstUUIDChar());
    assertEquals(uuid2.toString().charAt(0), ecid2.getFirstUUIDChar());
  }

  @Test
  public void testIllegal() {
    UUID uuid1 = UUID.randomUUID();
    assertThrows(IllegalArgumentException.class, () -> ExternalCompactionId.of(uuid1.toString()));
    assertThrows(IllegalArgumentException.class, () -> ExternalCompactionId.of("ECID:"));
    assertThrows(IllegalArgumentException.class, () -> ExternalCompactionId.of(""));
    assertThrows(IllegalArgumentException.class, () -> ExternalCompactionId.from(""));
    assertThrows(IllegalArgumentException.class, () -> ExternalCompactionId.of("ECID:"));
  }
}

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
package org.apache.accumulo.core.fate;

import static org.apache.accumulo.core.fate.FateInstanceType.META;
import static org.apache.accumulo.core.fate.FateInstanceType.USER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

public class FateIdTest {

  @Test
  public void testIllegal() {
    var uuid = UUID.randomUUID();

    var legalId = FateId.from(USER, uuid);

    // test the test assumptions about what is legal
    assertNotNull(FateId.from("FATE:" + META + ":" + uuid));
    assertEquals(legalId, FateId.from(legalId.canonical()));

    // The fate id has three fields, try making each field invalid and having the wrong number of
    // fields
    for (var illegalId : new String[] {"1234567890", "FATE:" + uuid, "FATE:GOLANG:" + uuid,
        META + ":" + uuid, USER + ":" + uuid, "RUST:" + META + ":" + uuid,
        "RUST:" + USER + ":" + uuid, legalId + ":JAVA", "JAVA:" + legalId, "FATE:" + legalId}) {
      assertThrows(IllegalArgumentException.class, () -> FateId.from(illegalId));
      assertFalse(FateId.isFateId(illegalId));
    }

    // Try different illegal uuids in the fate id
    for (var illegalUuid : List.of("3366-4485-92ab-6961bbd6d3f4", "075cd820-4485-92ab-6961bbd6d3f4",
        "075cd820-3366-4485-92ab", "075C++20-3366-4485-92ab-6961bbd6d3f4",
        "075cd820-    -4485-92ab-6961bbd6d3f4", "42b64e8-4307-4f7d-8466-a11e81eb56c7",
        "842b64e8-307-4f7d-8466-a11e81eb56c7", "842b64e8-4307-f7d-8466-a11e81eb56c7",
        "842b64e8-4307-4f7d-466-a11e81eb56c7", "842b64e8-4307-4f7d-8466-11e81eb56c7",
        "842b64e843074f7d8466a11e81eb56c7")) {
      var illegalId = "FATE:" + USER + ":" + illegalUuid;
      assertThrows(IllegalArgumentException.class, () -> FateId.from(illegalId));
      assertFalse(FateId.isFateId(illegalId));
    }

    // Try an illegal character in every position
    for (int i = 0; i < legalId.toString().length(); i++) {
      var chars = legalId.toString().toCharArray();
      chars[i] = '#';
      var illegalId = new String(chars);
      assertThrows(IllegalArgumentException.class, () -> FateId.from(illegalId));
      assertFalse(FateId.isFateId(illegalId));
    }

    // place number and dash chars in unexpected positions
    for (int i = 0; i < legalId.toString().length(); i++) {
      var chars = legalId.toString().toCharArray();
      if (chars[i] == '-') {
        chars[i] = '2';
      } else {
        chars[i] = '-';
      }
      var illegalId = new String(chars);
      assertThrows(IllegalArgumentException.class, () -> FateId.from(illegalId));
      assertFalse(FateId.isFateId(illegalId));

    }

  }
}

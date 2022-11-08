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
package org.apache.accumulo.core.security;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.security.SecureRandom;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer;
import org.apache.accumulo.core.client.security.tokens.NullToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.junit.jupiter.api.Test;

public class AuthenticationTokenTest {

  private static final SecureRandom random = new SecureRandom();

  @Test
  public void testSerializeDeserializeToken() {
    byte[] randomBytes = new byte[12];
    do {
      // random fill, but avoid all zeros case
      random.nextBytes(randomBytes);
    } while (IntStream.range(0, randomBytes.length).allMatch(i -> randomBytes[i] == 0));

    byte[] serialized = AuthenticationTokenSerializer.serialize(new PasswordToken(randomBytes));
    PasswordToken passwordToken =
        AuthenticationTokenSerializer.deserialize(PasswordToken.class, serialized);
    assertArrayEquals(randomBytes, passwordToken.getPassword());

    serialized = AuthenticationTokenSerializer.serialize(new NullToken());
    AuthenticationToken nullToken =
        AuthenticationTokenSerializer.deserialize(NullToken.class, serialized);
    assertEquals(new NullToken(), nullToken);
  }
}

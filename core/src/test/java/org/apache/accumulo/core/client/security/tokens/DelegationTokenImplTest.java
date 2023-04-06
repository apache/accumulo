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
package org.apache.accumulo.core.client.security.tokens;

import static org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier.createTAuthIdentifier;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.junit.jupiter.api.Test;

public class DelegationTokenImplTest {

  @Test
  public void testSerialization() throws IOException {
    byte[] passBytes = new byte[] {'f', 'a', 'k', 'e'};
    AuthenticationTokenIdentifier identifier = new AuthenticationTokenIdentifier(
        createTAuthIdentifier("user", 1, 1000L, 2000L, "instanceid"));
    DelegationTokenImpl token = new DelegationTokenImpl(passBytes, identifier);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    token.write(new DataOutputStream(baos));

    DelegationTokenImpl copy = new DelegationTokenImpl();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

    assertEquals(token.getServiceName(), copy.getServiceName());
    assertEquals(token, copy);
    assertEquals(token.hashCode(), copy.hashCode());
  }

  @Test
  public void testEquality() {
    AuthenticationTokenIdentifier identifier = new AuthenticationTokenIdentifier(
        createTAuthIdentifier("user", 1, 1000L, 2000L, "instanceid"));
    // We don't need a real serialized Token for the password
    DelegationTokenImpl token =
        new DelegationTokenImpl(new byte[] {'f', 'a', 'k', 'e'}, identifier);

    AuthenticationTokenIdentifier identifier2 = new AuthenticationTokenIdentifier(
        createTAuthIdentifier("user1", 1, 1000L, 2000L, "instanceid"));
    // We don't need a real serialized Token for the password
    DelegationTokenImpl token2 =
        new DelegationTokenImpl(new byte[] {'f', 'a', 'k', 'e'}, identifier2);

    assertNotEquals(token, token2);
    assertNotEquals(token.hashCode(), token2.hashCode());

    // We don't need a real serialized Token for the password
    DelegationTokenImpl token3 =
        new DelegationTokenImpl(new byte[] {'f', 'a', 'k', 'e', '0'}, identifier);

    assertNotEquals(token, token3);
    assertNotEquals(token.hashCode(), token3.hashCode());
    assertNotEquals(token2, token3);
    assertNotEquals(token2.hashCode(), token3.hashCode());
  }
}

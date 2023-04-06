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
package org.apache.accumulo.server.rpc;

import static org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier.createTAuthIdentifier;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import javax.crypto.KeyGenerator;
import javax.security.auth.callback.Callback;

import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.clientImpl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.rpc.SaslDigestCallbackHandler;
import org.apache.accumulo.server.security.delegation.AuthenticationKey;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SaslDigestCallbackHandlerTest {

  /**
   * Allows access to the methods on SaslDigestCallbackHandler
   */
  private static class SaslTestDigestCallbackHandler extends SaslDigestCallbackHandler {
    @Override
    public void handle(Callback[] callbacks) {
      throw new UnsupportedOperationException();
    }
  }

  // From org.apache.hadoop.security.token.SecretManager
  private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA1";
  private static final int KEY_LENGTH = 64;
  private static KeyGenerator keyGen;

  @BeforeAll
  public static void setupKeyGenerator() throws Exception {
    // From org.apache.hadoop.security.token.SecretManager
    keyGen = KeyGenerator.getInstance(DEFAULT_HMAC_ALGORITHM);
    keyGen.init(KEY_LENGTH);
  }

  private SaslTestDigestCallbackHandler handler;
  private DelegationTokenConfig cfg;

  @BeforeEach
  public void setup() {
    handler = new SaslTestDigestCallbackHandler();
    cfg = new DelegationTokenConfig();
  }

  @Test
  public void testIdentifierSerialization() throws IOException {
    var tAuthIdentifier = createTAuthIdentifier("user", 1, 100L, 1000L, "instanceid");
    var identifier = new AuthenticationTokenIdentifier(tAuthIdentifier);
    byte[] serialized = identifier.getBytes();
    String name = handler.encodeIdentifier(serialized);

    byte[] reserialized = handler.decodeIdentifier(name);
    assertArrayEquals(serialized, reserialized);

    AuthenticationTokenIdentifier copy = new AuthenticationTokenIdentifier();
    copy.readFields(new DataInputStream(new ByteArrayInputStream(reserialized)));

    assertEquals(identifier, copy);
  }

  @Test
  public void testTokenSerialization() throws Exception {
    var secretManager = new AuthenticationTokenSecretManager(InstanceId.of("instanceid"), 1000L);

    secretManager.addKey(new AuthenticationKey(1, 0L, 100L, keyGen.generateKey()));
    var entry = secretManager.generateToken("user", cfg);
    byte[] password = entry.getKey().getPassword();
    char[] encodedPassword = handler.encodePassword(password);

    char[] computedPassword = handler.getPassword(secretManager, entry.getValue());

    assertArrayEquals(computedPassword, encodedPassword);
  }

  @Test
  public void testTokenAndIdentifierSerialization() throws Exception {
    var secretManager = new AuthenticationTokenSecretManager(InstanceId.of("instanceid"), 1000L);
    var key = new AuthenticationKey(1, 0L, 100_000L, keyGen.generateKey());
    secretManager.addKey(key);
    var entry = secretManager.generateToken("user", cfg);
    byte[] password = entry.getKey().getPassword();
    char[] encodedPassword = handler.encodePassword(password);
    String name = handler.encodeIdentifier(entry.getValue().getBytes());

    byte[] decodedIdentifier = handler.decodeIdentifier(name);
    AuthenticationTokenIdentifier identifier = new AuthenticationTokenIdentifier();
    identifier.readFields(new DataInputStream(new ByteArrayInputStream(decodedIdentifier)));
    char[] computedPassword = handler.getPassword(secretManager, identifier);

    assertArrayEquals(computedPassword, encodedPassword);
  }
}

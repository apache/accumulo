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
package org.apache.accumulo.server.security.delegation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AuthenticationKeyTest {
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

  @Test
  public void testNullSecretKey() {
    assertThrows(NullPointerException.class, () -> new AuthenticationKey(0, 0, 0, null));
  }

  @Test
  public void testAuthKey() {
    SecretKey secretKey = keyGen.generateKey();
    int keyId = 20;
    long creationDate = 38383838L, expirationDate = 83838383L;
    AuthenticationKey authKey =
        new AuthenticationKey(keyId, creationDate, expirationDate, secretKey);
    assertEquals(secretKey, authKey.getKey());
    assertEquals(keyId, authKey.getKeyId());
    assertEquals(expirationDate, authKey.getExpirationDate());

    // Empty instance
    AuthenticationKey badCopy = new AuthenticationKey();

    assertNotEquals(badCopy, authKey);
    assertNotEquals(badCopy.hashCode(), authKey.hashCode());

    // Different object, same arguments
    AuthenticationKey goodCopy =
        new AuthenticationKey(keyId, creationDate, expirationDate, secretKey);
    assertEquals(authKey, goodCopy);
    assertEquals(authKey.hashCode(), goodCopy.hashCode());
  }

  @Test
  public void testWritable() throws IOException {
    SecretKey secretKey = keyGen.generateKey();
    int keyId = 20;
    long creationDate = 38383838L, expirationDate = 83838383L;
    AuthenticationKey authKey =
        new AuthenticationKey(keyId, creationDate, expirationDate, secretKey);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    authKey.write(out);
    byte[] serialized = baos.toByteArray();

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized));
    AuthenticationKey copy = new AuthenticationKey();
    copy.readFields(in);

    assertEquals(authKey, copy);
    assertEquals(authKey.hashCode(), copy.hashCode());
    assertEquals(secretKey, copy.getKey());
    assertEquals(keyId, copy.getKeyId());
    assertEquals(expirationDate, copy.getExpirationDate());
  }
}

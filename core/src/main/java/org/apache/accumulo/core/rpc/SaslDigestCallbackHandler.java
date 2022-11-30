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
package org.apache.accumulo.core.rpc;

import java.util.Base64;

import javax.security.auth.callback.CallbackHandler;

import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Common serialization methods across the client and server callback handlers for SASL.
 * Serialization and deserialization methods must be kept in sync.
 */
public abstract class SaslDigestCallbackHandler implements CallbackHandler {

  /**
   * Encode the serialized {@link TokenIdentifier} into a {@link String}.
   *
   * @param identifier The serialized identifier
   * @see #decodeIdentifier(String)
   */
  public String encodeIdentifier(byte[] identifier) {
    return Base64.getEncoder().encodeToString(identifier);
  }

  /**
   * Encode the token password into a character array.
   *
   * @param password The token password
   * @see #getPassword(SecretManager, TokenIdentifier)
   */
  public char[] encodePassword(byte[] password) {
    return Base64.getEncoder().encodeToString(password).toCharArray();
  }

  /**
   * Generate the password from the provided {@link SecretManager} and {@link TokenIdentifier}.
   *
   * @param secretManager The server SecretManager
   * @param tokenid The TokenIdentifier from the client
   * @see #encodePassword(byte[])
   */
  public <T extends TokenIdentifier> char[] getPassword(SecretManager<T> secretManager, T tokenid)
      throws InvalidToken {
    return encodePassword(secretManager.retrievePassword(tokenid));
  }

  /**
   * Decode the encoded {@link TokenIdentifier} into bytes suitable to reconstitute the identifier.
   *
   * @param identifier The encoded, serialized {@link TokenIdentifier}
   * @see #encodeIdentifier(byte[])
   */
  public byte[] decodeIdentifier(String identifier) {
    return Base64.getDecoder().decode(identifier);
  }

}

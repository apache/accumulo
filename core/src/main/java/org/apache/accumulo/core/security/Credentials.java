/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.security;

import static com.google.common.base.Charsets.UTF_8;

import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken.AuthenticationTokenSerializer;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.Base64;

/**
 * A wrapper for internal use. This class carries the instance, principal, and authentication token for use in the public API, in a non-serialized form. This is
 * important, so that the authentication token carried in a {@link Connector} can be destroyed, invalidating future RPC operations from that {@link Connector}.
 * <p>
 * See ACCUMULO-1312
 *
 * @since 1.6.0
 */
public class Credentials {

  private String principal;
  private AuthenticationToken token;

  /**
   * Creates a new credentials object.
   *
   * @param principal
   *          unique identifier for the entity (e.g. a user or service) authorized for these credentials
   * @param token
   *          authentication token used to prove that the principal for these credentials has been properly verified
   */
  public Credentials(String principal, AuthenticationToken token) {
    this.principal = principal;
    this.token = token;
  }

  /**
   * Gets the principal.
   *
   * @return unique identifier for the entity (e.g. a user or service) authorized for these credentials
   */
  public String getPrincipal() {
    return principal;
  }

  /**
   * Gets the authentication token.
   *
   * @return authentication token used to prove that the principal for these credentials has been properly verified
   */
  public AuthenticationToken getToken() {
    return token;
  }

  /**
   * Converts the current object to the relevant thrift type. The object returned from this contains a non-destroyable version of the
   * {@link AuthenticationToken}, so this should be used just before placing on the wire, and references to it should be tightly controlled.
   *
   * @param instance
   *          client instance
   * @return Thrift credentials
   * @throws RuntimeException
   *           if the authentication token has been destroyed (expired)
   */
  public TCredentials toThrift(Instance instance) {
    TCredentials tCreds = new TCredentials(getPrincipal(), getToken().getClass().getName(),
        ByteBuffer.wrap(AuthenticationTokenSerializer.serialize(getToken())), instance.getInstanceID());
    if (getToken().isDestroyed())
      throw new RuntimeException("Token has been destroyed", new AccumuloSecurityException(getPrincipal(), SecurityErrorCode.TOKEN_EXPIRED));
    return tCreds;
  }

  /**
   * Converts a given thrift object to our internal Credentials representation.
   *
   * @param serialized
   *          a Thrift encoded set of credentials
   * @return a new Credentials instance; destroy the token when you're done.
   */
  public static Credentials fromThrift(TCredentials serialized) {
    return new Credentials(serialized.getPrincipal(), AuthenticationTokenSerializer.deserialize(serialized.getTokenClassName(), serialized.getToken()));
  }

  /**
   * Converts the current object to a serialized form. The object returned from this contains a non-destroyable version of the {@link AuthenticationToken}, so
   * references to it should be tightly controlled.
   *
   * @return serialized form of these credentials
   */
  public final String serialize() {
    return (getPrincipal() == null ? "-" : Base64.encodeBase64String(getPrincipal().getBytes(UTF_8))) + ":"
        + (getToken() == null ? "-" : Base64.encodeBase64String(getToken().getClass().getName().getBytes(UTF_8))) + ":"
        + (getToken() == null ? "-" : Base64.encodeBase64String(AuthenticationTokenSerializer.serialize(getToken())));
  }

  /**
   * Converts the serialized form to an instance of {@link Credentials}. The original serialized form will not be affected.
   *
   * @param serializedForm
   *          serialized form of credentials
   * @return deserialized credentials
   */
  public static final Credentials deserialize(String serializedForm) {
    String[] split = serializedForm.split(":", 3);
    String principal = split[0].equals("-") ? null : new String(Base64.decodeBase64(split[0]), UTF_8);
    String tokenType = split[1].equals("-") ? null : new String(Base64.decodeBase64(split[1]), UTF_8);
    AuthenticationToken token = null;
    if (!split[2].equals("-")) {
      byte[] tokenBytes = Base64.decodeBase64(split[2]);
      token = AuthenticationTokenSerializer.deserialize(tokenType, tokenBytes);
    }
    return new Credentials(principal, token);
  }

  @Override
  public int hashCode() {
    return getPrincipal() == null ? 0 : getPrincipal().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof Credentials))
      return false;
    Credentials other = Credentials.class.cast(obj);
    boolean pEq = getPrincipal() == null ? (other.getPrincipal() == null) : (getPrincipal().equals(other.getPrincipal()));
    if (!pEq)
      return false;
    boolean tEq = getToken() == null ? (other.getToken() == null) : (getToken().equals(other.getToken()));
    return tEq;
  }

  @Override
  public String toString() {
    return getClass().getName() + ":" + getPrincipal() + ":" + (getToken() == null ? null : getToken().getClass().getName()) + ":<hidden>";
  }
}

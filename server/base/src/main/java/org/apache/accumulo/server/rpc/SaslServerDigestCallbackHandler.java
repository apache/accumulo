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
package org.apache.accumulo.server.rpc;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.apache.accumulo.core.client.impl.AuthenticationTokenIdentifier;
import org.apache.accumulo.core.rpc.SaslDigestCallbackHandler;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CallbackHandler for SASL DIGEST-MD5 mechanism. Modified copy from Hadoop, uses our TokenIdentifier and SecretManager implementations
 */
public class SaslServerDigestCallbackHandler extends SaslDigestCallbackHandler {
  private static final Logger log = LoggerFactory.getLogger(SaslServerDigestCallbackHandler.class);
  private static final String NAME = SaslServerDigestCallbackHandler.class.getSimpleName();

  private AuthenticationTokenSecretManager secretManager;

  public SaslServerDigestCallbackHandler(AuthenticationTokenSecretManager secretManager) {
    this.secretManager = secretManager;
  }

  private AuthenticationTokenIdentifier getIdentifier(String id, AuthenticationTokenSecretManager secretManager) throws InvalidToken {
    byte[] tokenId = decodeIdentifier(id);
    AuthenticationTokenIdentifier tokenIdentifier = secretManager.createIdentifier();
    try {
      tokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
    } catch (IOException e) {
      throw (InvalidToken) new InvalidToken("Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }

  @Override
  public void handle(Callback[] callbacks) throws InvalidToken, UnsupportedCallbackException {
    NameCallback nc = null;
    PasswordCallback pc = null;
    AuthorizeCallback ac = null;
    for (Callback callback : callbacks) {
      if (callback instanceof AuthorizeCallback) {
        ac = (AuthorizeCallback) callback;
      } else if (callback instanceof NameCallback) {
        nc = (NameCallback) callback;
      } else if (callback instanceof PasswordCallback) {
        pc = (PasswordCallback) callback;
      } else if (callback instanceof RealmCallback) {
        continue; // realm is ignored
      } else {
        throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback");
      }
    }

    if (pc != null) {
      AuthenticationTokenIdentifier tokenIdentifier = getIdentifier(nc.getDefaultName(), secretManager);
      char[] password = getPassword(secretManager, tokenIdentifier);
      UserGroupInformation user = null;
      user = tokenIdentifier.getUser();

      // Set the principal since we already deserialized the token identifier
      UGIAssumingProcessor.getRpcPrincipalThreadLocal().set(user.getUserName());

      log.trace("SASL server DIGEST-MD5 callback: setting password for client: {}", tokenIdentifier.getUser());
      pc.setPassword(password);
    }
    if (ac != null) {
      String authid = ac.getAuthenticationID();
      String authzid = ac.getAuthorizationID();
      if (authid.equals(authzid)) {
        ac.setAuthorized(true);
      } else {
        ac.setAuthorized(false);
      }
      if (ac.isAuthorized()) {
        String username = getIdentifier(authzid, secretManager).getUser().getUserName();
        log.trace("SASL server DIGEST-MD5 callback: setting canonicalized client ID: {}", username);
        ac.setAuthorizedID(authzid);
      }
    }
  }

  @Override
  public String toString() {
    return NAME;
  }
}

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

import static java.util.Objects.requireNonNull;

import java.util.Arrays;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;

import org.apache.accumulo.core.clientImpl.DelegationTokenImpl;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client-side callbackhandler for sasl authentication which is the client-side sibling to the
 * server-side {@link SaslDigestCallbackHandler}. Encoding of name, password and realm information
 * must be consistent across the pair.
 */
public class SaslClientDigestCallbackHandler extends SaslDigestCallbackHandler {
  private static final Logger log = LoggerFactory.getLogger(SaslClientDigestCallbackHandler.class);
  private static final String NAME = SaslClientDigestCallbackHandler.class.getSimpleName();

  private final String userName;
  private final char[] userPassword;

  public SaslClientDigestCallbackHandler(DelegationTokenImpl token) {
    requireNonNull(token);
    this.userName = encodeIdentifier(token.getIdentifier().getBytes());
    this.userPassword = encodePassword(token.getPassword());
  }

  public SaslClientDigestCallbackHandler(String userName, char[] userPassword) {
    requireNonNull(userName);
    requireNonNull(userPassword);
    this.userName = userName;
    this.userPassword = userPassword;
  }

  @Override
  public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
    NameCallback nc = null;
    PasswordCallback pc = null;
    RealmCallback rc = null;
    for (Callback callback : callbacks) {
      if (callback instanceof RealmChoiceCallback) {
        continue;
      } else if (callback instanceof NameCallback) {
        nc = (NameCallback) callback;
      } else if (callback instanceof PasswordCallback) {
        pc = (PasswordCallback) callback;
      } else if (callback instanceof RealmCallback) {
        rc = (RealmCallback) callback;
      } else {
        throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback");
      }
    }
    if (nc != null) {
      log.debug("SASL client callback: setting username: {}", userName);
      nc.setName(userName);
    }
    if (pc != null) {
      log.debug("SASL client callback: setting userPassword");
      pc.setPassword(userPassword);
    }
    if (rc != null) {
      log.debug("SASL client callback: setting realm: {}", rc.getDefaultText());
      rc.setText(rc.getDefaultText());
    }
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(41, 47);
    hcb.append(userName).append(userPassword);
    return hcb.toHashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o instanceof SaslClientDigestCallbackHandler) {
      SaslClientDigestCallbackHandler other = (SaslClientDigestCallbackHandler) o;
      return userName.equals(other.userName) && Arrays.equals(userPassword, other.userPassword);
    }
    return false;
  }
}

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
package org.apache.accumulo.core.client.impl;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegationTokenImpl extends PasswordToken implements DelegationToken {
  private static final Logger log = LoggerFactory.getLogger(DelegationTokenImpl.class);

  public static final String SERVICE_NAME = "AccumuloDelegationToken";

  private AuthenticationTokenIdentifier identifier;

  public DelegationTokenImpl() {
    super();
  }

  public DelegationTokenImpl(byte[] delegationTokenPassword, AuthenticationTokenIdentifier identifier) {
    requireNonNull(delegationTokenPassword);
    requireNonNull(identifier);
    setPassword(delegationTokenPassword);
    this.identifier = identifier;
  }

  public DelegationTokenImpl(Instance instance, UserGroupInformation user, AuthenticationTokenIdentifier identifier) {
    requireNonNull(instance);
    requireNonNull(user);
    requireNonNull(identifier);

    Credentials creds = user.getCredentials();
    Token<? extends TokenIdentifier> token = creds.getToken(new Text(SERVICE_NAME + "-" + instance.getInstanceID()));
    if (null == token) {
      throw new IllegalArgumentException("Did not find Accumulo delegation token in provided UserGroupInformation");
    }
    setPasswordFromToken(token, identifier);
  }

  public DelegationTokenImpl(Token<? extends TokenIdentifier> token, AuthenticationTokenIdentifier identifier) {
    requireNonNull(token);
    requireNonNull(identifier);
    setPasswordFromToken(token, identifier);
  }

  private void setPasswordFromToken(Token<? extends TokenIdentifier> token, AuthenticationTokenIdentifier identifier) {
    if (!AuthenticationTokenIdentifier.TOKEN_KIND.equals(token.getKind())) {
      String msg = "Expected an AuthenticationTokenIdentifier but got a " + token.getKind();
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }

    setPassword(token.getPassword());
    this.identifier = identifier;
  }

  /**
   * The identifier for this token, may be null.
   */
  public AuthenticationTokenIdentifier getIdentifier() {
    return identifier;
  }

  /**
   * The service name used to identify the {@link Token}
   */
  public Text getServiceName() {
    requireNonNull(identifier);
    return new Text(SERVICE_NAME + "-" + identifier.getInstanceId());
  }

  @Override
  public void init(Properties properties) {
    // Encourage use of UserGroupInformation as entry point
  }

  @Override
  public Set<TokenProperty> getProperties() {
    // Encourage use of UserGroupInformation as entry point
    return Collections.emptySet();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    identifier.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    identifier = new AuthenticationTokenIdentifier();
    identifier.readFields(in);
  }

  @Override
  public DelegationTokenImpl clone() {
    DelegationTokenImpl copy = (DelegationTokenImpl) super.clone();
    copy.setPassword(getPassword());
    copy.identifier = new AuthenticationTokenIdentifier(identifier);
    return copy;
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ identifier.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    // We assume we can cast obj to DelegationToken because the super.equals(obj) check ensures obj is of the same type as this
    return super.equals(obj) && identifier.equals(((DelegationTokenImpl) obj).identifier);
  }

}

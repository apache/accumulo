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
package org.apache.accumulo.core.clientImpl;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.securityImpl.thrift.TAuthenticationTokenIdentifier;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.ThriftMessageUtil;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Implementation that identifies the underlying {@link Token} for Accumulo.
 */
public class AuthenticationTokenIdentifier extends TokenIdentifier {
  public static final Text TOKEN_KIND = new Text("ACCUMULO_AUTH_TOKEN");

  private final TAuthenticationTokenIdentifier impl;

  public AuthenticationTokenIdentifier() {
    impl = new TAuthenticationTokenIdentifier();
    populateFields(impl);
  }

  public AuthenticationTokenIdentifier(TAuthenticationTokenIdentifier identifier) {
    requireNonNull(identifier);
    impl = new TAuthenticationTokenIdentifier(identifier);
    populateFields(identifier);
  }

  public void setKeyId(int keyId) {
    impl.setKeyId(keyId);
  }

  public int getKeyId() {
    return impl.getKeyId();
  }

  public void setIssueDate(long issueDate) {
    impl.setIssueDate(issueDate);
  }

  public long getIssueDate() {
    return impl.getIssueDate();
  }

  public void setExpirationDate(long expirationDate) {
    impl.setExpirationDate(expirationDate);
  }

  public long getExpirationDate() {
    return impl.getExpirationDate();
  }

  public void setInstanceId(InstanceId instanceId) {
    impl.setInstanceId(instanceId.canonical());
  }

  public InstanceId getInstanceId() {
    if (impl.getInstanceId() == null) {
      return InstanceId.of("");
    } else {
      return InstanceId.of(impl.getInstanceId());
    }
  }

  public TAuthenticationTokenIdentifier getThriftIdentifier() {
    return impl;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ThriftMessageUtil msgUtil = new ThriftMessageUtil();
    ByteBuffer serialized = msgUtil.serialize(impl);
    out.writeInt(serialized.limit());
    ByteBufferUtil.write(out, serialized);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    if (length > 0) {
      ThriftMessageUtil msgUtil = new ThriftMessageUtil();
      byte[] serialized = new byte[length];
      in.readFully(serialized);
      var tAuthTokenId = msgUtil.deserialize(serialized, new TAuthenticationTokenIdentifier());
      populateFields(tAuthTokenId);
    }
  }

  private void populateFields(TAuthenticationTokenIdentifier tAuthTokenId) {
    impl.principal = tAuthTokenId.getPrincipal();
    setExpirationDate(tAuthTokenId.getExpirationDate());
    setIssueDate(tAuthTokenId.getIssueDate());
    if (tAuthTokenId.getInstanceId() != null) {
      setInstanceId(InstanceId.of(tAuthTokenId.getInstanceId()));
    }
    setKeyId(tAuthTokenId.getKeyId());
  }

  @Override
  public Text getKind() {
    return TOKEN_KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    if (impl.isSetPrincipal()) {
      return UserGroupInformation.createRemoteUser(impl.getPrincipal());
    }
    return null;
  }

  @Override
  public int hashCode() {
    HashCodeBuilder hcb = new HashCodeBuilder(7, 11);
    if (impl.isSetPrincipal()) {
      hcb.append(impl.getPrincipal());
    }
    if (impl.isSetKeyId()) {
      hcb.append(impl.getKeyId());
    }
    if (impl.isSetIssueDate()) {
      hcb.append(impl.getIssueDate());
    }
    if (impl.isSetExpirationDate()) {
      hcb.append(impl.getExpirationDate());
    }
    if (impl.isSetInstanceId()) {
      hcb.append(impl.getInstanceId());
    }
    return hcb.toHashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(128);
    sb.append("AuthenticationTokenIdentifier(").append(impl).append(")");
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (o instanceof AuthenticationTokenIdentifier) {
      AuthenticationTokenIdentifier other = (AuthenticationTokenIdentifier) o;
      return impl.equals(other.impl);
    }
    return false;
  }

  public static TAuthenticationTokenIdentifier createTAuthIdentifier(String principal, int keyId,
      long issueDate, long expirationDate, String instanceId) {
    TAuthenticationTokenIdentifier tIdentifier = new TAuthenticationTokenIdentifier(principal);
    tIdentifier.setKeyId(keyId);
    tIdentifier.setIssueDate(issueDate);
    tIdentifier.setExpirationDate(expirationDate);
    tIdentifier.setInstanceId(instanceId);
    return tIdentifier;
  }

  public boolean isSetIssueDate() {
    return impl.isSetIssueDate();
  }

  public boolean isSetExpirationDate() {
    return impl.isSetExpirationDate();
  }
}

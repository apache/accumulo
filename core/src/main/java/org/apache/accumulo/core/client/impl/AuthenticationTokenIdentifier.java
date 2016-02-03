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
import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.security.thrift.TAuthenticationTokenIdentifier;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.ThriftMessageUtil;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Implementation that identifies the underlying {@link Token} for Accumulo.
 */
public class AuthenticationTokenIdentifier extends TokenIdentifier {
  public static final Text TOKEN_KIND = new Text("ACCUMULO_AUTH_TOKEN");

  private TAuthenticationTokenIdentifier impl = null;
  private DelegationTokenConfig cfg = null;

  public AuthenticationTokenIdentifier() {
    // noop for Writable
  }

  public AuthenticationTokenIdentifier(String principal) {
    this(principal, null);
  }

  public AuthenticationTokenIdentifier(String principal, DelegationTokenConfig cfg) {
    requireNonNull(principal);
    impl = new TAuthenticationTokenIdentifier(principal);
    this.cfg = cfg;
  }

  public AuthenticationTokenIdentifier(String principal, int keyId, long issueDate, long expirationDate, String instanceId) {
    requireNonNull(principal);
    impl = new TAuthenticationTokenIdentifier(principal);
    impl.setKeyId(keyId);
    impl.setIssueDate(issueDate);
    impl.setExpirationDate(expirationDate);
    impl.setInstanceId(instanceId);
  }

  public AuthenticationTokenIdentifier(AuthenticationTokenIdentifier identifier) {
    requireNonNull(identifier);
    impl = new TAuthenticationTokenIdentifier(identifier.getThriftIdentifier());
  }

  public AuthenticationTokenIdentifier(TAuthenticationTokenIdentifier identifier) {
    requireNonNull(identifier);
    impl = new TAuthenticationTokenIdentifier(identifier);
  }

  public void setKeyId(int keyId) {
    impl.setKeyId(keyId);
  }

  public int getKeyId() {
    requireNonNull(impl, "Identifier not initialized");
    return impl.getKeyId();
  }

  public void setIssueDate(long issueDate) {
    requireNonNull(impl, "Identifier not initialized");
    impl.setIssueDate(issueDate);
  }

  public long getIssueDate() {
    requireNonNull(impl, "Identifier not initialized");
    return impl.getIssueDate();
  }

  public void setExpirationDate(long expirationDate) {
    requireNonNull(impl, "Identifier not initialized");
    impl.setExpirationDate(expirationDate);
  }

  public long getExpirationDate() {
    requireNonNull(impl, "Identifier not initialized");
    return impl.getExpirationDate();
  }

  public void setInstanceId(String instanceId) {
    requireNonNull(impl, "Identifier not initialized");
    impl.setInstanceId(instanceId);
  }

  public String getInstanceId() {
    requireNonNull(impl, "Identifier not initialized");
    return impl.getInstanceId();
  }

  public TAuthenticationTokenIdentifier getThriftIdentifier() {
    requireNonNull(impl);
    return impl;
  }

  /**
   * A configuration from the requesting user, may be null.
   */
  public DelegationTokenConfig getConfig() {
    return cfg;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (null != impl) {
      ThriftMessageUtil msgUtil = new ThriftMessageUtil();
      ByteBuffer serialized = msgUtil.serialize(impl);
      out.writeInt(serialized.limit());
      ByteBufferUtil.write(out, serialized);
    } else {
      out.writeInt(0);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = in.readInt();
    if (length > 0) {
      ThriftMessageUtil msgUtil = new ThriftMessageUtil();
      byte[] serialized = new byte[length];
      in.readFully(serialized);
      impl = new TAuthenticationTokenIdentifier();
      msgUtil.deserialize(serialized, impl);
    }
  }

  @Override
  public Text getKind() {
    return TOKEN_KIND;
  }

  @Override
  public UserGroupInformation getUser() {
    if (null != impl && impl.isSetPrincipal()) {
      return UserGroupInformation.createRemoteUser(impl.getPrincipal());
    }
    return null;
  }

  @Override
  public int hashCode() {
    if (null == impl) {
      return 0;
    }
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
    if (null == o) {
      return false;
    }
    if (o instanceof AuthenticationTokenIdentifier) {
      AuthenticationTokenIdentifier other = (AuthenticationTokenIdentifier) o;
      if (null == impl) {
        return null == other.impl;
      }
      return impl.equals(other.impl);
    }
    return false;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.accumulo.server.security.delegation;

import static java.util.Objects.requireNonNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import javax.crypto.SecretKey;

import org.apache.accumulo.core.security.thrift.TAuthenticationKey;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.ThriftMessageUtil;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Represents a secret key used for signing and verifying authentication tokens by {@link AuthenticationTokenSecretManager}.
 */
public class AuthenticationKey implements Writable {
  private TAuthenticationKey authKey;
  private SecretKey secret;

  public AuthenticationKey() {
    // for Writable
  }

  public AuthenticationKey(int keyId, long creationDate, long expirationDate, SecretKey key) {
    requireNonNull(key);
    authKey = new TAuthenticationKey(ByteBuffer.wrap(key.getEncoded()));
    authKey.setCreationDate(creationDate);
    authKey.setKeyId(keyId);
    authKey.setExpirationDate(expirationDate);
    this.secret = key;
  }

  public int getKeyId() {
    requireNonNull(authKey);
    return authKey.getKeyId();
  }

  public long getCreationDate() {
    requireNonNull(authKey);
    return authKey.getCreationDate();
  }

  public void setCreationDate(long creationDate) {
    requireNonNull(authKey);
    authKey.setCreationDate(creationDate);
  }

  public long getExpirationDate() {
    requireNonNull(authKey);
    return authKey.getExpirationDate();
  }

  public void setExpirationDate(long expirationDate) {
    requireNonNull(authKey);
    authKey.setExpirationDate(expirationDate);
  }

  SecretKey getKey() {
    return secret;
  }

  void setKey(SecretKey secret) {
    this.secret = secret;
  }

  @Override
  public int hashCode() {
    if (null == authKey) {
      return 1;
    }
    HashCodeBuilder hcb = new HashCodeBuilder(29, 31);
    hcb.append(authKey.getKeyId()).append(authKey.getExpirationDate()).append(authKey.getCreationDate()).append(secret.getEncoded());
    return hcb.toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj instanceof AuthenticationKey && Objects.equals(authKey, ((AuthenticationKey) obj).authKey);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("AuthenticationKey[");
    if (null == authKey) {
      buf.append("null]");
    } else {
      buf.append("id=").append(authKey.getKeyId()).append(", expiration=").append(authKey.getExpirationDate()).append(", creation=")
          .append(authKey.getCreationDate()).append("]");
    }
    return buf.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (null == authKey) {
      WritableUtils.writeVInt(out, 0);
      return;
    }
    ThriftMessageUtil util = new ThriftMessageUtil();
    ByteBuffer serialized = util.serialize(authKey);
    WritableUtils.writeVInt(out, serialized.limit() - serialized.arrayOffset());
    ByteBufferUtil.write(out, serialized);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    if (0 == length) {
      return;
    }

    ThriftMessageUtil util = new ThriftMessageUtil();
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    authKey = util.deserialize(bytes, new TAuthenticationKey());
    secret = AuthenticationTokenSecretManager.createSecretKey(authKey.getSecret());
  }
}

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
package org.apache.accumulo.core.client.security.tokens;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.security.auth.DestroyFailedException;

import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @since 1.5.0
 */

public class PasswordToken implements AuthenticationToken {
  private byte[] password = null;

  public byte[] getPassword() {
    return Arrays.copyOf(password, password.length);
  }

  /**
   * Constructor for use with {@link Writable}. Call {@link #readFields(DataInput)}.
   */
  public PasswordToken() {
    password = new byte[0];
  }

  /**
   * Constructs a token from a copy of the password. Destroying the argument after construction will not destroy the copy in this token, and destroying this
   * token will only destroy the copy held inside this token, not the argument.
   *
   * Password tokens created with this constructor will store the password as UTF-8 bytes.
   */
  public PasswordToken(CharSequence password) {
    setPassword(CharBuffer.wrap(password));
  }

  /**
   * Constructs a token from a copy of the password. Destroying the argument after construction will not destroy the copy in this token, and destroying this
   * token will only destroy the copy held inside this token, not the argument.
   */
  public PasswordToken(byte[] password) {
    this.password = Arrays.copyOf(password, password.length);
  }

  /**
   * Constructs a token from a copy of the password. Destroying the argument after construction will not destroy the copy in this token, and destroying this
   * token will only destroy the copy held inside this token, not the argument.
   */
  public PasswordToken(ByteBuffer password) {
    this.password = ByteBufferUtil.toBytes(password);
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    password = WritableUtils.readCompressedByteArray(arg0);
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    WritableUtils.writeCompressedByteArray(arg0, password);
  }

  @Override
  public void destroy() throws DestroyFailedException {
    Arrays.fill(password, (byte) 0x00);
    password = null;
  }

  @Override
  public boolean isDestroyed() {
    return password == null;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(password);
  }

  @Override
  public boolean equals(Object obj) {
    // Instances of PasswordToken should only be considered equal if they are of the same type.
    // This check is done here to ensure that this class is equal to the class of the object being checked.
    return this == obj || (obj != null && getClass().equals(obj.getClass()) && Arrays.equals(password, ((PasswordToken) obj).password));
  }

  @Override
  public PasswordToken clone() {
    try {
      PasswordToken clone = (PasswordToken) super.clone();
      clone.password = Arrays.copyOf(password, password.length);
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }

  protected void setPassword(byte[] password) {
    this.password = Arrays.copyOf(password, password.length);
  }

  protected void setPassword(CharBuffer charBuffer) {
    // encode() kicks back a C-string, which is not compatible with the old passwording system
    ByteBuffer bb = UTF_8.encode(charBuffer);
    // create array using byter buffer length
    this.password = new byte[bb.remaining()];
    bb.get(this.password);
    if (!bb.isReadOnly()) {
      // clear byte buffer
      bb.rewind();
      while (bb.remaining() > 0) {
        bb.put((byte) 0);
      }
    }
  }

  @Override
  public void init(Properties properties) {
    if (properties.containsKey("password")) {
      setPassword(CharBuffer.wrap(properties.get("password")));
    } else
      throw new IllegalArgumentException("Missing 'password' property");
  }

  @Override
  public Set<TokenProperty> getProperties() {
    Set<TokenProperty> internal = new LinkedHashSet<>();
    internal.add(new TokenProperty("password", "the password for the principal", true));
    return internal;
  }
}

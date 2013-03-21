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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.security.auth.DestroyFailedException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

/**
 * @since 1.5.0
 */

public class PasswordToken implements AuthenticationToken {
  private byte[] password = null;
  
  public byte[] getPassword() {
    return password;
  }
  
  /**
   * Constructor for use with {@link Writable}. Call {@link #readFields(DataInput)}.
   */
  public PasswordToken() {}
  
  /**
   * Constructs a token from a copy of the password. Destroying the argument after construction will not destroy the copy in this token, and destroying this
   * token will only destroy the copy held inside this token, not the argument.
   * 
   * Password tokens created with this constructor will store the password as UTF-8 bytes.
   */
  public PasswordToken(CharSequence password) {
    this.password = password.toString().getBytes(Constants.UTF8);
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
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof PasswordToken))
      return false;
    PasswordToken other = (PasswordToken) obj;
    return Arrays.equals(password, other.password);
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
}

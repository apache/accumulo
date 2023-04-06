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
package org.apache.accumulo.core.client.security.tokens;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
   * Constructs a token from a copy of the password. Destroying the argument after construction will
   * not destroy the copy in this token, and destroying this token will only destroy the copy held
   * inside this token, not the argument.
   *
   * Password tokens created with this constructor will store the password as UTF-8 bytes.
   */
  public PasswordToken(CharSequence password) {
    setPassword(CharBuffer.wrap(password));
  }

  /**
   * Constructs a token from a copy of the password. Destroying the argument after construction will
   * not destroy the copy in this token, and destroying this token will only destroy the copy held
   * inside this token, not the argument.
   */
  public PasswordToken(byte[] password) {
    this.password = Arrays.copyOf(password, password.length);
  }

  /**
   * Constructs a token from a copy of the password. Destroying the argument after construction will
   * not destroy the copy in this token, and destroying this token will only destroy the copy held
   * inside this token, not the argument.
   */
  public PasswordToken(ByteBuffer password) {
    this.password = ByteBufferUtil.toBytes(password);
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    int version = arg0.readInt();
    // -1 is null, consistent with legacy format; legacy format length must be >= -1
    // so, use -2 as a magic number to indicate the new format
    if (version == -1) {
      password = null;
    } else if (version == -2) {
      byte[] passwordTmp = new byte[arg0.readInt()];
      arg0.readFully(passwordTmp);
      password = passwordTmp;
    } else {
      // legacy format; should avoid reading/writing compressed byte arrays using WritableUtils,
      // because GZip is expensive and it doesn't actually compress passwords very well
      AtomicBoolean calledFirstReadInt = new AtomicBoolean(false);
      DataInput wrapped = (DataInput) Proxy.newProxyInstance(DataInput.class.getClassLoader(),
          arg0.getClass().getInterfaces(), (obj, method, args) -> {
            // wrap the original DataInput in order to return the integer that was read
            // and then not used, because it didn't match -2
            if (!calledFirstReadInt.get() && method.getName().equals("readInt")) {
              calledFirstReadInt.set(true);
              return version;
            }
            try {
              return method.invoke(arg0, args);
            } catch (InvocationTargetException e) {
              throw e.getCause();
            }
          });
      password = WritableUtils.readCompressedByteArray(wrapped);
    }
  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    if (password == null) {
      arg0.writeInt(-1);
      return;
    }
    arg0.writeInt(-2); // magic number
    arg0.writeInt(password.length);
    arg0.write(password);
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
    // This check is done here to ensure that this class is equal to the class of the object being
    // checked.
    return this == obj || (obj != null && getClass().equals(obj.getClass())
        && Arrays.equals(password, ((PasswordToken) obj).password));
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
    // create array using byte buffer length
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
    } else {
      throw new IllegalArgumentException("Missing 'password' property");
    }
  }

  @Override
  public Set<TokenProperty> getProperties() {
    Set<TokenProperty> internal = new LinkedHashSet<>();
    internal.add(new TokenProperty("password", "the password for the principal", true));
    return internal;
  }
}

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
package org.apache.accumulo.core.security.thrift.tokens;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import javax.security.auth.DestroyFailedException;

import org.apache.hadoop.io.WritableUtils;

public class PasswordToken implements SecurityToken {
  private byte[] password = null;
  
  public byte[] getPassword() {
    return password;
  }

  public PasswordToken setPassword(byte[] password) {
    this.password = password;
    return this;
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
    Arrays.fill(password, (byte)0x00);
    password = null;
  }

  @Override
  public boolean isDestroyed() {
    return password==null;
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
    if (!Arrays.equals(password, other.password))
      return false;
    return true;
  }
  
  public PasswordToken clone() {
    return new PasswordToken().setPassword(Arrays.copyOf(password, password.length));
  }
}

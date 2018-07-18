/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.core.security.crypto;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class CryptoEnvironment {
  /**
   * Where in Accumulo the on-disk file encryption takes place.
   *
   * The parameters won't be present until after Encryption but is required to be set prior to
   * decryption.
   */
  public enum Scope {
    WAL, RFILE;
  }

  private Scope scope;
  private Map<String,String> conf;
  private byte[] parameters;

  public CryptoEnvironment(Scope scope, Map<String,String> conf) {
    this.scope = scope;
    this.conf = conf;
  }

  public Scope getScope() {
    return this.scope;
  }

  public Map<String,String> getConf() {
    return this.conf;
  }

  public byte[] getParameters() {
    return parameters;
  }

  public void setParameters(byte[] params) {
    this.parameters = params;
  }

  /**
   * Read the crypto parameters from the DataInputStream into the crypto environment
   */
  public void readParams(DataInputStream in) throws IOException {
    int len = in.readInt();
    this.parameters = new byte[len];
    int bytesRead = in.read(this.parameters);
    if (bytesRead != len) {
      throw new CryptoService.CryptoException("Incorrect number of bytes read for crypto params.");
    }
  }

  /**
   * Write the crypto parameters in this crypto environment to the DataOutputStream
   */
  public void writeParams(DataOutputStream out) throws IOException {
    Objects.requireNonNull(this.parameters, "Crypto parameters are null. Please read them into "
        + "this crypto environment before calling writeParams.");
    out.writeInt(this.parameters.length);
    out.write(this.parameters);
  }
}

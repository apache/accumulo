/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.crypto;

import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;
import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_SERVICE;
import static org.apache.accumulo.core.conf.Property.TSERV_WAL_CRYPTO_PREFIX;
import static org.apache.accumulo.core.conf.Property.TSERV_WAL_CRYPTO_SERVICE;
import static org.apache.accumulo.core.spi.crypto.CryptoEnvironment.Scope.TABLE;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Objects;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoService.CryptoException;
import org.apache.accumulo.core.spi.crypto.FileDecrypter;
import org.apache.commons.io.IOUtils;

public class CryptoUtils {

  public static SecureRandom newSha1SecureRandom() {
    return newSecureRandom("SHA1PRNG", "SUN");
  }

  private static SecureRandom newSecureRandom(String secureRNG, String secureRNGProvider) {
    SecureRandom secureRandom = null;
    try {
      secureRandom = SecureRandom.getInstance(secureRNG, secureRNGProvider);

      // Immediately seed the generator
      byte[] throwAway = new byte[16];
      secureRandom.nextBytes(throwAway);
    } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
      throw new CryptoException("Unable to generate secure random.", e);
    }
    return secureRandom;
  }

  /**
   * Read the decryption parameters from the DataInputStream
   */
  public static byte[] readParams(DataInputStream in) throws IOException {
    Objects.requireNonNull(in);
    int len = in.readInt();
    byte[] decryptionParams = new byte[len];
    IOUtils.readFully(in, decryptionParams);
    return decryptionParams;
  }

  /**
   * Read the decryption parameters from the DataInputStream and get the FileDecrypter associated
   * with the provided CryptoService and CryptoEnvironment.Scope.
   */
  public static FileDecrypter getFileDecrypter(CryptoService cs, CryptoEnvironment.Scope scope,
      DataInputStream in) throws IOException {
    byte[] decryptionParams = readParams(in);
    CryptoEnvironment decEnv = new CryptoEnvironmentImpl(scope, decryptionParams);
    return cs.getFileDecrypter(decEnv);
  }

  /**
   * Write the decryption parameters to the DataOutputStream
   */
  public static void writeParams(byte[] decryptionParams, DataOutputStream out) throws IOException {
    Objects.requireNonNull(decryptionParams);
    Objects.requireNonNull(out);
    out.writeInt(decryptionParams.length);
    out.write(decryptionParams);
  }

  public static Property getPropPerScope(CryptoEnvironment.Scope scope) {
    return (scope == TABLE ? TABLE_CRYPTO_SERVICE : TSERV_WAL_CRYPTO_SERVICE);
  }

  public static Property getPrefixPerScope(CryptoEnvironment.Scope scope) {
    return (scope == TABLE ? TABLE_CRYPTO_PREFIX : TSERV_WAL_CRYPTO_PREFIX);
  }

}

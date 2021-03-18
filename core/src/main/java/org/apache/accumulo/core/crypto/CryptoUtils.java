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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_ENCRYPT_SERVICE;
import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;
import static org.apache.accumulo.core.conf.Property.TSERV_WALOG_CRYPTO_ENCRYPT_SERVICE;
import static org.apache.accumulo.core.conf.Property.TSERV_WALOG_CRYPTO_PREFIX;
import static org.apache.accumulo.core.spi.crypto.CryptoService.Scope.RFILE;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.spi.crypto.CryptoException;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.FileDecrypter;
import org.apache.accumulo.core.spi.crypto.NoCryptoService;
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
   * Write the decryption parameters to the DataOutputStream
   */
  public static void writeParams(byte[] decryptionParams, DataOutputStream out) throws IOException {
    Objects.requireNonNull(decryptionParams);
    Objects.requireNonNull(out);
    out.writeInt(decryptionParams.length);
    out.write(decryptionParams);
  }

  public static Property getPropPerScope(CryptoService.Scope scope) {
    return (scope == RFILE ? TABLE_CRYPTO_ENCRYPT_SERVICE : TSERV_WALOG_CRYPTO_ENCRYPT_SERVICE);
  }

  public static Property getPrefixPerScope(CryptoService.Scope scope) {
    return (scope == RFILE ? TABLE_CRYPTO_PREFIX : TSERV_WALOG_CRYPTO_PREFIX);
  }

  /**
   * Read the crypto service name from decryptionParams and if it matches one of the provided
   * decrypters, call the init method and return the FileDecrypter. The crypto service name is
   * required to be the first bytes written to the decryptionParams.
   */
  public static FileDecrypter getDecrypterInitialized(CryptoService.Scope scope,
      List<CryptoService> decrypters, byte[] decryptionParams) {
    if (decryptionParams == null || checkNoCrypto(decryptionParams))
      return NoCryptoService.NO_DECRYPT;

    String nameInFile = getCryptoServiceName(decryptionParams);
    CryptoService cs = null;
    // check if service in file matches what was loaded from config
    for (CryptoService d : decrypters) {
      if (d.getClass().getName().equals(nameInFile)) {
        cs = d;
        break;
      }
    }
    if (cs == null)
      throw new CryptoException("Unknown crypto class found in " + scope + " file: " + nameInFile);

    // init decrypter and return
    var initParams = new FileDecrypter.InitParams() {
      @Override
      public byte[] getDecryptionParameters() {
        return decryptionParams;
      }
    };
    var decrypter = cs.getDecrypter();
    decrypter.init(initParams);
    return decrypter;
  }

  /**
   * The crypto service name is required to be the first bytes written to the decryption params.
   */
  private static String getCryptoServiceName(byte[] decryptionParams) {
    // the name is required to be first
    String name;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(decryptionParams);
        DataInputStream params = new DataInputStream(bais)) {
      name = params.readUTF();
    } catch (IOException e) {
      throw new CryptoException("Error reading crypto params", e);
    }
    return name;
  }

  private static boolean checkNoCrypto(byte[] params) {
    byte[] noCryptoBytes = NoCryptoService.VERSION.getBytes(UTF_8);
    return Arrays.equals(params, noCryptoBytes);
  }
}

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
package org.apache.accumulo.core.security.crypto;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Objects;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.NullCipher;

import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.CryptoService.CryptoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CryptoUtils {

  private static final Logger log = LoggerFactory.getLogger(CryptoUtils.class);

  public static SecureRandom getSha1SecureRandom() {
    return getSecureRandom("SHA1PRNG", "SUN");
  }

  public static SecureRandom getSecureRandom(String secureRNG, String secureRNGProvider) {
    SecureRandom secureRandom = null;
    try {
      secureRandom = SecureRandom.getInstance(secureRNG, secureRNGProvider);

      // Immediately seed the generator
      byte[] throwAway = new byte[16];
      secureRandom.nextBytes(throwAway);

    } catch (NoSuchAlgorithmException e) {
      log.error(String.format("Accumulo configuration file specified a secure"
          + " random generator \"%s\" that was not found by any provider.", secureRNG));
      throw new CryptoException(e);
    } catch (NoSuchProviderException e) {
      log.error(String.format("Accumulo configuration file specified a secure"
          + " random provider \"%s\" that does not exist", secureRNGProvider));
      throw new CryptoException(e);
    }
    return secureRandom;
  }

  public static Cipher getCipher(String cipherSuite, String securityProvider) {
    Cipher cipher = null;

    if (cipherSuite.equals("NullCipher")) {
      cipher = new NullCipher();
    } else {
      try {
        if (securityProvider == null || securityProvider.equals("")) {
          cipher = Cipher.getInstance(cipherSuite);
        } else {
          cipher = Cipher.getInstance(cipherSuite, securityProvider);
        }
      } catch (NoSuchAlgorithmException e) {
        log.error(String.format("Accumulo configuration file contained a cipher"
            + " suite \"%s\" that was not recognized by any providers", cipherSuite));
        throw new CryptoException(e);
      } catch (NoSuchPaddingException e) {
        log.error(String.format(
            "Accumulo configuration file contained a"
                + " cipher, \"%s\" with a padding that was not recognized by any" + " providers",
            cipherSuite));
        throw new CryptoException(e);
      } catch (NoSuchProviderException e) {
        log.error(String.format(
            "Accumulo configuration file contained a provider, \"%s\" an unrecognized provider",
            securityProvider));
        throw new CryptoException(e);
      }
    }
    return cipher;
  }

  /**
   * Read the decryption parameters from the DataInputStream
   */
  public static byte[] readParams(DataInputStream in) throws IOException {
    Objects.requireNonNull(in);
    int len = in.readInt();
    byte[] decryptionParams = new byte[len];
    int bytesRead = in.read(decryptionParams);
    if (bytesRead != len) {
      throw new CryptoService.CryptoException("Incorrect number of bytes read for crypto params.");
    }
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

}

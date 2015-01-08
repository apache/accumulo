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

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.NullCipher;

import org.apache.log4j.Logger;

public class DefaultCryptoModuleUtils {

  private static final Logger log = Logger.getLogger(DefaultCryptoModuleUtils.class);

  public static SecureRandom getSecureRandom(String secureRNG, String secureRNGProvider) {
    SecureRandom secureRandom = null;
    try {
      secureRandom = SecureRandom.getInstance(secureRNG, secureRNGProvider);

      // Immediately seed the generator
      byte[] throwAway = new byte[16];
      secureRandom.nextBytes(throwAway);

    } catch (NoSuchAlgorithmException e) {
      log.error(String.format("Accumulo configuration file specified a secure random generator \"%s\" that was not found by any provider.", secureRNG));
      throw new RuntimeException(e);
    } catch (NoSuchProviderException e) {
      log.error(String.format("Accumulo configuration file specified a secure random provider \"%s\" that does not exist", secureRNGProvider));
      throw new RuntimeException(e);
    }
    return secureRandom;
  }

  public static Cipher getCipher(String cipherSuite) {
    Cipher cipher = null;

    if (cipherSuite.equals("NullCipher")) {
      cipher = new NullCipher();
    } else {
      try {
        cipher = Cipher.getInstance(cipherSuite);
      } catch (NoSuchAlgorithmException e) {
        log.error(String.format("Accumulo configuration file contained a cipher suite \"%s\" that was not recognized by any providers", cipherSuite));
        throw new RuntimeException(e);
      } catch (NoSuchPaddingException e) {
        log.error(String.format("Accumulo configuration file contained a cipher, \"%s\" with a padding that was not recognized by any providers", cipherSuite));
        throw new RuntimeException(e);
      }
    }
    return cipher;
  }

}

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
package org.apache.accumulo.core.cryptoImpl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.spi.crypto.CryptoService.CryptoException;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class AESKeyUtils {

  public static final String URI = "uri";
  public static final String KEY_WRAP_TRANSFORM = "AESWrap";

  public static Key generateKey(SecureRandom sr, int size) {
    byte[] bytes = new byte[size];
    sr.nextBytes(bytes);
    return new SecretKeySpec(bytes, "AES");
  }

  @SuppressFBWarnings(value = "CIPHER_INTEGRITY",
      justification = "integrity not needed for key wrap")
  public static Key unwrapKey(byte[] fek, Key kek) {
    Key result = null;
    try {
      Cipher c = Cipher.getInstance(KEY_WRAP_TRANSFORM);
      c.init(Cipher.UNWRAP_MODE, kek);
      result = c.unwrap(fek, "AES", Cipher.SECRET_KEY);
    } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new CryptoException("Unable to unwrap file encryption key", e);
    }
    return result;
  }

  @SuppressFBWarnings(value = "CIPHER_INTEGRITY",
      justification = "integrity not needed for key wrap")
  public static byte[] wrapKey(Key fek, Key kek) {
    byte[] result = null;
    try {
      Cipher c = Cipher.getInstance(KEY_WRAP_TRANSFORM);
      c.init(Cipher.WRAP_MODE, kek);
      result = c.wrap(fek);
    } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
        | IllegalBlockSizeException e) {
      throw new CryptoException("Unable to wrap file encryption key", e);
    }

    return result;
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "keyId specified by admin")
  public static SecretKeySpec loadKekFromUri(String keyId) {
    URI uri;
    SecretKeySpec key = null;
    try {
      uri = new URI(keyId);
      key = new SecretKeySpec(Files.readAllBytes(Paths.get(uri.getPath())), "AES");
    } catch (URISyntaxException | IOException | IllegalArgumentException e) {
      throw new CryptoException("Unable to load key encryption key.", e);
    }

    return key;

  }
}

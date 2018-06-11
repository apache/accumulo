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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;

/**
 * Example encryption strategy that uses AES with CBC and No padding. This strategy requires one
 * property to be set, crypto.sensitive.key = 16-byte-key.
 */
public class AESCBCEncryptionStrategy implements EncryptionStrategy {
  private static final Integer IV_LENGTH_IN_BYTES = 16;
  private static final Integer KEY_LENGTH_IN_BYTES = 16;

  /**
   * The actual secret key to use
   */
  public static final String CRYPTO_SECRET_KEY_PROPERTY = Property.CRYPTO_SENSITIVE_PREFIX + "key";

  private final String transformation = "AES/CBC/NoPadding";
  private SecretKeySpec skeySpec;
  private final byte[] initVector = new byte[IV_LENGTH_IN_BYTES];
  private boolean initialized = false;

  @Override
  public boolean init(Scope encryptionScope, AccumuloConfiguration conf)
      throws EncryptionStrategyException {
    String key = conf.get(CRYPTO_SECRET_KEY_PROPERTY);

    // do some basic validation
    if (key == null) {
      throw new EncryptionStrategyException("Failed AESEncryptionStrategy init - missing required "
          + "configuration property: " + CRYPTO_SECRET_KEY_PROPERTY);
    }
    if (key.getBytes().length != KEY_LENGTH_IN_BYTES) {
      throw new EncryptionStrategyException("Failed AESEncryptionStrategy init - key length not "
          + KEY_LENGTH_IN_BYTES + " provided: " + key.getBytes().length);
    }

    this.skeySpec = new SecretKeySpec(key.getBytes(), "AES");
    return initialized = true;
  }

  @Override
  public OutputStream encryptStream(OutputStream outputStream) throws EncryptionStrategyException {
    if (!initialized)
      throw new EncryptionStrategyException("AESEncryptionStrategy not initialized.");

    CryptoUtils.getSha1SecureRandom().nextBytes(initVector);
    Cipher cipher;
    try {
      cipher = Cipher.getInstance(this.transformation);
      cipher.init(Cipher.ENCRYPT_MODE, skeySpec, new IvParameterSpec(initVector));
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
        | InvalidAlgorithmParameterException e) {
      throw new EncryptionStrategyException(e);
    }

    CipherOutputStream cos = new CipherOutputStream(outputStream, cipher);
    try {
      cos.write(initVector);
    } catch (IOException e) {
      try {
        cos.close();
      } catch (IOException ioe) {
        throw new EncryptionStrategyException(ioe);
      }
      throw new EncryptionStrategyException(e);
    }

    // Prevent underlying stream from being closed with DiscardCloseOutputStream
    // Without this, when the crypto stream is closed (in order to flush its last bytes)
    // the underlying RFile stream will *also* be closed, and that's undesirable as the cipher
    // stream is closed for every block written.
    return new BlockedOutputStream(new DiscardCloseOutputStream(cos), cipher.getBlockSize(), 1024);
  }

  @Override
  public InputStream decryptStream(InputStream inputStream) throws EncryptionStrategyException {
    if (!initialized)
      throw new EncryptionStrategyException("AESEncryptionStrategy not initialized.");
    int bytesRead;
    try {
      bytesRead = inputStream.read(initVector);
    } catch (IOException e) {
      throw new EncryptionStrategyException(e);
    }
    if (bytesRead != IV_LENGTH_IN_BYTES)
      throw new EncryptionStrategyException("Read " + bytesRead + " bytes, not IV length of "
          + IV_LENGTH_IN_BYTES + " in decryptStream.");

    Cipher cipher;
    try {
      cipher = Cipher.getInstance(this.transformation);
      cipher.init(Cipher.DECRYPT_MODE, skeySpec, new IvParameterSpec(initVector));
    } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
        | InvalidAlgorithmParameterException e) {
      throw new EncryptionStrategyException(e);
    }

    CipherInputStream cis = new CipherInputStream(inputStream, cipher);
    return new BlockedInputStream(cis, cipher.getBlockSize(), 1024);
  }

  @Override
  public void printCryptoInfoToStream(OutputStream outputStream) {
    // TODO implement

  }
}

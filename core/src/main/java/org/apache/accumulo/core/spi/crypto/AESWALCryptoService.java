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
package org.apache.accumulo.core.spi.crypto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.crypto.CryptoUtils;
import org.apache.accumulo.core.crypto.streams.BlockedInputStream;
import org.apache.accumulo.core.crypto.streams.BlockedOutputStream;
import org.apache.commons.io.IOUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An AES Encryption implementation for Write Ahead Log (WAL) encryption. This implementation uses
 * the Java Cipher class from the Java Cryptographic Extension (JCE) framework. The transform used
 * for encryption and decryption is AES/CBC/NoPadding with a key size of 128. For more information
 * see {@link Cipher}.
 */
public class AESWALCryptoService implements CryptoService {
  public static final String URI = "uri";
  public static final String KEY_WRAP_TRANSFORM = "AESWrap";
  public static final String CIPHER_TRANSFORM = "AES/CBC/NoPadding";
  public static final String ALGORITHM = "AES";
  public static final String KEY_URI_PROP = "key.uri";
  public static final String VERSION = "U+1f600"; // unicode grinning face emoji
  private static final Integer IV_LENGTH_IN_BYTES = 16;
  private static final Integer KEY_LENGTH_IN_BYTES = 16;

  @Override
  public FileEncrypter getEncrypter() {
    return new Encrypter();
  }

  @Override
  public FileDecrypter getDecrypter() {
    return new Decrypter();
  }

  @SuppressFBWarnings(value = "CIPHER_INTEGRITY", justification = "CBC is provided for WALs")
  public static class Encrypter implements FileEncrypter {
    private final SecureRandom sr = CryptoUtils.newSha1SecureRandom();
    private final Key fek;
    private final byte[] initVector;
    private Key encryptingKek;
    private String keyLocation;
    private String keyManager;

    public Encrypter() {
      this.fek = generateKey(sr, KEY_LENGTH_IN_BYTES, ALGORITHM);
      this.initVector = new byte[IV_LENGTH_IN_BYTES];
    }

    @Override
    public void init(FileEncrypter.InitParams params) throws CryptoException {
      String keyLocation = params.getOptions().get(KEY_URI_PROP);
      // get key from URI for now, keyMgr framework could be expanded on in the future
      String keyMgr = "uri";
      Objects.requireNonNull(keyLocation, "Config property " + KEY_URI_PROP + " is required.");

      switch (keyMgr) {
        case URI:
          this.keyManager = keyMgr;
          this.keyLocation = keyLocation;
          this.encryptingKek = loadKekFromUri(keyLocation, ALGORITHM);
          break;
        default:
          throw new CryptoException("Unrecognized key manager");
      }
      Objects.requireNonNull(this.encryptingKek,
          "Encrypting Key Encryption Key was null, init failed");
    }

    @Override
    public OutputStream encryptStream(OutputStream outputStream) throws CryptoException {

      sr.nextBytes(initVector);
      try {
        outputStream.write(initVector);
      } catch (IOException e) {
        throw new CryptoException("Unable to write IV to stream", e);
      }

      Cipher cipher;
      try {
        cipher = Cipher.getInstance(CIPHER_TRANSFORM);
        cipher.init(Cipher.ENCRYPT_MODE, fek, new IvParameterSpec(initVector));
      } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
          | InvalidAlgorithmParameterException e) {
        throw new CryptoException("Unable to initialize cipher", e);
      }

      CipherOutputStream cos = new CipherOutputStream(outputStream, cipher);
      return new BlockedOutputStream(cos, cipher.getBlockSize(), 1024);
    }

    @Override
    public byte[] getDecryptionParameters() {
      return createCryptoParameters(encryptingKek, keyLocation, keyManager, fek);
    }
  }

  public static class Decrypter implements FileDecrypter {
    private Key fek;
    private boolean initialized = false;

    @Override
    public void init(FileDecrypter.InitParams initParams) {
      var params = parseCryptoParameters(initParams.getDecryptionParameters());
      final Key wrappedKek = loadKekFromUri(params.getKekId(), ALGORITHM);
      this.fek = unwrapKey(params.getEncFek(), wrappedKek, KEY_WRAP_TRANSFORM, ALGORITHM);
      this.initialized = true;
    }

    @Override
    @SuppressFBWarnings(value = "CIPHER_INTEGRITY", justification = "CBC is provided for WALs")
    public InputStream decryptStream(InputStream inputStream) throws CryptoException {
      if (!initialized) {
        throw new CryptoException("AESCBCFileDecrypter has not been initialized.");
      }
      byte[] initVector = new byte[IV_LENGTH_IN_BYTES];
      try {
        IOUtils.readFully(inputStream, initVector);
      } catch (IOException e) {
        throw new CryptoException("Unable to read IV from stream", e);
      }

      Cipher cipher;
      try {
        cipher = Cipher.getInstance(CIPHER_TRANSFORM);
        cipher.init(Cipher.DECRYPT_MODE, fek, new IvParameterSpec(initVector));
      } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
          | InvalidAlgorithmParameterException e) {
        throw new CryptoException("Unable to initialize cipher", e);
      }

      CipherInputStream cis = new CipherInputStream(inputStream, cipher);
      return new BlockedInputStream(cis, cipher.getBlockSize(), 1024);
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "keyId specified by admin")
  public static Key loadKekFromUri(String keyId, String algorithm) {
    java.net.URI uri;
    SecretKeySpec key = null;
    try {
      uri = new URI(keyId);
      key = new SecretKeySpec(Files.readAllBytes(Paths.get(uri.getPath())), algorithm);
    } catch (URISyntaxException | IOException | IllegalArgumentException e) {
      throw new CryptoException("Unable to load key encryption key.", e);
    }
    return key;
  }

  private static ParsedCryptoParameters parseCryptoParameters(byte[] parameters) {
    ParsedCryptoParameters parsed = new ParsedCryptoParameters();
    try (ByteArrayInputStream bais = new ByteArrayInputStream(parameters);
        DataInputStream params = new DataInputStream(bais)) {
      // the name is already read by accumulo
      parsed.setCryptoServiceName(params.readUTF());
      parsed.setCryptoServiceVersion(params.readUTF());
      parsed.setKeyManagerVersion(params.readUTF());
      parsed.setKekId(params.readUTF());
      int encFekLen = params.readInt();
      byte[] encFek = new byte[encFekLen];
      int bytesRead = params.read(encFek);
      if (bytesRead != encFekLen)
        throw new CryptoException("Incorrect number of bytes read for encrypted FEK");
      parsed.setEncFek(encFek);
    } catch (IOException e) {
      throw new CryptoException("Error creating crypto params", e);
    }
    return parsed;
  }

  private static byte[] createCryptoParameters(Key encryptingKek, String encryptingKekId,
      String encryptingKeyManager, Key fek) {
    byte[] bytes;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream params = new DataOutputStream(baos)) {
      // the name is required to be first
      params.writeUTF(AESWALCryptoService.class.getName());
      params.writeUTF(VERSION);
      params.writeUTF(encryptingKeyManager);
      params.writeUTF(encryptingKekId);
      byte[] wrappedFek = wrapKey(fek, encryptingKek, KEY_WRAP_TRANSFORM);
      params.writeInt(wrappedFek.length);
      params.write(wrappedFek);

      bytes = baos.toByteArray();
    } catch (IOException e) {
      throw new CryptoException("Error creating crypto params", e);
    }
    return bytes;
  }

  static class ParsedCryptoParameters {
    String cryptoServiceName;
    String cryptoServiceVersion;
    String keyManagerVersion;
    String kekId;
    byte[] encFek;

    public void setCryptoServiceName(String cryptoServiceName) {
      this.cryptoServiceName = cryptoServiceName;
    }

    public String getCryptoServiceName() {
      return cryptoServiceName;
    }

    public String getCryptoServiceVersion() {
      return cryptoServiceVersion;
    }

    public void setCryptoServiceVersion(String cryptoServiceVersion) {
      this.cryptoServiceVersion = cryptoServiceVersion;
    }

    public String getKeyManagerVersion() {
      return keyManagerVersion;
    }

    public void setKeyManagerVersion(String keyManagerVersion) {
      this.keyManagerVersion = keyManagerVersion;
    }

    public String getKekId() {
      return kekId;
    }

    public void setKekId(String kekId) {
      this.kekId = kekId;
    }

    public byte[] getEncFek() {
      return encFek;
    }

    public void setEncFek(byte[] encFek) {
      this.encFek = encFek;
    }

  }

  /**
   * Generate secret key using the SecureRandom, specified size and algorithm. For all algorithms
   * supported by java see: https://docs.oracle.com/javase/9/docs/specs/security/standard-names.html
   */
  public static Key generateKey(SecureRandom sr, int size, String algorithm) {
    byte[] bytes = new byte[size];
    sr.nextBytes(bytes);
    return new SecretKeySpec(bytes, algorithm);
  }

  @SuppressFBWarnings(value = "CIPHER_INTEGRITY",
      justification = "integrity not needed for key wrap")
  public static byte[] wrapKey(Key fek, Key kek, String transform) {
    byte[] result;
    try {
      Cipher c = Cipher.getInstance(transform);
      c.init(Cipher.WRAP_MODE, kek);
      result = c.wrap(fek);
    } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException
        | IllegalBlockSizeException e) {
      throw new CryptoException("Unable to wrap file encryption key", e);
    }
    return result;
  }

  @SuppressFBWarnings(value = "CIPHER_INTEGRITY",
      justification = "integrity not needed for key wrap")
  public static Key unwrapKey(byte[] fek, Key kek, String transform, String wrappedKeyAlgorithm) {
    Key result = null;
    try {
      Cipher c = Cipher.getInstance(transform);
      c.init(Cipher.UNWRAP_MODE, kek);
      result = c.unwrap(fek, wrappedKeyAlgorithm, Cipher.SECRET_KEY);
    } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new CryptoException("Unable to unwrap file encryption key", e);
    }
    return result;
  }
}

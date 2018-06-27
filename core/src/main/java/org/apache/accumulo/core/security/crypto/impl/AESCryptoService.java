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
package org.apache.accumulo.core.security.crypto.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringJoiner;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;

import org.apache.accumulo.core.security.crypto.BlockedInputStream;
import org.apache.accumulo.core.security.crypto.BlockedOutputStream;
import org.apache.accumulo.core.security.crypto.CryptoEnvironment;
import org.apache.accumulo.core.security.crypto.CryptoService;
import org.apache.accumulo.core.security.crypto.CryptoUtils;
import org.apache.accumulo.core.security.crypto.DiscardCloseOutputStream;
import org.apache.accumulo.core.security.crypto.FileDecrypter;
import org.apache.accumulo.core.security.crypto.FileEncrypter;

/**
 * Example implementation of AES encryption for Accumulo
 */
public class AESCryptoService implements CryptoService {

  private static Key encryptingKek = null;
  private static String encryptingKekId = null;
  private static String encryptingKeyManager = null;
  // Lets just load keks for reading once
  private static HashMap<String,Key> decryptingKeys = new HashMap<String,Key>();

  // Each byte has a valid unicode representation
  private static final String unicodeCharset = "ISO_8859_1";

  AESCryptoService(String kekId, String encryptingKeyManager) {
    switch (encryptingKeyManager) {
      case KeyManager.URI:
        AESCryptoService.encryptingKeyManager = encryptingKeyManager;
        AESCryptoService.encryptingKekId = kekId;
        AESCryptoService.encryptingKek = KeyManager.loadKekFromUri(kekId);
        break;
      default:
        throw new CryptoException("Unrecognized key manager");
    }

  }

  @Override
  public FileEncrypter getFileEncrypter(CryptoEnvironment environment) {
    CryptoModule cm;
    switch (environment.getScope()) {
      case WAL:
        cm = new AESCBCCryptoModule();
        return cm.getEncrypter();

      case RFILE:
        cm = new AESGCMCryptoModule();
        return cm.getEncrypter();

      default:
        throw new CryptoException("Unknown scope: " + environment.getScope());
    }
  }

  @Override
  public FileDecrypter getFileDecrypter(CryptoEnvironment environment) {
    CryptoModule cm;
    ParsedCryptoParameters parsed = parseCryptoParameters(environment.getParameters());
    Key fek = loadDecryptionKek(parsed);
    switch (parsed.getCryptoServiceVersion()) {
      case AESCBCCryptoModule.VERSION:
        cm = new AESCBCCryptoModule();
        return (cm.getDecrypter(fek));
      case AESGCMCryptoModule.VERSION:
        cm = new AESGCMCryptoModule();
        return (cm.getDecrypter(fek));

      // TODO I suspect we want to leave "U+1F47B" and create an internal NoFileDecrypter
      // so that the crypto service .jar file can pluggable without requiring the
      // NoCryptoService.jar
      case NoCryptoService.VERSION:
        return new NoFileDecrypter();
      default:
        throw new CryptoException(
            "Unknown crypto module version: " + parsed.getCryptoServiceVersion());
    }
  }

  static class ParsedCryptoParameters {
    String cryptoServiceName;
    String cryptoServiceVersion;
    String keyManagerVersion;
    String kekId;
    byte[] encFek;

    public String getCryptoServiceName() {
      return cryptoServiceName;
    }

    public void setCryptoServiceName(String cryptoServiceName) {
      this.cryptoServiceName = cryptoServiceName;
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

  private static String createCryptoParameters(String version, Key fek) {

    StringJoiner sj = new StringJoiner("!");
    sj.add("org.apache.accumulo.core.security.crypto.impl.AESCryptoService");
    sj.add(version);
    sj.add(encryptingKeyManager);
    sj.add(encryptingKekId);
    byte[] wrappedFek = KeyManager.wrapKey(fek, encryptingKek);
    String fekString = new String(wrappedFek, Charset.forName(unicodeCharset));
    sj.add(Integer.toString(fekString.length()));
    sj.add(fekString);
    return (sj.toString());
  }

  private static ParsedCryptoParameters parseCryptoParameters(String parameters) {
    String[] parts = parameters.split("!");
    if (parts.length < 6) {
      throw new CryptoException(
          "Invalid parameters string, probably generated by a different CryptoService");
    }
    ParsedCryptoParameters parsed = new ParsedCryptoParameters();

    String service = parts[0];
    String version = parts[1];
    String keyVersion = parts[2];
    String kekId = parts[3];
    Integer keyLength;
    try {
      keyLength = Integer.parseInt(parts[4]);
    } catch (NumberFormatException e) {
      throw new CryptoException(
          "Invalid key length, possibly invalid parameters string. Check CryptoService", e);
    }

    Integer keyOffset = parts[0].length() + parts[1].length() + parts[2].length()
        + parts[3].length() + parts[4].length() + 5; // 5 delims prior to the key

    if (keyOffset + keyLength != parameters.length()) {
      throw new CryptoException(
          "Invalid parameters string, probably generated by a different CryptoService");
    }
    byte[] encFek = (parameters.substring(keyOffset, keyOffset + keyLength))
        .getBytes(Charset.forName(unicodeCharset));

    parsed.setCryptoServiceName(service);
    parsed.setCryptoServiceVersion(version);
    parsed.setKeyManagerVersion(keyVersion);
    parsed.setKekId(kekId);
    parsed.setEncFek(encFek);

    return parsed;
  }

  private static Key loadDecryptionKek(ParsedCryptoParameters params) {
    Key ret = null;
    String keyTag = params.getKeyManagerVersion() + "!" + params.getKekId();
    if (decryptingKeys.get(keyTag) != null) {
      return (decryptingKeys.get(keyTag));
    }

    switch (params.keyManagerVersion) {
      case KeyManager.URI:
        ret = KeyManager.loadKekFromUri(params.kekId);
        break;
      default:
        throw new CryptoException("Unable to load kek: " + params.kekId);
    }

    decryptingKeys.put(keyTag, ret);

    if (ret == null)
      throw new CryptoException("Unable to load decryption KEK");

    return (ret);
  }

  private static SecureRandom getSecureRandom(String secureRNG, String secureRNGProvider) {
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
   * 
   * This interface lists the methods needed by CryptoModules which are responsible for tracking
   * version and preparing encrypters/decrypters for use.
   *
   */
  private interface CryptoModule {
    FileEncrypter getEncrypter();

    FileDecrypter getDecrypter(Key fek);
  }

  public static class AESGCMCryptoModule implements CryptoModule {
    private static final String VERSION = "U+1F43B"; // unicode bear emoji rawr

    private final Integer GCM_IV_LENGTH_IN_BYTES = 12;
    private final Integer KEY_LENGTH_IN_BYTES = 32;

    // 128-bit tags are the longest available for GCM
    private final Integer GCM_TAG_LENGTH_IN_BITS = 16 * 8;
    private final String transformation = "AES/GCM/NoPadding";
    private boolean ivReused = false;

    @Override
    public FileEncrypter getEncrypter() {
      return new AESGCMFileEncrypter();
    }

    @Override
    public FileDecrypter getDecrypter(Key fek) {
      return new AESGCMFileDecrypter(fek);
    }

    public class AESGCMFileEncrypter implements FileEncrypter {

      private byte[] firstInitVector = new byte[GCM_IV_LENGTH_IN_BYTES];
      private SecureRandom sr = getSecureRandom("SHA1PRNG", "SUN");
      private Key fek = KeyManager.generateKey(sr, KEY_LENGTH_IN_BYTES);
      private byte[] initVector = new byte[GCM_IV_LENGTH_IN_BYTES];

      AESGCMFileEncrypter() {

        sr.nextBytes(initVector);
        firstInitVector = Arrays.copyOf(initVector, initVector.length);
      }

      @Override
      public OutputStream encryptStream(OutputStream outputStream) throws CryptoException {
        if (ivReused) {
          throw new CryptoException(
              "AESGCMCryptoModule is attempting a Key/IV reuse which is forbidden. Too many RBlocks.");
        }
        incrementIV(initVector, initVector.length - 1);
        if (Arrays.equals(initVector, firstInitVector)) {
          ivReused = true; // This will allow us to write the final block, since the
          // initialization vector
          // is always incremented before use.
        }

        Cipher cipher;
        try {
          cipher = Cipher.getInstance(transformation);
          cipher.init(Cipher.ENCRYPT_MODE, fek,
              new GCMParameterSpec(GCM_TAG_LENGTH_IN_BITS, initVector));
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
            | InvalidAlgorithmParameterException e) {
          throw new CryptoException("Unable to initialize cipher", e);
        }

        CipherOutputStream cos = new CipherOutputStream(outputStream, cipher);
        try {
          cos.write(initVector);
          cos.close();
        } catch (IOException e) {
          throw new CryptoException("Unable to write IV to stream", e);
        }

        // Prevent underlying stream from being closed with DiscardCloseOutputStream
        // Without this, when the crypto stream is closed (in order to flush its last bytes)
        // the underlying RFile stream will *also* be closed, and that's undesirable as the
        // cipher
        // stream is closed for every block written.
        return new BlockedOutputStream(new DiscardCloseOutputStream(cos), cipher.getBlockSize(),
            1024);
      }

      /**
       * Because IVs can be longer than longs, this increments arbitrarily sized byte arrays by 1,
       * with a roll over to 0 after the max value is reached.
       *
       * @param iv
       *          The iv to be incremented
       * @param i
       *          The current byte being incremented
       */
      void incrementIV(byte[] iv, int i) {
        iv[i]++;
        if (iv[i] == 0) {
          if (i != 0) {
            incrementIV(iv, i - 1);
          } else
            return;
        }

      }

      @Override
      public String getParameters() {
        return (createCryptoParameters(VERSION, fek));
      }
    }

    public class AESGCMFileDecrypter implements FileDecrypter {
      private Key fek = null;
      private byte[] initVector = new byte[GCM_IV_LENGTH_IN_BYTES];

      AESGCMFileDecrypter(Key fek) {
        this.fek = fek;
      }

      @Override
      public InputStream decryptStream(InputStream inputStream) throws CryptoException {
        int bytesRead;
        try {
          bytesRead = inputStream.read(initVector);
        } catch (IOException e) {
          throw new CryptoException("Unable to read IV from stream", e);
        }
        if (bytesRead != GCM_IV_LENGTH_IN_BYTES)
          throw new CryptoException("Read " + bytesRead + " bytes, not IV length of "
              + GCM_IV_LENGTH_IN_BYTES + " in decryptStream.");

        Cipher cipher;
        try {
          cipher = Cipher.getInstance(transformation);
          cipher.init(Cipher.DECRYPT_MODE, fek,
              new GCMParameterSpec(GCM_TAG_LENGTH_IN_BITS, initVector));
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
            | InvalidAlgorithmParameterException e) {
          throw new CryptoException("Unable to initialize cipher", e);
        }

        CipherInputStream cis = new CipherInputStream(inputStream, cipher);
        return new BlockedInputStream(cis, cipher.getBlockSize(), 1024);
      }
    }
  }

  public static class AESCBCCryptoModule implements CryptoModule {
    public static final String VERSION = "U+1f600"; // unicode grinning face emoji
    private final Integer IV_LENGTH_IN_BYTES = 16;
    private final Integer KEY_LENGTH_IN_BYTES = 16;
    private final String transformation = "AES/CBC/NoPadding";

    @Override
    public FileEncrypter getEncrypter() {
      return new AESCBCFileEncrypter();
    }

    @Override
    public FileDecrypter getDecrypter(Key fek) {
      return new AESCBCFileDecrypter(fek);
    }

    public class AESCBCFileEncrypter implements FileEncrypter {

      private SecureRandom sr = getSecureRandom("SHA1PRNG", "SUN");
      private Key fek = KeyManager.generateKey(sr, KEY_LENGTH_IN_BYTES);
      private byte[] initVector = new byte[IV_LENGTH_IN_BYTES];

      @Override
      public OutputStream encryptStream(OutputStream outputStream) throws CryptoException {

        CryptoUtils.getSha1SecureRandom().nextBytes(initVector);
        Cipher cipher;
        try {
          cipher = Cipher.getInstance(transformation);
          cipher.init(Cipher.ENCRYPT_MODE, fek, new IvParameterSpec(initVector));
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
            | InvalidAlgorithmParameterException e) {
          throw new CryptoException("Unable to initialize cipher", e);
        }

        CipherOutputStream cos = new CipherOutputStream(outputStream, cipher);
        try {
          cos.write(initVector);
          cos.close();
        } catch (IOException e) {
          throw new CryptoException("Unable to write IV to stream", e);
        }

        // Prevent underlying stream from being closed with DiscardCloseOutputStream
        // Without this, when the crypto stream is closed (in order to flush its last bytes)
        // the underlying RFile stream will *also* be closed, and that's undesirable as the
        // cipher
        // stream is closed for every block written.
        return new BlockedOutputStream(new DiscardCloseOutputStream(cos), cipher.getBlockSize(),
            1024);
      }

      @Override
      public String getParameters() {
        return (createCryptoParameters(VERSION, fek));
      }
    }

    public class AESCBCFileDecrypter implements FileDecrypter {
      private Key fek = null;
      private byte[] initVector = new byte[IV_LENGTH_IN_BYTES];

      AESCBCFileDecrypter(Key fek) {
        this.fek = fek;
      }

      @Override
      public InputStream decryptStream(InputStream inputStream) throws CryptoException {
        int bytesRead;
        try {
          bytesRead = inputStream.read(initVector);
        } catch (IOException e) {
          throw new CryptoException("Unable to read IV from stream", e);
        }
        if (bytesRead != IV_LENGTH_IN_BYTES)
          throw new CryptoException("Read " + bytesRead + " bytes, not IV length of "
              + IV_LENGTH_IN_BYTES + " in decryptStream.");

        Cipher cipher;
        try {
          cipher = Cipher.getInstance(transformation);
          cipher.init(Cipher.DECRYPT_MODE, fek, new IvParameterSpec(initVector));
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
            | InvalidAlgorithmParameterException e) {
          throw new CryptoException("Unable to initialize cipher", e);
        }

        CipherInputStream cis = new CipherInputStream(inputStream, cipher);
        return new BlockedInputStream(cis, cipher.getBlockSize(), 1024);
      }
    }
  }
}

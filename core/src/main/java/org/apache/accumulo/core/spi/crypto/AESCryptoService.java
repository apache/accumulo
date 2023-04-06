/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.spi.crypto;

import static java.nio.charset.StandardCharsets.UTF_8;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.crypto.streams.BlockedInputStream;
import org.apache.accumulo.core.crypto.streams.BlockedOutputStream;
import org.apache.accumulo.core.crypto.streams.DiscardCloseOutputStream;
import org.apache.accumulo.core.crypto.streams.RFileCipherOutputStream;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Example implementation of AES encryption for Accumulo
 */
public class AESCryptoService implements CryptoService {
  private static final Logger log = LoggerFactory.getLogger(AESCryptoService.class);
  private volatile boolean initialized = false;

  // properties required for using this service
  public static final String KEY_URI_PROPERTY = "general.custom.crypto.key.uri";
  // optional properties
  // defaults to true
  public static final String ENCRYPT_ENABLED_PROPERTY = "general.custom.crypto.enabled";

  // Hard coded NoCryptoService.VERSION - this permits the removal of NoCryptoService from the
  // core jar, allowing use of only one crypto service
  private static final String NO_CRYPTO_VERSION = "U+1F47B";
  private static final String URI = "uri";
  private static final String KEY_WRAP_TRANSFORM = "AESWrap";
  private static final SecureRandom random = new SecureRandom();

  private Key encryptingKek = null;
  private String keyLocation = null;
  private String keyManager = null;
  // Lets just load keks for reading once
  private HashMap<String,Key> decryptingKeys = null;
  private boolean encryptEnabled = true;

  private static final FileEncrypter DISABLED = new NoFileEncrypter();

  private static final ThreadLocal<Cipher> KEY_WRAP_CIPHER = new ThreadLocal<Cipher>() {
    @SuppressFBWarnings(value = "CIPHER_INTEGRITY",
        justification = "integrity not needed for key wrap")
    @Override
    protected Cipher initialValue() {
      try {
        return Cipher.getInstance(KEY_WRAP_TRANSFORM);
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        throw new CryptoException("Error creating Cipher for AESWrap", e);
      }
    }
  };

  private static final ThreadLocal<Cipher> KEY_UNWRAP_CIPHER = new ThreadLocal<Cipher>() {
    @SuppressFBWarnings(value = "CIPHER_INTEGRITY",
        justification = "integrity not needed for key wrap")
    @Override
    protected Cipher initialValue() {
      try {
        return Cipher.getInstance(KEY_WRAP_TRANSFORM);
      } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
        throw new CryptoException("Error creating Cipher for AESWrap", e);
      }
    }
  };

  @Override
  public void init(Map<String,String> conf) throws CryptoException {
    ensureNotInit();
    String keyLocation = Objects.requireNonNull(conf.get(KEY_URI_PROPERTY),
        "Config property " + KEY_URI_PROPERTY + " is required.");
    String enabledProp = conf.get(ENCRYPT_ENABLED_PROPERTY);
    if (enabledProp != null) {
      encryptEnabled = Boolean.parseBoolean(enabledProp);
    }

    // get key from URI for now, keyMgr framework could be expanded on in the future
    String keyMgr = "uri";
    this.decryptingKeys = new HashMap<>();
    switch (keyMgr) {
      case URI:
        this.keyManager = keyMgr;
        this.keyLocation = keyLocation;
        this.encryptingKek = loadKekFromUri(keyLocation);
        break;
      default:
        throw new CryptoException("Unrecognized key manager");
    }
    Objects.requireNonNull(this.encryptingKek,
        "Encrypting Key Encryption Key was null, init failed");
    log.debug("Successfully initialized crypto Key from {}", keyLocation);
    initialized = true;
  }

  @Override
  public FileEncrypter getFileEncrypter(CryptoEnvironment environment) {
    ensureInit();
    if (!encryptEnabled) {
      return DISABLED;
    }
    CryptoModule cm;
    switch (environment.getScope()) {
      case WAL:
        cm = new AESCBCCryptoModule(this.encryptingKek, this.keyLocation, this.keyManager);
        return cm.getEncrypter();

      case TABLE:
        cm = new AESGCMCryptoModule(this.encryptingKek, this.keyLocation, this.keyManager);
        return cm.getEncrypter();

      default:
        throw new CryptoException("Unknown scope: " + environment.getScope());
    }
  }

  @Override
  public FileDecrypter getFileDecrypter(CryptoEnvironment environment) {
    ensureInit();
    CryptoModule cm;
    var decryptionParams = environment.getDecryptionParams();
    if (decryptionParams.isEmpty() || checkNoCrypto(decryptionParams.get())) {
      return new NoFileDecrypter();
    }

    ParsedCryptoParameters parsed = parseCryptoParameters(decryptionParams.get());
    Key kek = loadDecryptionKek(parsed);
    Key fek = unwrapKey(parsed.getEncFek(), kek);
    switch (parsed.getCryptoServiceVersion()) {
      case AESCBCCryptoModule.VERSION:
        cm = new AESCBCCryptoModule(this.encryptingKek, this.keyLocation, this.keyManager);
        return cm.getDecrypter(fek);
      case AESGCMCryptoModule.VERSION:
        cm = new AESGCMCryptoModule(this.encryptingKek, this.keyLocation, this.keyManager);
        return cm.getDecrypter(fek);
      default:
        throw new CryptoException(
            "Unknown crypto module version: " + parsed.getCryptoServiceVersion());
    }
  }

  private static boolean checkNoCrypto(byte[] params) {
    byte[] noCryptoBytes = NO_CRYPTO_VERSION.getBytes(UTF_8);
    return Arrays.equals(params, noCryptoBytes);
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

  private static byte[] createCryptoParameters(String version, Key encryptingKek,
      String encryptingKekId, String encryptingKeyManager, Key fek) {

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream params = new DataOutputStream(baos)) {
      params.writeUTF(AESCryptoService.class.getName());
      params.writeUTF(version);
      params.writeUTF(encryptingKeyManager);
      params.writeUTF(encryptingKekId);
      byte[] wrappedFek = wrapKey(fek, encryptingKek);
      params.writeInt(wrappedFek.length);
      params.write(wrappedFek);

      params.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new CryptoException("Error creating crypto params", e);
    }
  }

  private static ParsedCryptoParameters parseCryptoParameters(byte[] parameters) {
    ParsedCryptoParameters parsed = new ParsedCryptoParameters();
    try (ByteArrayInputStream bais = new ByteArrayInputStream(parameters);
        DataInputStream params = new DataInputStream(bais)) {
      parsed.setCryptoServiceName(params.readUTF());
      parsed.setCryptoServiceVersion(params.readUTF());
      parsed.setKeyManagerVersion(params.readUTF());
      parsed.setKekId(params.readUTF());
      int encFekLen = params.readInt();
      byte[] encFek = new byte[encFekLen];
      int bytesRead = params.read(encFek);
      if (bytesRead != encFekLen) {
        throw new CryptoException("Incorrect number of bytes read for encrypted FEK");
      }
      parsed.setEncFek(encFek);
    } catch (IOException e) {
      throw new CryptoException("Error creating crypto params", e);
    }
    return parsed;
  }

  private Key loadDecryptionKek(ParsedCryptoParameters params) {
    Key ret = null;
    String keyTag = params.getKeyManagerVersion() + "!" + params.getKekId();
    if (this.decryptingKeys.get(keyTag) != null) {
      return this.decryptingKeys.get(keyTag);
    }

    switch (params.keyManagerVersion) {
      case URI:
        ret = loadKekFromUri(params.kekId);
        break;
      default:
        throw new CryptoException("Unable to load kek: " + params.kekId);
    }

    this.decryptingKeys.put(keyTag, ret);

    if (ret == null) {
      throw new CryptoException("Unable to load decryption KEK");
    }

    return ret;
  }

  /**
   * This interface lists the methods needed by CryptoModules which are responsible for tracking
   * version and preparing encrypters/decrypters for use.
   */
  private interface CryptoModule {
    FileEncrypter getEncrypter();

    FileDecrypter getDecrypter(Key fek);
  }

  public class AESGCMCryptoModule implements CryptoModule {
    private static final String VERSION = "U+1F43B"; // unicode bear emoji rawr

    private final Integer GCM_IV_LENGTH_IN_BYTES = 12;
    private final Integer KEY_LENGTH_IN_BYTES = 16;

    // 128-bit tags are the longest available for GCM
    private final Integer GCM_TAG_LENGTH_IN_BITS = 16 * 8;
    private final String transformation = "AES/GCM/NoPadding";
    private boolean ivReused = false;
    private final Key encryptingKek;
    private final String keyLocation;
    private final String keyManager;

    public AESGCMCryptoModule(Key encryptingKek, String keyLocation, String keyManager) {
      this.encryptingKek = encryptingKek;
      this.keyLocation = keyLocation;
      this.keyManager = keyManager;
    }

    @Override
    public FileEncrypter getEncrypter() {
      return new AESGCMFileEncrypter();
    }

    @Override
    public FileDecrypter getDecrypter(Key fek) {
      return new AESGCMFileDecrypter(fek);
    }

    public class AESGCMFileEncrypter implements FileEncrypter {

      private final byte[] firstInitVector;
      private final Key fek;
      private final byte[] initVector = new byte[GCM_IV_LENGTH_IN_BYTES];
      private final Cipher cipher;
      private final byte[] decryptionParameters;

      AESGCMFileEncrypter() {
        try {
          cipher = Cipher.getInstance(transformation);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
          throw new CryptoException("Error obtaining cipher for transform " + transformation, e);
        }
        this.fek = generateKey(random, KEY_LENGTH_IN_BYTES);
        random.nextBytes(this.initVector);
        this.firstInitVector = Arrays.copyOf(this.initVector, this.initVector.length);
        this.decryptionParameters =
            createCryptoParameters(VERSION, encryptingKek, keyLocation, keyManager, fek);
      }

      @Override
      public OutputStream encryptStream(OutputStream outputStream) throws CryptoException {
        if (ivReused) {
          throw new CryptoException(
              "Key/IV reuse is forbidden in AESGCMCryptoModule. Too many RBlocks.");
        }
        incrementIV(initVector, initVector.length - 1);
        if (Arrays.equals(initVector, firstInitVector)) {
          ivReused = true; // This will allow us to write the final block, since the
          // initialization vector is always incremented before use.
        }

        // write IV before encrypting
        try {
          outputStream.write(initVector);
        } catch (IOException e) {
          throw new CryptoException("Unable to write IV to stream", e);
        }

        try {
          cipher.init(Cipher.ENCRYPT_MODE, fek,
              new GCMParameterSpec(GCM_TAG_LENGTH_IN_BITS, initVector));
        } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
          throw new CryptoException("Unable to initialize cipher", e);
        }

        RFileCipherOutputStream cos =
            new RFileCipherOutputStream(new DiscardCloseOutputStream(outputStream), cipher);
        // Prevent underlying stream from being closed with DiscardCloseOutputStream
        // Without this, when the crypto stream is closed (in order to flush its last bytes)
        // the underlying RFile stream will *also* be closed, and that's undesirable as the
        // cipher stream is closed for every block written.
        return new BlockedOutputStream(cos, cipher.getBlockSize(), 1024);
      }

      /**
       * Because IVs can be longer than longs, this increments arbitrarily sized byte arrays by 1,
       * with a roll over to 0 after the max value is reached.
       *
       * @param iv The iv to be incremented
       * @param i The current byte being incremented
       */
      void incrementIV(byte[] iv, int i) {
        iv[i]++;
        if (iv[i] == 0) {
          if (i == 0) {
            return;
          } else {
            incrementIV(iv, i - 1);
          }
        }

      }

      @Override
      public byte[] getDecryptionParameters() {
        return decryptionParameters;
      }
    }

    public class AESGCMFileDecrypter implements FileDecrypter {
      private final Cipher cipher;
      private final Key fek;

      AESGCMFileDecrypter(Key fek) {
        try {
          cipher = Cipher.getInstance(transformation);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
          throw new CryptoException("Error obtaining cipher for transform " + transformation, e);
        }
        this.fek = fek;
      }

      @Override
      public InputStream decryptStream(InputStream inputStream) throws CryptoException {
        byte[] initVector = new byte[GCM_IV_LENGTH_IN_BYTES];
        try {
          IOUtils.readFully(inputStream, initVector);
        } catch (IOException e) {
          throw new CryptoException("Unable to read IV from stream", e);
        }

        try {
          cipher.init(Cipher.DECRYPT_MODE, fek,
              new GCMParameterSpec(GCM_TAG_LENGTH_IN_BITS, initVector));

        } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
          throw new CryptoException("Unable to initialize cipher", e);
        }

        CipherInputStream cis = new CipherInputStream(inputStream, cipher);
        return new BlockedInputStream(cis, cipher.getBlockSize(), 1024);
      }
    }
  }

  public class AESCBCCryptoModule implements CryptoModule {
    public static final String VERSION = "U+1f600"; // unicode grinning face emoji
    private final Integer IV_LENGTH_IN_BYTES = 16;
    private final Integer KEY_LENGTH_IN_BYTES = 16;
    private final String transformation = "AES/CBC/NoPadding";
    private final Key encryptingKek;
    private final String keyLocation;
    private final String keyManager;

    public AESCBCCryptoModule(Key encryptingKek, String keyLocation, String keyManager) {
      this.encryptingKek = encryptingKek;
      this.keyLocation = keyLocation;
      this.keyManager = keyManager;
    }

    @Override
    public FileEncrypter getEncrypter() {
      return new AESCBCFileEncrypter();
    }

    @Override
    public FileDecrypter getDecrypter(Key fek) {
      return new AESCBCFileDecrypter(fek);
    }

    @SuppressFBWarnings(value = "CIPHER_INTEGRITY", justification = "CBC is provided for WALs")
    public class AESCBCFileEncrypter implements FileEncrypter {
      private final Cipher cipher;
      private final Key fek;
      private final byte[] initVector = new byte[IV_LENGTH_IN_BYTES];
      private final byte[] decryptionParameters;

      AESCBCFileEncrypter() {
        try {
          cipher = Cipher.getInstance(transformation);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
          throw new CryptoException("Error obtaining cipher for transform " + transformation, e);
        }
        this.fek = generateKey(random, KEY_LENGTH_IN_BYTES);
        this.decryptionParameters =
            createCryptoParameters(VERSION, encryptingKek, keyLocation, keyManager, fek);
      }

      @Override
      public OutputStream encryptStream(OutputStream outputStream) throws CryptoException {
        random.nextBytes(initVector);
        try {
          outputStream.write(initVector);
        } catch (IOException e) {
          throw new CryptoException("Unable to write IV to stream", e);
        }

        try {
          cipher.init(Cipher.ENCRYPT_MODE, fek, new IvParameterSpec(initVector));
        } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
          throw new CryptoException("Unable to initialize cipher", e);
        }

        CipherOutputStream cos = new CipherOutputStream(outputStream, cipher);
        return new BlockedOutputStream(cos, cipher.getBlockSize(), 1024);
      }

      @Override
      public byte[] getDecryptionParameters() {
        return decryptionParameters;
      }
    }

    @SuppressFBWarnings(value = "CIPHER_INTEGRITY", justification = "CBC is provided for WALs")
    public class AESCBCFileDecrypter implements FileDecrypter {
      private final Cipher cipher;
      private final Key fek;

      AESCBCFileDecrypter(Key fek) {
        try {
          cipher = Cipher.getInstance(transformation);
        } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
          throw new CryptoException("Error obtaining cipher for transform " + transformation, e);
        }
        this.fek = fek;
      }

      @Override
      public InputStream decryptStream(InputStream inputStream) throws CryptoException {
        byte[] initVector = new byte[IV_LENGTH_IN_BYTES];
        try {
          IOUtils.readFully(inputStream, initVector);
        } catch (IOException e) {
          throw new CryptoException("Unable to read IV from stream", e);
        }

        try {
          cipher.init(Cipher.DECRYPT_MODE, fek, new IvParameterSpec(initVector));
        } catch (InvalidKeyException | InvalidAlgorithmParameterException e) {
          throw new CryptoException("Unable to initialize cipher", e);
        }

        CipherInputStream cis = new CipherInputStream(inputStream, cipher);
        return new BlockedInputStream(cis, cipher.getBlockSize(), 1024);
      }
    }
  }

  public static Key generateKey(SecureRandom random, int size) {
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return new SecretKeySpec(bytes, "AES");
  }

  public static Key unwrapKey(byte[] fek, Key kek) {
    try {
      final Cipher c = KEY_UNWRAP_CIPHER.get();
      c.init(Cipher.UNWRAP_MODE, kek);
      return c.unwrap(fek, "AES", Cipher.SECRET_KEY);
    } catch (InvalidKeyException | NoSuchAlgorithmException e) {
      throw new CryptoException("Unable to unwrap file encryption key", e);
    }
  }

  public static byte[] wrapKey(Key fek, Key kek) {
    try {
      final Cipher c = KEY_WRAP_CIPHER.get();
      c.init(Cipher.WRAP_MODE, kek);
      return c.wrap(fek);
    } catch (InvalidKeyException | IllegalBlockSizeException e) {
      throw new CryptoException("Unable to wrap file encryption key", e);
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "keyId specified by admin")
  public static Key loadKekFromUri(String keyId) {
    try {
      final java.net.URI uri = new URI(keyId);
      return new SecretKeySpec(Files.readAllBytes(Paths.get(uri.getPath())), "AES");
    } catch (URISyntaxException | IOException | IllegalArgumentException e) {
      throw new CryptoException("Unable to load key encryption key.", e);
    }
  }

  private void ensureInit() {
    if (!initialized) {
      throw new IllegalStateException("This Crypto Service has not been initialized.");
    }
  }

  private void ensureNotInit() {
    if (initialized) {
      throw new IllegalStateException("This Crypto Service has already been initialized.");
    }
  }
}

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
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SecretKeyEncryptionStrategy} that gets its key from HDFS and caches it for IO.
 */
public class CachingHDFSSecretKeyEncryptionStrategy implements SecretKeyEncryptionStrategy {

  private static final Logger log = LoggerFactory.getLogger(CachingHDFSSecretKeyEncryptionStrategy.class);
  private SecretKeyCache secretKeyCache = new SecretKeyCache();

  @Override
  public CryptoModuleParameters encryptSecretKey(CryptoModuleParameters context) {
    try {
      secretKeyCache.ensureSecretKeyCacheInitialized(context);
      doKeyEncryptionOperation(Cipher.WRAP_MODE, context);
    } catch (IOException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
    return context;
  }

  @Override
  public CryptoModuleParameters decryptSecretKey(CryptoModuleParameters context) {
    try {
      secretKeyCache.ensureSecretKeyCacheInitialized(context);
      doKeyEncryptionOperation(Cipher.UNWRAP_MODE, context);
    } catch (IOException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }
    return context;
  }

  private void doKeyEncryptionOperation(int encryptionMode, CryptoModuleParameters params) throws IOException {
    Cipher cipher = DefaultCryptoModuleUtils.getCipher(params.getAllOptions().get(Property.CRYPTO_DEFAULT_KEY_STRATEGY_CIPHER_SUITE.getKey()));

    try {
      cipher.init(encryptionMode, new SecretKeySpec(secretKeyCache.getKeyEncryptionKey(), params.getKeyAlgorithmName()));
    } catch (InvalidKeyException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }

    if (Cipher.UNWRAP_MODE == encryptionMode) {
      try {
        Key plaintextKey = cipher.unwrap(params.getEncryptedKey(), params.getKeyAlgorithmName(), Cipher.SECRET_KEY);
        params.setPlaintextKey(plaintextKey.getEncoded());
      } catch (InvalidKeyException e) {
        log.error("{}", e.getMessage(), e);
        throw new RuntimeException(e);
      } catch (NoSuchAlgorithmException e) {
        log.error("{}", e.getMessage(), e);
        throw new RuntimeException(e);
      }
    } else {
      Key plaintextKey = new SecretKeySpec(params.getPlaintextKey(), params.getKeyAlgorithmName());
      try {
        byte[] encryptedSecretKey = cipher.wrap(plaintextKey);
        params.setEncryptedKey(encryptedSecretKey);
        params.setOpaqueKeyEncryptionKeyID(secretKeyCache.getPathToKeyName());
      } catch (InvalidKeyException e) {
        log.error("{}", e.getMessage(), e);
        throw new RuntimeException(e);
      } catch (IllegalBlockSizeException e) {
        log.error("{}", e.getMessage(), e);
        throw new RuntimeException(e);
      }

    }
  }

  private static class SecretKeyCache {

    private boolean initialized = false;
    private byte[] keyEncryptionKey;
    private String pathToKeyName;

    public SecretKeyCache() {}

    public synchronized void ensureSecretKeyCacheInitialized(CryptoModuleParameters context) throws IOException {

      if (initialized) {
        return;
      }

      // First identify if the KEK already exists
      pathToKeyName = getFullPathToKey(context);

      if (pathToKeyName == null || pathToKeyName.equals("")) {
        pathToKeyName = Property.CRYPTO_DEFAULT_KEY_STRATEGY_KEY_LOCATION.getDefaultValue();
      }

      // TODO ACCUMULO-2530 Ensure volumes a properly supported
      Path pathToKey = new Path(pathToKeyName);
      FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());

      DataInputStream in = null;
      try {
        if (!fs.exists(pathToKey)) {
          initializeKeyEncryptionKey(fs, pathToKey, context);
        }

        in = fs.open(pathToKey);

        int keyEncryptionKeyLength = in.readInt();
        keyEncryptionKey = new byte[keyEncryptionKeyLength];
        in.readFully(keyEncryptionKey);

        initialized = true;

      } catch (IOException e) {
        log.error("Could not initialize key encryption cache", e);
      } finally {
        IOUtils.closeQuietly(in);
      }
    }

    private void initializeKeyEncryptionKey(FileSystem fs, Path pathToKey, CryptoModuleParameters params) throws IOException {
      DataOutputStream out = null;
      try {
        out = fs.create(pathToKey);
        // Very important, lets hedge our bets
        fs.setReplication(pathToKey, (short) 5);
        SecureRandom random = DefaultCryptoModuleUtils.getSecureRandom(params.getRandomNumberGenerator(), params.getRandomNumberGeneratorProvider());
        int keyLength = params.getKeyLength();
        byte[] newRandomKeyEncryptionKey = new byte[keyLength / 8];
        random.nextBytes(newRandomKeyEncryptionKey);
        out.writeInt(newRandomKeyEncryptionKey.length);
        out.write(newRandomKeyEncryptionKey);
        out.flush();
      } finally {
        if (out != null) {
          out.close();
        }
      }

    }

    @SuppressWarnings("deprecation")
    private String getFullPathToKey(CryptoModuleParameters params) {
      String pathToKeyName = params.getAllOptions().get(Property.CRYPTO_DEFAULT_KEY_STRATEGY_KEY_LOCATION.getKey());
      String instanceDirectory = params.getAllOptions().get(Property.INSTANCE_DFS_DIR.getKey());

      if (pathToKeyName == null) {
        pathToKeyName = Property.CRYPTO_DEFAULT_KEY_STRATEGY_KEY_LOCATION.getDefaultValue();
      }

      if (instanceDirectory == null) {
        instanceDirectory = Property.INSTANCE_DFS_DIR.getDefaultValue();
      }

      if (!pathToKeyName.startsWith("/")) {
        pathToKeyName = "/" + pathToKeyName;
      }

      String fullPath = instanceDirectory + pathToKeyName;
      return fullPath;
    }

    public byte[] getKeyEncryptionKey() {
      return keyEncryptionKey;
    }

    public String getPathToKeyName() {
      return pathToKeyName;
    }
  }

}

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO ACCUMULO-2530 Update properties to use a URI instead of a relative path to secret key
public class NonCachingSecretKeyEncryptionStrategy implements SecretKeyEncryptionStrategy {

  private static final Logger log = LoggerFactory.getLogger(NonCachingSecretKeyEncryptionStrategy.class);

  private void doKeyEncryptionOperation(int encryptionMode, CryptoModuleParameters params, String pathToKeyName, Path pathToKey, FileSystem fs)
      throws IOException {
    DataInputStream in = null;
    try {
      if (!fs.exists(pathToKey)) {

        if (encryptionMode == Cipher.UNWRAP_MODE) {
          log.error("There was a call to decrypt the session key but no key encryption key exists. "
              + "Either restore it, reconfigure the conf file to point to it in HDFS, or throw the affected data away and begin again.");
          throw new RuntimeException("Could not find key encryption key file in configured location in HDFS (" + pathToKeyName + ")");
        } else {
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
      }
      in = fs.open(pathToKey);

      int keyEncryptionKeyLength = in.readInt();
      byte[] keyEncryptionKey = new byte[keyEncryptionKeyLength];
      int bytesRead = in.read(keyEncryptionKey);

      Cipher cipher = DefaultCryptoModuleUtils.getCipher(params.getAllOptions().get(Property.CRYPTO_DEFAULT_KEY_STRATEGY_CIPHER_SUITE.getKey()));

      // check if the number of bytes read into the array is the same as the value of the length field,
      if (bytesRead == keyEncryptionKeyLength) {
        try {
          cipher.init(encryptionMode, new SecretKeySpec(keyEncryptionKey, params.getKeyAlgorithmName()));
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
            params.setOpaqueKeyEncryptionKeyID(pathToKeyName);
          } catch (InvalidKeyException e) {
            log.error("{}", e.getMessage(), e);
            throw new RuntimeException(e);
          } catch (IllegalBlockSizeException e) {
            log.error("{}", e.getMessage(), e);
            throw new RuntimeException(e);
          }

        }
      } else {
        log.error("{}", "Error:bytesRead does not match EncryptionkeyLength");
        throw new IllegalArgumentException("Error:bytesRead does not match EncryptionkeyLength");
      }
    } finally {
      if (in != null) {
        in.close();
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

  @SuppressWarnings("deprecation")
  @Override
  public CryptoModuleParameters encryptSecretKey(CryptoModuleParameters params) {
    String hdfsURI = params.getAllOptions().get(Property.INSTANCE_DFS_URI.getKey());
    if (hdfsURI == null) {
      hdfsURI = Property.INSTANCE_DFS_URI.getDefaultValue();
    }

    String fullPath = getFullPathToKey(params);
    Path pathToKey = new Path(fullPath);

    try {
      // TODO ACCUMULO-2530 Ensure volumes a properly supported
      FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
      doKeyEncryptionOperation(Cipher.WRAP_MODE, params, fullPath, pathToKey, fs);

    } catch (IOException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }

    return params;
  }

  @SuppressWarnings("deprecation")
  @Override
  public CryptoModuleParameters decryptSecretKey(CryptoModuleParameters params) {
    String hdfsURI = params.getAllOptions().get(Property.INSTANCE_DFS_URI.getKey());
    if (hdfsURI == null) {
      hdfsURI = Property.INSTANCE_DFS_URI.getDefaultValue();
    }

    String pathToKeyName = getFullPathToKey(params);
    Path pathToKey = new Path(pathToKeyName);

    try {
      // TODO ACCUMULO-2530 Ensure volumes a properly supported
      FileSystem fs = FileSystem.get(CachedConfiguration.getInstance());
      doKeyEncryptionOperation(Cipher.UNWRAP_MODE, params, pathToKeyName, pathToKey, fs);

    } catch (IOException e) {
      log.error("{}", e.getMessage(), e);
      throw new RuntimeException(e);
    }

    return params;
  }

}

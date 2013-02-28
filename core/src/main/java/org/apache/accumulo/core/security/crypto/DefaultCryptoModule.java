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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.conf.Property;
import org.apache.log4j.Logger;

/**
 * This class contains the gritty details around setting up encrypted streams for reading and writing the log file. It obeys the interface CryptoModule, which
 * other developers can implement to change out this logic as necessary.
 * 
 * @deprecated This feature is experimental and may go away in future versions.
 */
@Deprecated
public class DefaultCryptoModule implements CryptoModule {
  
  // This is how *I* like to format my variable declarations. Your mileage may vary.
  
  private static final String ENCRYPTION_HEADER_MARKER = "---Log File Encrypted (v1)---";
  private static Logger log = Logger.getLogger(DefaultCryptoModule.class);
  
  public DefaultCryptoModule() {}
  
  @Override
  public OutputStream getEncryptingOutputStream(OutputStream out, Map<String,String> cryptoOpts) throws IOException {
    
    log.debug("Initializing crypto output stream");
    
    String cipherSuite = cryptoOpts.get(Property.CRYPTO_CIPHER_SUITE.getKey());
    
    if (cipherSuite.equals("NullCipher")) {
      return out;
    }
    
    String algorithmName = cryptoOpts.get(Property.CRYPTO_CIPHER_ALGORITHM_NAME.getKey());
    String secureRNG = cryptoOpts.get(Property.CRYPTO_SECURE_RNG.getKey());
    String secureRNGProvider = cryptoOpts.get(Property.CRYPTO_SECURE_RNG_PROVIDER.getKey());
    SecureRandom secureRandom = DefaultCryptoModuleUtils.getSecureRandom(secureRNG, secureRNGProvider);
    int keyLength = Integer.parseInt(cryptoOpts.get(Property.CRYPTO_CIPHER_KEY_LENGTH.getKey()));
    
    byte[] randomKey = new byte[keyLength / 8];
    
    Map<CryptoInitProperty,Object> cryptoInitParams = new HashMap<CryptoInitProperty,Object>();
    
    secureRandom.nextBytes(randomKey);
    cryptoInitParams.put(CryptoInitProperty.PLAINTEXT_SESSION_KEY, randomKey);
    
    SecretKeyEncryptionStrategy keyEncryptionStrategy = CryptoModuleFactory.getSecretKeyEncryptionStrategy(cryptoOpts
        .get(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey()));
    SecretKeyEncryptionStrategyContext keyEncryptionStrategyContext = keyEncryptionStrategy.getNewContext();
    
    keyEncryptionStrategyContext.setPlaintextSecretKey(randomKey);
    keyEncryptionStrategyContext.setContext(cryptoOpts);
    
    keyEncryptionStrategyContext = keyEncryptionStrategy.encryptSecretKey(keyEncryptionStrategyContext);
    
    byte[] encryptedRandomKey = keyEncryptionStrategyContext.getEncryptedSecretKey();
    String opaqueId = keyEncryptionStrategyContext.getOpaqueKeyEncryptionKeyID();
    
    OutputStream cipherOutputStream = getEncryptingOutputStream(out, cryptoOpts, cryptoInitParams);
    
    // Get the IV from the init params, since we didn't create it but the other getEncryptingOutputStream did
    byte[] initVector = (byte[]) cryptoInitParams.get(CryptoInitProperty.INITIALIZATION_VECTOR);
    
    DataOutputStream dataOut = new DataOutputStream(out);
    
    // Write a marker to indicate this is an encrypted log file (in case we read it a plain one and need to
    // not try to decrypt it. Can happen during a failure when the log's encryption settings are changing.
    dataOut.writeUTF(ENCRYPTION_HEADER_MARKER);
    
    // Write out the cipher suite and algorithm used to encrypt this file. In case the admin changes, we want to still
    // decode the old format.
    dataOut.writeUTF(cipherSuite);
    dataOut.writeUTF(algorithmName);
    
    // Write the init vector to the log file
    dataOut.writeInt(initVector.length);
    dataOut.write(initVector);
    
    // Write out the encrypted session key and the opaque ID
    dataOut.writeUTF(opaqueId);
    dataOut.writeInt(encryptedRandomKey.length);
    dataOut.write(encryptedRandomKey);
    
    // Write the secret key (encrypted) into the log file
    // dataOut.writeInt(randomKey.length);
    // dataOut.write(randomKey);
    
    return cipherOutputStream;
  }
  
  @Override
  public InputStream getDecryptingInputStream(InputStream in, Map<String,String> cryptoOpts) throws IOException {
    DataInputStream dataIn = new DataInputStream(in);
    
    String marker = dataIn.readUTF();
    
    log.debug("Read encryption header");
    if (marker.equals(ENCRYPTION_HEADER_MARKER)) {
      
      String cipherSuiteFromFile = dataIn.readUTF();
      String algorithmNameFromFile = dataIn.readUTF();
      
      // Read the secret key and initialization vector from the file
      int initVectorLength = dataIn.readInt();
      byte[] initVector = new byte[initVectorLength];
      dataIn.read(initVector, 0, initVectorLength);
      
      // Read the opaque ID and encrypted session key
      String opaqueId = dataIn.readUTF();
      int encryptedSecretKeyLength = dataIn.readInt();
      byte[] encryptedSecretKey = new byte[encryptedSecretKeyLength];
      dataIn.read(encryptedSecretKey);
      
      SecretKeyEncryptionStrategy keyEncryptionStrategy = CryptoModuleFactory.getSecretKeyEncryptionStrategy(cryptoOpts
          .get(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey()));
      SecretKeyEncryptionStrategyContext keyEncryptionStrategyContext = keyEncryptionStrategy.getNewContext();
      
      keyEncryptionStrategyContext.setOpaqueKeyEncryptionKeyID(opaqueId);
      keyEncryptionStrategyContext.setContext(cryptoOpts);
      keyEncryptionStrategyContext.setEncryptedSecretKey(encryptedSecretKey);
      
      keyEncryptionStrategyContext = keyEncryptionStrategy.decryptSecretKey(keyEncryptionStrategyContext);
      
      byte[] secretKey = keyEncryptionStrategyContext.getPlaintextSecretKey();
      
      // int secretKeyLength = dataIn.readInt();
      // byte[] secretKey = new byte[secretKeyLength];
      // dataIn.read(secretKey, 0, secretKeyLength);
      
      Map<CryptoModule.CryptoInitProperty,Object> cryptoInitParams = new HashMap<CryptoModule.CryptoInitProperty,Object>();
      cryptoInitParams.put(CryptoInitProperty.CIPHER_SUITE, cipherSuiteFromFile);
      cryptoInitParams.put(CryptoInitProperty.ALGORITHM_NAME, algorithmNameFromFile);
      cryptoInitParams.put(CryptoInitProperty.PLAINTEXT_SESSION_KEY, secretKey);
      cryptoInitParams.put(CryptoInitProperty.INITIALIZATION_VECTOR, initVector);
      
      InputStream cipherInputStream = getDecryptingInputStream(dataIn, cryptoOpts, cryptoInitParams);
      return cipherInputStream;
      
    } else {
      // Push these bytes back on to the stream. This method is a bit roundabout but isolates our code
      // from having to understand the format that DataOuputStream uses for its bytes.
      ByteArrayOutputStream tempByteOut = new ByteArrayOutputStream();
      DataOutputStream tempOut = new DataOutputStream(tempByteOut);
      tempOut.writeUTF(marker);
      
      byte[] bytesToPutBack = tempByteOut.toByteArray();
      
      PushbackInputStream pushbackStream = new PushbackInputStream(in, bytesToPutBack.length);
      pushbackStream.unread(bytesToPutBack);
      
      return pushbackStream;
    }
    
  }
  
  @Override
  public OutputStream getEncryptingOutputStream(OutputStream out, Map<String,String> conf, Map<CryptoModule.CryptoInitProperty,Object> cryptoInitParams) {
    
    log.debug("Initializing crypto output stream");
    
    String cipherSuite = conf.get(Property.CRYPTO_CIPHER_SUITE.getKey());
    
    if (cipherSuite.equals("NullCipher")) {
      return out;
    }
    
    String algorithmName = conf.get(Property.CRYPTO_CIPHER_ALGORITHM_NAME.getKey());
    String secureRNG = conf.get(Property.CRYPTO_SECURE_RNG.getKey());
    String secureRNGProvider = conf.get(Property.CRYPTO_SECURE_RNG_PROVIDER.getKey());
    int keyLength = Integer.parseInt(conf.get(Property.CRYPTO_CIPHER_KEY_LENGTH.getKey()));
    String keyStrategyName = conf.get(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey());
    
    log.debug(String.format(
        "Using cipher suite \"%s\" (algorithm \"%s\") with key length %d with RNG \"%s\" and RNG provider \"%s\" and key encryption strategy %s", cipherSuite,
        algorithmName, keyLength, secureRNG, secureRNGProvider, keyStrategyName));
    
    SecureRandom secureRandom = DefaultCryptoModuleUtils.getSecureRandom(secureRNG, secureRNGProvider);
    Cipher cipher = DefaultCryptoModuleUtils.getCipher(cipherSuite);
    byte[] randomKey = (byte[]) cryptoInitParams.get(CryptoInitProperty.PLAINTEXT_SESSION_KEY);
    byte[] initVector = (byte[]) cryptoInitParams.get(CryptoInitProperty.INITIALIZATION_VECTOR);
    
    // If they pass us an IV, use it...
    if (initVector != null) {
      
      try {
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(randomKey, algorithmName), new IvParameterSpec(initVector));
      } catch (InvalidKeyException e) {
        log.error("Accumulo encountered an unknown error in generating the secret key object (SecretKeySpec) for an encrypted stream");
        throw new RuntimeException(e);
      } catch (InvalidAlgorithmParameterException e) {
        log.error("Accumulo encountered an unknown error in generating the secret key object (SecretKeySpec) for an encrypted stream");
        throw new RuntimeException(e);
      }
      
    } else {
      // We didn't get an IV, so we'll let the cipher make one for us and then put its value back into the map so
      // that the caller has access to it, to persist it.
      try {
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(randomKey, algorithmName), secureRandom);
      } catch (InvalidKeyException e) {
        log.error("Accumulo encountered an unknown error in generating the secret key object (SecretKeySpec) for the write-ahead log");
        throw new RuntimeException(e);
      }
      
      // Since the IV length is determined by the algorithm, we let the cipher generate our IV for us,
      // rather than calling secure random directly.
      initVector = cipher.getIV();
      cryptoInitParams.put(CryptoInitProperty.INITIALIZATION_VECTOR, initVector);
    }
    
    CipherOutputStream cipherOutputStream = new CipherOutputStream(out, cipher);
    BufferedOutputStream bufferedCipherOutputStream = new BufferedOutputStream(cipherOutputStream);
    
    return bufferedCipherOutputStream;
  }
  
  @Override
  public InputStream getDecryptingInputStream(InputStream in, Map<String,String> cryptoOpts, Map<CryptoModule.CryptoInitProperty,Object> cryptoInitParams)
      throws IOException {
    String cipherSuite = cryptoOpts.get(Property.CRYPTO_CIPHER_SUITE.getKey());
    String algorithmName = cryptoOpts.get(Property.CRYPTO_CIPHER_ALGORITHM_NAME.getKey());
    String cipherSuiteFromInitParams = (String) cryptoInitParams.get(CryptoInitProperty.CIPHER_SUITE);
    String algorithmNameFromInitParams = (String) cryptoInitParams.get(CryptoInitProperty.ALGORITHM_NAME);
    byte[] initVector = (byte[]) cryptoInitParams.get(CryptoInitProperty.INITIALIZATION_VECTOR);
    byte[] secretKey = (byte[]) cryptoInitParams.get(CryptoInitProperty.PLAINTEXT_SESSION_KEY);
    
    if (initVector == null || secretKey == null || cipherSuiteFromInitParams == null || algorithmNameFromInitParams == null) {
      log.error("Called getDecryptingInputStream() without proper crypto init params.  Need initVector, plaintext key, cipher suite and algorithm name");
      throw new RuntimeException("Called getDecryptingInputStream() without initialization vector and/or plaintext session key");
    }
    
    // Always use the init param's cipher suite, but check it against configured one and warn about discrepencies.
    if (!cipherSuiteFromInitParams.equals(cipherSuite) || !algorithmNameFromInitParams.equals(algorithmName))
      log.warn(String.format("Configured cipher suite and algorithm (\"%s\" and \"%s\") is different "
          + "from cipher suite found in log file (\"%s\" and \"%s\")", cipherSuite, algorithmName, cipherSuiteFromInitParams, algorithmNameFromInitParams));
    
    Cipher cipher = DefaultCryptoModuleUtils.getCipher(cipherSuiteFromInitParams);
    
    try {
      cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(secretKey, algorithmNameFromInitParams), new IvParameterSpec(initVector));
    } catch (InvalidKeyException e) {
      log.error("Error when trying to initialize cipher with secret key");
      throw new RuntimeException(e);
    } catch (InvalidAlgorithmParameterException e) {
      log.error("Error when trying to initialize cipher with initialization vector");
      throw new RuntimeException(e);
    }
    
    BufferedInputStream bufferedDecryptingInputStream = new BufferedInputStream(new CipherInputStream(in, cipher));
    
    return bufferedDecryptingInputStream;
    
  }
}

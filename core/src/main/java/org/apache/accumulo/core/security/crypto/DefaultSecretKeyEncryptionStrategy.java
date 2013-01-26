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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class DefaultSecretKeyEncryptionStrategy implements SecretKeyEncryptionStrategy {
  
  private static final Logger log = Logger.getLogger(DefaultSecretKeyEncryptionStrategy.class); 
  
  public static class DefaultSecretKeyEncryptionStrategyContext implements SecretKeyEncryptionStrategyContext {

    private byte[] plaintextSecretKey;
    private byte[] encryptedSecretKey;
    private Map<String, String> context;
    private String opaqueKeyId;
    
    @Override
    public String getOpaqueKeyEncryptionKeyID() {
      return opaqueKeyId;
    }

    @Override
    public void setOpaqueKeyEncryptionKeyID(String id) {
      this.opaqueKeyId = id;
    }

    @Override
    public byte[] getPlaintextSecretKey() {
      return plaintextSecretKey;
    }

    @Override
    public void setPlaintextSecretKey(byte[] key) {
      this.plaintextSecretKey = key;
    }

    @Override
    public byte[] getEncryptedSecretKey() {
      return encryptedSecretKey;
    }

    @Override
    public void setEncryptedSecretKey(byte[] key) {
      this.encryptedSecretKey = key;
    }

    @Override
    public Map<String,String> getContext() {
      return context;
    }

    @Override
    public void setContext(Map<String,String> context) {
      this.context = context;
    }
  }
  
  
  @Override
  public SecretKeyEncryptionStrategyContext encryptSecretKey(SecretKeyEncryptionStrategyContext context)  {
    String hdfsURI = context.getContext().get(Property.CRYPTO_DEFAULT_KEY_STRATEGY_HDFS_URI.getKey());
    String pathToKeyName = context.getContext().get(Property.CRYPTO_DEFAULT_KEY_STRATEGY_KEY_LOCATION.getKey());
    Path pathToKey = new Path(pathToKeyName);
    
    FileSystem fs = getHadoopFileSystem(hdfsURI);
    try {
      
      doKeyEncryptionOperation(Cipher.ENCRYPT_MODE, context, pathToKeyName, pathToKey, fs);
      
    } catch (IOException e) {
      log.error(e);
      throw new RuntimeException(e);
    }
  
    return context;
    
  }

  private void initializeKeyEncryptingKey(FileSystem fs, Path pathToKey, SecretKeyEncryptionStrategyContext context) throws IOException {
    Map<String, String> cryptoContext = context.getContext(); 
    DataOutputStream out = fs.create(pathToKey);
    // Very important, lets hedge our bets
    fs.setReplication(pathToKey, (short) 5);
    
    // Write number of context entries
    out.writeInt(cryptoContext.size());
    
    for (String key : cryptoContext.keySet()) {
      out.writeUTF(key);
      out.writeUTF(cryptoContext.get(key));
    }
    
    SecureRandom random = DefaultCryptoModuleUtils.getSecureRandom(cryptoContext.get(Property.CRYPTO_SECURE_RNG.getKey()), cryptoContext.get(Property.CRYPTO_SECURE_RNG_PROVIDER.getKey()));
    int keyLength = Integer.parseInt(cryptoContext.get(Property.CRYPTO_CIPHER_KEY_LENGTH.getKey()));
    byte[] newRandomKeyEncryptionKey = new byte[keyLength / 8];
    
    random.nextBytes(newRandomKeyEncryptionKey);
    
    Cipher cipher = DefaultCryptoModuleUtils.getCipher(cryptoContext.get(Property.CRYPTO_CIPHER_SUITE.getKey()));
    try {
      cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(newRandomKeyEncryptionKey, cryptoContext.get(Property.CRYPTO_CIPHER_ALGORITHM_NAME.getKey())), random);
    } catch (InvalidKeyException e) {
      log.error(e);
      throw new RuntimeException(e);
    }
    
    byte[] initVector = cipher.getIV();
    
    out.writeInt(initVector.length);
    out.write(initVector);
    
    out.writeInt(newRandomKeyEncryptionKey.length);
    out.write(newRandomKeyEncryptionKey);
    
    out.flush();
    out.close();
    
  }

  private FileSystem getHadoopFileSystem(String hdfsURI) {
    FileSystem fs = null;
    
    if (hdfsURI != null && !hdfsURI.equals("")) {
      try {
        fs = FileSystem.get(CachedConfiguration.getInstance());
      } catch (IOException e) {
        log.error(e);
        throw new RuntimeException(e);
      }
    }
    else {
      try {
        fs = FileSystem.get(new URI(hdfsURI), CachedConfiguration.getInstance());
      } catch (URISyntaxException e) {
        log.error(e);
        throw new RuntimeException(e);
      } catch (IOException e) {
        log.error(e);
        throw new RuntimeException(e);
      }
      
      
    }
    return fs;
  }
  
  @Override
  public SecretKeyEncryptionStrategyContext decryptSecretKey(SecretKeyEncryptionStrategyContext context) {
    String hdfsURI = context.getContext().get(Property.CRYPTO_DEFAULT_KEY_STRATEGY_HDFS_URI.getKey());
    String pathToKeyName = context.getContext().get(Property.CRYPTO_DEFAULT_KEY_STRATEGY_KEY_LOCATION.getKey());
    Path pathToKey = new Path(pathToKeyName);
    
    FileSystem fs = getHadoopFileSystem(hdfsURI);
    try {
      doKeyEncryptionOperation(Cipher.DECRYPT_MODE, context, pathToKeyName, pathToKey, fs);
      
      
    } catch (IOException e) {
      log.error(e);
      throw new RuntimeException(e);
    }
    
    return context;
  }

  private void doKeyEncryptionOperation(int encryptionMode, SecretKeyEncryptionStrategyContext context, String pathToKeyName, Path pathToKey, FileSystem fs)
      throws IOException {
    DataInputStream in = null;
    try {
      if (!fs.exists(pathToKey)) {
        
        if (encryptionMode == Cipher.DECRYPT_MODE) {
          log.error("There was a call to decrypt the session key but no key encryption key exists.  Either restore it, reconfigure the conf file to point to it in HDFS, or throw the affected data away and begin again.");
          throw new RuntimeException("Could not find key encryption key file in configured location in HDFS ("+pathToKeyName+")");
        } else {
          initializeKeyEncryptingKey(fs, pathToKey, context);
        }
      }
      in = fs.open(pathToKey);
      
      int numOfOpts = in.readInt();
      Map<String, String> optsFromFile = new HashMap<String, String>();
      
      for (int i = 0; i < numOfOpts; i++) {
        String key = in.readUTF();
        String value = in.readUTF();
        
        optsFromFile.put(key, value);
      }
      
      int ivLength = in.readInt();
      byte[] iv = new byte[ivLength];
      in.read(iv);
      
      
      int keyEncryptionKeyLength = in.readInt();
      byte[] keyEncryptionKey = new byte[keyEncryptionKeyLength];
      in.read(keyEncryptionKey);
      
      Cipher cipher = DefaultCryptoModuleUtils.getCipher(optsFromFile.get(Property.CRYPTO_CIPHER_SUITE.getKey()));

      try {
        cipher.init(encryptionMode, new SecretKeySpec(keyEncryptionKey, optsFromFile.get(Property.CRYPTO_CIPHER_ALGORITHM_NAME.getKey())), new IvParameterSpec(iv));
      } catch (InvalidKeyException e) {
        log.error(e);
        throw new RuntimeException(e);
      } catch (InvalidAlgorithmParameterException e) {
        log.error(e);
        throw new RuntimeException(e);
      }

      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      CipherOutputStream cipherStream = new CipherOutputStream(byteArrayOutputStream, cipher);
      
      
      if (Cipher.DECRYPT_MODE == encryptionMode) {
        cipherStream.write(context.getEncryptedSecretKey());
        cipherStream.flush();        
        byte[] plaintextSecretKey = byteArrayOutputStream.toByteArray();

        cipherStream.close();
        
        context.setPlaintextSecretKey(plaintextSecretKey);
      } else {
        cipherStream.write(context.getPlaintextSecretKey());
        cipherStream.flush();        
        byte[] encryptedSecretKey = byteArrayOutputStream.toByteArray();

        cipherStream.close();
        
        context.setEncryptedSecretKey(encryptedSecretKey);
        context.setOpaqueKeyEncryptionKeyID(pathToKeyName);
      }
      
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  @Override
  public SecretKeyEncryptionStrategyContext getNewContext() {
    return new DefaultSecretKeyEncryptionStrategyContext();
  }
  
}

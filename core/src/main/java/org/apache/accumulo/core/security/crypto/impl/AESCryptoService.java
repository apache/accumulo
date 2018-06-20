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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.crypto.BlockedInputStream;
import org.apache.accumulo.core.security.crypto.BlockedOutputStream;
import org.apache.accumulo.core.security.crypto.CryptoEnvironment;
import org.apache.accumulo.core.security.crypto.CryptoService;
import org.apache.accumulo.core.security.crypto.CryptoUtils;
import org.apache.accumulo.core.security.crypto.DiscardCloseOutputStream;
import org.apache.accumulo.core.security.crypto.FileDecrypter;
import org.apache.accumulo.core.security.crypto.FileEncrypter;

/**
 * Example encryption strategy that uses AES with CBC and No padding. This strategy requires one
 * property to be set, crypto.sensitive.key = 16-byte-key.
 */
public class AESCryptoService implements CryptoService {

  @Override
  public FileEncrypter encryptFile(CryptoEnvironment environment) {
    CryptoModule cm;
    switch (environment.getScope()) {
      case WAL:
        cm = new AESCBCCryptoModule();
        cm.init(environment);
        return cm.getEncrypter();

      case RFILE:
        cm = new AESGCMCryptoModule();
        cm.init(environment);
        return cm.getEncrypter();

      default:
        throw new CryptoException("Unknown scope: " + environment.getScope());
    }
  }

  @Override
  public FileDecrypter decryptFile(CryptoEnvironment environment) {
    CryptoModule cm;
    switch (environment.getVersion()) {
      case AESCBCCryptoModule.VERSION:
        cm = new AESCBCCryptoModule();
        cm.init(environment);
        return (cm.getDecrypter());
      case AESGCMCryptoModule.VERSION:
        cm = new AESGCMCryptoModule();
        cm.init(environment);
        return (cm.getDecrypter());
       
        //TODO I suspect we want to leave "U+1F47B" and create an internal NoFileDecrypter
        //so that the crypto service .jar file can pluggable without requiring the NoCryptoService.jar
      case NoCryptoService.VERSION:  
        return new NoFileDecrypter();
      default:
        throw new CryptoException("Unknown crypto module version: " + environment.getVersion());
    }
  }

  /**
   * 
   * This interface lists the methods needed by CryptoModules which are responsible for tracking
   * version and preparing encrypters/decrypters for use.
   *
   */
  private interface CryptoModule {
    void init(CryptoEnvironment environment);

    FileEncrypter getEncrypter();

    FileDecrypter getDecrypter();
  }

  public static class AESGCMCryptoModule implements CryptoModule {
    private static final String VERSION = "U+1F43B"; // unicode bear emoji rawr

    private final Integer GCM_IV_LENGTH_IN_BYTES = 12;
    private final Integer KEY_LENGTH_IN_BYTES = 16;
    // 128-bit tags are the longest available for GCM
    private final Integer GCM_TAG_LENGTH_IN_BITS = 16 * 8;
    
    /**
     * The actual secret key to use
     */
    public static final String CRYPTO_SECRET_KEY_PROPERTY = Property.TABLE_CRYPTO_SENSITIVE_PREFIX
        + "key";

    private final String transformation = "AES/GCM/NoPadding";
    private SecretKeySpec skeySpec;
    private byte[] initVector = new byte[GCM_IV_LENGTH_IN_BYTES];
    private boolean initialized = false;
    private boolean ivReused = false;
    
    public void init(CryptoEnvironment environment) {
        String key = environment.getConf().get(CRYPTO_SECRET_KEY_PROPERTY);

        // do some basic validation
        if (key == null) {
          throw new CryptoException("Failed AESEncryptionStrategy init - missing required "
              + "configuration property: " + CRYPTO_SECRET_KEY_PROPERTY);
        }
        if (key.getBytes().length != KEY_LENGTH_IN_BYTES) {
          throw new CryptoException("Failed AESEncryptionStrategy init - key length not "
              + KEY_LENGTH_IN_BYTES + " provided: " + key.getBytes().length);
        }

        this.skeySpec = new SecretKeySpec(key.getBytes(), "AES");
        initialized = true;

        this.initialized = true;
    }

    @Override
    public FileEncrypter getEncrypter() {
    	if (this.initialized) {
            return new AESGCMFileEncrypter();
          } else {
            throw new CryptoException("Unable to encrypt with an uninitialized module");
          }
    }

    @Override
    public FileDecrypter getDecrypter() {
    	if (this.initialized) {
            return new AESGCMFileDecrypter();
          } else {
            throw new CryptoException("Unable to encrypt with an uninitialized module");
          }
    }

    public class AESGCMFileEncrypter implements FileEncrypter {
    	
    	private byte[] firstInitVector = new byte[GCM_IV_LENGTH_IN_BYTES];
    	
    	AESGCMFileEncrypter()
    	{
    		CryptoUtils.getSha1SecureRandom().nextBytes(initVector);
    		firstInitVector = Arrays.copyOf(initVector, initVector.length);
    	}
    	
        @Override
        public OutputStream encryptStream(OutputStream outputStream) throws CryptoException {
          if (!initialized)
            throw new CryptoException("AESGCMCryptoModule not initialized.");

          if (ivReused)
          {
        	  throw new CryptoException("AESGCMCryptoModule is attempting a Key/IV reuse which is forbidden. Too many RBlocks.");
          }
          incrementIV(initVector, initVector.length - 1);
          if(Arrays.equals(initVector, firstInitVector))
          {
        	ivReused = true; //This will allow us to write the final block, since the initialization vector
        					 //is always incremented before use.
          }
          
          Cipher cipher;
          try {
            cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.ENCRYPT_MODE, skeySpec, 
            		new GCMParameterSpec(GCM_TAG_LENGTH_IN_BITS, initVector));
          } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
              | InvalidAlgorithmParameterException e) {
            throw new CryptoException(e);
          }

          CipherOutputStream cos = new CipherOutputStream(outputStream, cipher);
          try {
            cos.write(initVector);
          } catch (IOException e) {
            try {
              cos.close();
            } catch (IOException ioe) {
              throw new CryptoException(ioe);
            }
            throw new CryptoException(e);
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
        public void addParamsToStream(OutputStream outputStream) throws CryptoException {
          try {
            outputStream.write(VERSION.getBytes());
          } catch (IOException e) {
            throw new CryptoException("Unable to record AES parameters to stream");
          }
        }
      
      /**
       * Because IVs can be longer than longs, this increments arbitrarily sized byte arrays by 1, with
       * a roll over to 0 after the max value is reached.
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
    }

    public class AESGCMFileDecrypter implements FileDecrypter {
        @Override
        public InputStream decryptStream(InputStream inputStream) throws CryptoException {
          if (!initialized)
            throw new CryptoException("AESEncryptionStrategy not initialized.");
          int bytesRead;
          try {
            bytesRead = inputStream.read(initVector);
          } catch (IOException e) {
            throw new CryptoException(e);
          }
          if (bytesRead != GCM_IV_LENGTH_IN_BYTES)
            throw new CryptoException("Read " + bytesRead + " bytes, not IV length of "
                + GCM_IV_LENGTH_IN_BYTES + " in decryptStream.");

          Cipher cipher;
          try {
            cipher = Cipher.getInstance(transformation);
            cipher.init(Cipher.DECRYPT_MODE, skeySpec,
            		new GCMParameterSpec(GCM_TAG_LENGTH_IN_BITS, initVector));
          } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
              | InvalidAlgorithmParameterException e) {
            throw new CryptoException(e);
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
    /**
     * The actual secret key to use
     */
    public static final String CRYPTO_SECRET_KEY_PROPERTY = Property.TABLE_CRYPTO_SENSITIVE_PREFIX
        + "key";

    private final String transformation = "AES/CBC/NoPadding";
    private SecretKeySpec skeySpec;
    private final byte[] initVector = new byte[IV_LENGTH_IN_BYTES];
    private boolean initialized = false;

    @Override
    public void init(CryptoEnvironment environment) {

      String key = environment.getConf().get(CRYPTO_SECRET_KEY_PROPERTY);

      // do some basic validation
      if (key == null) {
        throw new CryptoException("Failed AESEncryptionStrategy init - missing required "
            + "configuration property: " + CRYPTO_SECRET_KEY_PROPERTY);
      }
      if (key.getBytes().length != KEY_LENGTH_IN_BYTES) {
        throw new CryptoException("Failed AESEncryptionStrategy init - key length not "
            + KEY_LENGTH_IN_BYTES + " provided: " + key.getBytes().length);
      }

      this.skeySpec = new SecretKeySpec(key.getBytes(), "AES");
      initialized = true;

      this.initialized = true;
    }

    @Override
    public FileEncrypter getEncrypter() {
      if (this.initialized) {
        return new AESCBCFileEncrypter();
      } else {
        throw new CryptoException("Unable to encrypt with an uninitialized module");
      }
    }

    @Override
    public FileDecrypter getDecrypter() {
      if (this.initialized) {
        return new AESCBCFileDecrypter();
      } else {
        throw new CryptoException("Unable to decrypt with an uninitialized module");
      }
    }

    public class AESCBCFileEncrypter implements FileEncrypter {
      @Override
      public OutputStream encryptStream(OutputStream outputStream) throws CryptoException {
        if (!initialized)
          throw new CryptoException("AESCBCCryptoModule not initialized.");

        CryptoUtils.getSha1SecureRandom().nextBytes(initVector);
        Cipher cipher;
        try {
          cipher = Cipher.getInstance(transformation);
          cipher.init(Cipher.ENCRYPT_MODE, skeySpec, new IvParameterSpec(initVector));
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
            | InvalidAlgorithmParameterException e) {
          throw new CryptoException(e);
        }

        CipherOutputStream cos = new CipherOutputStream(outputStream, cipher);
        try {
          cos.write(initVector);
        } catch (IOException e) {
          try {
            cos.close();
          } catch (IOException ioe) {
            throw new CryptoException(ioe);
          }
          throw new CryptoException(e);
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
      public void addParamsToStream(OutputStream outputStream) throws CryptoException {
        try {
          outputStream.write(VERSION.getBytes());
        } catch (IOException e) {
          throw new CryptoException("Unable to record AES parameters to stream");
        }
      }
    }

    public class AESCBCFileDecrypter implements FileDecrypter {
      @Override
      public InputStream decryptStream(InputStream inputStream) throws CryptoException {
        if (!initialized)
          throw new CryptoException("AESEncryptionStrategy not initialized.");
        int bytesRead;
        try {
          bytesRead = inputStream.read(initVector);
        } catch (IOException e) {
          throw new CryptoException(e);
        }
        if (bytesRead != IV_LENGTH_IN_BYTES)
          throw new CryptoException("Read " + bytesRead + " bytes, not IV length of "
              + IV_LENGTH_IN_BYTES + " in decryptStream.");

        Cipher cipher;
        try {
          cipher = Cipher.getInstance(transformation);
          cipher.init(Cipher.DECRYPT_MODE, skeySpec, new IvParameterSpec(initVector));
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
            | InvalidAlgorithmParameterException e) {
          throw new CryptoException(e);
        }

        CipherInputStream cis = new CipherInputStream(inputStream, cipher);
        return new BlockedInputStream(cis, cipher.getBlockSize(), 1024);
      }
    }
  }
}

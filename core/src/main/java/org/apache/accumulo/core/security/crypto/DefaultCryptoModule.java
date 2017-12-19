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
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the {@link CryptoModule} interface, defining how calling applications can receive encrypted input and output streams. While the default
 * implementation given here allows for a lot of flexibility in terms of choices of algorithm, key encryption strategies, and so on, some Accumulo users may
 * choose to swap out this implementation for others, and can base their implementation details off of this class's work.
 *
 * In general, the module is quite straightforward: provide it with crypto-related settings and an input/output stream, and it will hand back those streams
 * wrapped in encrypting (or decrypting) streams.
 *
 */
public class DefaultCryptoModule implements CryptoModule {

  private static final String ENCRYPTION_HEADER_MARKER_V1 = "---Log File Encrypted (v1)---";
  private static final String ENCRYPTION_HEADER_MARKER_V2 = "---Log File Encrypted (v2)---";
  public static final String ALGORITHM_PARAMETER_SPEC_GCM = "GCM";

  // 128-bit tags are the longest available for GCM
  private static final Integer GCM_TAG_LENGTH_IN_BYTES = 16;

  /*
   * According to NIST Special Publication 800-38D, Section 5.2.1.1: "For IVs, it is recommended that implementations restrict support to the length of 96 bits,
   * to promote interoperability, efficiency, and simplicity of design"
   */
  private static final Integer GCM_IV_LENGTH_IN_BYTES = 12;

  private static final Logger log = LoggerFactory.getLogger(DefaultCryptoModule.class);

  public DefaultCryptoModule() {}

  @Override
  public CryptoModuleParameters initializeCipher(CryptoModuleParameters params) {

    log.trace(String.format("Using cipher suite \"%s\" with key length %d with RNG \"%s\" and RNG provider \"%s\" and key encryption strategy \"%s\"",
        params.getCipherSuite(), params.getKeyLength(), params.getRandomNumberGenerator(), params.getRandomNumberGeneratorProvider(),
        params.getKeyEncryptionStrategyClass()));

    if (params.getSecureRandom() == null) {
      SecureRandom secureRandom = DefaultCryptoModuleUtils.getSecureRandom(params.getRandomNumberGenerator(), params.getRandomNumberGeneratorProvider());
      params.setSecureRandom(secureRandom);
    }

    Cipher cipher = DefaultCryptoModuleUtils.getCipher(params.getCipherSuite(), params.getSecurityProvider());

    if (params.getInitializationVector() == null) {
      if (params.getCipherSuiteEncryptionMode().equals(ALGORITHM_PARAMETER_SPEC_GCM)) {
        byte[] gcmIV = new byte[GCM_IV_LENGTH_IN_BYTES];
        params.getSecureRandom().nextBytes(gcmIV);
        params.setInitializationVector(gcmIV);
      }
    }

    try {
      initCipher(params, cipher, Cipher.ENCRYPT_MODE);
    } catch (InvalidKeyException e) {
      log.error("Accumulo encountered an unknown error in generating the secret key object (SecretKeySpec) for an encrypted stream");
      throw new RuntimeException(e);
    } catch (InvalidAlgorithmParameterException e) {
      log.error("Accumulo encountered an unknown error in setting up the initialization vector for an encrypted stream");
      throw new RuntimeException(e);
    }

    params.setCipher(cipher);

    return params;

  }

  private boolean validateNotEmpty(String givenValue, boolean allIsWell, StringBuilder buf, String errorMessage) {
    if (givenValue == null || givenValue.equals("")) {
      buf.append(errorMessage);
      buf.append("\n");
      return false;
    }

    return true && allIsWell;
  }

  private boolean validateNotNull(Object givenValue, boolean allIsWell, StringBuilder buf, String errorMessage) {
    if (givenValue == null) {
      buf.append(errorMessage);
      buf.append("\n");
      return false;
    }

    return true && allIsWell;
  }

  private boolean validateNotZero(int givenValue, boolean allIsWell, StringBuilder buf, String errorMessage) {
    if (givenValue == 0) {
      buf.append(errorMessage);
      buf.append("\n");
      return false;
    }

    return true && allIsWell;
  }

  private boolean validateParamsObject(CryptoModuleParameters params, int cipherMode) {

    if (cipherMode == Cipher.ENCRYPT_MODE) {

      StringBuilder errorBuf = new StringBuilder(
          "The following problems were found with the CryptoModuleParameters object you provided for an encrypt operation:\n");
      boolean allIsWell = true;

      allIsWell = validateNotEmpty(params.getCipherSuite(), allIsWell, errorBuf, "No cipher suite was specified.");

      if (allIsWell && params.getCipherSuite().equals("NullCipher")) {
        return true;
      }

      allIsWell = validateNotZero(params.getKeyLength(), allIsWell, errorBuf, "No key length was specified.");
      allIsWell = validateNotEmpty(params.getKeyAlgorithmName(), allIsWell, errorBuf, "No key algorithm name was specified.");
      allIsWell = validateNotEmpty(params.getRandomNumberGenerator(), allIsWell, errorBuf, "No random number generator was specified.");
      allIsWell = validateNotEmpty(params.getRandomNumberGeneratorProvider(), allIsWell, errorBuf, "No random number generate provider was specified.");
      allIsWell = validateNotNull(params.getPlaintextOutputStream(), allIsWell, errorBuf, "No plaintext output stream was specified.");

      if (!allIsWell) {
        log.error("CryptoModulesParameters object is not valid.");
        log.error(errorBuf.toString());
        throw new RuntimeException("CryptoModulesParameters object is not valid.");
      }

      return allIsWell;

    } else if (cipherMode == Cipher.DECRYPT_MODE) {
      StringBuilder errorBuf = new StringBuilder(
          "The following problems were found with the CryptoModuleParameters object you provided for a decrypt operation:\n");
      boolean allIsWell = true;

      allIsWell = validateNotZero(params.getKeyLength(), allIsWell, errorBuf, "No key length was specified.");
      allIsWell = validateNotEmpty(params.getRandomNumberGenerator(), allIsWell, errorBuf, "No random number generator was specified.");
      allIsWell = validateNotEmpty(params.getRandomNumberGeneratorProvider(), allIsWell, errorBuf, "No random number generate provider was specified.");
      allIsWell = validateNotNull(params.getEncryptedInputStream(), allIsWell, errorBuf, "No encrypted input stream was specified.");
      allIsWell = validateNotNull(params.getInitializationVector(), allIsWell, errorBuf, "No initialization vector was specified.");
      allIsWell = validateNotNull(params.getEncryptedKey(), allIsWell, errorBuf, "No encrypted key was specified.");

      if (params.getKeyEncryptionStrategyClass() != null && !params.getKeyEncryptionStrategyClass().equals("NullSecretKeyEncryptionStrategy")) {
        allIsWell = validateNotEmpty(params.getOpaqueKeyEncryptionKeyID(), allIsWell, errorBuf, "No opqaue key encryption ID was specified.");
      }

      if (!allIsWell) {
        log.error("CryptoModulesParameters object is not valid.");
        log.error(errorBuf.toString());
        throw new RuntimeException("CryptoModulesParameters object is not valid.");
      }

      return allIsWell;

    }

    return false;
  }

  @Override
  public CryptoModuleParameters getEncryptingOutputStream(CryptoModuleParameters params) throws IOException {

    log.trace("Initializing crypto output stream (new style)");

    boolean allParamsOK = validateParamsObject(params, Cipher.ENCRYPT_MODE);
    if (!allParamsOK) {
      // This would be weird because the above call should throw an exception, but if they don't we'll check and throw.

      log.error("CryptoModuleParameters was not valid.");
      throw new RuntimeException("Invalid CryptoModuleParameters");
    }

    // If they want a null output stream, just return their plaintext stream as the encrypted stream
    if (params.getCipherSuite().equals("NullCipher")) {
      params.setEncryptedOutputStream(params.getPlaintextOutputStream());
      return params;
    }

    // Get the secret key
    if (params.getPlaintextKey() == null) {
      params = generateNewRandomSessionKey(params);
    }

    // Encrypt the secret key

    SecretKeyEncryptionStrategy keyEncryptionStrategy = CryptoModuleFactory.getSecretKeyEncryptionStrategy(params.getKeyEncryptionStrategyClass());
    params = keyEncryptionStrategy.encryptSecretKey(params);

    // Now the encrypted version of the key and any opaque ID are within the params object. Initialize the cipher.

    // Check if the caller wants us to close the downstream stream when close() is called on the
    // cipher object. Calling close() on a CipherOutputStream is necessary for it to write out
    // padding bytes.
    if (!params.getCloseUnderylingStreamAfterCryptoStreamClose()) {
      params.setPlaintextOutputStream(new DiscardCloseOutputStream(params.getPlaintextOutputStream()));
    }

    Cipher cipher = params.getCipher();
    if (cipher == null) {
      initializeCipher(params);
      cipher = params.getCipher();
    }

    if (0 == cipher.getBlockSize()) {
      throw new RuntimeException("Encryption cipher must be a block cipher");
    }

    RFileCipherOutputStream cipherOutputStream = new RFileCipherOutputStream(params.getPlaintextOutputStream(), cipher);
    BlockedOutputStream blockedOutputStream = new BlockedOutputStream(cipherOutputStream, cipher.getBlockSize(), params.getBlockStreamSize());

    params.setEncryptedOutputStream(blockedOutputStream);

    if (params.getRecordParametersToStream()) {
      DataOutputStream dataOut = new DataOutputStream(params.getPlaintextOutputStream());

      // Write a marker to indicate this is an encrypted log file (in case we read it a plain one and need to
      // not try to decrypt it. Can happen during a failure when the log's encryption settings are changing.
      dataOut.writeUTF(ENCRYPTION_HEADER_MARKER_V2);

      // Write out all the parameters
      dataOut.writeInt(params.getAllOptions().size());
      for (String key : params.getAllOptions().keySet()) {
        dataOut.writeUTF(key);
        dataOut.writeUTF(params.getAllOptions().get(key));
      }

      // Write out the cipher suite and algorithm used to encrypt this file. In case the admin changes, we want to still
      // decode the old format.
      dataOut.writeUTF(params.getCipherSuite());
      dataOut.writeUTF(params.getKeyAlgorithmName());

      // Write the init vector to the log file
      dataOut.writeInt(params.getInitializationVector().length);
      dataOut.write(params.getInitializationVector());

      // Write out the encrypted session key and the opaque ID
      dataOut.writeUTF(params.getOpaqueKeyEncryptionKeyID());
      dataOut.writeInt(params.getEncryptedKey().length);
      dataOut.write(params.getEncryptedKey());
      dataOut.writeInt(params.getBlockStreamSize());
    }

    return params;
  }

  @Override
  public CryptoModuleParameters getDecryptingInputStream(CryptoModuleParameters params) throws IOException {
    log.trace("About to initialize decryption stream (new style)");

    if (params.getRecordParametersToStream()) {
      DataInputStream dataIn = new DataInputStream(params.getEncryptedInputStream());
      log.trace("About to read encryption parameters from underlying stream");

      String marker = dataIn.readUTF();
      if (marker.equals(ENCRYPTION_HEADER_MARKER_V1) || marker.equals(ENCRYPTION_HEADER_MARKER_V2)) {

        Map<String,String> paramsFromFile = new HashMap<>();

        // Read in the bulk of parameters
        int paramsCount = dataIn.readInt();
        for (int i = 0; i < paramsCount; i++) {
          String key = dataIn.readUTF();
          String value = dataIn.readUTF();

          paramsFromFile.put(key, value);
        }

        // Set the cipher parameters
        String cipherSuiteFromFile = dataIn.readUTF();
        String algorithmNameFromFile = dataIn.readUTF();
        params.setCipherSuite(cipherSuiteFromFile);
        params.setKeyAlgorithmName(algorithmNameFromFile);

        // Read the secret key and initialization vector from the file
        int initVectorLength = dataIn.readInt();
        byte[] initVector = new byte[initVectorLength];
        dataIn.readFully(initVector);

        params.setInitializationVector(initVector);

        // Read the opaque ID and encrypted session key
        String opaqueId = dataIn.readUTF();
        params.setOpaqueKeyEncryptionKeyID(opaqueId);

        int encryptedSecretKeyLength = dataIn.readInt();
        byte[] encryptedSecretKey = new byte[encryptedSecretKeyLength];
        dataIn.readFully(encryptedSecretKey);
        params.setEncryptedKey(encryptedSecretKey);

        if (params.getOverrideStreamsSecretKeyEncryptionStrategy()) {
          // Merge in options from file selectively
          for (String name : paramsFromFile.keySet()) {
            if (!name.equals(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey())) {
              params.getAllOptions().put(name, paramsFromFile.get(name));
            }
          }
          params.setKeyEncryptionStrategyClass(params.getAllOptions().get(Property.CRYPTO_SECRET_KEY_ENCRYPTION_STRATEGY_CLASS.getKey()));
        } else {
          params = CryptoModuleFactory.fillParamsObjectFromStringMap(params, paramsFromFile);
        }

        SecretKeyEncryptionStrategy keyEncryptionStrategy = CryptoModuleFactory.getSecretKeyEncryptionStrategy(params.getKeyEncryptionStrategyClass());

        params = keyEncryptionStrategy.decryptSecretKey(params);

        if (marker.equals(ENCRYPTION_HEADER_MARKER_V2))
          params.setBlockStreamSize(dataIn.readInt());
        else
          params.setBlockStreamSize(0);
      } else {

        log.trace("Read something off of the encrypted input stream that was not the encryption header marker, so pushing back bytes and returning the given stream");
        // Push these bytes back on to the stream. This method is a bit roundabout but isolates our code
        // from having to understand the format that DataOuputStream uses for its bytes.
        ByteArrayOutputStream tempByteOut = new ByteArrayOutputStream();
        DataOutputStream tempOut = new DataOutputStream(tempByteOut);
        tempOut.writeUTF(marker);

        byte[] bytesToPutBack = tempByteOut.toByteArray();

        PushbackInputStream pushbackStream = new PushbackInputStream(params.getEncryptedInputStream(), bytesToPutBack.length);
        pushbackStream.unread(bytesToPutBack);

        params.setPlaintextInputStream(pushbackStream);

        return params;
      }
    }

    // We validate here after reading parameters from the stream, not at the top of the function.
    boolean allParamsOK = validateParamsObject(params, Cipher.DECRYPT_MODE);

    if (!allParamsOK) {
      log.error("CryptoModuleParameters object failed validation for decrypt");
      throw new RuntimeException("CryptoModuleParameters object failed validation for decrypt");
    }

    Cipher cipher = DefaultCryptoModuleUtils.getCipher(params.getCipherSuite(), params.getSecurityProvider());

    try {
      initCipher(params, cipher, Cipher.DECRYPT_MODE);
    } catch (InvalidKeyException e) {
      log.error("Error when trying to initialize cipher with secret key");
      throw new RuntimeException(e);
    } catch (InvalidAlgorithmParameterException e) {
      log.error("Error when trying to initialize cipher with initialization vector");
      throw new RuntimeException(e);
    }

    InputStream blockedDecryptingInputStream = new CipherInputStream(params.getEncryptedInputStream(), cipher);

    if (params.getBlockStreamSize() > 0)
      blockedDecryptingInputStream = new BlockedInputStream(blockedDecryptingInputStream, cipher.getBlockSize(), params.getBlockStreamSize());

    log.trace("Initialized cipher input stream with transformation [{}]", params.getCipherSuite());

    params.setPlaintextInputStream(blockedDecryptingInputStream);

    return params;
  }

  /**
   *
   * @param params
   *          the crypto parameters
   * @param cipher
   *          the Java Cipher object to be init'd
   * @param opMode
   *          encrypt or decrypt
   * @throws InvalidKeyException
   *           if the crypto params are missing necessary values
   * @throws InvalidAlgorithmParameterException
   *           if an invalid algorithm is chosen
   */
  private void initCipher(CryptoModuleParameters params, Cipher cipher, int opMode) throws InvalidKeyException, InvalidAlgorithmParameterException {
    if (params.getCipherSuiteEncryptionMode().equals(ALGORITHM_PARAMETER_SPEC_GCM)) {
      cipher.init(opMode, new SecretKeySpec(params.getPlaintextKey(), params.getKeyAlgorithmName()),
          new GCMParameterSpec(GCM_TAG_LENGTH_IN_BYTES * 8, params.getInitializationVector()));
    } else {
      if (params.getInitializationVector() == null) {
        cipher.init(opMode, new SecretKeySpec(params.getPlaintextKey(), params.getKeyAlgorithmName()), params.getSecureRandom());
        params.setInitializationVector(cipher.getIV());
      } else {
        cipher.init(opMode, new SecretKeySpec(params.getPlaintextKey(), params.getKeyAlgorithmName()), new IvParameterSpec(params.getInitializationVector()));
      }
    }
  }

  @Override
  public CryptoModuleParameters generateNewRandomSessionKey(CryptoModuleParameters params) {

    if (params.getSecureRandom() == null) {
      params.setSecureRandom(DefaultCryptoModuleUtils.getSecureRandom(params.getRandomNumberGenerator(), params.getRandomNumberGeneratorProvider()));
    }
    byte[] newSessionKey = new byte[params.getKeyLength() / 8];

    params.getSecureRandom().nextBytes(newSessionKey);
    params.setPlaintextKey(newSessionKey);

    return params;
  }
}

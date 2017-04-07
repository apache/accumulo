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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;

/**
 * Classes that obey this interface may be used to provide encrypting and decrypting streams to the rest of Accumulo. Classes that obey this interface may be
 * configured as the crypto module by setting the property crypto.module.class in the accumulo-site.xml file.
 *
 *
 */
public interface CryptoModule {

  /**
   * Takes a {@link CryptoModuleParameters} object containing an {@link OutputStream} to wrap within a {@link CipherOutputStream}. The various other parts of
   * the {@link CryptoModuleParameters} object specify the details about the type of encryption to use. Callers should pay special attention to the
   * {@link CryptoModuleParameters#getRecordParametersToStream()} and {@link CryptoModuleParameters#getCloseUnderylingStreamAfterCryptoStreamClose()} flags
   * within the {@link CryptoModuleParameters} object, as they control whether or not this method will write to the given {@link OutputStream} in
   * {@link CryptoModuleParameters#getPlaintextOutputStream()}.
   *
   * <p>
   *
   * This method returns a {@link CryptoModuleParameters} object. Implementers of this interface maintain a contract that the returned object is <i>the same</i>
   * as the one passed in, always. Return values are enclosed within that object, as some other calls will typically return more than one value.
   *
   * @param params
   *          the {@link CryptoModuleParameters} object that specifies how to set up the encrypted stream.
   * @return the same {@link CryptoModuleParameters} object with the {@link CryptoModuleParameters#getEncryptedOutputStream()} set to a stream that is not null.
   *         That stream may be exactly the same stream as {@link CryptoModuleParameters#getPlaintextInputStream()} if the params object specifies no
   *         cryptography.
   */
  CryptoModuleParameters getEncryptingOutputStream(CryptoModuleParameters params) throws IOException;

  /**
   * Takes a {@link CryptoModuleParameters} object containing an {@link InputStream} to wrap within a {@link CipherInputStream}. The various other parts of the
   * {@link CryptoModuleParameters} object specify the details about the type of encryption to use. Callers should pay special attention to the
   * {@link CryptoModuleParameters#getRecordParametersToStream()} and {@link CryptoModuleParameters#getCloseUnderylingStreamAfterCryptoStreamClose()} flags
   * within the {@link CryptoModuleParameters} object, as they control whether or not this method will read from the given {@link InputStream} in
   * {@link CryptoModuleParameters#getEncryptedInputStream()}.
   *
   * <p>
   *
   * This method returns a {@link CryptoModuleParameters} object. Implementers of this interface maintain a contract that the returned object is <i>the same</i>
   * as the one passed in, always. Return values are enclosed within that object, as some other calls will typically return more than one value.
   *
   * @param params
   *          the {@link CryptoModuleParameters} object that specifies how to set up the encrypted stream.
   * @return the same {@link CryptoModuleParameters} object with the {@link CryptoModuleParameters#getPlaintextInputStream()} set to a stream that is not null.
   *         That stream may be exactly the same stream as {@link CryptoModuleParameters#getEncryptedInputStream()} if the params object specifies no
   *         cryptography.
   */
  CryptoModuleParameters getDecryptingInputStream(CryptoModuleParameters params) throws IOException;

  /**
   * Generates a random session key and sets it into the {@link CryptoModuleParameters#getPlaintextKey()} property. Saves callers from having to set up their
   * own secure random provider. Also will set the {@link CryptoModuleParameters#getSecureRandom()} property if it has not already been set by some other
   * function.
   *
   * @param params
   *          a {@link CryptoModuleParameters} object contained a correctly instantiated set of properties.
   * @return the same {@link CryptoModuleParameters} object with the plaintext key set
   */
  CryptoModuleParameters generateNewRandomSessionKey(CryptoModuleParameters params);

  /**
   * Generates a {@link Cipher} object based on the parameters in the given {@link CryptoModuleParameters} object and places it into the
   * {@link CryptoModuleParameters#getCipher()} property. Callers may choose to use this method if they want to get the initialization vector from the cipher
   * before proceeding to create wrapped streams.
   *
   * @param params
   *          a {@link CryptoModuleParameters} object contained a correctly instantiated set of properties.
   * @return the same {@link CryptoModuleParameters} object with the cipher set.
   */
  CryptoModuleParameters initializeCipher(CryptoModuleParameters params);

}

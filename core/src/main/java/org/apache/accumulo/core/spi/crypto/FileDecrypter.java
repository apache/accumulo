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

import java.io.InputStream;

/**
 * Class implementation that will decrypt a file. Make sure implementation is thread safe. All
 * initialization of the class should be done in {@link #init(InitParams)}. Any classes configured
 * for decryption will be loaded ahead of time. Then when an encrypted file is read, the class name
 * in the file will be matched against the configuration. Only if there is a match with what is in
 * the file and what is configured will {@link #init(InitParams)} be called. If the matching class
 * is initialized without exception, then {@link #decryptStream(InputStream)} will be called to
 * perform decryption.
 *
 * @since 2.1
 */
public interface FileDecrypter {

  /**
   * Initialize the class. This is done after the class name is matched against the first bytes of
   * the decryptionParameters read from the file. If the class name matches what is configured, then
   * this method is called.
   *
   * @param initParams
   *          Objects needed for decryption
   */
  void init(InitParams initParams);

  interface InitParams {
    byte[] getDecryptionParameters();
  }

  /**
   * Decrypt the InputStream
   */
  InputStream decryptStream(InputStream inputStream) throws CryptoException;
}

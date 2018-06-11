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
package org.apache.accumulo.core.security.crypto;

import java.io.InputStream;
import java.io.OutputStream;

import org.apache.accumulo.core.conf.AccumuloConfiguration;

public interface EncryptionStrategy {

  /**
   * Where in Accumulo the on-disk file encryption takes place.
   */
  enum Scope {
    WAL, RFILE;
  }

  /**
   * Initialize the EncryptionStrategy.
   *
   * @param encryptionScope
   *          where the encryption takes places
   * @return true if initialization was successful
   * @since 2.0
   */
  boolean init(Scope encryptionScope, AccumuloConfiguration configuration)
      throws EncryptionStrategyException;

  /**
   * Encrypts the OutputStream.
   *
   * @since 2.0
   */
  OutputStream encryptStream(OutputStream outputStream) throws EncryptionStrategyException;

  /**
   * Decrypts the InputStream.
   *
   * @since 2.0
   */
  InputStream decryptStream(InputStream inputStream) throws EncryptionStrategyException;

  /**
   * This method is responsible for printing all information required for decrypting to a stream
   *
   * @param outputStream
   *          The stream being written in requiring crypto information
   * @throws EncryptionStrategyException
   *           if the print fails
   */
  void printCryptoInfoToStream(OutputStream outputStream);

  public class EncryptionStrategyException extends RuntimeException {
    EncryptionStrategyException() {
      super();
    }

    EncryptionStrategyException(String message) {
      super(message);
    }

    EncryptionStrategyException(String message, Throwable cause) {
      super(message, cause);
    }

    EncryptionStrategyException(Throwable cause) {
      super(cause);
    }

  }
}

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

import java.util.Map;

/**
 * Self contained cryptographic service. All on disk encryption and decryption will take place
 * through this interface. Each implementation must implement a {@link FileEncrypter} for encryption
 * and a {@link FileDecrypter} for decryption.
 *
 * @since 2.0
 */
public interface CryptoService {

  /**
   * Initialize CryptoService. This is called once at Tablet Server startup.
   *
   * @since 2.0
   */
  void init(Map<String,String> conf) throws CryptoException;

  /**
   * Initialize the FileEncrypter for the environment and return
   *
   * @since 2.0
   */
  FileEncrypter getFileEncrypter(CryptoEnvironment environment);

  /**
   * Initialize the FileDecrypter for the environment and return
   *
   * @since 2.0
   */
  FileDecrypter getFileDecrypter(CryptoEnvironment environment);

  /**
   * Runtime Crypto exception
   */
  class CryptoException extends RuntimeException {

    private static final long serialVersionUID = -7588781060677839664L;

    public CryptoException() {
      super();
    }

    public CryptoException(String message) {
      super(message);
    }

    public CryptoException(String message, Throwable cause) {
      super(message, cause);
    }

    public CryptoException(Throwable cause) {
      super(cause);
    }
  }
}

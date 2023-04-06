/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.spi.crypto;

import java.io.OutputStream;

/**
 * Class implementation that will encrypt a file. Make sure implementation is thread safe.
 *
 * @since 2.0
 */
public interface FileEncrypter {
  /**
   * Encrypt the OutputStream.
   */
  OutputStream encryptStream(OutputStream outputStream) throws CryptoService.CryptoException;

  /**
   * Get all the parameters required for decryption. WARNING: This byte[] will get written as part
   * of the OutputStream as it is returned (either before or after the encrypted data). Do not
   * return any unencrypted sensitive information.
   *
   * For example, return information about the encryption taking place such as version, class name
   * or a wrapped File Encryption Key. This information will get written at the beginning of an
   * encrypted Write Ahead Log (WAL) or at the end of an encrypted R-File. Later, it will be read
   * from the file and passed to the {@link FileDecrypter} as part of {@link CryptoEnvironment} for
   * everything it needs for decryption.
   */
  byte[] getDecryptionParameters();
}

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

import java.io.OutputStream;
import java.util.Map;

/**
 * Class implementation that will encrypt a file. Make sure implementation is thread safe.
 *
 * @since 2.0
 */
public interface FileEncrypter {

  /**
   * Initialize encryption. This is called once at Tablet Server startup or during the creation of a
   * Write Ahead Log. It may get called other times for writing or reading R-Files (Bulk imports,
   * AccumuloFileOutputFormat, rfile-info command)
   *
   * @since 2.1
   */
  void init(InitParams initParams) throws CryptoException;

  interface InitParams {
    Map<String,String> getOptions();

    CryptoService.Scope getScope();
  }

  /**
   * Encrypt the OutputStream.
   */
  OutputStream encryptStream(OutputStream outputStream) throws CryptoException;

  /**
   * Get all the parameters required for decryption. WARNING: This byte[] will get written as part
   * of the OutputStream as it is returned (either before or after the encrypted data). Do not
   * return any unencrypted sensitive information.
   *
   * Accumulo requires the first bytes in the byte array to be the class name written in UTF. This
   * can be done like so:
   *
   * <pre>
   * try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
   *     DataOutputStream params = new DataOutputStream(baos)) {
   *   // the name is required to be first
   *   params.writeUTF(MyCryptoService.class.getName());
   *   // write whatever your class needs for decryption
   *   bytes = baos.toByteArray();
   * }
   * return bytes;
   * </pre>
   *
   * Other information about the encryption taking place such as version or a wrapped File
   * Encryption Key would be written after the class name. This information will get written at the
   * beginning of an encrypted Write Ahead Log (WAL) or at the end of an encrypted R-File. Later, it
   * will be read from the file and passed to the {@link FileDecrypter} for decryption.
   */
  byte[] getDecryptionParameters();
}

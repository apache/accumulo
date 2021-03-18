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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * The default encryption strategy which does nothing.
 */
public class NoCryptoService implements CryptoService {
  public static final String VERSION = "U+1F47B"; // unicode ghost emoji
  public static final CryptoService NONE = new NoCryptoService();
  public static final FileEncrypter NO_ENCRYPT = new NoFileEncrypter();
  public static final FileDecrypter NO_DECRYPT = new NoFileDecrypter();

  public static byte[] decryptParams() {
    return NO_ENCRYPT.getDecryptionParameters();
  }

  public static class NoFileEncrypter implements FileEncrypter {

    @Override
    public void init(InitParams initParams) throws CryptoException {
      // do nothing
    }

    @Override
    public OutputStream encryptStream(OutputStream outputStream) throws CryptoException {
      return outputStream;
    }

    @Override
    public byte[] getDecryptionParameters() {
      return NoCryptoService.VERSION.getBytes(UTF_8);
    }
  }

  @Override
  public FileEncrypter getEncrypter() {
    return new NoFileEncrypter();
  }

  @Override
  public FileDecrypter getDecrypter() {
    return new NoFileDecrypter();
  }

  public static class NoFileDecrypter implements FileDecrypter {

    @Override
    public void init(InitParams initParams) {
      // do nothing
    }

    @Override
    public InputStream decryptStream(InputStream inputStream) throws CryptoException {
      return inputStream;

    }
  }
}

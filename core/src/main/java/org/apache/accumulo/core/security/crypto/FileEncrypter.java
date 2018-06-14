package org.apache.accumulo.core.security.crypto;

import java.io.OutputStream;

public interface FileEncrypter {
  OutputStream encryptStream(OutputStream outputStream) throws CryptoService.CryptoException;

  /**
   * This method is responsible for printing all information required for decrypting to a stream
   *
   * @param outputStream
   *          The stream being written to requiring crypto information
   * @throws CryptoService.CryptoException
   *           if the action fails
   *
   * @since 2.0
   */
  void addParamsToStream(OutputStream outputStream) throws CryptoService.CryptoException;
}

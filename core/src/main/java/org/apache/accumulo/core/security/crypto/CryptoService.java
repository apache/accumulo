package org.apache.accumulo.core.security.crypto;

public interface CryptoService {

  /**
   * Initialize the FileEncrypter for the environment and return
   *
   * @return FileEncrypter
   */
  FileEncrypter encryptFile(CryptoServiceEnvironment environment);

  /**
   * Initialize the FileDecrypter for the environment and return
   *
   * @return FileDecrypter
   */
  FileDecrypter decryptFile(CryptoServiceEnvironment environment);

  /**
   * Runtime Crypto exception
   */
  class CryptoException extends RuntimeException {
    CryptoException() {
      super();
    }

    CryptoException(String message) {
      super(message);
    }

    CryptoException(String message, Throwable cause) {
      super(message, cause);
    }

    CryptoException(Throwable cause) {
      super(cause);
    }
  }
}

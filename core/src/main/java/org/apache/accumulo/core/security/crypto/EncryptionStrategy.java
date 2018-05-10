package org.apache.accumulo.core.security.crypto;

import java.io.InputStream;
import java.io.OutputStream;

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
   *           where the encryption takes places
   * @return true
   *           if initialization was successful
   * @since 2.0
   */
  boolean init(Scope encryptionScope);

  /**
   * Encrypts the OutputStream.
   *
   * @param outputStream
   * @since 2.0
   */
  void encryptStream(OutputStream outputStream);

  /**
   * Decrypts the InputStream.
   *
   * @param inputStream
   * @since 2.0
   */
  void decryptStream(InputStream inputStream);

}

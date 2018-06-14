package org.apache.accumulo.core.security.crypto;

import java.io.InputStream;

public interface FileDecrypter {
  InputStream decryptStream(InputStream inputStream) throws CryptoService.CryptoException;

}

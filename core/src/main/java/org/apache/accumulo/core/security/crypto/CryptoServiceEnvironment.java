package org.apache.accumulo.core.security.crypto;

public interface CryptoServiceEnvironment {
  /**
   * Where in Accumulo the on-disk file encryption takes place.
   */
  enum Scope {
    WAL, RFILE;
  }

  Scope getScope();
}

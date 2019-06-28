package org.apache.accumulo.server.security.handler;

import org.apache.accumulo.server.ServerContext;

/**
 * TODO Move this into SPI once ready.
 */
public interface SecurityModule {

  /**
   * Initialize the security for Accumulo. WARNING: Calling this will drop all users for Accumulo
   * and reset security. This is automatically called when Accumulo is initialized.
   */
  void initialize(String rootUser, byte[] token);

  /**
   * Return the implemented {@link Auth} for this SecurityModule.
   *
   * @return Auth
   */
  Auth auth();

  /**
   * Return the implemented {@link Perm} for this SecurityModule.
   *
   * @return Perm
   */
  Perm perm();

}

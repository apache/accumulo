package org.apache.accumulo.core.spi.security;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;

public interface SecurityModule {

  /**
   * Initialize the security for Accumulo. WARNING: Calling this will drop all users for Accumulo
   * and reset security. This is automatically called when Accumulo is initialized.
   *
   * @param env
   *          ServiceEnvironment server configuration
   */
  void initialize(ServiceEnvironment env);

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

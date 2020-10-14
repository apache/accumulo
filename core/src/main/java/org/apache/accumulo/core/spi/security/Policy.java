package org.apache.accumulo.core.spi.security;

import org.apache.accumulo.core.client.AccumuloSecurityException;

public interface Policy {

  /**
   * Determine if the provided User can perform the specified Action.
   *
   * @param user
   *          the User to check
   * @param action
   *          the Action to check
   * @return
   *      true if the user can perform the action, false otherwise
   */
  default boolean canPerform(String user, Action action){
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Allow the provided User to perform the specified Action.
   *
   * @param user
   *          the User to check
   * @param action
   *          the Action to check
   */
  default void grant(String user, Action action) throws AccumuloSecurityException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Revokes the specified Action from the provided User.
   *
   * @param user
   *          the User to check
   * @param action
   *          the Action to check
   */
  default void revoke(String user, Action action) {
    throw new UnsupportedOperationException("Not implemented");
  }
}

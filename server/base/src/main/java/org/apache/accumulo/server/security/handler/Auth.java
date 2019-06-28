package org.apache.accumulo.server.security.handler;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Pluggable authentication and authorization module returned by {@link SecurityModule#auth()}.
 */
public interface Auth {

  /**
   * Verify the userPrincipal and serialized {@link AuthenticationToken} are valid.
   *
   * @param userPrincipal
   *          the user to authenticate
   * @param token
   *          the {@link AuthenticationToken}
   * @return boolean true if successful or false otherwise
   * @throws AccumuloSecurityException
   *           if a problem occurred during authentication
   */
  boolean authenticate(String userPrincipal, AuthenticationToken token)
      throws AccumuloSecurityException;

  /**
   * Returns Authorizations for the provided userPrincipal. Requires a previous call to
   * {@link #authenticate(String, AuthenticationToken)} otherwise throws IllegalStateException.
   *
   * @param userPrincipal
   *          get authorizations of this user
   * @return Authorizations of the provided user
   */
  Authorizations getAuthorizations(String userPrincipal);

  /**
   * Used to check if a user has valid auths.
   */
  boolean hasAuths(String user, Authorizations authorizations);

  /**
   * Used to change the authorizations for the user
   */
  void changeAuthorizations(String principal, Authorizations authorizations)
      throws AccumuloSecurityException;

  /**
   * Used to change the password for the user
   */
  void changePassword(String principal, AuthenticationToken token) throws AccumuloSecurityException;

  /**
   * Create the provided user
   *
   * @param principal
   *          user to create
   * @param token
   *          AuthenticationToken
   * @throws AccumuloSecurityException
   *           if a problem occurs
   */
  void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException;

  /**
   * Drop the provided user.
   *
   * @param principal
   *          user to drop
   * @throws AccumuloSecurityException
   *           if a problem occurs
   */
  void dropUser(String principal) throws AccumuloSecurityException;

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.security.handler;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.Authorizations;

/**
 * Pluggable authentication and authorization module returned by {@link SecurityModule#auth()}.
 *
 * @since 2.1
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
   * Returns Authorizations (cached) for the provided userPrincipal.
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
   * Create the provided user and init permissions.
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
   * Drop the provided user and clean permissions.
   *
   * @param principal
   *          user to drop
   * @throws AccumuloSecurityException
   *           if a problem occurs
   */
  void dropUser(String principal) throws AccumuloSecurityException;

  /**
   * Get all the users.
   */
  Set<String> listUsers();
}

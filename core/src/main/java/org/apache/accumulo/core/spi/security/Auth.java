/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.spi.security;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.security.SecurityModule;

/**
 * Pluggable authentication and authorization module returned by {@link SecurityModule#auth()}.
 *
 * @since 2.1
 */
public interface Auth {

  /**
   * Verify the user principal and serialized {@link AuthenticationToken} are valid.
   *
   * @param principal
   *          the user to authenticate
   * @param token
   *          the {@link AuthenticationToken}
   * @return boolean true if successful or false otherwise
   * @throws AccumuloSecurityException
   *           if a problem occurred during authentication
   */
  default boolean authenticate(String principal, AuthenticationToken token)
      throws AccumuloSecurityException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns Authorizations (cached) for the provided user principal.
   *
   * @param principal
   *          get authorizations of this user
   * @return Authorizations of the provided user
   */
  default Authorizations getAuths(String principal){
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Sets the authorizations for the user principal.
   *
   * @param principal
   *          the user to set the authorizations
   * @param auths
   *          the authorizations to set
   */
  default void setAuths(String principal, Authorizations auths) throws AccumuloSecurityException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Used to change the password for the user
   */
  default void changePassword(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    throw new UnsupportedOperationException("Not implemented");
  }

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
  default void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Drop the provided user and clean permissions.
   *
   * @param principal
   *          user to drop
   * @throws AccumuloSecurityException
   *           if a problem occurs
   */
  default void dropUser(String principal) throws AccumuloSecurityException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Get all the users.
   */
  default Set<String> listUsers() {
    throw new UnsupportedOperationException("Not implemented");
  }
}

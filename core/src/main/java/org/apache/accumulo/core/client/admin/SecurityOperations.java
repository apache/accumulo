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
package org.apache.accumulo.core.client.admin;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

/**
 * Provides a class for managing users and permissions
 */
public interface SecurityOperations {

  /**
   * Create a user
   *
   * @param user
   *          the name of the user to create
   * @param password
   *          the plaintext password for the user
   * @param authorizations
   *          the authorizations that the user has for scanning
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to create a user
   * @deprecated since 1.5.0; use {@link #createLocalUser(String, PasswordToken)} or the user management functions of your configured authenticator instead.
   */
  @Deprecated
  void createUser(String user, byte[] password, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException;

  /**
   * Create a user
   *
   * @param principal
   *          the name of the user to create
   * @param password
   *          the plaintext password for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to create a user
   * @since 1.5.0
   */
  void createLocalUser(String principal, PasswordToken password) throws AccumuloException, AccumuloSecurityException;

  /**
   * Delete a user
   *
   * @param user
   *          the user name to delete
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to delete a user
   * @deprecated since 1.5.0; use {@link #dropUser(String)} or the user management functions of your configured authenticator instead.
   */
  @Deprecated
  void dropUser(String user) throws AccumuloException, AccumuloSecurityException;

  /**
   * Delete a user
   *
   * @param principal
   *          the user name to delete
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to delete a user
   * @since 1.5.0
   */
  void dropLocalUser(String principal) throws AccumuloException, AccumuloSecurityException;

  /**
   * Verify a username/password combination is valid
   *
   * @param user
   *          the name of the user to authenticate
   * @param password
   *          the plaintext password for the user
   * @return true if the user asking is allowed to know and the specified user/password is valid, false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to ask
   * @deprecated since 1.5.0; use {@link #authenticateUser(String, AuthenticationToken)} instead.
   */
  @Deprecated
  boolean authenticateUser(String user, byte[] password) throws AccumuloException, AccumuloSecurityException;

  /**
   * Verify a username/password combination is valid
   *
   * @param principal
   *          the name of the user to authenticate
   * @param token
   *          the SecurityToken for the user
   * @return true if the user asking is allowed to know and the specified principal/token is valid, false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to ask
   * @since 1.5.0
   */
  boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloException, AccumuloSecurityException;

  /**
   * Set the user's password
   *
   * @param user
   *          the name of the user to modify
   * @param password
   *          the plaintext password for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to modify a user
   * @deprecated since 1.5.0; use {@link #changeLocalUserPassword(String, PasswordToken)} or the user management functions of your configured authenticator
   *             instead.
   */
  @Deprecated
  void changeUserPassword(String user, byte[] password) throws AccumuloException, AccumuloSecurityException;

  /**
   * Set the user's password
   *
   * @param principal
   *          the name of the user to modify
   * @param token
   *          the plaintext password for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to modify a user
   * @since 1.5.0
   */
  void changeLocalUserPassword(String principal, PasswordToken token) throws AccumuloException, AccumuloSecurityException;

  /**
   * Set the user's record-level authorizations
   *
   * @param principal
   *          the name of the user to modify
   * @param authorizations
   *          the authorizations that the user has for scanning
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to modify a user
   */
  void changeUserAuthorizations(String principal, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException;

  /**
   * Retrieves the user's authorizations for scanning
   *
   * @param principal
   *          the name of the user to query
   * @return the set of authorizations the user has available for scanning
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query a user
   */
  Authorizations getUserAuthorizations(String principal) throws AccumuloException, AccumuloSecurityException;

  /**
   * Verify the user has a particular system permission
   *
   * @param principal
   *          the name of the user to query
   * @param perm
   *          the system permission to check for
   * @return true if user has that permission; false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query a user
   */
  boolean hasSystemPermission(String principal, SystemPermission perm) throws AccumuloException, AccumuloSecurityException;

  /**
   * Verify the user has a particular table permission
   *
   * @param principal
   *          the name of the user to query
   * @param table
   *          the name of the table to query about
   * @param perm
   *          the table permission to check for
   * @return true if user has that permission; false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query a user
   */
  boolean hasTablePermission(String principal, String table, TablePermission perm) throws AccumuloException, AccumuloSecurityException;

  /**
   * Verify the user has a particular namespace permission
   *
   * @param principal
   *          the name of the user to query
   * @param namespace
   *          the name of the namespace to query about
   * @param perm
   *          the namespace permission to check for
   * @return true if user has that permission; false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query a user
   */
  boolean hasNamespacePermission(String principal, String namespace, NamespacePermission perm) throws AccumuloException, AccumuloSecurityException;

  /**
   * Grant a user a system permission
   *
   * @param principal
   *          the name of the user to modify
   * @param permission
   *          the system permission to grant to the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to grant a user permissions
   */
  void grantSystemPermission(String principal, SystemPermission permission) throws AccumuloException, AccumuloSecurityException;

  /**
   * Grant a user a specific permission for a specific table
   *
   * @param principal
   *          the name of the user to modify
   * @param table
   *          the name of the table to modify for the user
   * @param permission
   *          the table permission to grant to the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to grant a user permissions
   */
  void grantTablePermission(String principal, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException;

  /**
   * Grant a user a specific permission for a specific namespace
   *
   * @param principal
   *          the name of the user to modify
   * @param namespace
   *          the name of the namespace to modify for the user
   * @param permission
   *          the namespace permission to grant to the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to grant a user permissions
   */
  void grantNamespacePermission(String principal, String namespace, NamespacePermission permission) throws AccumuloException, AccumuloSecurityException;

  /**
   * Revoke a system permission from a user
   *
   * @param principal
   *          the name of the user to modify
   * @param permission
   *          the system permission to revoke for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to revoke a user's permissions
   */
  void revokeSystemPermission(String principal, SystemPermission permission) throws AccumuloException, AccumuloSecurityException;

  /**
   * Revoke a table permission for a specific user on a specific table
   *
   * @param principal
   *          the name of the user to modify
   * @param table
   *          the name of the table to modify for the user
   * @param permission
   *          the table permission to revoke for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to revoke a user's permissions
   */
  void revokeTablePermission(String principal, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException;

  /**
   * Revoke a namespace permission for a specific user on a specific namespace
   *
   * @param principal
   *          the name of the user to modify
   * @param namespace
   *          the name of the namespace to modify for the user
   * @param permission
   *          the namespace permission to revoke for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to revoke a user's permissions
   */
  void revokeNamespacePermission(String principal, String namespace, NamespacePermission permission) throws AccumuloException, AccumuloSecurityException;

  /**
   * Return a list of users in accumulo
   *
   * @return a set of user names
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query users
   * @deprecated since 1.5.0; use {@link #listLocalUsers()} or the user management functions of your configured authenticator instead.
   */
  @Deprecated
  Set<String> listUsers() throws AccumuloException, AccumuloSecurityException;

  /**
   * Return a list of users in accumulo
   *
   * @return a set of user names
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query users
   * @since 1.5.0
   */
  Set<String> listLocalUsers() throws AccumuloException, AccumuloSecurityException;

}

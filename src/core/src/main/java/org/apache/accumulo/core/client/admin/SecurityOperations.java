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
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

/**
 * Provides a class for managing users and permissions
 * 
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
   */
  public void createUser(String user, byte[] password, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Delete a user
   * 
   * @param user
   *          the user name to delete
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to delete a user
   */
  public void dropUser(String user) throws AccumuloException, AccumuloSecurityException;
  
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
   */
  public boolean authenticateUser(String user, byte[] password) throws AccumuloException, AccumuloSecurityException;
  
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
   */
  public void changeUserPassword(String user, byte[] password) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Set the user's record-level authorizations
   * 
   * @param user
   *          the name of the user to modify
   * @param authorizations
   *          the authorizations that the user has for scanning
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to modify a user
   */
  public void changeUserAuthorizations(String user, Authorizations authorizations) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Retrieves the user's authorizations for scanning
   * 
   * @param user
   *          the name of the user to query
   * @return the set of authorizations the user has available for scanning
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query a user
   */
  public Authorizations getUserAuthorizations(String user) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Verify the user has a particular system permission
   * 
   * @param user
   *          the name of the user to query
   * @param perm
   *          the system permission to check for
   * @return true if user has that permission; false otherwise
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query a user
   */
  public boolean hasSystemPermission(String user, SystemPermission perm) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Verify the user has a particular table permission
   * 
   * @param user
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
  public boolean hasTablePermission(String user, String table, TablePermission perm) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Grant a user a system permission
   * 
   * @param user
   *          the name of the user to modify
   * @param permission
   *          the system permission to grant to the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to grant a user permissions
   */
  public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Grant a user a specific permission for a specific table
   * 
   * @param user
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
  public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Revoke a system permission from a user
   * 
   * @param user
   *          the name of the user to modify
   * @param permission
   *          the system permission to revoke for the user
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to revoke a user's permissions
   */
  public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Revoke a table permission for a specific user on a specific table
   * 
   * @param user
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
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloException, AccumuloSecurityException;
  
  /**
   * Return a list of users in accumulo
   * 
   * @return a set of user names
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission to query users
   */
  public Set<String> listUsers() throws AccumuloException, AccumuloSecurityException;
  
}

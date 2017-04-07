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

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;

/**
 * This interface is used for the system which will be used for getting a users permissions. If the implementation does not support configuration through
 * Accumulo, it should throw an AccumuloSecurityException with the error code UNSUPPORTED_OPERATION
 */
public interface PermissionHandler {

  /**
   * Sets up the permission handler for a new instance of Accumulo
   */
  void initialize(String instanceId, boolean initialize);

  /**
   * Used to validate that the Authorizor, Authenticator, and permission handler can coexist
   */
  boolean validSecurityHandlers(Authenticator authent, Authorizor author);

  /**
   * Used to initialize security for the root user
   */
  void initializeSecurity(TCredentials credentials, String rootuser) throws AccumuloSecurityException, ThriftSecurityException;

  /**
   * Used to get the system permission for the user
   */
  boolean hasSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;

  /**
   * Used to get the system permission for the user, with caching due to high frequency operation. NOTE: At this time, this method is unused but is included
   * just in case we need it in the future.
   */
  boolean hasCachedSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;

  /**
   * Used to get the table permission of a user for a table
   */
  boolean hasTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;

  /**
   * Used to get the table permission of a user for a table, with caching. This method is for high frequency operations
   */
  boolean hasCachedTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;

  /**
   * Used to get the namespace permission of a user for a namespace
   */
  boolean hasNamespacePermission(String user, String namespace, NamespacePermission permission) throws AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Used to get the namespace permission of a user for a namespace, with caching. This method is for high frequency operations
   */
  boolean hasCachedNamespacePermission(String user, String namespace, NamespacePermission permission) throws AccumuloSecurityException,
      NamespaceNotFoundException;

  /**
   * Gives the user the given system permission
   */
  void grantSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;

  /**
   * Denies the user the given system permission
   */
  void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;

  /**
   * Gives the user the given table permission
   */
  void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;

  /**
   * Denies the user the given table permission.
   */
  void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;

  /**
   * Gives the user the given namespace permission
   */
  void grantNamespacePermission(String user, String namespace, NamespacePermission permission) throws AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Denies the user the given namespace permission.
   */
  void revokeNamespacePermission(String user, String namespace, NamespacePermission permission) throws AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Cleans up the permissions for a table. Used when a table gets deleted.
   */
  void cleanTablePermissions(String table) throws AccumuloSecurityException, TableNotFoundException;

  /**
   * Cleans up the permissions for a namespace. Used when a namespace gets deleted.
   */
  void cleanNamespacePermissions(String namespace) throws AccumuloSecurityException, NamespaceNotFoundException;

  /**
   * Initializes a new user
   */
  void initUser(String user) throws AccumuloSecurityException;

  /**
   * Initializes a new user
   */
  void initTable(String table) throws AccumuloSecurityException;

  /**
   * Deletes a user
   */
  void cleanUser(String user) throws AccumuloSecurityException;
}

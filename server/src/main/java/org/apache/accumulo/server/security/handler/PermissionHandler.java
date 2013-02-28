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
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
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
  public void initialize(String instanceId, boolean initialize);
  
  /**
   * Used to validate that the Authorizor, Authenticator, and permission handler can coexist
   */
  public boolean validSecurityHandlers(Authenticator authent, Authorizor author);
  
  /**
   * Used to initialize security for the root user
   */
  public void initializeSecurity(TCredentials credentials, String rootuser) throws AccumuloSecurityException, ThriftSecurityException;
  
  /**
   * Used to get the system permission for the user
   */
  public boolean hasSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;
  
  /**
   * Used to get the system permission for the user, with caching due to high frequency operation. NOTE: At this time, this method is unused but is included
   * just in case we need it in the future.
   */
  public boolean hasCachedSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;
  
  /**
   * Used to get the table permission of a user for a table
   */
  public boolean hasTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;
  
  /**
   * Used to get the table permission of a user for a table, with caching. This method is for high frequency operations
   */
  public boolean hasCachedTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;
  
  /**
   * Gives the user the given system permission
   */
  public void grantSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;
  
  /**
   * Denies the user the given system permission
   */
  public void revokeSystemPermission(String user, SystemPermission permission) throws AccumuloSecurityException;
  
  /**
   * Gives the user the given table permission
   */
  public void grantTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;
  
  /**
   * Denies the user the given table permission.
   */
  public void revokeTablePermission(String user, String table, TablePermission permission) throws AccumuloSecurityException, TableNotFoundException;
  
  /**
   * Cleans up the permissions for a table. Used when a table gets deleted.
   */
  public void cleanTablePermissions(String table) throws AccumuloSecurityException, TableNotFoundException;
  
  /**
   * Initializes a new user
   */
  public void initUser(String user) throws AccumuloSecurityException;
  
  /**
   * Initializes a new user
   */
  public void initTable(String table) throws AccumuloSecurityException;
  
  /**
   * Deletes a user
   */
  public void cleanUser(String user) throws AccumuloSecurityException;
}

/**
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
package org.apache.accumulo.server.security;

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;

/**
 * Utility class for performing various security operations with the appropriate checks
 */
public interface SecurityOperation {
  public void initializeSecurity(AuthInfo credentials, String rootuser, byte[] rootpass) throws AccumuloSecurityException, ThriftSecurityException;
  public String getRootUsername();
  
  /**
   * @param credentials
   * @param user
   * @param password
   * @return
   * @throws ThriftSecurityException
   */
  public boolean authenticateUser(AuthInfo credentials, String user, ByteBuffer password) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @return The given user's authorizations
   * @throws ThriftSecurityException
   */
  public Authorizations getUserAuthorizations(AuthInfo credentials, String user) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @return
   * @throws ThriftSecurityException
   */
  public Authorizations getUserAuthorizations(AuthInfo credentials) throws ThriftSecurityException;
    
  /**
   * @param credentials
   * @param string
   * @return
   * @throws ThriftSecurityException
   * @throws TableNotFoundException
   */
  public boolean canScan(AuthInfo credentials, String table) throws ThriftSecurityException;

  /**
   * @param credentials
   * @param string
   * @return
   * @throws ThriftSecurityException
   * @throws TableNotFoundException
   */
  public boolean canWrite(AuthInfo credentials, String table) throws ThriftSecurityException;

  /**
   * @param credentials
   * @param string
   * @return
   * @throws ThriftSecurityException
   * @throws TableNotFoundException
   */
  public boolean canSplitTablet(AuthInfo credentials, String table) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @return
   * @throws ThriftSecurityException
   * 
   *           This is the check to perform any system action. This includes tserver's loading of a tablet, shutting the system down, or altering system
   *           properties.
   */
  public boolean canPerformSystemActions(AuthInfo credentials) throws ThriftSecurityException;

  /**
   * @param c
   * @param tableId
   * @throws ThriftSecurityException
   * @throws ThriftTableOperationException
   */
  public boolean canFlush(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param c
   * @param tableId
   * @throws ThriftSecurityException
   * @throws ThriftTableOperationException
   */
  public boolean canAlterTable(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param c
   * @throws ThriftSecurityException
   */
  public boolean canCreateTable(AuthInfo c) throws ThriftSecurityException;
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canRenameTable(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param c
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canCloneTable(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canDeleteTable(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canOnlineOfflineTable(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canMerge(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canDeleteRange(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canBulkImport(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canCompact(AuthInfo c, String tableId) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canChangeAuthorizations(AuthInfo c, String user) throws ThriftSecurityException;

  /**
   * @param credentials
   * @param user
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canChangePassword(AuthInfo c, String user) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canCreateUser(AuthInfo c, String user) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canDropUser(AuthInfo c, String user) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param sysPerm
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canGrantSystem(AuthInfo c, String user, SystemPermission sysPerm) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param table
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canGrantTable(AuthInfo c, String user, String table) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param sysPerm
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canRevokeSystem(AuthInfo c, String user, SystemPermission sysPerm) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param table
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canRevokeTable(AuthInfo c, String user, String table) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param authorizations
   * @throws ThriftSecurityException
   */
  public void changeAuthorizations(AuthInfo credentials, String user, Authorizations authorizations) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param bytes
   * @throws ThriftSecurityException
   */
  public void changePassword(AuthInfo credentials, String user, byte[] pass) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param bytes
   * @param authorizations
   * @throws ThriftSecurityException
   */
  public void createUser(AuthInfo credentials, String user, byte[] pass, Authorizations authorizations) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @throws ThriftSecurityException
   */
  public void dropUser(AuthInfo credentials, String user) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param permissionById
   * @throws ThriftSecurityException
   */
  public void grantSystemPermission(AuthInfo credentials, String user, SystemPermission permissionById) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param tableId
   * @param permissionById
   * @throws ThriftSecurityException
   */
  public void grantTablePermission(AuthInfo c, String user, String tableId, TablePermission permissionById) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param permissionById
   * @throws ThriftSecurityException
   */
  public void revokeSystemPermission(AuthInfo credentials, String user, SystemPermission permissionById) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param tableId
   * @param permissionById
   * @throws ThriftSecurityException
   */
  public void revokeTablePermission(AuthInfo c, String user, String tableId, TablePermission permissionById) throws ThriftSecurityException;

  /**
   * @param credentials
   * @param user
   * @param permissionById
   * @return
   * @throws ThriftSecurityException
   */
  public boolean hasSystemPermission(AuthInfo credentials, String user, SystemPermission permissionById) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @param user
   * @param tableId
   * @param permissionById
   * @return
   * @throws ThriftSecurityException
   */
  public boolean hasTablePermission(AuthInfo credentials, String user, String tableId, TablePermission permissionById) throws ThriftSecurityException;
  
  /**
   * @param credentials
   * @return
   * @throws ThriftSecurityException
   */
  public Set<String> listUsers(AuthInfo credentials) throws ThriftSecurityException;
  
  /**
   * @param systemCredentials
   * @param tableId
   * @throws ThriftSecurityException
   */
  public void deleteTable(AuthInfo credentials, String tableId) throws ThriftSecurityException;
  
  public void clearCache(String user, boolean password, boolean auths, boolean system, Set<String> tables) throws ThriftSecurityException;
  
  public void clearCache(String table) throws ThriftSecurityException;
  
  public boolean cachesToClear() throws ThriftSecurityException;
}

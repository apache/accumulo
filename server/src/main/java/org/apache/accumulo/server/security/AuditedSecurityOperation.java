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
import org.apache.accumulo.core.security.AuditLevel;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.log4j.Logger;

/**
 * 
 */
public class AuditedSecurityOperation implements SecurityOperation {
  public static final Logger log = Logger.getLogger(AuditedSecurityOperation.class);
  private SecurityOperation impl;
  
  public AuditedSecurityOperation(SecurityOperation impl) {
    this.impl = impl;
  }
  
  private void audit(AuthInfo credentials, ThriftSecurityException ex, String template, Object... args) {
    log.log(AuditLevel.AUDIT, "Error: authenticated operation failed: " + credentials.user + ": " + String.format(template, args));
  }
  
  private void audit(AuthInfo credentials, String template, Object... args) {
    log.log(AuditLevel.AUDIT, "Using credentials " + credentials.user + ": " + String.format(template, args));
  }
  
  public synchronized String getRootUsername() {
    return impl.getRootUsername();
  }
  
  /**
   * @param credentials
   * @param user
   * @param password
   * @return
   * @throws ThriftSecurityException
   */
  public boolean authenticateUser(AuthInfo credentials, String user, ByteBuffer password) throws ThriftSecurityException {
    try {
      boolean result = impl.authenticateUser(credentials, user, password);
      audit(credentials, result ? "authenticated" : "failed authentication");
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "authenticateUser");
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @return The given user's authorizations
   * @throws ThriftSecurityException
   */
  public Authorizations getUserAuthorizations(AuthInfo credentials, String user) throws ThriftSecurityException {
    try {
      Authorizations result = impl.getUserAuthorizations(credentials, user);
      audit(credentials, "got authorizations for %s", user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "getting authorizations for %s", user);
      throw ex;
    }

  }
  
  /**
   * @param credentials
   * @return
   * @throws ThriftSecurityException
   */
  public Authorizations getUserAuthorizations(AuthInfo credentials) throws ThriftSecurityException {
    return getUserAuthorizations(credentials, credentials.user);
  }
    
  /**
   * @param credentials
   * @param string
   * @return
   * @throws ThriftSecurityException
   * @throws TableNotFoundException
   */
  public boolean canScan(AuthInfo credentials, String table) throws ThriftSecurityException {
    return impl.canScan(credentials, table);
  }
  
  /**
   * @param credentials
   * @param string
   * @return
   * @throws ThriftSecurityException
   * @throws TableNotFoundException
   */
  public boolean canWrite(AuthInfo credentials, String table) throws ThriftSecurityException {
    return impl.canWrite(credentials, table);
  }
  
  /**
   * @param credentials
   * @param string
   * @return
   * @throws ThriftSecurityException
   * @throws TableNotFoundException
   */
  public boolean canSplitTablet(AuthInfo credentials, String table) throws ThriftSecurityException {
    return impl.canSplitTablet(credentials, table);
  }
  
  /**
   * @param credentials
   * @return
   * @throws ThriftSecurityException
   * 
   *           This is the check to perform any system action. This includes tserver's loading of a tablet, shutting the system down, or altering system
   *           properties.
   */
  public boolean canPerformSystemActions(AuthInfo credentials) throws ThriftSecurityException {
    return impl.canPerformSystemActions(credentials);
  }
  
  /**
   * @param c
   * @param tableId
   * @throws ThriftSecurityException
   * @throws ThriftTableOperationException
   */
  public boolean canFlush(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canFlush(c, tableId);
  }
  
  /**
   * @param c
   * @param tableId
   * @throws ThriftSecurityException
   * @throws ThriftTableOperationException
   */
  public boolean canAlterTable(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canAlterTable(c, tableId);
  }
  
  /**
   * @param c
   * @throws ThriftSecurityException
   */
  public boolean canCreateTable(AuthInfo c) throws ThriftSecurityException {
    return impl.canCreateTable(c);
  }
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canRenameTable(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canRenameTable(c, tableId);
  }
  
  /**
   * @param c
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canCloneTable(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canCloneTable(c, tableId);
  }
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canDeleteTable(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canDeleteTable(c, tableId);
  }
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canOnlineOfflineTable(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canOnlineOfflineTable(c, tableId);
  }
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canMerge(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canMerge(c, tableId);
  }
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canDeleteRange(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canDeleteRange(c, tableId);
  }
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canBulkImport(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canBulkImport(c, tableId);
  }
  
  /**
   * @param c
   * @param tableId
   * @return
   * @throws TableNotFoundException
   * @throws ThriftSecurityException
   */
  public boolean canCompact(AuthInfo c, String tableId) throws ThriftSecurityException {
    return impl.canCompact(c, tableId);
  }
  
  /**
   * @param credentials
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canChangeAuthorizations(AuthInfo c, String user) throws ThriftSecurityException {
    return impl.canChangeAuthorizations(c, user);
  }
  
  /**
   * @param credentials
   * @param user
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canChangePassword(AuthInfo c, String user) throws ThriftSecurityException {
    return impl.canChangePassword(c, user);
  }
  
  /**
   * @param credentials
   * @param user
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canCreateUser(AuthInfo c, String user) throws ThriftSecurityException {
    return impl.canCreateUser(c, user);
  }
  
  /**
   * @param credentials
   * @param user
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canDropUser(AuthInfo c, String user) throws ThriftSecurityException {
    return impl.canDropUser(c, user);
  }
  
  /**
   * @param credentials
   * @param user
   * @param sysPerm
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canGrantSystem(AuthInfo c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    return impl.canGrantSystem(c, user, sysPerm);
  }
  
  /**
   * @param credentials
   * @param user
   * @param table
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canGrantTable(AuthInfo c, String user, String table) throws ThriftSecurityException {
    return impl.canGrantTable(c, user, table);
  }
  
  /**
   * @param credentials
   * @param user
   * @param sysPerm
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canRevokeSystem(AuthInfo c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    return impl.canRevokeSystem(c, user, sysPerm);
  }
  
  /**
   * @param credentials
   * @param user
   * @param table
   * @return
   * @throws ThriftSecurityException
   */
  public boolean canRevokeTable(AuthInfo c, String user, String table) throws ThriftSecurityException {
    return impl.canRevokeTable(c, user, table);
  }
  
  /**
   * @param credentials
   * @param user
   * @param authorizations
   * @throws ThriftSecurityException
   */
  public void changeAuthorizations(AuthInfo credentials, String user, Authorizations authorizations) throws ThriftSecurityException {
    try {
      impl.changeAuthorizations(credentials, user, authorizations);
      audit(credentials, "changed authorizations for %s to %s", user, authorizations);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "changing authorizations for %s", user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @param bytes
   * @throws ThriftSecurityException
   */
  public void changePassword(AuthInfo credentials, String user, byte[] pass) throws ThriftSecurityException {
    try {
      impl.changePassword(credentials, user, pass);
      audit(credentials, "changed password for %s", user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "changing password for %s", user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @param bytes
   * @param authorizations
   * @throws ThriftSecurityException
   */
  public void createUser(AuthInfo credentials, String user, byte[] pass, Authorizations authorizations) throws ThriftSecurityException {
    try {
      impl.createUser(credentials, user, pass, authorizations);
      audit(credentials, "createUser");
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "createUser %s", user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @throws ThriftSecurityException
   */
  public void dropUser(AuthInfo credentials, String user) throws ThriftSecurityException {
    try {
      impl.dropUser(credentials, user);
      audit(credentials, "dropUser");
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "dropUser %s", user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @param permission
   * @throws ThriftSecurityException
   */
  public void grantSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    try {
      impl.grantSystemPermission(credentials, user, permission);
      audit(credentials, "granted permission %s for %s", permission, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "granting permission %s for %s", permission, user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @param table
   * @param permission
   * @throws ThriftSecurityException
   */
  public void grantTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws ThriftSecurityException {
    try {
      impl.grantTablePermission(credentials, user, table, permission);
      audit(credentials, "granted permission %s on table %s for %s", permission, table, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "granting permission %s on table for %s", permission, table, user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @param permission
   * @throws ThriftSecurityException
   */
  public void revokeSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    try {
      impl.revokeSystemPermission(credentials, user, permission);
      audit(credentials, "revoked permission %s for %s", permission, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "revoking permission %s on %s", permission, user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @param table
   * @param permission
   * @throws ThriftSecurityException
   */
  public void revokeTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws ThriftSecurityException {
    try {
      impl.revokeTablePermission(credentials, user, table, permission);
      audit(credentials, "revoked permission %s on table %s for %s", permission, table, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "revoking permission %s on table for %s", permission, table, user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @param permission
   * @return
   * @throws ThriftSecurityException
   */
  public boolean hasSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    try {
      boolean result = impl.hasSystemPermission(credentials, user, permission);
      audit(credentials, "checked permission %s on %s", permission, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "checking permission %s on %s", permission, user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @param user
   * @param table
   * @param permission
   * @return
   * @throws ThriftSecurityException
   */
  public boolean hasTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws ThriftSecurityException {
    try {
      boolean result = impl.hasTablePermission(credentials, user, table, permission);
      audit(credentials, "checked permission %s on table %s for %s", permission, table, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "checking permission %s on %s", permission, user);
      throw ex;
    }
  }
  
  /**
   * @param credentials
   * @return
   * @throws ThriftSecurityException
   */
  public Set<String> listUsers(AuthInfo credentials) throws ThriftSecurityException {
    try {
      Set<String> result = impl.listUsers(credentials);
      audit(credentials, "listUsers");
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "listUsers");
      throw ex;
    }
  }
  
  /**
   * @param systemCredentials
   * @param table
   * @throws ThriftSecurityException
   */
  public void deleteTable(AuthInfo credentials, String table) throws ThriftSecurityException {
    try {
      impl.deleteTable(credentials, table);
      audit(credentials, "deleted table %s", table);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "deleting table %s", table);
      throw ex;
    }
  }

  @Override
  public void initializeSecurity(AuthInfo credentials, String rootuser, byte[] rootpass) throws AccumuloSecurityException, ThriftSecurityException {
    impl.initializeSecurity(credentials, rootuser, rootpass);
    log.info("Initialized root user with username: " + rootuser + " at the request of user " + credentials.user);
  }
}

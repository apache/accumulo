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
package org.apache.accumulo.server.security;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.security.AuditLevel;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.log4j.Logger;

/**
 *
 */
public class AuditedSecurityOperation extends SecurityOperation {

  public AuditedSecurityOperation(Authorizor author, Authenticator authent, PermissionHandler pm, String instanceId) {
    super(author, authent, pm, instanceId);
  }

  public static final Logger log = Logger.getLogger(AuditedSecurityOperation.class);

  public static synchronized SecurityOperation getInstance() {
    // ACCUMULO-3939 Ensure that an AuditedSecurityOperation instance gets returned.
    String instanceId = HdfsZooInstance.getInstance().getInstanceID();
    return getInstance(instanceId, false);
  }

  public static synchronized SecurityOperation getInstance(String instanceId, boolean initialize) {
    if (instance == null) {
      instance = new AuditedSecurityOperation(getAuthorizor(instanceId, initialize), getAuthenticator(instanceId, initialize), getPermHandler(instanceId,
          initialize), instanceId);
    }
    return instance;
  }

  private void audit(TCredentials credentials, ThriftSecurityException ex, String template, Object... args) {
    log.log(AuditLevel.AUDIT, "Error: authenticated operation failed: " + credentials.getPrincipal() + ": " + String.format(template, args));
  }

  private void audit(TCredentials credentials, String template, Object... args) {
    log.log(AuditLevel.AUDIT, "Using credentials " + credentials.getPrincipal() + ": " + String.format(template, args));
  }

  @Override
  public boolean authenticateUser(TCredentials credentials, TCredentials toAuth) throws ThriftSecurityException {
    try {
      boolean result = super.authenticateUser(credentials, toAuth);
      audit(credentials, result ? "authenticated" : "failed authentication");
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "authenticateUser");
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public Authorizations getUserAuthorizations(TCredentials credentials, String user) throws ThriftSecurityException {
    try {
      Authorizations result = super.getUserAuthorizations(credentials, user);
      audit(credentials, "got authorizations for %s", user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "getting authorizations for %s", user);
      log.debug(ex);
      throw ex;
    }

  }

  @Override
  public Authorizations getUserAuthorizations(TCredentials credentials) throws ThriftSecurityException {
    try {
      return getUserAuthorizations(credentials, credentials.getPrincipal());
    } catch (ThriftSecurityException ex) {
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public void changeAuthorizations(TCredentials credentials, String user, Authorizations authorizations) throws ThriftSecurityException {
    try {
      super.changeAuthorizations(credentials, user, authorizations);
      audit(credentials, "changed authorizations for %s to %s", user, authorizations);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "changing authorizations for %s", user);
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public void changePassword(TCredentials credentials, TCredentials newInfo) throws ThriftSecurityException {
    try {
      super.changePassword(credentials, newInfo);
      audit(credentials, "changed password for %s", newInfo.getPrincipal());
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "changing password for %s", newInfo.getPrincipal());
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public void createUser(TCredentials credentials, TCredentials newUser, Authorizations authorizations) throws ThriftSecurityException {
    try {
      super.createUser(credentials, newUser, authorizations);
      audit(credentials, "createUser");
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "createUser %s", newUser.getPrincipal());
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public void dropUser(TCredentials credentials, String user) throws ThriftSecurityException {
    try {
      super.dropUser(credentials, user);
      audit(credentials, "dropUser");
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "dropUser %s", user);
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public void grantSystemPermission(TCredentials credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    try {
      super.grantSystemPermission(credentials, user, permission);
      audit(credentials, "granted permission %s for %s", permission, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "granting permission %s for %s", permission, user);
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public void grantTablePermission(TCredentials credentials, String user, String table, TablePermission permission) throws ThriftSecurityException {
    try {
      super.grantTablePermission(credentials, user, table, permission);
      audit(credentials, "granted permission %s on table %s for %s", permission, table, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "granting permission %s on table for %s", permission, table, user);
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public void revokeSystemPermission(TCredentials credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    try {
      super.revokeSystemPermission(credentials, user, permission);
      audit(credentials, "revoked permission %s for %s", permission, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "revoking permission %s on %s", permission, user);
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public void revokeTablePermission(TCredentials credentials, String user, String table, TablePermission permission) throws ThriftSecurityException {
    try {
      super.revokeTablePermission(credentials, user, table, permission);
      audit(credentials, "revoked permission %s on table %s for %s", permission, table, user);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "revoking permission %s on table for %s", permission, table, user);
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public boolean hasSystemPermission(TCredentials credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    try {
      boolean result = super.hasSystemPermission(credentials, user, permission);
      if (result)
        audit(credentials, "checked permission %s on %s", permission, user);
      else
        audit(credentials, "checked permission %s on %s denied", permission, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "checking permission %s on %s denied", permission, user);
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public boolean hasTablePermission(TCredentials credentials, String user, String table, TablePermission permission) throws ThriftSecurityException {
    try {
      boolean result = super.hasTablePermission(credentials, user, table, permission);
      audit(credentials, "checked permission %s on table %s for %s", permission, table, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "checking permission %s on %s", permission, user);
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public Set<String> listUsers(TCredentials credentials) throws ThriftSecurityException {
    try {
      Set<String> result = super.listUsers(credentials);
      audit(credentials, "listUsers");
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "listUsers");
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public void deleteTable(TCredentials credentials, String table) throws ThriftSecurityException {
    try {
      super.deleteTable(credentials, table);
      audit(credentials, "deleted table %s", table);
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "deleting table %s", table);
      log.debug(ex);
      throw ex;
    }
  }

  @Override
  public boolean canCreateTable(TCredentials c, String tablename) throws ThriftSecurityException {
    try {
      boolean result = super.canCreateTable(c, tablename);
      if (result)
        audit(c, "create table %s allowed", tablename);
      else
        audit(c, "create table %s denied", tablename);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "create table %s denied", tablename);
      throw ex;
    }
  }

  @Override
  public boolean canRenameTable(TCredentials c, String tableId, String newTableName, String oldTableName) throws ThriftSecurityException {
    try {
      boolean result = super.canRenameTable(c, tableId, newTableName, oldTableName);
      if (result)
        audit(c, "rename table on tableId %s from %s to %s allowed", tableId, oldTableName, newTableName);
      else
        audit(c, "rename table on tableId %s from %s to %s denied", tableId, oldTableName, newTableName);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "rename table on tableId %s from %s to %s denied", tableId, oldTableName, newTableName);
      throw ex;
    }
  }

  @Override
  public boolean canSplitTablet(TCredentials credentials, String table) throws ThriftSecurityException {
    try {
      boolean result = super.canSplitTablet(credentials, table);
      if (result)
        audit(credentials, "split tablet on table %s allowed", table);
      else
        audit(credentials, "split tablet on table %s denied", table);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "split tablet on table %s denied", table);
      throw ex;
    }
  }

  @Override
  public boolean canPerformSystemActions(TCredentials credentials) throws ThriftSecurityException {
    try {
      boolean result = super.canPerformSystemActions(credentials);
      if (result)
        audit(credentials, "system action allowed");
      else
        audit(credentials, "system action denied");
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "system action denied");
      throw ex;
    }
  }

  @Override
  public boolean canFlush(TCredentials c, String tableId) throws ThriftSecurityException {
    try {
      boolean result = super.canFlush(c, tableId);
      if (result)
        audit(c, "flush on tableId %s allowed ", tableId);
      else
        audit(c, "flush on tableId %s denied ", tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "flush on tableId %s denied", tableId);
      throw ex;
    }
  }

  @Override
  public boolean canAlterTable(TCredentials c, String tableId) throws ThriftSecurityException {
    try {
      boolean result = super.canAlterTable(c, tableId);
      if (result)
        audit(c, "alter table on tableId %s allowed", tableId);
      else
        audit(c, "alter table on tableId %s denied", tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "alter table on tableId %s denied", tableId);
      throw ex;
    }
  }

  @Override
  public boolean canCloneTable(TCredentials c, String tableId) throws ThriftSecurityException {
    try {
      boolean result = super.canCloneTable(c, tableId);
      if (result)
        audit(c, "clone table on tableId %s allowed", tableId);
      else
        audit(c, "clone table on tableId %s denied", tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "clone table on tableId %s denied", tableId);
      throw ex;
    }
  }

  @Override
  public boolean canDeleteTable(TCredentials c, String tableId) throws ThriftSecurityException {
    try {
      boolean result = super.canDeleteTable(c, tableId);
      if (result)
        audit(c, "delete table on tableId %s allowed", tableId);
      else
        audit(c, "delete table on tableId %s denied", tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "delete table on tableId %s denied", tableId);
      throw ex;
    }
  }

  @Override
  public boolean canOnlineOfflineTable(TCredentials c, String tableId) throws ThriftSecurityException {
    try {
      boolean result = super.canOnlineOfflineTable(c, tableId);
      if (result)
        audit(c, "offline table on tableId %s allowed", tableId);
      else
        audit(c, "offline table on tableId %s denied", tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "offline table on tableId %s denied", tableId);
      throw ex;
    }
  }

  @Override
  public boolean canMerge(TCredentials c, String tableId) throws ThriftSecurityException {
    try {
      boolean result = super.canMerge(c, tableId);
      if (result)
        audit(c, "merge table on tableId %s allowed", tableId);
      else
        audit(c, "merge table on tableId %s denied", tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "merge table on tableId %s denied", tableId);
      throw ex;
    }
  }

  @Override
  public boolean canDeleteRange(TCredentials c, String tableId) throws ThriftSecurityException {
    try {
      boolean result = super.canDeleteRange(c, tableId);
      if (result)
        audit(c, "delete range on tableId %s allowed", tableId);
      else
        audit(c, "delete range on tableId %s denied", tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "delete range on tableId %s denied", tableId);
      throw ex;
    }
  }

  @Override
  public boolean canBulkImport(TCredentials c, String tableId, String importDir) throws ThriftSecurityException {
    try {
      boolean result = super.canBulkImport(c, tableId, importDir);
      if (result)
        audit(c, "bulk import on tableId %s from directory %s allowed", tableId, importDir);
      else
        audit(c, "bulk import on tableId %s from directory %s denied", tableId, importDir);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "bulk import on tableId %s from directory %s denied", tableId, importDir);
      throw ex;
    }
  }

  @Override
  public boolean canCompact(TCredentials c, String tableId) throws ThriftSecurityException {
    try {
      boolean result = super.canCompact(c, tableId);
      if (result)
        audit(c, "compact on tableId %s allowed", tableId);
      else
        audit(c, "compact on tableId %s denied", tableId);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "compact on tableId %s denied", tableId);
      throw ex;
    }
  }

  @Override
  public boolean canChangeAuthorizations(TCredentials c, String user) throws ThriftSecurityException {
    try {
      boolean result = super.canChangeAuthorizations(c, user);
      if (result)
        audit(c, "change authorizations on user %s allowed", user);
      else
        audit(c, "change authorizations on user %s denied", user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "change authorizations on user %s denied", user);
      throw ex;
    }
  }

  @Override
  public boolean canChangePassword(TCredentials c, String user) throws ThriftSecurityException {
    try {
      boolean result = super.canChangePassword(c, user);
      if (result)
        audit(c, "change password on user %s allowed", user);
      else
        audit(c, "change password on user %s denied", user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "change password on user %s denied", user);
      throw ex;
    }
  }

  @Override
  public boolean canCreateUser(TCredentials c, String user) throws ThriftSecurityException {
    try {
      boolean result = super.canCreateUser(c, user);
      if (result)
        audit(c, "create user on user %s allowed", user);
      else
        audit(c, "create user on user %s denied", user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "create user on user %s denied", user);
      throw ex;
    }
  }

  @Override
  public boolean canDropUser(TCredentials c, String user) throws ThriftSecurityException {
    try {
      boolean result = super.canDropUser(c, user);
      if (result)
        audit(c, "drop user on user %s allowed", user);
      else
        audit(c, "drop user on user %s denied", user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "drop user on user %s denied", user);

      throw ex;
    }
  }

  @Override
  public boolean canGrantSystem(TCredentials c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    try {
      boolean result = super.canGrantSystem(c, user, sysPerm);
      if (result)
        audit(c, "grant system permission %s for user %s allowed", sysPerm, user);
      else
        audit(c, "grant system permission %s for user %s denied", sysPerm, user);
      return result;

    } catch (ThriftSecurityException ex) {
      audit(c, ex, "grant system permission %s for user %s denied", sysPerm, user);

      throw ex;
    }
  }

  @Override
  public boolean canGrantTable(TCredentials c, String user, String table) throws ThriftSecurityException {
    try {
      boolean result = super.canGrantTable(c, user, table);
      if (result)
        audit(c, "grant table on table %s for user %s allowed", table, user);
      else
        audit(c, "grant table on table %s for user %s denied", table, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "grant table on table %s for user %s denied", table, user);
      throw ex;
    }
  }

  @Override
  public boolean canRevokeSystem(TCredentials c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    try {
      boolean result = super.canRevokeSystem(c, user, sysPerm);
      if (result)
        audit(c, "revoke system permission %s for user %s allowed", sysPerm, user);
      else
        audit(c, "revoke system permission %s for user %s denied", sysPerm, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "revoke system permission %s for user %s denied", sysPerm, user);
      throw ex;
    }
  }

  @Override
  public boolean canRevokeTable(TCredentials c, String user, String table) throws ThriftSecurityException {
    try {
      boolean result = super.canRevokeTable(c, user, table);
      if (result)
        audit(c, "revoke table on table %s for user %s allowed", table, user);
      else
        audit(c, "revoke table on table %s for user %s denied", table, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(c, ex, "revoke table on table %s for user %s denied", table, user);
      throw ex;
    }
  }

  @Override
  public boolean canExport(TCredentials credentials, String tableId, String exportDir) throws ThriftSecurityException {
    try {
      boolean result = super.canExport(credentials, tableId, exportDir);
      if (result)
        audit(credentials, "export table on tableId %s to directory %s allowed", tableId, exportDir);
      else
        audit(credentials, "export table on tableId %s to directory %s denied", tableId, exportDir);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "export table on tableId %s to directory %s denied", tableId, exportDir);
      throw ex;
    }
  }

  @Override
  public boolean canImport(TCredentials credentials, String tableName, String importDir) throws ThriftSecurityException {
    try {
      boolean result = super.canImport(credentials, tableName, importDir);
      if (result)
        audit(credentials, "import table %s from directory %s allowed", tableName, importDir);
      else
        audit(credentials, "import table %s from directory %s denied", tableName, importDir);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "import table %s from directory %s denied", tableName, importDir);
      throw ex;
    }
  }

  @Override
  public void initializeSecurity(TCredentials credentials, String principal, byte[] token) throws AccumuloSecurityException, ThriftSecurityException {
    super.initializeSecurity(credentials, principal, token);
    log.info("Initialized root user with username: " + principal + " at the request of user " + credentials.getPrincipal());
  }
}

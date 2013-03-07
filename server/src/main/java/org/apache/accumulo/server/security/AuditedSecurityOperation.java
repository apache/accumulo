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
      audit(credentials, "checked permission %s on %s", permission, user);
      return result;
    } catch (ThriftSecurityException ex) {
      audit(credentials, ex, "checking permission %s on %s", permission, user);
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
  public void initializeSecurity(TCredentials credentials, String principal, byte[] token) throws AccumuloSecurityException, ThriftSecurityException {
    super.initializeSecurity(credentials, principal, token);
    log.info("Initialized root user with username: " + principal + " at the request of user " + credentials.getPrincipal());
  }
}

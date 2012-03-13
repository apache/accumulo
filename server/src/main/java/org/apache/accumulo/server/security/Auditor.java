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

import java.nio.ByteBuffer;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.AuditLevel;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.log4j.Logger;

/**
 * Wrap an authenticator with calls to log4j on success/error.
 */
public class Auditor implements Authenticator {
  
  public static final Logger log = Logger.getLogger(Auditor.class);
  
  Authenticator impl;
  
  public Auditor(Authenticator impl) {
    this.impl = impl;
  }
  
  @Override
  public void initializeSecurity(AuthInfo credentials, String rootuser, byte[] rootpass) throws AccumuloSecurityException {
    try {
      impl.initializeSecurity(credentials, rootuser, rootpass);
      audit(credentials, "initialized security with root user %s", rootuser);
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "initializeSecurity");
    }
  }
  
  private void audit(AuthInfo credentials, AccumuloSecurityException ex, String template, Object... args) {
    log.log(AuditLevel.AUDIT, "Error: authentication operation failed: " + credentials.user + ": " + String.format(template, args));
  }
  
  private void audit(AuthInfo credentials, String template, Object... args) {
    log.log(AuditLevel.AUDIT, "Using credentials " + credentials.user + ": " + String.format(template, args));
  }
  
  @Override
  public String getRootUsername() {
    return impl.getRootUsername();
  }
  
  @Override
  public boolean authenticateUser(AuthInfo credentials, String user, ByteBuffer pass) throws AccumuloSecurityException {
    try {
      boolean result = impl.authenticateUser(credentials, user, pass);
      audit(credentials, result ? "authenticated" : "failed authentication");
      return result;
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "authenticateUser");
      throw ex;
    }
  }
  
  @Override
  public Set<String> listUsers(AuthInfo credentials) throws AccumuloSecurityException {
    try {
      Set<String> result = impl.listUsers(credentials);
      audit(credentials, "listUsers");
      return result;
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "listUsers");
      throw ex;
    }
  }
  
  @Override
  public void createUser(AuthInfo credentials, String user, byte[] pass, Authorizations authorizations) throws AccumuloSecurityException {
    try {
      impl.createUser(credentials, user, pass, authorizations);
      audit(credentials, "createUser");
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "createUser %s", user);
      throw ex;
    }
  }
  
  @Override
  public void dropUser(AuthInfo credentials, String user) throws AccumuloSecurityException {
    try {
      impl.dropUser(credentials, user);
      audit(credentials, "dropUser");
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "dropUser %s", user);
      throw ex;
    }
  }
  
  @Override
  public void changePassword(AuthInfo credentials, String user, byte[] pass) throws AccumuloSecurityException {
    try {
      impl.changePassword(credentials, user, pass);
      audit(credentials, "changed password for %s", user);
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "changing password for %s", user);
      throw ex;
    }
  }
  
  @Override
  public void changeAuthorizations(AuthInfo credentials, String user, Authorizations authorizations) throws AccumuloSecurityException {
    try {
      impl.changeAuthorizations(credentials, user, authorizations);
      audit(credentials, "changed authorizations for %s to %s", user, authorizations);
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "changing authorizations for %s", user);
      throw ex;
    }
  }
  
  @Override
  public Authorizations getUserAuthorizations(AuthInfo credentials, String user) throws AccumuloSecurityException {
    try {
      Authorizations result = impl.getUserAuthorizations(credentials, user);
      audit(credentials, "got authorizations for %s", user);
      return result;
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "getting authorizations for %s", user);
      throw ex;
    }
  }
  
  @Override
  public boolean hasSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws AccumuloSecurityException {
    try {
      boolean result = impl.hasSystemPermission(credentials, user, permission);
      audit(credentials, "checked permission %s on %s", permission, user);
      return result;
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "checking permission %s on %s", permission, user);
      throw ex;
    }
  }
  
  @Override
  public boolean hasTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws AccumuloSecurityException {
    try {
      boolean result = impl.hasTablePermission(credentials, user, table, permission);
      audit(credentials, "checked permission %s on table %s for %s", permission, table, user);
      return result;
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "checking permission %s on %s", permission, user);
      throw ex;
    }
  }
  
  @Override
  public void grantSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws AccumuloSecurityException {
    try {
      impl.grantSystemPermission(credentials, user, permission);
      audit(credentials, "granted permission %s for %s", permission, user);
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "granting permission %s for %s", permission, user);
      throw ex;
    }
  }
  
  @Override
  public void revokeSystemPermission(AuthInfo credentials, String user, SystemPermission permission) throws AccumuloSecurityException {
    try {
      impl.revokeSystemPermission(credentials, user, permission);
      audit(credentials, "revoked permission %s for %s", permission, user);
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "revoking permission %s on %s", permission, user);
      throw ex;
    }
  }
  
  @Override
  public void grantTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws AccumuloSecurityException {
    try {
      impl.grantTablePermission(credentials, user, table, permission);
      audit(credentials, "granted permission %s on table %s for %s", permission, table, user);
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "granting permission %s on table for %s", permission, table, user);
      throw ex;
    }
  }
  
  @Override
  public void revokeTablePermission(AuthInfo credentials, String user, String table, TablePermission permission) throws AccumuloSecurityException {
    try {
      impl.revokeTablePermission(credentials, user, table, permission);
      audit(credentials, "revoked permission %s on table %s for %s", permission, table, user);
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "revoking permission %s on table for %s", permission, table, user);
      throw ex;
    }
  }
  
  @Override
  public void deleteTable(AuthInfo credentials, String table) throws AccumuloSecurityException {
    try {
      impl.deleteTable(credentials, table);
      audit(credentials, "deleted table %s", table);
    } catch (AccumuloSecurityException ex) {
      audit(credentials, ex, "deleting table %s", table);
      throw ex;
    }
  }
  
  @Override
  public void clearCache(String user) {
    impl.clearCache(user);
  }
  
  @Override
  public void clearCache(String user, String tableId) {
    impl.clearCache(user, tableId);
  }
}

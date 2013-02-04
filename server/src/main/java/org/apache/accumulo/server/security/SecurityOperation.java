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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperationsImpl;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.Credentials;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.accumulo.server.security.handler.ZKAuthenticator;
import org.apache.accumulo.server.security.handler.ZKAuthorizor;
import org.apache.accumulo.server.security.handler.ZKPermHandler;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.log4j.Logger;

/**
 * Utility class for performing various security operations with the appropriate checks
 */
public class SecurityOperation {
  private static final Logger log = Logger.getLogger(SecurityOperationsImpl.class);
  
  protected static Authorizor authorizor;
  protected static Authenticator authenticator;
  protected static PermissionHandler permHandle;
  private static String rootUserName = null;
  private final ZooCache zooCache;
  private final String ZKUserPath;
  
  protected static SecurityOperation instance;
  
  public static synchronized SecurityOperation getInstance() {
    String instanceId = HdfsZooInstance.getInstance().getInstanceID();
    return getInstance(instanceId, false);
  }
  
  public static synchronized SecurityOperation getInstance(String instanceId, boolean initialize) {
    if (instance == null) {
      instance = new SecurityOperation(getAuthorizor(instanceId, initialize), getAuthenticator(instanceId, initialize), getPermHandler(instanceId, initialize),
          instanceId);
    }
    return instance;
  }
  
  @SuppressWarnings("deprecation")
  protected static Authorizor getAuthorizor(String instanceId, boolean initialize) {
    Authorizor toRet = Master.createInstanceFromPropertyName(AccumuloConfiguration.getSiteConfiguration(), Property.INSTANCE_SECURITY_AUTHORIZOR,
        Authorizor.class, ZKAuthorizor.getInstance());
    toRet.initialize(instanceId, initialize);
    return toRet;
  }
  
  @SuppressWarnings("deprecation")
  protected static Authenticator getAuthenticator(String instanceId, boolean initialize) {
    Authenticator toRet = Master.createInstanceFromPropertyName(AccumuloConfiguration.getSiteConfiguration(), Property.INSTANCE_SECURITY_AUTHENTICATOR,
        Authenticator.class, ZKAuthenticator.getInstance());
    toRet.initialize(instanceId, initialize);
    return toRet;
  }
  
  @SuppressWarnings("deprecation")
  protected static PermissionHandler getPermHandler(String instanceId, boolean initialize) {
    PermissionHandler toRet = Master.createInstanceFromPropertyName(AccumuloConfiguration.getSiteConfiguration(),
        Property.INSTANCE_SECURITY_PERMISSION_HANDLER, PermissionHandler.class, ZKPermHandler.getInstance());
    toRet.initialize(instanceId, initialize);
    return toRet;
  }
  
  /**
   * 
   * @Deprecated not for client use
   */
  public SecurityOperation(String instanceId) {
    ZKUserPath = Constants.ZROOT + "/" + instanceId + "/users";
    zooCache = new ZooCache();
  }
  
  public SecurityOperation(Authorizor author, Authenticator authent, PermissionHandler pm, String instanceId) {
    this(instanceId);
    authorizor = author;
    authenticator = authent;
    permHandle = pm;
    
    if (!authorizor.validSecurityHandlers(authenticator, pm) || !authenticator.validSecurityHandlers(authorizor, pm)
        || !permHandle.validSecurityHandlers(authent, author))
      throw new RuntimeException(authorizor + ", " + authenticator + ", and " + pm
          + " do not play nice with eachother. Please choose authentication and authorization mechanisms that are compatible with one another.");
  }
  
  public void initializeSecurity(Credentials credentials, String rootPrincipal, byte[] token) throws AccumuloSecurityException, ThriftSecurityException {
    authenticate(credentials);
    
    if (!credentials.getPrincipal().equals(SecurityConstants.SYSTEM_PRINCIPAL))
      throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    authenticator.initializeSecurity(credentials, rootPrincipal, token);
    authorizor.initializeSecurity(credentials, rootPrincipal);
    permHandle.initializeSecurity(credentials, rootPrincipal);
    try {
      permHandle.grantTablePermission(rootPrincipal, Constants.METADATA_TABLE_ID, TablePermission.ALTER_TABLE);
    } catch (TableNotFoundException e) {
      // Shouldn't happen
      throw new RuntimeException(e);
    }
  }
  
  public synchronized String getRootUsername() {
    if (rootUserName == null)
      rootUserName = new String(zooCache.get(ZKUserPath));
    return rootUserName;
  }
  
  private void authenticate(Credentials credentials) throws ThriftSecurityException {
    if (!credentials.getInstanceId().equals(HdfsZooInstance.getInstance().getInstanceID()))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.INVALID_INSTANCEID);
    
    if (SecurityConstants.getSystemCredentials().equals(credentials))
      return;
    else if (credentials.getPrincipal().equals(SecurityConstants.SYSTEM_PRINCIPAL)) {
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
    }
    
    try {
      if (!authenticator.authenticateUser(credentials.getPrincipal(), credentials.getToken())) {
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
      }
    } catch (AccumuloSecurityException e) {
      log.debug(e);
      throw e.asThriftException();
    }
  }
  
  public boolean canAskAboutUser(Credentials credentials, String user) throws ThriftSecurityException {
    // Authentication done in canPerformSystemActions
    if (!(canPerformSystemActions(credentials) || credentials.getPrincipal().equals(user)))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return true;
  }
  
  public boolean authenticateUser(Credentials credentials, String principal, byte[] token) throws ThriftSecurityException {
    canAskAboutUser(credentials, principal);
    // User is already authenticated from canAskAboutUser, this gets around issues with !SYSTEM user
    if (credentials.getToken().equals(token))
      return true;
    try {
      return authenticator.authenticateUser(principal, token);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public Authorizations getUserAuthorizations(Credentials credentials, String user) throws ThriftSecurityException {
    authenticate(credentials);
    
    targetUserExists(user);
    
    if (!credentials.getPrincipal().equals(user) && !hasSystemPermission(credentials.getPrincipal(), SystemPermission.SYSTEM, false))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    // system user doesn't need record-level authorizations for the tables it reads (for now)
    if (user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      return Constants.NO_AUTHS;
    
    try {
      return authorizor.getCachedUserAuthorizations(user);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public Authorizations getUserAuthorizations(Credentials credentials) throws ThriftSecurityException {
    return getUserAuthorizations(credentials, credentials.getPrincipal());
  }
  
  /**
   * Checks if a user has a system permission
   * 
   * @return true if a user exists and has permission; false otherwise
   */
  private boolean hasSystemPermission(String user, SystemPermission permission, boolean useCached) throws ThriftSecurityException {
    if (user.equals(getRootUsername()) || user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      return true;
    
    targetUserExists(user);
    
    try {
      if (useCached)
        return permHandle.hasCachedSystemPermission(user, permission);
      return permHandle.hasSystemPermission(user, permission);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  /**
   * Checks if a user has a table permission
   * 
   * @return true if a user exists and has permission; false otherwise
   */
  private boolean hasTablePermission(String user, String table, TablePermission permission, boolean useCached) throws ThriftSecurityException {
    if (user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      return true;
    
    targetUserExists(user);
    
    if (table.equals(Constants.METADATA_TABLE_ID) && permission.equals(TablePermission.READ))
      return true;
    
    try {
      if (useCached)
        return permHandle.hasCachedTablePermission(user, table, permission);
      return permHandle.hasTablePermission(user, table, permission);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(user, SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }
  
  // some people just aren't allowed to ask about other users; here are those who can ask
  private boolean canAskAboutOtherUsers(Credentials credentials, String user) throws ThriftSecurityException {
    authenticate(credentials);
    return credentials.getPrincipal().equals(user) || hasSystemPermission(credentials.getPrincipal(), SystemPermission.SYSTEM, false)
        || hasSystemPermission(credentials.getPrincipal(), SystemPermission.CREATE_USER, false)
        || hasSystemPermission(credentials.getPrincipal(), SystemPermission.ALTER_USER, false)
        || hasSystemPermission(credentials.getPrincipal(), SystemPermission.DROP_USER, false);
  }
  
  private void targetUserExists(String user) throws ThriftSecurityException {
    if (user.equals(SecurityConstants.SYSTEM_PRINCIPAL) || user.equals(getRootUsername()))
      return;
    
    try {
      if (!authenticator.userExists(user))
        throw new ThriftSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public boolean canScan(Credentials credentials, String table) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials.getPrincipal(), table, TablePermission.READ, true);
  }
  
  public boolean canWrite(Credentials credentials, String table) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials.getPrincipal(), table, TablePermission.WRITE, true);
  }
  
  public boolean canSplitTablet(Credentials credentials, String table) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials.getPrincipal(), SystemPermission.ALTER_TABLE, false)
        || hasSystemPermission(credentials.getPrincipal(), SystemPermission.SYSTEM, false)
        || hasTablePermission(credentials.getPrincipal(), table, TablePermission.ALTER_TABLE, false);
  }
  
  /**
   * This is the check to perform any system action. This includes tserver's loading of a tablet, shutting the system down, or altering system properties.
   */
  public boolean canPerformSystemActions(Credentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials.getPrincipal(), SystemPermission.SYSTEM, false);
  }
  
  public boolean canFlush(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c.getPrincipal(), tableId, TablePermission.WRITE, false)
        || hasTablePermission(c.getPrincipal(), tableId, TablePermission.ALTER_TABLE, false);
  }
  
  public boolean canAlterTable(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c.getPrincipal(), tableId, TablePermission.ALTER_TABLE, false)
        || hasSystemPermission(c.getPrincipal(), SystemPermission.ALTER_TABLE, false);
  }
  
  public boolean canCreateTable(Credentials c) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c.getPrincipal(), SystemPermission.CREATE_TABLE, false);
  }
  
  public boolean canRenameTable(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c.getPrincipal(), SystemPermission.ALTER_TABLE, false)
        || hasTablePermission(c.getPrincipal(), tableId, TablePermission.ALTER_TABLE, false);
  }
  
  public boolean canCloneTable(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c.getPrincipal(), SystemPermission.CREATE_TABLE, false)
        && hasTablePermission(c.getPrincipal(), tableId, TablePermission.READ, false);
  }
  
  public boolean canDeleteTable(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c.getPrincipal(), SystemPermission.DROP_TABLE, false)
        || hasTablePermission(c.getPrincipal(), tableId, TablePermission.DROP_TABLE, false);
  }
  
  public boolean canOnlineOfflineTable(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c.getPrincipal(), SystemPermission.SYSTEM, false) || hasSystemPermission(c.getPrincipal(), SystemPermission.ALTER_TABLE, false)
        || hasTablePermission(c.getPrincipal(), tableId, TablePermission.ALTER_TABLE, false);
  }
  
  public boolean canMerge(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c.getPrincipal(), SystemPermission.SYSTEM, false) || hasSystemPermission(c.getPrincipal(), SystemPermission.ALTER_TABLE, false)
        || hasTablePermission(c.getPrincipal(), tableId, TablePermission.ALTER_TABLE, false);
  }
  
  public boolean canDeleteRange(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c.getPrincipal(), SystemPermission.SYSTEM, false) || hasTablePermission(c.getPrincipal(), tableId, TablePermission.WRITE, false);
  }
  
  public boolean canBulkImport(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c.getPrincipal(), tableId, TablePermission.BULK_IMPORT, false);
  }
  
  public boolean canCompact(Credentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c.getPrincipal(), SystemPermission.ALTER_TABLE, false)
        || hasTablePermission(c.getPrincipal(), tableId, TablePermission.ALTER_TABLE, false)
        || hasTablePermission(c.getPrincipal(), tableId, TablePermission.WRITE, false);
  }
  
  public boolean canChangeAuthorizations(Credentials c, String user) throws ThriftSecurityException {
    authenticate(c);
    if (user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return hasSystemPermission(c.getPrincipal(), SystemPermission.ALTER_USER, false);
  }
  
  public boolean canChangePassword(Credentials c, String user) throws ThriftSecurityException {
    authenticate(c);
    if (user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return c.getPrincipal().equals(user) || hasSystemPermission(c.getPrincipal(), SystemPermission.ALTER_USER, false);
  }
  
  public boolean canCreateUser(Credentials c, String user) throws ThriftSecurityException {
    authenticate(c);
    
    // don't allow creating a user with the same name as system user
    if (user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    return hasSystemPermission(c.getPrincipal(), SystemPermission.CREATE_USER, false);
  }
  
  public boolean canDropUser(Credentials c, String user) throws ThriftSecurityException {
    authenticate(c);
    
    // can't delete root or system users
    if (user.equals(getRootUsername()) || user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    return hasSystemPermission(c.getPrincipal(), SystemPermission.DROP_USER, false);
  }
  
  public boolean canGrantSystem(Credentials c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    authenticate(c);
    
    // can't modify system user
    if (user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    // can't grant GRANT
    if (sysPerm.equals(SystemPermission.GRANT))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.GRANT_INVALID);
    
    return hasSystemPermission(c.getPrincipal(), SystemPermission.GRANT, false);
  }
  
  public boolean canGrantTable(Credentials c, String user, String table) throws ThriftSecurityException {
    authenticate(c);
    
    // can't modify system user
    if (user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    return hasSystemPermission(c.getPrincipal(), SystemPermission.ALTER_TABLE, false)
        || hasTablePermission(c.getPrincipal(), table, TablePermission.GRANT, false);
  }
  
  public boolean canRevokeSystem(Credentials c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    authenticate(c);
    
    // can't modify system or root user
    if (user.equals(getRootUsername()) || user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    // can't revoke GRANT
    if (sysPerm.equals(SystemPermission.GRANT))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.GRANT_INVALID);
    
    return hasSystemPermission(c.getPrincipal(), SystemPermission.GRANT, false);
  }
  
  public boolean canRevokeTable(Credentials c, String user, String table) throws ThriftSecurityException {
    authenticate(c);
    
    // can't modify system user
    if (user.equals(SecurityConstants.SYSTEM_PRINCIPAL))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    return hasSystemPermission(c.getPrincipal(), SystemPermission.ALTER_TABLE, false)
        || hasTablePermission(c.getPrincipal(), table, TablePermission.GRANT, false);
  }
  
  public void changeAuthorizations(Credentials credentials, String user, Authorizations authorizations) throws ThriftSecurityException {
    if (!canChangeAuthorizations(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    targetUserExists(user);
    
    try {
      authorizor.changeAuthorizations(user, authorizations);
      log.info("Changed authorizations for user " + user + " at the request of user " + credentials.getPrincipal());
    } catch (AccumuloSecurityException ase) {
      throw ase.asThriftException();
    }
  }
  
  public void changePassword(Credentials credentials, String principal, byte[] token) throws ThriftSecurityException {
    if (!canChangePassword(credentials, principal))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    try {
      authenticator.changePassword(principal, token);
      log.info("Changed password for user " + principal + " at the request of user " + credentials.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public void createUser(Credentials credentials, String principal, byte[] token, Authorizations authorizations) throws ThriftSecurityException {
    if (!canCreateUser(credentials, principal))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    try {
      authenticator.createUser(principal, token);
      authorizor.initUser(principal);
      permHandle.initUser(principal);
      log.info("Created user " + principal + " at the request of user " + credentials.getPrincipal());
      if (canChangeAuthorizations(credentials, principal))
        authorizor.changeAuthorizations(principal, authorizations);
    } catch (AccumuloSecurityException ase) {
      throw ase.asThriftException();
    }
  }
  
  public void dropUser(Credentials credentials, String user) throws ThriftSecurityException {
    if (!canDropUser(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    try {
      authorizor.dropUser(user);
      authenticator.dropUser(user);
      permHandle.cleanUser(user);
      log.info("Deleted user " + user + " at the request of user " + credentials.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public void grantSystemPermission(Credentials credentials, String user, SystemPermission permissionById) throws ThriftSecurityException {
    if (!canGrantSystem(credentials, user, permissionById))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    targetUserExists(user);
    
    try {
      permHandle.grantSystemPermission(user, permissionById);
      log.info("Granted system permission " + permissionById + " for user " + user + " at the request of user " + credentials.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public void grantTablePermission(Credentials c, String user, String tableId, TablePermission permission) throws ThriftSecurityException {
    if (!canGrantTable(c, user, tableId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    targetUserExists(user);
    
    try {
      permHandle.grantTablePermission(user, tableId, permission);
      log.info("Granted table permission " + permission + " for user " + user + " on the table " + tableId + " at the request of user " + c.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }
  
  public void revokeSystemPermission(Credentials credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    if (!canRevokeSystem(credentials, user, permission))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    targetUserExists(user);
    
    try {
      permHandle.revokeSystemPermission(user, permission);
      log.info("Revoked system permission " + permission + " for user " + user + " at the request of user " + credentials.getPrincipal());
      
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public void revokeTablePermission(Credentials c, String user, String tableId, TablePermission permission) throws ThriftSecurityException {
    if (!canRevokeTable(c, user, tableId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    targetUserExists(user);
    
    try {
      permHandle.revokeTablePermission(user, tableId, permission);
      log.info("Revoked table permission " + permission + " for user " + user + " on the table " + tableId + " at the request of user " + c.getPrincipal());
      
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }
  
  public boolean hasSystemPermission(Credentials credentials, String user, SystemPermission permissionById) throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return hasSystemPermission(user, permissionById, false);
  }
  
  public boolean hasTablePermission(Credentials credentials, String user, String tableId, TablePermission permissionById)
      throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return hasTablePermission(user, tableId, permissionById, false);
  }
  
  public Set<String> listUsers(Credentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    try {
      return authenticator.listUsers();
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public void deleteTable(Credentials credentials, String tableId) throws ThriftSecurityException {
    if (!canDeleteTable(credentials, tableId))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    try {
      permHandle.cleanTablePermissions(tableId);
    } catch (AccumuloSecurityException e) {
      e.setUser(credentials.getPrincipal());
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }
  
  public boolean canExport(Credentials credentials, String tableId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials.getPrincipal(), tableId, TablePermission.READ, false);
  }
  
  public boolean canImport(Credentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials.getPrincipal(), SystemPermission.CREATE_TABLE, false);
  }
  
  public String getAuthorizorName() {
    return authenticator.getAuthorizorName();
  }
}

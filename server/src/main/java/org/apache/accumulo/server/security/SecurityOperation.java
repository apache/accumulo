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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SecurityOperationsImpl;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.master.thrift.TableOperation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.accumulo.server.security.handler.ZKAuthenticator;
import org.apache.accumulo.server.security.handler.ZKAuthorizor;
import org.apache.accumulo.server.security.handler.ZKPermHandler;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Utility class for performing various security operations with the appropriate checks
 */
public class SecurityOperation {
  private static final Logger log = Logger.getLogger(SecurityOperationsImpl.class);
  
  protected Authorizor authorizor;
  protected Authenticator authenticator;
  protected PermissionHandler permHandle;
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
  
  protected static Authorizor getAuthorizor(String instanceId, boolean initialize) {
    Authorizor toRet = Master.createInstanceFromPropertyName(ServerConfiguration.getSiteConfiguration(), Property.INSTANCE_SECURITY_AUTHORIZOR,
        Authorizor.class, ZKAuthorizor.getInstance());
    toRet.initialize(instanceId, initialize);
    return toRet;
  }
  
  protected static Authenticator getAuthenticator(String instanceId, boolean initialize) {
    Authenticator toRet = Master.createInstanceFromPropertyName(ServerConfiguration.getSiteConfiguration(), Property.INSTANCE_SECURITY_AUTHENTICATOR,
        Authenticator.class, ZKAuthenticator.getInstance());
    toRet.initialize(instanceId, initialize);
    return toRet;
  }
  
  protected static PermissionHandler getPermHandler(String instanceId, boolean initialize) {
    PermissionHandler toRet = Master.createInstanceFromPropertyName(ServerConfiguration.getSiteConfiguration(), Property.INSTANCE_SECURITY_PERMISSION_HANDLER,
        PermissionHandler.class, ZKPermHandler.getInstance());
    toRet.initialize(instanceId, initialize);
    return toRet;
  }
  
  protected SecurityOperation(String instanceId) {
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
  
  public void initializeSecurity(TCredentials credentials, String rootPrincipal, byte[] token) throws AccumuloSecurityException, ThriftSecurityException {
    authenticate(credentials);
    
    if (!isSystemUser(credentials))
      throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    authenticator.initializeSecurity(credentials, rootPrincipal, token);
    authorizor.initializeSecurity(credentials, rootPrincipal);
    permHandle.initializeSecurity(credentials, rootPrincipal);
    try {
      permHandle.grantTablePermission(rootPrincipal, MetadataTable.ID, TablePermission.ALTER_TABLE);
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
  
  public boolean isSystemUser(TCredentials credentials) {
    return SystemCredentials.get().getToken().getClass().getName().equals(credentials.getTokenClassName());
  }
  
  private void authenticate(TCredentials credentials) throws ThriftSecurityException {
    if (!credentials.getInstanceId().equals(HdfsZooInstance.getInstance().getInstanceID()))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.INVALID_INSTANCEID);
    
    if (isSystemUser(credentials)) {
      authenticateSystemUser(credentials);
    } else {
      try {
        AuthenticationToken token = reassembleToken(credentials);
        if (!authenticator.authenticateUser(credentials.getPrincipal(), token)) {
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
        }
      } catch (AccumuloSecurityException e) {
        log.debug(e);
        throw e.asThriftException();
      }
    }
  }
  
  private void authenticateSystemUser(TCredentials credentials) throws ThriftSecurityException {
    if (SystemCredentials.get().getToken().equals(credentials.getToken()))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
  }
  
  public boolean canAskAboutUser(TCredentials credentials, String user) throws ThriftSecurityException {
    // Authentication done in canPerformSystemActions
    if (!(canPerformSystemActions(credentials) || credentials.getPrincipal().equals(user)))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return true;
  }
  
  public boolean authenticateUser(TCredentials credentials, TCredentials toAuth) throws ThriftSecurityException {
    canAskAboutUser(credentials, toAuth.getPrincipal());
    // User is already authenticated from canAskAboutUser
    if (credentials.equals(toAuth))
      return true;
    try {
      AuthenticationToken token = reassembleToken(toAuth);
      return authenticator.authenticateUser(toAuth.getPrincipal(), token);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  private AuthenticationToken reassembleToken(TCredentials toAuth) throws AccumuloSecurityException {
    String tokenClass = toAuth.getTokenClassName();
    if (authenticator.validTokenClass(tokenClass)) {
      return CredentialHelper.extractToken(toAuth);
    }
    throw new AccumuloSecurityException(toAuth.getPrincipal(), SecurityErrorCode.INVALID_TOKEN);
  }
  
  public Authorizations getUserAuthorizations(TCredentials credentials, String user) throws ThriftSecurityException {
    authenticate(credentials);
    
    targetUserExists(user);
    
    if (!credentials.getPrincipal().equals(user) && !hasSystemPermission(credentials, SystemPermission.SYSTEM, false))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    try {
      return authorizor.getCachedUserAuthorizations(user);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public Authorizations getUserAuthorizations(TCredentials credentials) throws ThriftSecurityException {
    // system user doesn't need record-level authorizations for the tables it reads
    if (isSystemUser(credentials)) {
      authenticate(credentials);
      return Authorizations.EMPTY;
    }
    return getUserAuthorizations(credentials, credentials.getPrincipal());
  }
  
  /**
   * Checks if a user has a system permission
   * 
   * @return true if a user exists and has permission; false otherwise
   */
  private boolean hasSystemPermission(TCredentials credentials, SystemPermission permission, boolean useCached) throws ThriftSecurityException {
    if (isSystemUser(credentials))
      return true;
    return _hasSystemPermission(credentials.getPrincipal(), permission, useCached);
  }
  
  /**
   * Checks if a user has a system permission<br/>
   * This cannot check if a system user has permission.
   * 
   * @return true if a user exists and has permission; false otherwise
   */
  private boolean _hasSystemPermission(String user, SystemPermission permission, boolean useCached) throws ThriftSecurityException {
    if (user.equals(getRootUsername()))
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
  protected boolean hasTablePermission(TCredentials credentials, String table, TablePermission permission, boolean useCached) throws ThriftSecurityException {
    if (isSystemUser(credentials))
      return true;
    return _hasTablePermission(credentials.getPrincipal(), table, permission, useCached);
  }
  
  /**
   * Checks if a user has a table permission<br/>
   * This cannot check if a system user has permission.
   * 
   * @return true if a user exists and has permission; false otherwise
   */
  protected boolean _hasTablePermission(String user, String table, TablePermission permission, boolean useCached) throws ThriftSecurityException {
    targetUserExists(user);
    
    if ((table.equals(MetadataTable.ID) || table.equals(RootTable.ID)) && permission.equals(TablePermission.READ))
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
  private boolean canAskAboutOtherUsers(TCredentials credentials, String user) throws ThriftSecurityException {
    authenticate(credentials);
    return credentials.getPrincipal().equals(user) || hasSystemPermission(credentials, SystemPermission.SYSTEM, false)
        || hasSystemPermission(credentials, SystemPermission.CREATE_USER, false) || hasSystemPermission(credentials, SystemPermission.ALTER_USER, false)
        || hasSystemPermission(credentials, SystemPermission.DROP_USER, false);
  }
  
  private void targetUserExists(String user) throws ThriftSecurityException {
    if (user.equals(getRootUsername()))
      return;
    try {
      if (!authenticator.userExists(user))
        throw new ThriftSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public boolean canScan(TCredentials credentials, String table) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, table, TablePermission.READ, true);
  }
  
  public boolean canScan(TCredentials credentials, String table, TRange range, List<TColumn> columns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    return canScan(credentials, table);
  }
  
  public boolean canScan(TCredentials credentials, String table, Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    return canScan(credentials, table);
  }
  
  public boolean canWrite(TCredentials credentials, String table) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, table, TablePermission.WRITE, true);
  }
  
  public boolean canConditionallyUpdate(TCredentials credentials, String tableID, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    
    authenticate(credentials);
    
    return hasTablePermission(credentials, tableID, TablePermission.WRITE, true) && hasTablePermission(credentials, tableID, TablePermission.READ, true);
  }

  public boolean canSplitTablet(TCredentials credentials, String table) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials, SystemPermission.ALTER_TABLE, false) || hasSystemPermission(credentials, SystemPermission.SYSTEM, false)
        || hasTablePermission(credentials, table, TablePermission.ALTER_TABLE, false);
  }
  
  /**
   * This is the check to perform any system action. This includes tserver's loading of a tablet, shutting the system down, or altering system properties.
   */
  public boolean canPerformSystemActions(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials, SystemPermission.SYSTEM, false);
  }
  
  public boolean canFlush(TCredentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c, tableId, TablePermission.WRITE, false) || hasTablePermission(c, tableId, TablePermission.ALTER_TABLE, false);
  }
  
  public boolean canAlterTable(TCredentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c, tableId, TablePermission.ALTER_TABLE, false) || hasSystemPermission(c, SystemPermission.ALTER_TABLE, false);
  }
  
  public boolean canCreateTable(TCredentials c, String tableName) throws ThriftSecurityException {
    return canCreateTable(c);
  }
  
  public boolean canCreateTable(TCredentials c) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.CREATE_TABLE, false);
  }
  
  public boolean canRenameTable(TCredentials c, String tableId, String oldTableName, String newTableName) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.ALTER_TABLE, false) || hasTablePermission(c, tableId, TablePermission.ALTER_TABLE, false);
  }
  
  public boolean canCloneTable(TCredentials c, String tableId, String tableName) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.CREATE_TABLE, false) && hasTablePermission(c, tableId, TablePermission.READ, false);
  }
  
  public boolean canDeleteTable(TCredentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.DROP_TABLE, false) || hasTablePermission(c, tableId, TablePermission.DROP_TABLE, false);
  }
  
  public boolean canOnlineOfflineTable(TCredentials c, String tableId, TableOperation op) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.SYSTEM, false) || hasSystemPermission(c, SystemPermission.ALTER_TABLE, false)
        || hasTablePermission(c, tableId, TablePermission.ALTER_TABLE, false);
  }
  
  public boolean canMerge(TCredentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.SYSTEM, false) || hasSystemPermission(c, SystemPermission.ALTER_TABLE, false)
        || hasTablePermission(c, tableId, TablePermission.ALTER_TABLE, false);
  }
  
  public boolean canDeleteRange(TCredentials c, String tableId, String tableName, Text startRow, Text endRow) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.SYSTEM, false) || hasTablePermission(c, tableId, TablePermission.WRITE, false);
  }
  
  public boolean canBulkImport(TCredentials c, String tableId, String tableName, String dir, String failDir) throws ThriftSecurityException {
    return canBulkImport(c, tableId);
  }
  
  public boolean canBulkImport(TCredentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c, tableId, TablePermission.BULK_IMPORT, false);
  }
  
  public boolean canCompact(TCredentials c, String tableId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.ALTER_TABLE, false) || hasTablePermission(c, tableId, TablePermission.ALTER_TABLE, false)
        || hasTablePermission(c, tableId, TablePermission.WRITE, false);
  }
  
  public boolean canChangeAuthorizations(TCredentials c, String user) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.ALTER_USER, false);
  }
  
  public boolean canChangePassword(TCredentials c, String user) throws ThriftSecurityException {
    authenticate(c);
    return c.getPrincipal().equals(user) || hasSystemPermission(c, SystemPermission.ALTER_USER, false);
  }
  
  public boolean canCreateUser(TCredentials c, String user) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.CREATE_USER, false);
  }
  
  public boolean canDropUser(TCredentials c, String user) throws ThriftSecurityException {
    authenticate(c);
    if (user.equals(getRootUsername()))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return hasSystemPermission(c, SystemPermission.DROP_USER, false);
  }
  
  public boolean canGrantSystem(TCredentials c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    authenticate(c);
    // can't grant GRANT
    if (sysPerm.equals(SystemPermission.GRANT))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.GRANT_INVALID);
    return hasSystemPermission(c, SystemPermission.GRANT, false);
  }
  
  public boolean canGrantTable(TCredentials c, String user, String table) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.ALTER_TABLE, false) || hasTablePermission(c, table, TablePermission.GRANT, false);
  }
  
  public boolean canRevokeSystem(TCredentials c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    authenticate(c);
    // can't modify root user
    if (user.equals(getRootUsername()))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    
    // can't revoke GRANT
    if (sysPerm.equals(SystemPermission.GRANT))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.GRANT_INVALID);
    
    return hasSystemPermission(c, SystemPermission.GRANT, false);
  }
  
  public boolean canRevokeTable(TCredentials c, String user, String table) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.ALTER_TABLE, false) || hasTablePermission(c, table, TablePermission.GRANT, false);
  }
  
  public void changeAuthorizations(TCredentials credentials, String user, Authorizations authorizations) throws ThriftSecurityException {
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
  
  public void changePassword(TCredentials credentials, TCredentials toChange) throws ThriftSecurityException {
    if (!canChangePassword(credentials, toChange.getPrincipal()))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    try {
      AuthenticationToken token = reassembleToken(toChange);
      authenticator.changePassword(toChange.getPrincipal(), token);
      log.info("Changed password for user " + toChange.getPrincipal() + " at the request of user " + credentials.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public void createUser(TCredentials credentials, TCredentials newUser, Authorizations authorizations) throws ThriftSecurityException {
    if (!canCreateUser(credentials, newUser.getPrincipal()))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    try {
      AuthenticationToken token = reassembleToken(newUser);
      authenticator.createUser(newUser.getPrincipal(), token);
      authorizor.initUser(newUser.getPrincipal());
      permHandle.initUser(newUser.getPrincipal());
      log.info("Created user " + newUser.getPrincipal() + " at the request of user " + credentials.getPrincipal());
      if (canChangeAuthorizations(credentials, newUser.getPrincipal()))
        authorizor.changeAuthorizations(newUser.getPrincipal(), authorizations);
    } catch (AccumuloSecurityException ase) {
      throw ase.asThriftException();
    }
  }
  
  public void dropUser(TCredentials credentials, String user) throws ThriftSecurityException {
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
  
  public void grantSystemPermission(TCredentials credentials, String user, SystemPermission permissionById) throws ThriftSecurityException {
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
  
  public void grantTablePermission(TCredentials c, String user, String tableId, TablePermission permission) throws ThriftSecurityException {
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
  
  public void revokeSystemPermission(TCredentials credentials, String user, SystemPermission permission) throws ThriftSecurityException {
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
  
  public void revokeTablePermission(TCredentials c, String user, String tableId, TablePermission permission) throws ThriftSecurityException {
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
  
  public boolean hasSystemPermission(TCredentials credentials, String user, SystemPermission permissionById) throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return _hasSystemPermission(user, permissionById, false);
  }
  
  public boolean hasTablePermission(TCredentials credentials, String user, String tableId, TablePermission permissionById) throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return _hasTablePermission(user, tableId, permissionById, false);
  }
  
  public Set<String> listUsers(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    try {
      return authenticator.listUsers();
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  public void deleteTable(TCredentials credentials, String tableId) throws ThriftSecurityException {
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
  
  public boolean canExport(TCredentials credentials, String tableId, String tableName, String exportDir) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, tableId, TablePermission.READ, false);
  }
  
  public boolean canImport(TCredentials credentials, String tableName, String importDir) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials, SystemPermission.CREATE_TABLE, false);
  }
}

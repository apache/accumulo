/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.KerberosAuthenticator;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.accumulo.server.security.handler.ZKAuthenticator;
import org.apache.accumulo.server.security.handler.ZKAuthorizor;
import org.apache.accumulo.server.security.handler.ZKPermHandler;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for performing various security operations with the appropriate checks
 */
public class SecurityOperation {
  private static final Logger log = LoggerFactory.getLogger(SecurityOperation.class);

  protected Authorizor authorizor;
  protected Authenticator authenticator;
  protected PermissionHandler permHandle;
  protected boolean isKerberos;
  private static String rootUserName = null;
  private final ZooCache zooCache;
  private final String ZKUserPath;

  protected final ServerContext context;

  static SecurityOperation instance;

  public static synchronized SecurityOperation getInstance(ServerContext context) {
    if (instance == null) {
      instance = new SecurityOperation(context, getAuthorizor(context), getAuthenticator(context),
          getPermHandler(context));
    }
    return instance;
  }

  protected static Authorizor getAuthorizor(ServerContext context) {
    Authorizor toRet = Property.createInstanceFromPropertyName(context.getConfiguration(),
        Property.INSTANCE_SECURITY_AUTHORIZOR, Authorizor.class, new ZKAuthorizor());
    toRet.initialize(context);
    return toRet;
  }

  protected static Authenticator getAuthenticator(ServerContext context) {
    Authenticator toRet = Property.createInstanceFromPropertyName(context.getConfiguration(),
        Property.INSTANCE_SECURITY_AUTHENTICATOR, Authenticator.class, new ZKAuthenticator());
    toRet.initialize(context);
    return toRet;
  }

  protected static PermissionHandler getPermHandler(ServerContext context) {
    PermissionHandler toRet = Property.createInstanceFromPropertyName(context.getConfiguration(),
        Property.INSTANCE_SECURITY_PERMISSION_HANDLER, PermissionHandler.class,
        new ZKPermHandler());
    toRet.initialize(context);
    return toRet;
  }

  protected SecurityOperation(ServerContext context) {
    this.context = context;
    ZKUserPath = Constants.ZROOT + "/" + context.getInstanceID() + "/users";
    zooCache = new ZooCache(context.getZooReaderWriter(), null);
  }

  public SecurityOperation(ServerContext context, Authorizor author, Authenticator authent,
      PermissionHandler pm) {
    this(context);
    authorizor = author;
    authenticator = authent;
    permHandle = pm;

    if (!authorizor.validSecurityHandlers(authenticator, pm)
        || !authenticator.validSecurityHandlers()
        || !permHandle.validSecurityHandlers(authent, author))
      throw new RuntimeException(authorizor + ", " + authenticator + ", and " + pm
          + " do not play nice with eachother. Please choose authentication and"
          + " authorization mechanisms that are compatible with one another.");

    isKerberos = KerberosAuthenticator.class.isAssignableFrom(authenticator.getClass());
  }

  public void initializeSecurity(TCredentials credentials, String rootPrincipal, byte[] token)
      throws AccumuloSecurityException {
    if (!isSystemUser(credentials))
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);

    authenticator.initializeSecurity(rootPrincipal, token);
    authorizor.initializeSecurity(credentials, rootPrincipal);
    permHandle.initializeSecurity(credentials, rootPrincipal);
    try {
      permHandle.grantTablePermission(rootPrincipal, MetadataTable.ID.canonical(),
          TablePermission.ALTER_TABLE);
    } catch (TableNotFoundException e) {
      // Shouldn't happen
      throw new RuntimeException(e);
    }
  }

  public synchronized String getRootUsername() {
    if (rootUserName == null)
      rootUserName = new String(zooCache.get(ZKUserPath), UTF_8);
    return rootUserName;
  }

  public boolean isSystemUser(TCredentials credentials) {
    return context.getCredentials().getToken().getClass().getName()
        .equals(credentials.getTokenClassName());
  }

  protected void authenticate(TCredentials credentials) throws ThriftSecurityException {
    if (!credentials.getInstanceId().equals(context.getInstanceID()))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.INVALID_INSTANCEID);

    Credentials creds = Credentials.fromThrift(credentials);

    if (isSystemUser(credentials)) {
      if (isKerberos) {
        // Don't need to re-check the principal as TCredentialsUpdatingInvocationHandler will check
        // the provided against
        // the credentials provided on the wire.
        if (!context.getCredentials().getToken().equals(creds.getToken())) {
          log.debug("With SASL enabled, System AuthenticationTokens did not match.");
          throw new ThriftSecurityException(creds.getPrincipal(),
              SecurityErrorCode.BAD_CREDENTIALS);
        }
      } else {
        if (!(context.getCredentials().equals(creds))) {
          log.debug("Provided credentials did not match server's expected"
              + " credentials. Expected {} but got {}", context.getCredentials(), creds);
          throw new ThriftSecurityException(creds.getPrincipal(),
              SecurityErrorCode.BAD_CREDENTIALS);
        }
      }
    } else {
      // Not the system user

      if (isKerberos) {
        // If we have kerberos credentials for a user from the network but no account
        // in the system, we need to make one before proceeding
        if (!authenticator.userExists(creds.getPrincipal())) {
          // If we call the normal createUser method, it will loop back into this method
          // when it tries to check if the user has permission to create users
          try {
            _createUser(credentials, creds);
          } catch (ThriftSecurityException e) {
            if (e.getCode() != SecurityErrorCode.USER_EXISTS) {
              // For Kerberos, a user acct is automatically created because there is no notion of
              // a password
              // in the traditional sense of Accumulo users. As such, if a user acct already
              // exists when we
              // try to automatically create a user account, we should avoid returning this
              // exception back to the user.
              // We want to let USER_EXISTS code pass through and continue
              throw e;
            }
          }
        }
      }

      // Check that the user is authenticated (a no-op at this point for kerberos)
      try {
        if (!authenticator.authenticateUser(creds.getPrincipal(), creds.getToken())) {
          throw new ThriftSecurityException(creds.getPrincipal(),
              SecurityErrorCode.BAD_CREDENTIALS);
        }
      } catch (AccumuloSecurityException e) {
        log.debug("AccumuloSecurityException", e);
        throw e.asThriftException();
      }
    }
  }

  public boolean authenticateUser(TCredentials credentials, TCredentials toAuth)
      throws ThriftSecurityException {
    authenticate(credentials); // authenticate the current user first

    // if the user to authenticate is not the current user, and the current user lacks
    // the SYSTEM permission, then deny the request
    if (!credentials.getPrincipal().equals(toAuth.getPrincipal())
        && !hasSystemPermission(credentials, SystemPermission.SYSTEM, false))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);

    // the user is already authenticated above if the credentials to authenticate are the same
    if (credentials.equals(toAuth))
      return true;
    try {
      Credentials toCreds = Credentials.fromThrift(toAuth);

      if (isKerberos) {
        // If we have kerberos credentials for a user from the network but no account
        // in the system, we need to make one before proceeding
        if (!authenticator.userExists(toCreds.getPrincipal())) {
          createUser(credentials, toCreds, Authorizations.EMPTY);
        }
        // Likely that the KerberosAuthenticator will fail as we don't have the credentials for the
        // other user,
        // we only have our own Kerberos credentials.
      }

      return authenticator.authenticateUser(toCreds.getPrincipal(), toCreds.getToken());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public Authorizations getUserAuthorizations(TCredentials credentials, String user)
      throws ThriftSecurityException {
    authenticate(credentials);

    targetUserExists(user);

    if (!credentials.getPrincipal().equals(user)
        && !hasSystemPermission(credentials, SystemPermission.SYSTEM, false)
        && !hasSystemPermission(credentials, SystemPermission.ALTER_USER, false))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);

    return authorizor.getCachedUserAuthorizations(user);
  }

  public Authorizations getUserAuthorizations(TCredentials credentials)
      throws ThriftSecurityException {
    // system user doesn't need record-level authorizations for the tables it reads
    if (isSystemUser(credentials)) {
      authenticate(credentials);
      return Authorizations.EMPTY;
    }
    return getUserAuthorizations(credentials, credentials.getPrincipal());
  }

  /**
   * Check if an already authenticated user has specified authorizations.
   */
  public boolean authenticatedUserHasAuthorizations(TCredentials credentials,
      List<ByteBuffer> list) {
    if (isSystemUser(credentials)) {
      // system user doesn't need record-level authorizations for the tables it reads (for now)
      return list.isEmpty();
    }
    return authorizor.isValidAuthorizations(credentials.getPrincipal(), list);
  }

  private boolean hasSystemPermission(TCredentials credentials, SystemPermission permission,
      boolean useCached) throws ThriftSecurityException {
    return hasSystemPermissionWithNamespaceId(credentials, permission, null, useCached);
  }

  /**
   * Checks if a user has a system permission
   *
   * @return true if a user exists and has permission; false otherwise
   */
  private boolean hasSystemPermissionWithNamespaceId(TCredentials credentials,
      SystemPermission permission, NamespaceId namespaceId, boolean useCached)
      throws ThriftSecurityException {
    if (isSystemUser(credentials))
      return true;

    if (_hasSystemPermission(credentials.getPrincipal(), permission, useCached))
      return true;
    if (namespaceId != null) {
      return _hasNamespacePermission(credentials.getPrincipal(), namespaceId,
          NamespacePermission.getEquivalent(permission), useCached);
    }

    return false;
  }

  /**
   * Checks if a user has a system permission<br>
   * This cannot check if a system user has permission.
   *
   * @return true if a user exists and has permission; false otherwise
   */
  private boolean _hasSystemPermission(String user, SystemPermission permission, boolean useCached)
      throws ThriftSecurityException {
    if (user.equals(getRootUsername()))
      return true;

    targetUserExists(user);

    if (useCached)
      return permHandle.hasCachedSystemPermission(user, permission);
    return permHandle.hasSystemPermission(user, permission);
  }

  /**
   * Checks if a user has a table permission
   *
   * @return true if a user exists and has permission; false otherwise
   */
  protected boolean hasTablePermission(TCredentials credentials, TableId tableId,
      NamespaceId namespaceId, TablePermission permission, boolean useCached)
      throws ThriftSecurityException {
    if (isSystemUser(credentials))
      return true;
    return _hasTablePermission(credentials.getPrincipal(), tableId, permission, useCached)
        || _hasNamespacePermission(credentials.getPrincipal(), namespaceId,
            NamespacePermission.getEquivalent(permission), useCached);
  }

  /**
   * Checks if a user has a table permission<br>
   * This cannot check if a system user has permission.
   *
   * @return true if a user exists and has permission; false otherwise
   */
  protected boolean _hasTablePermission(String user, TableId table, TablePermission permission,
      boolean useCached) throws ThriftSecurityException {
    targetUserExists(user);

    if ((table.equals(MetadataTable.ID) || table.equals(RootTable.ID)
        || table.equals(ReplicationTable.ID)) && permission.equals(TablePermission.READ))
      return true;

    try {
      if (useCached)
        return permHandle.hasCachedTablePermission(user, table.canonical(), permission);
      return permHandle.hasTablePermission(user, table.canonical(), permission);
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(user, SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }

  /**
   * Checks if a user has a namespace permission<br>
   * This cannot check if a system user has permission.
   *
   * @return true if a user exists and has permission; false otherwise
   */
  protected boolean _hasNamespacePermission(String user, NamespaceId namespace,
      NamespacePermission permission, boolean useCached) throws ThriftSecurityException {
    if (permission == null)
      return false;

    targetUserExists(user);

    if (namespace.equals(Namespace.ACCUMULO.id()) && permission.equals(NamespacePermission.READ))
      return true;

    try {
      if (useCached)
        return permHandle.hasCachedNamespacePermission(user, namespace.canonical(), permission);
      return permHandle.hasNamespacePermission(user, namespace.canonical(), permission);
    } catch (NamespaceNotFoundException e) {
      throw new ThriftSecurityException(user, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    }
  }

  // some people just aren't allowed to ask about other users; here are those who can ask
  private boolean canAskAboutOtherUsers(TCredentials credentials, String user)
      throws ThriftSecurityException {
    authenticate(credentials);
    return credentials.getPrincipal().equals(user)
        || hasSystemPermission(credentials, SystemPermission.SYSTEM, false)
        || hasSystemPermission(credentials, SystemPermission.CREATE_USER, false)
        || hasSystemPermission(credentials, SystemPermission.ALTER_USER, false)
        || hasSystemPermission(credentials, SystemPermission.DROP_USER, false);
  }

  private void targetUserExists(String user) throws ThriftSecurityException {
    if (user.equals(getRootUsername()))
      return;
    if (!authenticator.userExists(user))
      throw new ThriftSecurityException(user, SecurityErrorCode.USER_DOESNT_EXIST);
  }

  public boolean canScan(TCredentials credentials, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, tableId, namespaceId, TablePermission.READ, true);
  }

  public boolean canScan(TCredentials credentials, TableId tableId, NamespaceId namespaceId,
      TRange range, List<TColumn> columns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations)
      throws ThriftSecurityException {
    return canScan(credentials, tableId, namespaceId);
  }

  public boolean canScan(TCredentials credentials, TableId table, NamespaceId namespaceId,
      Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations)
      throws ThriftSecurityException {
    return canScan(credentials, table, namespaceId);
  }

  public boolean canWrite(TCredentials credentials, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, tableId, namespaceId, TablePermission.WRITE, true);
  }

  public boolean canConditionallyUpdate(TCredentials credentials, TableId tableID,
      NamespaceId namespaceId) throws ThriftSecurityException {

    authenticate(credentials);

    return hasTablePermission(credentials, tableID, namespaceId, TablePermission.WRITE, true)
        && hasTablePermission(credentials, tableID, namespaceId, TablePermission.READ, true);
  }

  public boolean canSplitTablet(TCredentials credentials, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.ALTER_TABLE,
        namespaceId, false)
        || hasSystemPermissionWithNamespaceId(credentials, SystemPermission.SYSTEM, namespaceId,
            false)
        || hasTablePermission(credentials, tableId, namespaceId, TablePermission.ALTER_TABLE,
            false);
  }

  /**
   * This is the check to perform any system action. This includes tserver's loading of a tablet,
   * shutting the system down, or altering system properties.
   */
  public boolean canPerformSystemActions(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials, SystemPermission.SYSTEM, false);
  }

  public boolean canFlush(TCredentials c, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c, tableId, namespaceId, TablePermission.WRITE, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false);
  }

  public boolean canAlterTable(TCredentials c, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false)
        || hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false);
  }

  public boolean canCreateTable(TCredentials c, String tableName, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.CREATE_TABLE, namespaceId, false);
  }

  public boolean canRenameTable(TCredentials c, TableId tableId, String oldTableName,
      String newTableName, NamespaceId namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false);
  }

  public boolean canCloneTable(TCredentials c, TableId tableId, String tableName,
      NamespaceId destinationNamespaceId, NamespaceId srcNamespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.CREATE_TABLE,
        destinationNamespaceId, false)
        && hasTablePermission(c, tableId, srcNamespaceId, TablePermission.READ, false);
  }

  public boolean canDeleteTable(TCredentials c, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.DROP_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.DROP_TABLE, false);
  }

  public boolean canOnlineOfflineTable(TCredentials c, TableId tableId, FateOperation op,
      NamespaceId namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.SYSTEM, namespaceId, false)
        || hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false);
  }

  public boolean canMerge(TCredentials c, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.SYSTEM, namespaceId, false)
        || hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false);
  }

  public boolean canDeleteRange(TCredentials c, TableId tableId, String tableName, Text startRow,
      Text endRow, NamespaceId namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.SYSTEM, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.WRITE, false);
  }

  public boolean canBulkImport(TCredentials c, TableId tableId, String tableName, String dir,
      String failDir, NamespaceId namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c, tableId, namespaceId, TablePermission.BULK_IMPORT, false);
  }

  public boolean canCompact(TCredentials c, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.WRITE, false);
  }

  public boolean canChangeAuthorizations(TCredentials c, String user)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.ALTER_USER, false);
  }

  public boolean canChangePassword(TCredentials c, String user) throws ThriftSecurityException {
    authenticate(c);
    return c.getPrincipal().equals(user)
        || hasSystemPermission(c, SystemPermission.ALTER_USER, false);
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

  public boolean canGrantSystem(TCredentials c, String user, SystemPermission sysPerm)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermission(c, SystemPermission.GRANT, false);
  }

  public boolean canGrantTable(TCredentials c, String user, TableId tableId,
      NamespaceId namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.GRANT, false);
  }

  public boolean canGrantNamespace(TCredentials c, NamespaceId namespace)
      throws ThriftSecurityException {
    return canModifyNamespacePermission(c, namespace);
  }

  private boolean canModifyNamespacePermission(TCredentials c, NamespaceId namespace)
      throws ThriftSecurityException {
    authenticate(c);
    // The one case where Table/SystemPermission -> NamespacePermission breaks down. The alternative
    // is to make SystemPermission.ALTER_NAMESPACE provide
    // NamespacePermission.GRANT & ALTER_NAMESPACE, but then it would cause some permission checks
    // to succeed with GRANT when they shouldn't

    // This is a bit hackier then I (vines) wanted, but I think this one hackiness makes the overall
    // SecurityOperations more succinct.
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_NAMESPACE, namespace, false)
        || hasNamespacePermission(c, c.principal, namespace, NamespacePermission.GRANT);
  }

  public boolean canRevokeSystem(TCredentials c, String user, SystemPermission sysPerm)
      throws ThriftSecurityException {
    authenticate(c);
    // can't modify root user
    if (user.equals(getRootUsername()))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    return hasSystemPermission(c, SystemPermission.GRANT, false);
  }

  public boolean canRevokeTable(TCredentials c, String user, TableId tableId,
      NamespaceId namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.GRANT, false);
  }

  public boolean canRevokeNamespace(TCredentials c, NamespaceId namespace)
      throws ThriftSecurityException {
    return canModifyNamespacePermission(c, namespace);
  }

  public void changeAuthorizations(TCredentials credentials, String user,
      Authorizations authorizations) throws ThriftSecurityException {
    if (!canChangeAuthorizations(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      authorizor.changeAuthorizations(user, authorizations);
      log.info("Changed authorizations for user {} at the request of user {}", user,
          credentials.getPrincipal());
    } catch (AccumuloSecurityException ase) {
      throw ase.asThriftException();
    }
  }

  public void changePassword(TCredentials credentials, Credentials toChange)
      throws ThriftSecurityException {
    if (!canChangePassword(credentials, toChange.getPrincipal()))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    try {
      AuthenticationToken token = toChange.getToken();
      authenticator.changePassword(toChange.getPrincipal(), token);
      log.info("Changed password for user {} at the request of user {}", toChange.getPrincipal(),
          credentials.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public void createUser(TCredentials credentials, Credentials newUser,
      Authorizations authorizations) throws ThriftSecurityException {
    if (!canCreateUser(credentials, newUser.getPrincipal()))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    _createUser(credentials, newUser);
    if (canChangeAuthorizations(credentials, newUser.getPrincipal())) {
      try {
        authorizor.changeAuthorizations(newUser.getPrincipal(), authorizations);
      } catch (AccumuloSecurityException ase) {
        throw ase.asThriftException();
      }
    }
  }

  protected void _createUser(TCredentials credentials, Credentials newUser)
      throws ThriftSecurityException {
    try {
      AuthenticationToken token = newUser.getToken();
      authenticator.createUser(newUser.getPrincipal(), token);
      authorizor.initUser(newUser.getPrincipal());
      permHandle.initUser(newUser.getPrincipal());
      log.info("Created user {} at the request of user {}", newUser.getPrincipal(),
          credentials.getPrincipal());
    } catch (AccumuloSecurityException ase) {
      throw ase.asThriftException();
    }
  }

  public void dropUser(TCredentials credentials, String user) throws ThriftSecurityException {
    if (!canDropUser(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    try {
      authorizor.dropUser(user);
      authenticator.dropUser(user);
      permHandle.cleanUser(user);
      log.info("Deleted user {} at the request of user {}", user, credentials.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public void grantSystemPermission(TCredentials credentials, String user,
      SystemPermission permissionById) throws ThriftSecurityException {
    if (!canGrantSystem(credentials, user, permissionById))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.grantSystemPermission(user, permissionById);
      log.info("Granted system permission {} for user {} at the request of user {}", permissionById,
          user, credentials.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public void grantTablePermission(TCredentials c, String user, TableId tableId,
      TablePermission permission, NamespaceId namespaceId) throws ThriftSecurityException {
    if (!canGrantTable(c, user, tableId, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.grantTablePermission(user, tableId.canonical(), permission);
      log.info("Granted table permission {} for user {} on the table {} at the request of user {}",
          permission, user, tableId, c.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }

  public void grantNamespacePermission(TCredentials c, String user, NamespaceId namespace,
      NamespacePermission permission) throws ThriftSecurityException {
    if (!canGrantNamespace(c, namespace))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.grantNamespacePermission(user, namespace.canonical(), permission);
      log.info("Granted namespace permission {} for user {} on the namespace {}"
          + " at the request of user {}", permission, user, namespace, c.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (NamespaceNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    }
  }

  public void revokeSystemPermission(TCredentials credentials, String user,
      SystemPermission permission) throws ThriftSecurityException {
    if (!canRevokeSystem(credentials, user, permission))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.revokeSystemPermission(user, permission);
      log.info("Revoked system permission {} for user {} at the request of user {}", permission,
          user, credentials.getPrincipal());

    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public void revokeTablePermission(TCredentials c, String user, TableId tableId,
      TablePermission permission, NamespaceId namespaceId) throws ThriftSecurityException {
    if (!canRevokeTable(c, user, tableId, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.revokeTablePermission(user, tableId.canonical(), permission);
      log.info("Revoked table permission {} for user {} on the table {} at the request of user {}",
          permission, user, tableId, c.getPrincipal());

    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }

  public void revokeNamespacePermission(TCredentials c, String user, NamespaceId namespace,
      NamespacePermission permission) throws ThriftSecurityException {
    if (!canRevokeNamespace(c, namespace))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.revokeNamespacePermission(user, namespace.canonical(), permission);
      log.info("Revoked namespace permission {} for user {} on the namespace {}"
          + " at the request of user {}", permission, user, namespace, c.getPrincipal());

    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (NamespaceNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    }
  }

  public boolean hasSystemPermission(TCredentials credentials, String user,
      SystemPermission permissionById) throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    return _hasSystemPermission(user, permissionById, false);
  }

  public boolean hasTablePermission(TCredentials credentials, String user, TableId tableId,
      TablePermission permissionById) throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    return _hasTablePermission(user, tableId, permissionById, false);
  }

  public boolean hasNamespacePermission(TCredentials credentials, String user,
      NamespaceId namespace, NamespacePermission permissionById) throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    return _hasNamespacePermission(user, namespace, permissionById, false);
  }

  public Set<String> listUsers(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return authenticator.listUsers();
  }

  public void deleteTable(TCredentials credentials, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    if (!canDeleteTable(credentials, tableId, namespaceId))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    try {
      permHandle.cleanTablePermissions(tableId.canonical());
    } catch (AccumuloSecurityException e) {
      e.setUser(credentials.getPrincipal());
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }

  public void deleteNamespace(TCredentials credentials, NamespaceId namespace)
      throws ThriftSecurityException {
    if (!canDeleteNamespace(credentials, namespace))
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    try {
      permHandle.cleanNamespacePermissions(namespace.canonical());
    } catch (AccumuloSecurityException e) {
      e.setUser(credentials.getPrincipal());
      throw e.asThriftException();
    } catch (NamespaceNotFoundException e) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    }
  }

  public boolean canExport(TCredentials credentials, TableId tableId, String tableName,
      String exportDir, NamespaceId namespaceId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, tableId, namespaceId, TablePermission.READ, false);
  }

  public boolean canImport(TCredentials credentials, String tableName, Set<String> importDir,
      NamespaceId namespaceId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.CREATE_TABLE,
        namespaceId, false);
  }

  public boolean canAlterNamespace(TCredentials credentials, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.ALTER_NAMESPACE,
        namespaceId, false);
  }

  public boolean canCreateNamespace(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials, SystemPermission.CREATE_NAMESPACE, false);
  }

  public boolean canDeleteNamespace(TCredentials credentials, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.DROP_NAMESPACE,
        namespaceId, false);
  }

  public boolean canRenameNamespace(TCredentials credentials, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.ALTER_NAMESPACE,
        namespaceId, false);
  }

  public boolean canObtainDelegationToken(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials, SystemPermission.OBTAIN_DELEGATION_TOKEN, false);
  }

  public boolean canGetSummaries(TCredentials credentials, TableId tableId, NamespaceId namespaceId)
      throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, tableId, namespaceId, TablePermission.GET_SUMMARIES,
        false);
  }

  public boolean validateStoredUserCreditentials() {
    if (authenticator instanceof ZKAuthenticator) {
      return !((ZKAuthenticator) authenticator).hasOutdatedHashes();
    }
    return true;
  }
}

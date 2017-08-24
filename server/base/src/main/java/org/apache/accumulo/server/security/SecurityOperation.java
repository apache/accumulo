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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.KerberosAuthenticator;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.accumulo.server.security.handler.ZKAuthenticator;
import org.apache.accumulo.server.security.handler.ZKAuthorizor;
import org.apache.accumulo.server.security.handler.ZKPermHandler;
import org.apache.accumulo.server.zookeeper.ZooCache;
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

  protected final AccumuloServerContext context;

  static SecurityOperation instance;

  public static synchronized SecurityOperation getInstance(AccumuloServerContext context, boolean initialize) {
    if (instance == null) {
      String instanceId = context.getInstance().getInstanceID();
      instance = new SecurityOperation(context, getAuthorizor(instanceId, initialize), getAuthenticator(instanceId, initialize), getPermHandler(instanceId,
          initialize));
    }
    return instance;
  }

  protected static Authorizor getAuthorizor(String instanceId, boolean initialize) {
    Authorizor toRet = SiteConfiguration.getInstance().instantiateClassProperty(Property.INSTANCE_SECURITY_AUTHORIZOR, Authorizor.class,
        ZKAuthorizor.getInstance());
    toRet.initialize(instanceId, initialize);
    return toRet;
  }

  protected static Authenticator getAuthenticator(String instanceId, boolean initialize) {
    Authenticator toRet = SiteConfiguration.getInstance().instantiateClassProperty(Property.INSTANCE_SECURITY_AUTHENTICATOR, Authenticator.class,
        ZKAuthenticator.getInstance());
    toRet.initialize(instanceId, initialize);
    return toRet;
  }

  protected static PermissionHandler getPermHandler(String instanceId, boolean initialize) {
    PermissionHandler toRet = SiteConfiguration.getInstance().instantiateClassProperty(Property.INSTANCE_SECURITY_PERMISSION_HANDLER, PermissionHandler.class,
        ZKPermHandler.getInstance());
    toRet.initialize(instanceId, initialize);
    return toRet;
  }

  protected SecurityOperation(AccumuloServerContext context) {
    this.context = context;
    ZKUserPath = Constants.ZROOT + "/" + context.getInstance().getInstanceID() + "/users";
    zooCache = new ZooCache();
  }

  public SecurityOperation(AccumuloServerContext context, Authorizor author, Authenticator authent, PermissionHandler pm) {
    this(context);
    authorizor = author;
    authenticator = authent;
    permHandle = pm;

    if (!authorizor.validSecurityHandlers(authenticator, pm) || !authenticator.validSecurityHandlers(authorizor, pm)
        || !permHandle.validSecurityHandlers(authent, author))
      throw new RuntimeException(authorizor + ", " + authenticator + ", and " + pm
          + " do not play nice with eachother. Please choose authentication and authorization mechanisms that are compatible with one another.");

    isKerberos = KerberosAuthenticator.class.isAssignableFrom(authenticator.getClass());
  }

  public void initializeSecurity(TCredentials credentials, String rootPrincipal, byte[] token) throws AccumuloSecurityException, ThriftSecurityException {
    if (!isSystemUser(credentials))
      throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    authenticator.initializeSecurity(credentials, rootPrincipal, token);
    authorizor.initializeSecurity(credentials, rootPrincipal);
    permHandle.initializeSecurity(credentials, rootPrincipal);
    try {
      permHandle.grantTablePermission(rootPrincipal, MetadataTable.ID.canonicalID(), TablePermission.ALTER_TABLE);
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
    return context.getCredentials().getToken().getClass().getName().equals(credentials.getTokenClassName());
  }

  protected void authenticate(TCredentials credentials) throws ThriftSecurityException {
    if (!credentials.getInstanceId().equals(context.getInstance().getInstanceID()))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.INVALID_INSTANCEID);

    Credentials creds = Credentials.fromThrift(credentials);

    if (isSystemUser(credentials)) {
      if (isKerberos) {
        // Don't need to re-check the principal as TCredentialsUpdatingInvocationHandler will check the provided against
        // the credentials provided on the wire.
        if (!context.getCredentials().getToken().equals(creds.getToken())) {
          log.debug("With SASL enabled, System AuthenticationTokens did not match.");
          throw new ThriftSecurityException(creds.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
        }
      } else {
        if (!(context.getCredentials().equals(creds))) {
          log.debug("Provided credentials did not match server's expected credentials. Expected {} but got {}", context.getCredentials(), creds);
          throw new ThriftSecurityException(creds.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
        }
      }
    } else {
      // Not the system user

      if (isKerberos) {
        // If we have kerberos credentials for a user from the network but no account
        // in the system, we need to make one before proceeding
        try {
          if (!authenticator.userExists(creds.getPrincipal())) {
            // If we call the normal createUser method, it will loop back into this method
            // when it tries to check if the user has permission to create users
            try {
              _createUser(credentials, creds, Authorizations.EMPTY);
            } catch (ThriftSecurityException e) {
              if (SecurityErrorCode.USER_EXISTS != e.getCode()) {
                // For Kerberos, a user acct is automatically created because there is no notion of a password
                // in the traditional sense of Accumulo users. As such, if a user acct already exists when we
                // try to automatically create a user account, we should avoid returning this exception back to the user.
                // We want to let USER_EXISTS code pass through and continue
                throw e;
              }
            }
          }
        } catch (AccumuloSecurityException e) {
          log.debug("Failed to determine if user exists", e);
          throw e.asThriftException();
        }
      }

      // Check that the user is authenticated (a no-op at this point for kerberos)
      try {
        if (!authenticator.authenticateUser(creds.getPrincipal(), creds.getToken())) {
          throw new ThriftSecurityException(creds.getPrincipal(), SecurityErrorCode.BAD_CREDENTIALS);
        }
      } catch (AccumuloSecurityException e) {
        log.debug("AccumuloSecurityException", e);
        throw e.asThriftException();
      }
    }
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
      Credentials toCreds = Credentials.fromThrift(toAuth);

      if (isKerberos) {
        // If we have kerberos credentials for a user from the network but no account
        // in the system, we need to make one before proceeding
        if (!authenticator.userExists(toCreds.getPrincipal())) {
          createUser(credentials, toCreds, Authorizations.EMPTY);
        }
        // Likely that the KerberosAuthenticator will fail as we don't have the credentials for the other user,
        // we only have our own Kerberos credentials.
      }

      return authenticator.authenticateUser(toCreds.getPrincipal(), toCreds.getToken());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public Authorizations getUserAuthorizations(TCredentials credentials, String user) throws ThriftSecurityException {
    authenticate(credentials);

    targetUserExists(user);

    if (!credentials.getPrincipal().equals(user) && !hasSystemPermission(credentials, SystemPermission.SYSTEM, false)
        && !hasSystemPermission(credentials, SystemPermission.ALTER_USER, false))
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

  public boolean userHasAuthorizations(TCredentials credentials, List<ByteBuffer> list) throws ThriftSecurityException {
    authenticate(credentials);

    if (isSystemUser(credentials)) {
      // system user doesn't need record-level authorizations for the tables it reads (for now)
      return list.isEmpty();
    }

    try {
      return authorizor.isValidAuthorizations(credentials.getPrincipal(), list);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  private boolean hasSystemPermission(TCredentials credentials, SystemPermission permission, boolean useCached) throws ThriftSecurityException {
    return hasSystemPermissionWithNamespaceId(credentials, permission, null, useCached);
  }

  /**
   * Checks if a user has a system permission
   *
   * @return true if a user exists and has permission; false otherwise
   */
  private boolean hasSystemPermissionWithNamespaceId(TCredentials credentials, SystemPermission permission, Namespace.ID namespaceId, boolean useCached)
      throws ThriftSecurityException {
    if (isSystemUser(credentials))
      return true;

    if (_hasSystemPermission(credentials.getPrincipal(), permission, useCached))
      return true;
    if (namespaceId != null) {
      return _hasNamespacePermission(credentials.getPrincipal(), namespaceId, NamespacePermission.getEquivalent(permission), useCached);
    }

    return false;
  }

  /**
   * Checks if a user has a system permission<br>
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
  protected boolean hasTablePermission(TCredentials credentials, Table.ID tableId, Namespace.ID namespaceId, TablePermission permission, boolean useCached)
      throws ThriftSecurityException {
    if (isSystemUser(credentials))
      return true;
    return _hasTablePermission(credentials.getPrincipal(), tableId, permission, useCached)
        || _hasNamespacePermission(credentials.getPrincipal(), namespaceId, NamespacePermission.getEquivalent(permission), useCached);
  }

  /**
   * Checks if a user has a table permission<br>
   * This cannot check if a system user has permission.
   *
   * @return true if a user exists and has permission; false otherwise
   */
  protected boolean _hasTablePermission(String user, Table.ID table, TablePermission permission, boolean useCached) throws ThriftSecurityException {
    targetUserExists(user);

    if ((table.equals(MetadataTable.ID) || table.equals(RootTable.ID) || table.equals(ReplicationTable.ID)) && permission.equals(TablePermission.READ))
      return true;

    try {
      if (useCached)
        return permHandle.hasCachedTablePermission(user, table.canonicalID(), permission);
      return permHandle.hasTablePermission(user, table.canonicalID(), permission);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
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
  protected boolean _hasNamespacePermission(String user, Namespace.ID namespace, NamespacePermission permission, boolean useCached)
      throws ThriftSecurityException {
    if (permission == null)
      return false;

    targetUserExists(user);

    if (namespace.equals(Namespace.ID.ACCUMULO) && permission.equals(NamespacePermission.READ))
      return true;

    try {
      if (useCached)
        return permHandle.hasCachedNamespacePermission(user, namespace, permission);
      return permHandle.hasNamespacePermission(user, namespace, permission);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (NamespaceNotFoundException e) {
      throw new ThriftSecurityException(user, SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
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

  public boolean canScan(TCredentials credentials, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, tableId, namespaceId, TablePermission.READ, true);
  }

  public boolean canScan(TCredentials credentials, Table.ID tableId, Namespace.ID namespaceId, TRange range, List<TColumn> columns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    return canScan(credentials, tableId, namespaceId);
  }

  public boolean canScan(TCredentials credentials, Table.ID table, Namespace.ID namespaceId, Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns,
      List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    return canScan(credentials, table, namespaceId);
  }

  public boolean canWrite(TCredentials credentials, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, tableId, namespaceId, TablePermission.WRITE, true);
  }

  public boolean canConditionallyUpdate(TCredentials credentials, Table.ID tableID, Namespace.ID namespaceId, List<ByteBuffer> authorizations)
      throws ThriftSecurityException {

    authenticate(credentials);

    return hasTablePermission(credentials, tableID, namespaceId, TablePermission.WRITE, true)
        && hasTablePermission(credentials, tableID, namespaceId, TablePermission.READ, true);
  }

  public boolean canSplitTablet(TCredentials credentials, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasSystemPermissionWithNamespaceId(credentials, SystemPermission.SYSTEM, namespaceId, false)
        || hasTablePermission(credentials, tableId, namespaceId, TablePermission.ALTER_TABLE, false);
  }

  /**
   * This is the check to perform any system action. This includes tserver's loading of a tablet, shutting the system down, or altering system properties.
   */
  public boolean canPerformSystemActions(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials, SystemPermission.SYSTEM, false);
  }

  public boolean canFlush(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c, tableId, namespaceId, TablePermission.WRITE, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false);
  }

  public boolean canAlterTable(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false)
        || hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false);
  }

  public boolean canCreateTable(TCredentials c, String tableName, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.CREATE_TABLE, namespaceId, false);
  }

  public boolean canRenameTable(TCredentials c, Table.ID tableId, String oldTableName, String newTableName, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false);
  }

  public boolean canCloneTable(TCredentials c, Table.ID tableId, String tableName, Namespace.ID destinationNamespaceId, Namespace.ID srcNamespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.CREATE_TABLE, destinationNamespaceId, false)
        && hasTablePermission(c, tableId, srcNamespaceId, TablePermission.READ, false);
  }

  public boolean canDeleteTable(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.DROP_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.DROP_TABLE, false);
  }

  public boolean canOnlineOfflineTable(TCredentials c, Table.ID tableId, FateOperation op, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.SYSTEM, namespaceId, false)
        || hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false);
  }

  public boolean canMerge(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.SYSTEM, namespaceId, false)
        || hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false);
  }

  public boolean canDeleteRange(TCredentials c, Table.ID tableId, String tableName, Text startRow, Text endRow, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.SYSTEM, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.WRITE, false);
  }

  public boolean canBulkImport(TCredentials c, Table.ID tableId, String tableName, String dir, String failDir, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    return canBulkImport(c, tableId, namespaceId);
  }

  public boolean canBulkImport(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasTablePermission(c, tableId, namespaceId, TablePermission.BULK_IMPORT, false);
  }

  public boolean canCompact(TCredentials c, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.ALTER_TABLE, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.WRITE, false);
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
    return hasSystemPermission(c, SystemPermission.GRANT, false);
  }

  public boolean canGrantTable(TCredentials c, String user, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.GRANT, false);
  }

  public boolean canGrantNamespace(TCredentials c, String user, Namespace.ID namespace) throws ThriftSecurityException {
    return canModifyNamespacePermission(c, user, namespace);
  }

  private boolean canModifyNamespacePermission(TCredentials c, String user, Namespace.ID namespace) throws ThriftSecurityException {
    authenticate(c);
    // The one case where Table/SystemPermission -> NamespacePermission breaks down. The alternative is to make SystemPermission.ALTER_NAMESPACE provide
    // NamespacePermission.GRANT & ALTER_NAMESPACE, but then it would cause some permission checks to succeed with GRANT when they shouldn't

    // This is a bit hackier then I (vines) wanted, but I think this one hackiness makes the overall SecurityOperations more succinct.
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_NAMESPACE, namespace, false)
        || hasNamespacePermission(c, c.principal, namespace, NamespacePermission.GRANT);
  }

  public boolean canRevokeSystem(TCredentials c, String user, SystemPermission sysPerm) throws ThriftSecurityException {
    authenticate(c);
    // can't modify root user
    if (user.equals(getRootUsername()))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    return hasSystemPermission(c, SystemPermission.GRANT, false);
  }

  public boolean canRevokeTable(TCredentials c, String user, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(c);
    return hasSystemPermissionWithNamespaceId(c, SystemPermission.ALTER_TABLE, namespaceId, false)
        || hasTablePermission(c, tableId, namespaceId, TablePermission.GRANT, false);
  }

  public boolean canRevokeNamespace(TCredentials c, String user, Namespace.ID namespace) throws ThriftSecurityException {
    return canModifyNamespacePermission(c, user, namespace);
  }

  public void changeAuthorizations(TCredentials credentials, String user, Authorizations authorizations) throws ThriftSecurityException {
    if (!canChangeAuthorizations(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      authorizor.changeAuthorizations(user, authorizations);
      log.info("Changed authorizations for user {} at the request of user {}", user, credentials.getPrincipal());
    } catch (AccumuloSecurityException ase) {
      throw ase.asThriftException();
    }
  }

  public void changePassword(TCredentials credentials, Credentials toChange) throws ThriftSecurityException {
    if (!canChangePassword(credentials, toChange.getPrincipal()))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    try {
      AuthenticationToken token = toChange.getToken();
      authenticator.changePassword(toChange.getPrincipal(), token);
      log.info("Changed password for user {} at the request of user {}", toChange.getPrincipal(), credentials.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public void createUser(TCredentials credentials, Credentials newUser, Authorizations authorizations) throws ThriftSecurityException {
    if (!canCreateUser(credentials, newUser.getPrincipal()))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    _createUser(credentials, newUser, authorizations);
    if (canChangeAuthorizations(credentials, newUser.getPrincipal())) {
      try {
        authorizor.changeAuthorizations(newUser.getPrincipal(), authorizations);
      } catch (AccumuloSecurityException ase) {
        throw ase.asThriftException();
      }
    }
  }

  protected void _createUser(TCredentials credentials, Credentials newUser, Authorizations authorizations) throws ThriftSecurityException {
    try {
      AuthenticationToken token = newUser.getToken();
      authenticator.createUser(newUser.getPrincipal(), token);
      authorizor.initUser(newUser.getPrincipal());
      permHandle.initUser(newUser.getPrincipal());
      log.info("Created user {} at the request of user {}", newUser.getPrincipal(), credentials.getPrincipal());
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
      log.info("Deleted user {} at the request of user {}", user, credentials.getPrincipal());
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
      log.info("Granted system permission {} for user {} at the request of user {}", permissionById, user, credentials.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public void grantTablePermission(TCredentials c, String user, Table.ID tableId, TablePermission permission, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    if (!canGrantTable(c, user, tableId, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.grantTablePermission(user, tableId.canonicalID(), permission);
      log.info("Granted table permission {} for user {} on the table {} at the request of user {}", permission, user, tableId, c.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }

  public void grantNamespacePermission(TCredentials c, String user, Namespace.ID namespace, NamespacePermission permission) throws ThriftSecurityException {
    if (!canGrantNamespace(c, user, namespace))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.grantNamespacePermission(user, namespace, permission);
      log.info("Granted namespace permission {} for user {} on the namespace {} at the request of user {}", permission, user, namespace, c.getPrincipal());
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (NamespaceNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    }
  }

  public void revokeSystemPermission(TCredentials credentials, String user, SystemPermission permission) throws ThriftSecurityException {
    if (!canRevokeSystem(credentials, user, permission))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.revokeSystemPermission(user, permission);
      log.info("Revoked system permission {} for user {} at the request of user {}", permission, user, credentials.getPrincipal());

    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public void revokeTablePermission(TCredentials c, String user, Table.ID tableId, TablePermission permission, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    if (!canRevokeTable(c, user, tableId, namespaceId))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.revokeTablePermission(user, tableId.canonicalID(), permission);
      log.info("Revoked table permission {} for user {} on the table {} at the request of user {}", permission, user, tableId, c.getPrincipal());

    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }

  public void revokeNamespacePermission(TCredentials c, String user, Namespace.ID namespace, NamespacePermission permission) throws ThriftSecurityException {
    if (!canRevokeNamespace(c, user, namespace))
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

    targetUserExists(user);

    try {
      permHandle.revokeNamespacePermission(user, namespace, permission);
      log.info("Revoked namespace permission {} for user {} on the namespace {} at the request of user {}", permission, user, namespace, c.getPrincipal());

    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (NamespaceNotFoundException e) {
      throw new ThriftSecurityException(c.getPrincipal(), SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    }
  }

  public boolean hasSystemPermission(TCredentials credentials, String user, SystemPermission permissionById) throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return _hasSystemPermission(user, permissionById, false);
  }

  public boolean hasTablePermission(TCredentials credentials, String user, Table.ID tableId, TablePermission permissionById) throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return _hasTablePermission(user, tableId, permissionById, false);
  }

  public boolean hasNamespacePermission(TCredentials credentials, String user, Namespace.ID namespace, NamespacePermission permissionById)
      throws ThriftSecurityException {
    if (!canAskAboutOtherUsers(credentials, user))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    return _hasNamespacePermission(user, namespace, permissionById, false);
  }

  public Set<String> listUsers(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    try {
      return authenticator.listUsers();
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }

  public void deleteTable(TCredentials credentials, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    if (!canDeleteTable(credentials, tableId, namespaceId))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    try {
      permHandle.cleanTablePermissions(tableId.canonicalID());
    } catch (AccumuloSecurityException e) {
      e.setUser(credentials.getPrincipal());
      throw e.asThriftException();
    } catch (TableNotFoundException e) {
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }

  public void deleteNamespace(TCredentials credentials, Namespace.ID namespace) throws ThriftSecurityException {
    if (!canDeleteNamespace(credentials, namespace))
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
    try {
      permHandle.cleanNamespacePermissions(namespace);
    } catch (AccumuloSecurityException e) {
      e.setUser(credentials.getPrincipal());
      throw e.asThriftException();
    } catch (NamespaceNotFoundException e) {
      throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.NAMESPACE_DOESNT_EXIST);
    }
  }

  public boolean canExport(TCredentials credentials, Table.ID tableId, String tableName, String exportDir, Namespace.ID namespaceId)
      throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, tableId, namespaceId, TablePermission.READ, false);
  }

  public boolean canImport(TCredentials credentials, String tableName, String importDir, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.CREATE_TABLE, namespaceId, false);
  }

  public boolean canAlterNamespace(TCredentials credentials, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.ALTER_NAMESPACE, namespaceId, false);
  }

  public boolean canCreateNamespace(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials, SystemPermission.CREATE_NAMESPACE, false);
  }

  public boolean canDeleteNamespace(TCredentials credentials, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.DROP_NAMESPACE, namespaceId, false);
  }

  public boolean canRenameNamespace(TCredentials credentials, Namespace.ID namespaceId, String oldName, String newName) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermissionWithNamespaceId(credentials, SystemPermission.ALTER_NAMESPACE, namespaceId, false);
  }

  public boolean canObtainDelegationToken(TCredentials credentials) throws ThriftSecurityException {
    authenticate(credentials);
    return hasSystemPermission(credentials, SystemPermission.OBTAIN_DELEGATION_TOKEN, false);
  }

  public boolean canGetSummaries(TCredentials credentials, Table.ID tableId, Namespace.ID namespaceId) throws ThriftSecurityException {
    authenticate(credentials);
    return hasTablePermission(credentials, tableId, namespaceId, TablePermission.GET_SUMMARIES, false);
  }
}

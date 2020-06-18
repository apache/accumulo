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
package org.apache.accumulo.server.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.Credentials;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.clientImpl.thrift.ConfigurationType;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TDiskUsage;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.BulkImportStatus;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.util.ServerBulkImportStatus;
import org.apache.accumulo.server.util.TableDiskUsage;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientServiceHandler implements ClientService.Iface {
  private static final Logger log = LoggerFactory.getLogger(ClientServiceHandler.class);
  protected final TransactionWatcher transactionWatcher;
  protected final ServerContext context;
  protected final VolumeManager fs;
  protected final SecurityOperation security;
  private final ServerBulkImportStatus bulkImportStatus = new ServerBulkImportStatus();

  public ClientServiceHandler(ServerContext context, TransactionWatcher transactionWatcher,
      VolumeManager fs) {
    this.context = context;
    this.transactionWatcher = transactionWatcher;
    this.fs = fs;
    this.security = AuditedSecurityOperation.getInstance(context);
  }

  public static TableId checkTableId(ClientContext context, String tableName,
      TableOperation operation) throws ThriftTableOperationException {
    TableOperationExceptionType reason = null;
    try {
      return Tables._getTableId(context, tableName);
    } catch (NamespaceNotFoundException e) {
      reason = TableOperationExceptionType.NAMESPACE_NOTFOUND;
    } catch (TableNotFoundException e) {
      reason = TableOperationExceptionType.NOTFOUND;
    }
    throw new ThriftTableOperationException(null, tableName, operation, reason, null);
  }

  public static NamespaceId checkNamespaceId(ClientContext context, String namespaceName,
      TableOperation operation) throws ThriftTableOperationException {
    NamespaceId namespaceId = Namespaces.lookupNamespaceId(context, namespaceName);
    if (namespaceId == null) {
      // maybe the namespace exists, but the cache was not updated yet... so try to clear the cache
      // and check again
      Tables.clearCache(context);
      namespaceId = Namespaces.lookupNamespaceId(context, namespaceName);
      if (namespaceId == null)
        throw new ThriftTableOperationException(null, namespaceName, operation,
            TableOperationExceptionType.NAMESPACE_NOTFOUND, null);
    }
    return namespaceId;
  }

  @Override
  public String getInstanceId() {
    return context.getInstanceID();
  }

  @Override
  public String getRootTabletLocation() {
    return context.getRootTabletLocation();
  }

  @Override
  public String getZooKeepers() {
    return context.getZooKeepers();
  }

  @Override
  public void ping(TCredentials credentials) {
    // anybody can call this; no authentication check
    log.info("Master reports: I just got pinged!");
  }

  @Override
  public boolean authenticate(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    try {
      return security.authenticateUser(credentials, credentials);
    } catch (ThriftSecurityException e) {
      log.error("ThriftSecurityException", e);
      throw e;
    }
  }

  @Override
  public boolean authenticateUser(TInfo tinfo, TCredentials credentials, TCredentials toAuth)
      throws ThriftSecurityException {
    try {
      return security.authenticateUser(credentials, toAuth);
    } catch (ThriftSecurityException e) {
      log.error("ThriftSecurityException", e);
      throw e;
    }
  }

  @Override
  public void changeAuthorizations(TInfo tinfo, TCredentials credentials, String user,
      List<ByteBuffer> authorizations) throws ThriftSecurityException {
    security.changeAuthorizations(credentials, user, new Authorizations(authorizations));
  }

  @Override
  public void changeLocalUserPassword(TInfo tinfo, TCredentials credentials, String principal,
      ByteBuffer password) throws ThriftSecurityException {
    PasswordToken token = new PasswordToken(password);
    Credentials toChange = new Credentials(principal, token);
    security.changePassword(credentials, toChange);
  }

  @Override
  public void createLocalUser(TInfo tinfo, TCredentials credentials, String principal,
      ByteBuffer password) throws ThriftSecurityException {
    AuthenticationToken token;
    if (context.getSaslParams() != null) {
      try {
        token = new KerberosToken();
      } catch (IOException e) {
        log.warn("Failed to create KerberosToken");
        throw new ThriftSecurityException(e.getMessage(), SecurityErrorCode.DEFAULT_SECURITY_ERROR);
      }
    } else {
      token = new PasswordToken(password);
    }
    Credentials newUser = new Credentials(principal, token);
    security.createUser(credentials, newUser, new Authorizations());
  }

  @Override
  public void dropLocalUser(TInfo tinfo, TCredentials credentials, String user)
      throws ThriftSecurityException {
    security.dropUser(credentials, user);
  }

  @Override
  public List<ByteBuffer> getUserAuthorizations(TInfo tinfo, TCredentials credentials, String user)
      throws ThriftSecurityException {
    return security.getUserAuthorizations(credentials, user).getAuthorizationsBB();
  }

  @Override
  public void grantSystemPermission(TInfo tinfo, TCredentials credentials, String user,
      byte permission) throws ThriftSecurityException {
    security.grantSystemPermission(credentials, user,
        SystemPermission.getPermissionById(permission));
  }

  @Override
  public void grantTablePermission(TInfo tinfo, TCredentials credentials, String user,
      String tableName, byte permission) throws TException {
    TableId tableId = checkTableId(context, tableName, TableOperation.PERMISSION);
    NamespaceId namespaceId;
    try {
      namespaceId = Tables.getNamespaceId(context, tableId);
    } catch (TableNotFoundException e) {
      throw new TException(e);
    }

    security.grantTablePermission(credentials, user, tableId,
        TablePermission.getPermissionById(permission), namespaceId);
  }

  @Override
  public void grantNamespacePermission(TInfo tinfo, TCredentials credentials, String user,
      String ns, byte permission) throws ThriftSecurityException, ThriftTableOperationException {
    NamespaceId namespaceId = checkNamespaceId(context, ns, TableOperation.PERMISSION);
    security.grantNamespacePermission(credentials, user, namespaceId,
        NamespacePermission.getPermissionById(permission));
  }

  @Override
  public void revokeSystemPermission(TInfo tinfo, TCredentials credentials, String user,
      byte permission) throws ThriftSecurityException {
    security.revokeSystemPermission(credentials, user,
        SystemPermission.getPermissionById(permission));
  }

  @Override
  public void revokeTablePermission(TInfo tinfo, TCredentials credentials, String user,
      String tableName, byte permission) throws TException {
    TableId tableId = checkTableId(context, tableName, TableOperation.PERMISSION);
    NamespaceId namespaceId;
    try {
      namespaceId = Tables.getNamespaceId(context, tableId);
    } catch (TableNotFoundException e) {
      throw new TException(e);
    }

    security.revokeTablePermission(credentials, user, tableId,
        TablePermission.getPermissionById(permission), namespaceId);
  }

  @Override
  public boolean hasSystemPermission(TInfo tinfo, TCredentials credentials, String user,
      byte sysPerm) throws ThriftSecurityException {
    return security.hasSystemPermission(credentials, user,
        SystemPermission.getPermissionById(sysPerm));
  }

  @Override
  public boolean hasTablePermission(TInfo tinfo, TCredentials credentials, String user,
      String tableName, byte tblPerm)
      throws ThriftSecurityException, ThriftTableOperationException {
    TableId tableId = checkTableId(context, tableName, TableOperation.PERMISSION);
    return security.hasTablePermission(credentials, user, tableId,
        TablePermission.getPermissionById(tblPerm));
  }

  @Override
  public boolean hasNamespacePermission(TInfo tinfo, TCredentials credentials, String user,
      String ns, byte perm) throws ThriftSecurityException, ThriftTableOperationException {
    NamespaceId namespaceId = checkNamespaceId(context, ns, TableOperation.PERMISSION);
    return security.hasNamespacePermission(credentials, user, namespaceId,
        NamespacePermission.getPermissionById(perm));
  }

  @Override
  public void revokeNamespacePermission(TInfo tinfo, TCredentials credentials, String user,
      String ns, byte permission) throws ThriftSecurityException, ThriftTableOperationException {
    NamespaceId namespaceId = checkNamespaceId(context, ns, TableOperation.PERMISSION);
    security.revokeNamespacePermission(credentials, user, namespaceId,
        NamespacePermission.getPermissionById(permission));
  }

  @Override
  public Set<String> listLocalUsers(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException {
    return security.listUsers(credentials);
  }

  private Map<String,String> conf(TCredentials credentials, AccumuloConfiguration conf)
      throws TException {
    security.authenticateUser(credentials, credentials);
    conf.invalidateCache();

    Map<String,String> result = new HashMap<>();
    for (Entry<String,String> entry : conf) {
      String key = entry.getKey();
      if (!Property.isSensitive(key))
        result.put(key, entry.getValue());
    }
    return result;
  }

  @Override
  public Map<String,String> getConfiguration(TInfo tinfo, TCredentials credentials,
      ConfigurationType type) throws TException {
    switch (type) {
      case CURRENT:
        return conf(credentials, context.getConfiguration());
      case SITE:
        return conf(credentials, context.getSiteConfiguration());
      case DEFAULT:
        return conf(credentials, context.getDefaultConfiguration());
    }
    throw new RuntimeException("Unexpected configuration type " + type);
  }

  @Override
  public Map<String,String> getTableConfiguration(TInfo tinfo, TCredentials credentials,
      String tableName) throws TException, ThriftTableOperationException {
    TableId tableId = checkTableId(context, tableName, null);
    AccumuloConfiguration config = context.getTableConfiguration(tableId);
    return conf(credentials, config);
  }

  @Override
  public List<String> bulkImportFiles(TInfo tinfo, final TCredentials credentials, final long tid,
      final String tableId, final List<String> files, final String errorDir, final boolean setTime)
      throws ThriftSecurityException, ThriftTableOperationException, TException {
    try {
      if (!security.canPerformSystemActions(credentials))
        throw new AccumuloSecurityException(credentials.getPrincipal(),
            SecurityErrorCode.PERMISSION_DENIED);
      bulkImportStatus.updateBulkImportStatus(files, BulkImportState.INITIAL);
      log.debug("Got request to bulk import files to table({}): {}", tableId, files);

      bulkImportStatus.updateBulkImportStatus(files, BulkImportState.PROCESSING);
      try {
        return BulkImporter.bulkLoad(context, tid, tableId, files, setTime);
      } finally {
        bulkImportStatus.removeBulkImportStatus(files);
      }
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (Exception ex) {
      throw new TException(ex);
    }
  }

  @Override
  public boolean isActive(TInfo tinfo, long tid) {
    return transactionWatcher.isActive(tid);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public boolean checkClass(TInfo tinfo, TCredentials credentials, String className,
      String interfaceMatch) throws TException {
    security.authenticateUser(credentials, credentials);

    ClassLoader loader = getClass().getClassLoader();
    Class shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);
      Class test = AccumuloVFSClassLoader.loadClass(className, shouldMatch);
      test.getDeclaredConstructor().newInstance();
      return true;
    } catch (ClassCastException | ReflectiveOperationException e) {
      log.warn("Error checking object types", e);
      return false;
    }
  }

  @Override
  public boolean checkTableClass(TInfo tinfo, TCredentials credentials, String tableName,
      String className, String interfaceMatch)
      throws TException, ThriftTableOperationException, ThriftSecurityException {

    security.authenticateUser(credentials, credentials);

    TableId tableId = checkTableId(context, tableName, null);

    ClassLoader loader = getClass().getClassLoader();
    Class<?> shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);

      AccumuloConfiguration conf = context.getTableConfiguration(tableId);

      String context = conf.get(Property.TABLE_CLASSPATH);

      ClassLoader currentLoader;

      if (context != null && !context.equals("")) {
        currentLoader = AccumuloVFSClassLoader.getContextManager().getClassLoader(context);
      } else {
        currentLoader = AccumuloVFSClassLoader.getClassLoader();
      }

      Class<?> test = currentLoader.loadClass(className).asSubclass(shouldMatch);
      test.getDeclaredConstructor().newInstance();
      return true;
    } catch (Exception e) {
      log.warn("Error checking object types", e);
      return false;
    }
  }

  @Override
  public boolean checkNamespaceClass(TInfo tinfo, TCredentials credentials, String ns,
      String className, String interfaceMatch)
      throws TException, ThriftTableOperationException, ThriftSecurityException {

    security.authenticateUser(credentials, credentials);

    NamespaceId namespaceId = checkNamespaceId(context, ns, null);

    ClassLoader loader = getClass().getClassLoader();
    Class<?> shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);

      AccumuloConfiguration conf = context.getNamespaceConfiguration(namespaceId);

      String context = conf.get(Property.TABLE_CLASSPATH);

      ClassLoader currentLoader;

      if (context != null && !context.equals("")) {
        currentLoader = AccumuloVFSClassLoader.getContextManager().getClassLoader(context);
      } else {
        currentLoader = AccumuloVFSClassLoader.getClassLoader();
      }

      Class<?> test = currentLoader.loadClass(className).asSubclass(shouldMatch);
      test.getDeclaredConstructor().newInstance();
      return true;
    } catch (Exception e) {
      log.warn("Error checking object types", e);
      return false;
    }
  }

  @Override
  public List<TDiskUsage> getDiskUsage(Set<String> tables, TCredentials credentials)
      throws ThriftTableOperationException, ThriftSecurityException, TException {
    try {
      HashSet<TableId> tableIds = new HashSet<>();

      for (String table : tables) {
        // ensure that table table exists
        TableId tableId = checkTableId(context, table, null);
        tableIds.add(tableId);
        NamespaceId namespaceId = Tables.getNamespaceId(context, tableId);
        if (!security.canScan(credentials, tableId, namespaceId))
          throw new ThriftSecurityException(credentials.getPrincipal(),
              SecurityErrorCode.PERMISSION_DENIED);
      }

      // use the same set of tableIds that were validated above to avoid race conditions
      Map<TreeSet<String>,Long> diskUsage = TableDiskUsage.getDiskUsage(tableIds, fs, context);
      List<TDiskUsage> retUsages = new ArrayList<>();
      for (Map.Entry<TreeSet<String>,Long> usageItem : diskUsage.entrySet()) {
        retUsages.add(new TDiskUsage(new ArrayList<>(usageItem.getKey()), usageItem.getValue()));
      }
      return retUsages;

    } catch (TableNotFoundException | IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public Map<String,String> getNamespaceConfiguration(TInfo tinfo, TCredentials credentials,
      String ns) throws ThriftTableOperationException, TException {
    NamespaceId namespaceId;
    try {
      namespaceId = Namespaces.getNamespaceId(context, ns);
    } catch (NamespaceNotFoundException e) {
      String why = "Could not find namespace while getting configuration.";
      throw new ThriftTableOperationException(null, ns, null,
          TableOperationExceptionType.NAMESPACE_NOTFOUND, why);
    }
    AccumuloConfiguration config = context.getNamespaceConfiguration(namespaceId);
    return conf(credentials, config);
  }

  public List<BulkImportStatus> getBulkLoadStatus() {
    return bulkImportStatus.getBulkLoadStatus();
  }
}

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
import java.util.concurrent.Callable;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ConfigurationType;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.TDiskUsage;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.KerberosToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.BulkImportStatus;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
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
  private final AccumuloServerContext context;
  private final Instance instance;
  private final VolumeManager fs;
  private final SecurityOperation security;
  private final ServerBulkImportStatus bulkImportStatus = new ServerBulkImportStatus();

  public ClientServiceHandler(AccumuloServerContext context, TransactionWatcher transactionWatcher, VolumeManager fs) {
    this.context = context;
    this.instance = context.getInstance();
    this.transactionWatcher = transactionWatcher;
    this.fs = fs;
    this.security = AuditedSecurityOperation.getInstance(context);
  }

  public static String checkTableId(Instance instance, String tableName, TableOperation operation) throws ThriftTableOperationException {
    TableOperationExceptionType reason = null;
    try {
      return Tables._getTableId(instance, tableName);
    } catch (NamespaceNotFoundException e) {
      reason = TableOperationExceptionType.NAMESPACE_NOTFOUND;
    } catch (TableNotFoundException e) {
      reason = TableOperationExceptionType.NOTFOUND;
    }
    throw new ThriftTableOperationException(null, tableName, operation, reason, null);
  }

  public static String checkNamespaceId(Instance instance, String namespace, TableOperation operation) throws ThriftTableOperationException {
    String namespaceId = Namespaces.getNameToIdMap(instance).get(namespace);
    if (namespaceId == null) {
      // maybe the namespace exists, but the cache was not updated yet... so try to clear the cache and check again
      Tables.clearCache(instance);
      namespaceId = Namespaces.getNameToIdMap(instance).get(namespace);
      if (namespaceId == null)
        throw new ThriftTableOperationException(null, namespace, operation, TableOperationExceptionType.NAMESPACE_NOTFOUND, null);
    }
    return namespaceId;
  }

  @Override
  public String getInstanceId() {
    return instance.getInstanceID();
  }

  @Override
  public String getRootTabletLocation() {
    return instance.getRootTabletLocation();
  }

  @Override
  public String getZooKeepers() {
    return instance.getZooKeepers();
  }

  @Override
  public void ping(TCredentials credentials) {
    // anybody can call this; no authentication check
    log.info("Master reports: I just got pinged!");
  }

  @Override
  public boolean authenticate(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException {
    try {
      return security.authenticateUser(credentials, credentials);
    } catch (ThriftSecurityException e) {
      log.error("ThriftSecurityException", e);
      throw e;
    }
  }

  @Override
  public boolean authenticateUser(TInfo tinfo, TCredentials credentials, TCredentials toAuth) throws ThriftSecurityException {
    try {
      return security.authenticateUser(credentials, toAuth);
    } catch (ThriftSecurityException e) {
      log.error("ThriftSecurityException", e);
      throw e;
    }
  }

  @Override
  public void changeAuthorizations(TInfo tinfo, TCredentials credentials, String user, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    security.changeAuthorizations(credentials, user, new Authorizations(authorizations));
  }

  @Override
  public void changeLocalUserPassword(TInfo tinfo, TCredentials credentials, String principal, ByteBuffer password) throws ThriftSecurityException {
    PasswordToken token = new PasswordToken(password);
    Credentials toChange = new Credentials(principal, token);
    security.changePassword(credentials, toChange);
  }

  @Override
  public void createLocalUser(TInfo tinfo, TCredentials credentials, String principal, ByteBuffer password) throws ThriftSecurityException {
    AuthenticationToken token;
    if (null != context.getSaslParams()) {
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
  public void dropLocalUser(TInfo tinfo, TCredentials credentials, String user) throws ThriftSecurityException {
    security.dropUser(credentials, user);
  }

  @Override
  public List<ByteBuffer> getUserAuthorizations(TInfo tinfo, TCredentials credentials, String user) throws ThriftSecurityException {
    return security.getUserAuthorizations(credentials, user).getAuthorizationsBB();
  }

  @Override
  public void grantSystemPermission(TInfo tinfo, TCredentials credentials, String user, byte permission) throws ThriftSecurityException {
    security.grantSystemPermission(credentials, user, SystemPermission.getPermissionById(permission));
  }

  @Override
  public void grantTablePermission(TInfo tinfo, TCredentials credentials, String user, String tableName, byte permission) throws TException {
    String tableId = checkTableId(instance, tableName, TableOperation.PERMISSION);
    String namespaceId;
    try {
      namespaceId = Tables.getNamespaceId(instance, tableId);
    } catch (TableNotFoundException e) {
      throw new TException(e);
    }

    security.grantTablePermission(credentials, user, tableId, TablePermission.getPermissionById(permission), namespaceId);
  }

  @Override
  public void grantNamespacePermission(TInfo tinfo, TCredentials credentials, String user, String ns, byte permission) throws ThriftSecurityException,
      ThriftTableOperationException {
    String namespaceId = checkNamespaceId(instance, ns, TableOperation.PERMISSION);
    security.grantNamespacePermission(credentials, user, namespaceId, NamespacePermission.getPermissionById(permission));
  }

  @Override
  public void revokeSystemPermission(TInfo tinfo, TCredentials credentials, String user, byte permission) throws ThriftSecurityException {
    security.revokeSystemPermission(credentials, user, SystemPermission.getPermissionById(permission));
  }

  @Override
  public void revokeTablePermission(TInfo tinfo, TCredentials credentials, String user, String tableName, byte permission) throws TException {
    String tableId = checkTableId(instance, tableName, TableOperation.PERMISSION);
    String namespaceId;
    try {
      namespaceId = Tables.getNamespaceId(instance, tableId);
    } catch (TableNotFoundException e) {
      throw new TException(e);
    }

    security.revokeTablePermission(credentials, user, tableId, TablePermission.getPermissionById(permission), namespaceId);
  }

  @Override
  public boolean hasSystemPermission(TInfo tinfo, TCredentials credentials, String user, byte sysPerm) throws ThriftSecurityException {
    return security.hasSystemPermission(credentials, user, SystemPermission.getPermissionById(sysPerm));
  }

  @Override
  public boolean hasTablePermission(TInfo tinfo, TCredentials credentials, String user, String tableName, byte tblPerm) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(instance, tableName, TableOperation.PERMISSION);
    return security.hasTablePermission(credentials, user, tableId, TablePermission.getPermissionById(tblPerm));
  }

  @Override
  public boolean hasNamespacePermission(TInfo tinfo, TCredentials credentials, String user, String ns, byte perm) throws ThriftSecurityException,
      ThriftTableOperationException {
    String namespaceId = checkNamespaceId(instance, ns, TableOperation.PERMISSION);
    return security.hasNamespacePermission(credentials, user, namespaceId, NamespacePermission.getPermissionById(perm));
  }

  @Override
  public void revokeNamespacePermission(TInfo tinfo, TCredentials credentials, String user, String ns, byte permission) throws ThriftSecurityException,
      ThriftTableOperationException {
    String namespaceId = checkNamespaceId(instance, ns, TableOperation.PERMISSION);
    security.revokeNamespacePermission(credentials, user, namespaceId, NamespacePermission.getPermissionById(permission));
  }

  @Override
  public Set<String> listLocalUsers(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException {
    return security.listUsers(credentials);
  }

  private Map<String,String> conf(TCredentials credentials, AccumuloConfiguration conf) throws TException {
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
  public Map<String,String> getConfiguration(TInfo tinfo, TCredentials credentials, ConfigurationType type) throws TException {
    ServerConfigurationFactory factory = context.getServerConfigurationFactory();
    switch (type) {
      case CURRENT:
        return conf(credentials, factory.getSystemConfiguration());
      case SITE:
        return conf(credentials, factory.getSiteConfiguration());
      case DEFAULT:
        return conf(credentials, factory.getDefaultConfiguration());
    }
    throw new RuntimeException("Unexpected configuration type " + type);
  }

  @Override
  public Map<String,String> getTableConfiguration(TInfo tinfo, TCredentials credentials, String tableName) throws TException, ThriftTableOperationException {
    String tableId = checkTableId(instance, tableName, null);
    AccumuloConfiguration config = context.getServerConfigurationFactory().getTableConfiguration(tableId);
    return conf(credentials, config);
  }

  @Override
  public List<String> bulkImportFiles(TInfo tinfo, final TCredentials credentials, final long tid, final String tableId, final List<String> files,
      final String errorDir, final boolean setTime) throws ThriftSecurityException, ThriftTableOperationException, TException {
    try {
      if (!security.canPerformSystemActions(credentials))
        throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
      bulkImportStatus.updateBulkImportStatus(files, BulkImportState.INITIAL);
      log.debug("Got request to bulk import files to table(" + tableId + "): " + files);
      return transactionWatcher.run(Constants.BULK_ARBITRATOR_TYPE, tid, new Callable<List<String>>() {
        @Override
        public List<String> call() throws Exception {
          bulkImportStatus.updateBulkImportStatus(files, BulkImportState.PROCESSING);
          try {
            return BulkImporter.bulkLoad(context, tid, tableId, files, errorDir, setTime);
          } finally {
            bulkImportStatus.removeBulkImportStatus(files);
          }
        }
      });
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (Exception ex) {
      throw new TException(ex);
    }
  }

  @Override
  public boolean isActive(TInfo tinfo, long tid) throws TException {
    return transactionWatcher.isActive(tid);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public boolean checkClass(TInfo tinfo, TCredentials credentials, String className, String interfaceMatch) throws TException {
    security.authenticateUser(credentials, credentials);

    ClassLoader loader = getClass().getClassLoader();
    Class shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);
      Class test = AccumuloVFSClassLoader.loadClass(className, shouldMatch);
      test.newInstance();
      return true;
    } catch (ClassCastException e) {
      log.warn("Error checking object types", e);
      return false;
    } catch (ClassNotFoundException e) {
      log.warn("Error checking object types", e);
      return false;
    } catch (InstantiationException e) {
      log.warn("Error checking object types", e);
      return false;
    } catch (IllegalAccessException e) {
      log.warn("Error checking object types", e);
      return false;
    }
  }

  @Override
  public boolean checkTableClass(TInfo tinfo, TCredentials credentials, String tableName, String className, String interfaceMatch) throws TException,
      ThriftTableOperationException, ThriftSecurityException {

    security.authenticateUser(credentials, credentials);

    String tableId = checkTableId(instance, tableName, null);

    ClassLoader loader = getClass().getClassLoader();
    Class<?> shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);

      AccumuloConfiguration conf = context.getServerConfigurationFactory().getTableConfiguration(tableId);

      String context = conf.get(Property.TABLE_CLASSPATH);

      ClassLoader currentLoader;

      if (context != null && !context.equals("")) {
        currentLoader = AccumuloVFSClassLoader.getContextManager().getClassLoader(context);
      } else {
        currentLoader = AccumuloVFSClassLoader.getClassLoader();
      }

      Class<?> test = currentLoader.loadClass(className).asSubclass(shouldMatch);
      test.newInstance();
      return true;
    } catch (Exception e) {
      log.warn("Error checking object types", e);
      return false;
    }
  }

  @Override
  public boolean checkNamespaceClass(TInfo tinfo, TCredentials credentials, String ns, String className, String interfaceMatch) throws TException,
      ThriftTableOperationException, ThriftSecurityException {

    security.authenticateUser(credentials, credentials);

    String namespaceId = checkNamespaceId(instance, ns, null);

    ClassLoader loader = getClass().getClassLoader();
    Class<?> shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);

      AccumuloConfiguration conf = context.getServerConfigurationFactory().getNamespaceConfiguration(namespaceId);

      String context = conf.get(Property.TABLE_CLASSPATH);

      ClassLoader currentLoader;

      if (context != null && !context.equals("")) {
        currentLoader = AccumuloVFSClassLoader.getContextManager().getClassLoader(context);
      } else {
        currentLoader = AccumuloVFSClassLoader.getClassLoader();
      }

      Class<?> test = currentLoader.loadClass(className).asSubclass(shouldMatch);
      test.newInstance();
      return true;
    } catch (Exception e) {
      log.warn("Error checking object types", e);
      return false;
    }
  }

  @Override
  public List<TDiskUsage> getDiskUsage(Set<String> tables, TCredentials credentials) throws ThriftTableOperationException, ThriftSecurityException, TException {
    try {
      HashSet<String> tableIds = new HashSet<>();

      for (String table : tables) {
        // ensure that table table exists
        String tableId = checkTableId(instance, table, null);
        tableIds.add(tableId);
        String namespaceId = Tables.getNamespaceId(instance, tableId);
        if (!security.canScan(credentials, tableId, namespaceId))
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
      }

      // use the same set of tableIds that were validated above to avoid race conditions
      Map<TreeSet<String>,Long> diskUsage = TableDiskUsage.getDiskUsage(context.getServerConfigurationFactory().getSystemConfiguration(), tableIds, fs,
          context.getConnector());
      List<TDiskUsage> retUsages = new ArrayList<>();
      for (Map.Entry<TreeSet<String>,Long> usageItem : diskUsage.entrySet()) {
        retUsages.add(new TDiskUsage(new ArrayList<>(usageItem.getKey()), usageItem.getValue()));
      }
      return retUsages;

    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (AccumuloException e) {
      throw new TException(e);
    } catch (IOException e) {
      throw new TException(e);
    } catch (TableNotFoundException e) {
      throw new TException(e);
    }
  }

  @Override
  public Map<String,String> getNamespaceConfiguration(TInfo tinfo, TCredentials credentials, String ns) throws ThriftTableOperationException, TException {
    String namespaceId;
    try {
      namespaceId = Namespaces.getNamespaceId(instance, ns);
    } catch (NamespaceNotFoundException e) {
      String why = "Could not find namespace while getting configuration.";
      throw new ThriftTableOperationException(null, ns, null, TableOperationExceptionType.NAMESPACE_NOTFOUND, why);
    }
    AccumuloConfiguration config = context.getServerConfigurationFactory().getNamespaceConfiguration(namespaceId);
    return conf(credentials, config);
  }

  public List<BulkImportStatus> getBulkLoadStatus() {
    return bulkImportStatus.getBulkLoadStatus();
  }
}

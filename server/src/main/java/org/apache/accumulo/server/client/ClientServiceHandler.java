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
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ConfigurationType;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.TDiskUsage;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.CredentialHelper;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.util.TableDiskUsage;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

public class ClientServiceHandler implements ClientService.Iface {
  private static final Logger log = Logger.getLogger(ClientServiceHandler.class);
  private static SecurityOperation security = AuditedSecurityOperation.getInstance();
  protected final TransactionWatcher transactionWatcher;
  private final Instance instance;
  
  public ClientServiceHandler(Instance instance, TransactionWatcher transactionWatcher) {
    this.instance = instance;
    this.transactionWatcher = transactionWatcher;
  }
  
  protected String checkTableId(String tableName, TableOperation operation) throws ThriftTableOperationException {
    String tableId = Tables.getNameToIdMap(instance).get(tableName);
    if (tableId == null) {
      // maybe the table exist, but the cache was not updated yet... so try to clear the cache and check again
      Tables.clearCache(instance);
      tableId = Tables.getNameToIdMap(instance).get(tableName);
      if (tableId == null)
        throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.NOTFOUND, null);
    }
    return tableId;
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
      log.error(e);
      throw e;
    }
  }
  
  @Override
  public boolean authenticateUser(TInfo tinfo, TCredentials credentials, TCredentials toAuth) throws ThriftSecurityException {
    try {
      return security.authenticateUser(credentials, toAuth);
    } catch (ThriftSecurityException e) {
      log.error(e);
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
    TCredentials toChange = CredentialHelper.createSquelchError(principal, token, credentials.getInstanceId());
    security.changePassword(credentials, toChange);
  }
  
  @Override
  public void createLocalUser(TInfo tinfo, TCredentials credentials, String principal, ByteBuffer password) throws ThriftSecurityException {
    PasswordToken token = new PasswordToken(password);
    TCredentials newUser = CredentialHelper.createSquelchError(principal, token, credentials.getInstanceId());
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
  public void grantTablePermission(TInfo tinfo, TCredentials credentials, String user, String tableName, byte permission) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    security.grantTablePermission(credentials, user, tableId, TablePermission.getPermissionById(permission));
  }
  
  @Override
  public void revokeSystemPermission(TInfo tinfo, TCredentials credentials, String user, byte permission) throws ThriftSecurityException {
    security.revokeSystemPermission(credentials, user, SystemPermission.getPermissionById(permission));
  }
  
  @Override
  public void revokeTablePermission(TInfo tinfo, TCredentials credentials, String user, String tableName, byte permission) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    security.revokeTablePermission(credentials, user, tableId, TablePermission.getPermissionById(permission));
  }
  
  @Override
  public boolean hasSystemPermission(TInfo tinfo, TCredentials credentials, String user, byte sysPerm) throws ThriftSecurityException {
    return security.hasSystemPermission(credentials, user, SystemPermission.getPermissionById(sysPerm));
  }
  
  @Override
  public boolean hasTablePermission(TInfo tinfo, TCredentials credentials, String user, String tableName, byte tblPerm) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    return security.hasTablePermission(credentials, user, tableId, TablePermission.getPermissionById(tblPerm));
  }
  
  @Override
  public Set<String> listLocalUsers(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException {
    return security.listUsers(credentials);
  }
  
  private static Map<String,String> conf(TCredentials credentials, AccumuloConfiguration conf) throws TException {
    security.authenticateUser(credentials, credentials);
    conf.invalidateCache();
    
    Map<String,String> result = new HashMap<String,String>();
    for (Entry<String,String> entry : conf) {
      String key = entry.getKey();
      if (!Property.isSensitive(key))
        result.put(key, entry.getValue());
    }
    return result;
  }
  
  @Override
  public Map<String,String> getConfiguration(TInfo tinfo, TCredentials credentials, ConfigurationType type) throws TException {
    switch (type) {
      case CURRENT:
        return conf(credentials, new ServerConfiguration(instance).getConfiguration());
      case SITE:
        return conf(credentials, ServerConfiguration.getSiteConfiguration());
      case DEFAULT:
        return conf(credentials, AccumuloConfiguration.getDefaultConfiguration());
    }
    throw new RuntimeException("Unexpected configuration type " + type);
  }
  
  @Override
  public Map<String,String> getTableConfiguration(TInfo tinfo, TCredentials credentials, String tableName) throws TException, ThriftTableOperationException {
    String tableId = checkTableId(tableName, null);
    return conf(credentials, new ServerConfiguration(instance).getTableConfiguration(tableId));
  }
  
  @Override
  public List<String> bulkImportFiles(TInfo tinfo, final TCredentials credentials, final long tid, final String tableId, final List<String> files,
      final String errorDir, final boolean setTime) throws ThriftSecurityException, ThriftTableOperationException, TException {
    try {
      if (!security.canPerformSystemActions(credentials))
        throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
      return transactionWatcher.run(Constants.BULK_ARBITRATOR_TYPE, tid, new Callable<List<String>>() {
        @Override
        public List<String> call() throws Exception {
          return BulkImporter.bulkLoad(new ServerConfiguration(instance).getConfiguration(), instance, credentials, tid, tableId, files, errorDir, setTime);
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
    
    String tableId = checkTableId(tableName, null);
    
    ClassLoader loader = getClass().getClassLoader();
    Class<?> shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);
      
      new ServerConfiguration(instance).getTableConfiguration(tableId);
      
      String context = new ServerConfiguration(instance).getTableConfiguration(tableId).get(Property.TABLE_CLASSPATH);
      
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
      Connector conn = instance.getConnector(credentials.getPrincipal(), CredentialHelper.extractToken(credentials));
      
      HashSet<String> tableIds = new HashSet<String>();
      
      for (String table : tables) {
        // ensure that table table exists
        String tableId = checkTableId(table, null);
        tableIds.add(tableId);
        if (!security.canScan(credentials, tableId))
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
      }
      
      AccumuloConfiguration conf = new ServerConfiguration(instance).getConfiguration();
      FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), conf);
      
      // use the same set of tableIds that were validated above to avoid race conditions
      Map<TreeSet<String>,Long> diskUsage = TableDiskUsage.getDiskUsage(new ServerConfiguration(instance).getConfiguration(), tableIds, fs, conn);
      List<TDiskUsage> retUsages = new ArrayList<TDiskUsage>();
      for (Map.Entry<TreeSet<String>,Long> usageItem : diskUsage.entrySet()) {
        retUsages.add(new TDiskUsage(new ArrayList<String>(usageItem.getKey()), usageItem.getValue()));
      }
      return retUsages;
      
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    } catch (AccumuloException e) {
      throw new TException(e);
    } catch (IOException e) {
      throw new TException(e);
    }
  }
}

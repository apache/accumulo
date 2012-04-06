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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.accumulo.cloudtrace.thrift.TInfo;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ConfigurationType;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.SecurityErrorCode;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.security.Authenticator;
import org.apache.accumulo.server.security.ZKAuthenticator;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;


public class ClientServiceHandler implements ClientService.Iface {
  private static final Logger log = Logger.getLogger(ClientServiceHandler.class);
  private static Authenticator authenticator = ZKAuthenticator.getInstance();
  private final TransactionWatcher transactionWatcher;
  private final Instance instance;
  
  public ClientServiceHandler(Instance instance, TransactionWatcher transactionWatcher) {
    this.instance = instance;
    this.transactionWatcher = transactionWatcher;
  }
  
  protected String checkTableId(String tableName, TableOperation operation) throws ThriftTableOperationException {
    String tableId = Tables.getNameToIdMap(HdfsZooInstance.getInstance()).get(tableName);
    if (tableId == null) {
      // maybe the table exist, but the cache was not updated yet... so try to clear the cache and check again
      Tables.clearCache(HdfsZooInstance.getInstance());
      tableId = Tables.getNameToIdMap(HdfsZooInstance.getInstance()).get(tableName);
      if (tableId == null)
        throw new ThriftTableOperationException(null, tableName, operation, TableOperationExceptionType.NOTFOUND, null);
    }
    return tableId;
  }
  
  @Override
  public String getInstanceId() {
    return HdfsZooInstance.getInstance().getInstanceID();
  }
  
  @Override
  public String getRootTabletLocation() {
    return HdfsZooInstance.getInstance().getRootTabletLocation();
  }
  
  @Override
  public String getZooKeepers() {
    return instance.getZooKeepers();
  }
  
  @Override
  public void ping(AuthInfo credentials) {
    // anybody can call this; no authentication check
    log.info("Master reports: I just got pinged!");
  }
  
  @Override
  public boolean authenticateUser(TInfo tinfo, AuthInfo credentials, String user, ByteBuffer password) throws ThriftSecurityException {
    try {
      return authenticator.authenticateUser(credentials, user, password);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void changeAuthorizations(TInfo tinfo, AuthInfo credentials, String user, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    try {
      authenticator.changeAuthorizations(credentials, user, new Authorizations(authorizations));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void changePassword(TInfo tinfo, AuthInfo credentials, String user, ByteBuffer password) throws ThriftSecurityException {
    try {
      authenticator.changePassword(credentials, user, ByteBufferUtil.toBytes(password));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void createUser(TInfo tinfo, AuthInfo credentials, String user, ByteBuffer password, List<ByteBuffer> authorizations) throws ThriftSecurityException {
    try {
      authenticator.createUser(credentials, user, ByteBufferUtil.toBytes(password), new Authorizations(authorizations));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void dropUser(TInfo tinfo, AuthInfo credentials, String user) throws ThriftSecurityException {
    try {
      authenticator.dropUser(credentials, user);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public List<ByteBuffer> getUserAuthorizations(TInfo tinfo, AuthInfo credentials, String user) throws ThriftSecurityException {
    try {
      return authenticator.getUserAuthorizations(credentials, user).getAuthorizationsBB();
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void grantSystemPermission(TInfo tinfo, AuthInfo credentials, String user, byte permission) throws ThriftSecurityException {
    try {
      authenticator.grantSystemPermission(credentials, user, SystemPermission.getPermissionById(permission));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void grantTablePermission(TInfo tinfo, AuthInfo credentials, String user, String tableName, byte permission) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    try {
      authenticator.grantTablePermission(credentials, user, tableId, TablePermission.getPermissionById(permission));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void revokeSystemPermission(TInfo tinfo, AuthInfo credentials, String user, byte permission) throws ThriftSecurityException {
    try {
      authenticator.revokeSystemPermission(credentials, user, SystemPermission.getPermissionById(permission));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public void revokeTablePermission(TInfo tinfo, AuthInfo credentials, String user, String tableName, byte permission) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    try {
      authenticator.revokeTablePermission(credentials, user, tableId, TablePermission.getPermissionById(permission));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public boolean hasSystemPermission(TInfo tinfo, AuthInfo credentials, String user, byte sysPerm) throws ThriftSecurityException {
    try {
      return authenticator.hasSystemPermission(credentials, user, SystemPermission.getPermissionById(sysPerm));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public boolean hasTablePermission(TInfo tinfo, AuthInfo credentials, String user, String tableName, byte tblPerm) throws ThriftSecurityException,
      ThriftTableOperationException {
    String tableId = checkTableId(tableName, TableOperation.PERMISSION);
    try {
      return authenticator.hasTablePermission(credentials, user, tableId, TablePermission.getPermissionById(tblPerm));
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  @Override
  public Set<String> listUsers(TInfo tinfo, AuthInfo credentials) throws ThriftSecurityException {
    try {
      return authenticator.listUsers(credentials);
    } catch (AccumuloSecurityException e) {
      throw e.asThriftException();
    }
  }
  
  static private Map<String,String> conf(AccumuloConfiguration conf) {
    Map<String,String> result = new HashMap<String,String>();
    for (Entry<String,String> entry : conf) {
      // TODO: do we need to send any instance information?
      if (!entry.getKey().equals(Property.INSTANCE_SECRET.getKey()))
        result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
  
  @Override
  public Map<String,String> getConfiguration(ConfigurationType type) throws TException {
    switch (type) {
      case CURRENT:
        return conf(new ServerConfiguration(instance).getConfiguration());
      case SITE:
        return conf(ServerConfiguration.getSiteConfiguration());
      case DEFAULT:
        return conf(AccumuloConfiguration.getDefaultConfiguration());
    }
    throw new RuntimeException("Unexpected configuration type " + type);
  }
  
  @Override
  public Map<String,String> getTableConfiguration(String tableName) throws TException, ThriftTableOperationException {
    String tableId = checkTableId(tableName, null);
    return conf(new ServerConfiguration(instance).getTableConfiguration(tableId));
  }
  
  @Override
  public List<String> bulkImportFiles(TInfo tinfo, final AuthInfo credentials, final long tid, final String tableId, final List<String> files,
      final String errorDir, final boolean setTime) throws ThriftSecurityException, ThriftTableOperationException, TException {
    try {
      if (!authenticator.hasSystemPermission(credentials, credentials.getUser(), SystemPermission.SYSTEM))
        throw new AccumuloSecurityException(credentials.getUser(), SecurityErrorCode.PERMISSION_DENIED);
      return transactionWatcher.run(Constants.BULK_ARBITRATOR_TYPE, tid, new Callable<List<String>>() {
        public List<String> call() throws Exception {
          return BulkImporter.bulkLoad(new ServerConfiguration(instance).getConfiguration(), instance, credentials, tid, tableId, files, errorDir,
              setTime);
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
  public boolean checkClass(TInfo tinfo, String className, String interfaceMatch) throws TException {
    ClassLoader loader = getClass().getClassLoader();
    Class shouldMatch;
    try {
      shouldMatch = loader.loadClass(interfaceMatch);
      Class test = AccumuloClassLoader.loadClass(className, shouldMatch);
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
}

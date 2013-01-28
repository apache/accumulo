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
package org.apache.accumulo.proxy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.ActiveCompaction;
import org.apache.accumulo.core.client.admin.ActiveScan;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.security.tokens.UserPassToken;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.BatchScanOptions;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.CompactionReason;
import org.apache.accumulo.proxy.thrift.CompactionType;
import org.apache.accumulo.proxy.thrift.KeyValue;
import org.apache.accumulo.proxy.thrift.KeyValueAndPeek;
import org.apache.accumulo.proxy.thrift.NoMoreEntriesException;
import org.apache.accumulo.proxy.thrift.ScanColumn;
import org.apache.accumulo.proxy.thrift.ScanOptions;
import org.apache.accumulo.proxy.thrift.ScanResult;
import org.apache.accumulo.proxy.thrift.ScanState;
import org.apache.accumulo.proxy.thrift.ScanType;
import org.apache.accumulo.proxy.thrift.UnknownScanner;
import org.apache.accumulo.proxy.thrift.UnknownWriter;
import org.apache.accumulo.proxy.thrift.UserPass;
import org.apache.accumulo.proxy.thrift.WriterOptions;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class ProxyServer implements AccumuloProxy.Iface {
  
  public static final Logger logger = Logger.getLogger(ProxyServer.class);
  protected Instance instance;
  
  static protected class ScannerPlusIterator {
    public ScannerBase scanner;
    public Iterator<Map.Entry<Key,Value>> iterator;
  }
  
  static class CloseWriter implements RemovalListener<UUID,BatchWriter> {
    @Override
    public void onRemoval(RemovalNotification<UUID,BatchWriter> notification) {
      try {
        notification.getValue().close();
      } catch (MutationsRejectedException e) {
        logger.warn(e, e);
      }
    }
    public CloseWriter() {}
  }
  
  static class CloseScanner implements RemovalListener<UUID,ScannerPlusIterator> {
    @Override
    public void onRemoval(RemovalNotification<UUID,ScannerPlusIterator> notification) {
      final ScannerBase base = notification.getValue().scanner;
      if (base instanceof BatchScanner) {
        final BatchScanner scanner = (BatchScanner)base;
        scanner.close();
      }
    }
    public CloseScanner() {}
  }

  protected Cache<UUID,ScannerPlusIterator> scannerCache;
  protected Cache<UUID,BatchWriter> writerCache;
  
  public ProxyServer(Properties props) {
    String useMock = props.getProperty("org.apache.accumulo.proxy.ProxyServer.useMockInstance");
    if (useMock != null && Boolean.parseBoolean(useMock))
      instance = new MockInstance();
    else
      instance = new ZooKeeperInstance(props.getProperty("org.apache.accumulo.proxy.ProxyServer.instancename"),
          props.getProperty("org.apache.accumulo.proxy.ProxyServer.zookeepers"));
    
    scannerCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000).removalListener(new CloseScanner()).build();
    
    writerCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000).removalListener(new CloseWriter()).build();
  }
  
  protected Connector getConnector(UserPass userpass) throws Exception {
    Connector connector = instance.getConnector(new UserPassToken(userpass.getUsername(), userpass.bufferForPassword()));
    return connector;
  }
  
  private TException translateException(Exception ex) {
    try {
      throw ex;
    } catch (AccumuloException e) {
      return new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
    } catch (AccumuloSecurityException e) {
      return new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
    } catch (TableNotFoundException e) {
      return new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
    } catch (TableExistsException e) {
      return new org.apache.accumulo.proxy.thrift.TableExistsException(e.toString());
    } catch (RuntimeException e) {
      if (e.getCause() != null) {
        if (e.getCause() instanceof Exception)
          return translateException((Exception)e.getCause());
      }
      return new TException(e);
    } catch (Exception e) {
      return new TException(ex);
    }
  }
  
  @Override
  public boolean ping(UserPass userpass) throws TException {
    return true;
  }
  
  @Override
  public int addConstraint(UserPass userpass, String tableName, String constraintClassName) throws TException {
    try {
      return getConnector(userpass).tableOperations().addConstraint(tableName, constraintClassName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void addSplits(UserPass userpass, String tableName, Set<ByteBuffer> splits) throws TException {
    try {
      SortedSet<Text> sorted = new TreeSet<Text>();
      for (ByteBuffer split : splits) {
        sorted.add(ByteBufferUtil.toText(split));
      }
      getConnector(userpass).tableOperations().addSplits(tableName, sorted);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void clearLocatorCache(UserPass userpass, String tableName) throws TException {
    try {
      getConnector(userpass).tableOperations().clearLocatorCache(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void compactTable(UserPass userpass, String tableName, ByteBuffer start, ByteBuffer end, List<org.apache.accumulo.proxy.thrift.IteratorSetting> iterators, boolean flush,
      boolean wait) throws TException {
    try {
      getConnector(userpass).tableOperations().compact(tableName, ByteBufferUtil.toText(start), ByteBufferUtil.toText(end), getIteratorSettings(iterators),
          flush, wait);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void cancelCompaction(UserPass userpass, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, org.apache.accumulo.proxy.thrift.AccumuloException, TException {
    try {
      getConnector(userpass).tableOperations().cancelCompaction(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }

  private List<IteratorSetting> getIteratorSettings(List<org.apache.accumulo.proxy.thrift.IteratorSetting> iterators) {
    List<IteratorSetting> result = new ArrayList<IteratorSetting>();
    if (iterators != null) {
      for (org.apache.accumulo.proxy.thrift.IteratorSetting is : iterators) {
        result.add(getIteratorSetting(is));
      }
    }
    return result;
  }
  
  @Override
  public void createTable(UserPass userpass, String tableName, boolean versioningIter, org.apache.accumulo.proxy.thrift.TimeType timeType) throws TException {
    try {
      if (timeType == null)
        timeType = org.apache.accumulo.proxy.thrift.TimeType.MILLIS;
      
      getConnector(userpass).tableOperations().create(tableName, versioningIter, TimeType.valueOf(timeType.toString()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void deleteTable(UserPass userpass, String tableName) throws TException {
    try {
      getConnector(userpass).tableOperations().delete(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void deleteRows(UserPass userpass, String tableName, ByteBuffer start, ByteBuffer end) throws TException {
    try {
      getConnector(userpass).tableOperations().deleteRows(tableName, ByteBufferUtil.toText(start), ByteBufferUtil.toText(end));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean tableExists(UserPass userpass, String tableName) throws TException {
    try {
      return getConnector(userpass).tableOperations().exists(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void flushTable(UserPass userpass, String tableName, ByteBuffer startRow, ByteBuffer endRow, boolean wait) throws TException {
    try {
      getConnector(userpass).tableOperations().flush(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow), wait);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,Set<String>> getLocalityGroups(UserPass userpass, String tableName) throws TException {
    try {
      Map<String,Set<Text>> groups = getConnector(userpass).tableOperations().getLocalityGroups(tableName);
      Map<String,Set<String>> ret = new HashMap<String,Set<String>>();
      for (String key : groups.keySet()) {
        ret.put(key, new HashSet<String>());
        for (Text val : groups.get(key)) {
          ret.get(key).add(val.toString());
        }
      }
      return ret;
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public ByteBuffer getMaxRow(UserPass userpass, String tableName, Set<ByteBuffer> auths, ByteBuffer startRow, boolean startinclusive,
      ByteBuffer endRow, boolean endinclusive) throws TException {
    try {
      Connector connector = getConnector(userpass);
      Text startText = ByteBufferUtil.toText(startRow);
      Text endText = ByteBufferUtil.toText(endRow);
      Authorizations auth;
      if (auths != null) {
        auth = getAuthorizations(auths);
      } else {
        auth = connector.securityOperations().getUserAuthorizations(userpass.getUsername());
      }
      Text max = connector.tableOperations().getMaxRow(tableName, auth, startText, startinclusive, endText, endinclusive);
      return TextUtil.getByteBuffer(max);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,String> getTableProperties(UserPass userpass, String tableName) throws TException {
    try {
      Map<String,String> ret = new HashMap<String,String>();
      
      for (Map.Entry<String,String> entry : getConnector(userpass).tableOperations().getProperties(tableName)) {
        ret.put(entry.getKey(), entry.getValue());
      }
      return ret;
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public List<ByteBuffer> getSplits(UserPass userpass, String tableName, int maxSplits) throws TException {
    try {
      Collection<Text> splits = getConnector(userpass).tableOperations().getSplits(tableName, maxSplits);
      List<ByteBuffer> ret = new ArrayList<ByteBuffer>();
      for (Text split : splits) {
        ret.add(TextUtil.getByteBuffer(split));
      }
      return ret;
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Set<String> listTables(UserPass userpass) throws TException {
    try {
      return getConnector(userpass).tableOperations().list();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,Integer> listConstraints(UserPass userpass, String arg2) throws TException {
    try {
      return getConnector(userpass).tableOperations().listConstraints(arg2);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void mergeTablets(UserPass userpass, String tableName, ByteBuffer start, ByteBuffer end) throws TException {
    try {
      getConnector(userpass).tableOperations().merge(tableName, ByteBufferUtil.toText(start), ByteBufferUtil.toText(end));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void offlineTable(UserPass userpass, String tableName) throws TException {
    try {
      getConnector(userpass).tableOperations().offline(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void onlineTable(UserPass userpass, String tableName) throws TException {
    try {
      getConnector(userpass).tableOperations().online(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void removeConstraint(UserPass userpass, String tableName, int number) throws TException {
    try {
      getConnector(userpass).tableOperations().removeConstraint(tableName, number);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void removeTableProperty(UserPass userpass, String tableName, String property) throws TException {
    try {
      getConnector(userpass).tableOperations().removeProperty(tableName, property);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void renameTable(UserPass userpass, String oldTableName, String newTableName) throws TException {
    try {
      getConnector(userpass).tableOperations().rename(oldTableName, newTableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void setLocalityGroups(UserPass userpass, String tableName, Map<String,Set<String>> groupStrings) throws TException {
    try {
      Map<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
      for (String key : groupStrings.keySet()) {
        groups.put(key, new HashSet<Text>());
        for (String val : groupStrings.get(key)) {
          groups.get(key).add(new Text(val));
        }
      }
      getConnector(userpass).tableOperations().setLocalityGroups(tableName, groups);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void setTableProperty(UserPass userpass, String tableName, String property, String value) throws TException {
    try {
      getConnector(userpass).tableOperations().setProperty(tableName, property, value);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,String> tableIdMap(UserPass userpass) throws TException {
    try {
      return getConnector(userpass).tableOperations().tableIdMap();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,String> getSiteConfiguration(UserPass userpass) throws TException {
    try {
      return getConnector(userpass).instanceOperations().getSiteConfiguration();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,String> getSystemConfiguration(UserPass userpass) throws TException {
    try {
      return getConnector(userpass).instanceOperations().getSystemConfiguration();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public List<String> getTabletServers(UserPass userpass) throws TException {
    try {
      return getConnector(userpass).instanceOperations().getTabletServers();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public List<org.apache.accumulo.proxy.thrift.ActiveScan> getActiveScans(UserPass userpass, String tserver) throws TException {
    List<org.apache.accumulo.proxy.thrift.ActiveScan> result = new ArrayList<org.apache.accumulo.proxy.thrift.ActiveScan>();
    try {
      List<ActiveScan> activeScans = getConnector(userpass).instanceOperations().getActiveScans(tserver);
      for (ActiveScan scan : activeScans) {
        org.apache.accumulo.proxy.thrift.ActiveScan pscan = new org.apache.accumulo.proxy.thrift.ActiveScan();
        pscan.client = scan.getClient();
        pscan.user = scan.getUser();
        pscan.table = scan.getTable();
        pscan.age = scan.getAge();
        pscan.idleTime = scan.getIdleTime();
        pscan.type = ScanType.valueOf(scan.getType().toString());
        pscan.state = ScanState.valueOf(scan.getState().toString());
        KeyExtent e = scan.getExtent();
        pscan.extent = new org.apache.accumulo.proxy.thrift.KeyExtent(e.getTableId().toString(), TextUtil.getByteBuffer(e.getEndRow()), TextUtil.getByteBuffer(e.getPrevEndRow()));
        pscan.columns = new ArrayList<org.apache.accumulo.proxy.thrift.Column>();
        if (scan.getColumns() != null) {
          for (Column c : scan.getColumns()) {
            org.apache.accumulo.proxy.thrift.Column column = new org.apache.accumulo.proxy.thrift.Column();
            column.setColFamily(c.getColumnFamily());
            column.setColQualifier(c.getColumnQualifier());
            column.setColVisibility(c.getColumnVisibility());
            pscan.columns.add(column);
          }
        }
        pscan.iterators = new ArrayList<org.apache.accumulo.proxy.thrift.IteratorSetting>();
        for (String iteratorString : scan.getSsiList()) {
          String[] parts = iteratorString.split("[=,]");
          if (parts.length == 3) {
            String name = parts[0];
            int priority = Integer.parseInt(parts[1]);
            String classname = parts[2];
            org.apache.accumulo.proxy.thrift.IteratorSetting settings = new org.apache.accumulo.proxy.thrift.IteratorSetting(priority, name, classname, scan.getSsio().get(name));
            pscan.iterators.add(settings);
          }
        }
        pscan.authorizations = new ArrayList<ByteBuffer>();
        if (scan.getAuthorizations() != null) {
          for (byte[] a : scan.getAuthorizations()) {
            pscan.authorizations.add(ByteBuffer.wrap(a));
          }
        }
        result.add(pscan);
      }
      return result;
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public List<org.apache.accumulo.proxy.thrift.ActiveCompaction> getActiveCompactions(UserPass userpass, String tserver) throws TException {
    try {
      List<org.apache.accumulo.proxy.thrift.ActiveCompaction> result = new ArrayList<org.apache.accumulo.proxy.thrift.ActiveCompaction>();
      List<ActiveCompaction> active = getConnector(userpass).instanceOperations().getActiveCompactions(tserver);
      for (ActiveCompaction comp : active) {
        org.apache.accumulo.proxy.thrift.ActiveCompaction pcomp = new org.apache.accumulo.proxy.thrift.ActiveCompaction();
        pcomp.age = comp.getAge();
        pcomp.entriesRead = comp.getEntriesRead();
        pcomp.entriesWritten = comp.getEntriesWritten();
        KeyExtent e = comp.getExtent();
        pcomp.extent = new org.apache.accumulo.proxy.thrift.KeyExtent(e.getTableId().toString(), TextUtil.getByteBuffer(e.getEndRow()), TextUtil.getByteBuffer(e.getPrevEndRow()));
        pcomp.inputFiles = new ArrayList<String>();
        if (comp.getInputFiles() != null) {
          pcomp.inputFiles.addAll(comp.getInputFiles());
        }
        pcomp.localityGroup = comp.getLocalityGroup();
        pcomp.outputFile = comp.getOutputFile();
        pcomp.reason = CompactionReason.valueOf(comp.getReason().toString());
        pcomp.type = CompactionType.valueOf(comp.getType().toString());
        
        pcomp.iterators = new ArrayList<org.apache.accumulo.proxy.thrift.IteratorSetting>();
        if (comp.getIterators() != null) {
          for (IteratorSetting setting : comp.getIterators()) {
            org.apache.accumulo.proxy.thrift.IteratorSetting psetting = new org.apache.accumulo.proxy.thrift.IteratorSetting(setting.getPriority(), setting.getName(), setting.getIteratorClass(), setting.getOptions());
            pcomp.iterators.add(psetting);
          }
        }
        result.add(pcomp);
      }
      return result;
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void removeProperty(UserPass userpass, String property) throws TException {
    try {
      getConnector(userpass).instanceOperations().removeProperty(property);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void setProperty(UserPass userpass, String property, String value) throws TException {
    try {
      getConnector(userpass).instanceOperations().setProperty(property, value);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean testClassLoad(UserPass userpass, String className, String asTypeName) throws TException {
    try {
      return getConnector(userpass).instanceOperations().testClassLoad(className, asTypeName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean authenticateUser(UserPass userpass, String user, ByteBuffer password) throws TException {
    try {
      return getConnector(userpass).securityOperations().authenticateUser(new UserPassToken(user, password.array()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void changeUserAuthorizations(UserPass userpass, String user, Set<ByteBuffer> authorizations) throws TException {
    try {
      Set<String> auths = new HashSet<String>();
      for (ByteBuffer auth : authorizations) {
        auths.add(ByteBufferUtil.toString(auth));
      }
      getConnector(userpass).securityOperations().changeUserAuthorizations(user, new Authorizations(auths.toArray(new String[0])));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void changeUserPassword(UserPass userpass, String user, ByteBuffer password) throws TException {
    try {
      getConnector(userpass).securityOperations().changeUserPassword(new UserPassToken(user, password.array()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void createUser(UserPass userpass, String user, ByteBuffer password) throws TException {
    try {
      getConnector(userpass).securityOperations().createUser(new UserPassToken(user, password));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void dropUser(UserPass userpass, String user) throws TException {
    try {
      getConnector(userpass).securityOperations().dropUser(user);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public List<ByteBuffer> getUserAuthorizations(UserPass userpass, String user) throws TException {
    try {
      return getConnector(userpass).securityOperations().getUserAuthorizations(user).getAuthorizationsBB();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void grantSystemPermission(UserPass userpass, String user, org.apache.accumulo.proxy.thrift.SystemPermission perm) throws TException {
    try {
      getConnector(userpass).securityOperations().grantSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void grantTablePermission(UserPass userpass, String user, String table, org.apache.accumulo.proxy.thrift.TablePermission perm) throws TException {
    try {
      getConnector(userpass).securityOperations().grantTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean hasSystemPermission(UserPass userpass, String user, org.apache.accumulo.proxy.thrift.SystemPermission perm) throws TException {
    try {
      return getConnector(userpass).securityOperations().hasSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean hasTablePermission(UserPass userpass, String user, String table, org.apache.accumulo.proxy.thrift.TablePermission perm) throws TException {
    try {
      return getConnector(userpass).securityOperations().hasTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Set<String> listUsers(UserPass userpass) throws TException {
    try {
      return getConnector(userpass).securityOperations().listUsers();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void revokeSystemPermission(UserPass userpass, String user, org.apache.accumulo.proxy.thrift.SystemPermission perm) throws TException {
    try {
      getConnector(userpass).securityOperations().revokeSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void revokeTablePermission(UserPass userpass, String user, String table, org.apache.accumulo.proxy.thrift.TablePermission perm) throws TException {
    try {
      getConnector(userpass).securityOperations().revokeTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  private Authorizations getAuthorizations(Set<ByteBuffer> authorizations) {
    List<String> auths = new ArrayList<String>();
    for (ByteBuffer bbauth : authorizations) {
      auths.add(ByteBufferUtil.toString(bbauth));
    }
    return new Authorizations(auths.toArray(new String[0]));
  }
  
  @Override
  public String createScanner(UserPass userpass, String tableName, ScanOptions opts)
      throws TException {
    try {
      Connector connector = getConnector(userpass);
      
      Authorizations auth;
      if (opts != null && opts.isSetAuthorizations()) {
        auth = getAuthorizations(opts.authorizations);
      } else {
        auth = connector.securityOperations().getUserAuthorizations(userpass.getUsername());
      }
      Scanner scanner = connector.createScanner(tableName, auth);
      
      if (opts != null) {
        if (opts.iterators != null) {
          for (org.apache.accumulo.proxy.thrift.IteratorSetting iter : opts.iterators) {
            IteratorSetting is = new IteratorSetting(iter.getPriority(), iter.getName(), iter.getIteratorClass(), iter.getProperties());
            scanner.addScanIterator(is);
          }
        }
        org.apache.accumulo.proxy.thrift.Range prange = opts.range;
        if (prange != null) {
          Range range = new Range(
              Util.fromThrift(prange.getStart()), prange.startInclusive, 
              Util.fromThrift(prange.getStop()), prange.stopInclusive
                  );
          scanner.setRange(range);
        }
        if (opts.columns != null) {
          for (ScanColumn col : opts.columns) {
            if (col.isSetColQualifier())
              scanner.fetchColumn(ByteBufferUtil.toText(col.colFamily), ByteBufferUtil.toText(col.colQualifier));
            else
              scanner.fetchColumnFamily(ByteBufferUtil.toText(col.colFamily));
          }
        }
      }
      
      UUID uuid = UUID.randomUUID();
      
      ScannerPlusIterator spi = new ScannerPlusIterator();
      spi.scanner = scanner;
      spi.iterator = scanner.iterator();
      scannerCache.put(uuid, spi);
      return uuid.toString();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public String createBatchScanner(UserPass userpass, String tableName, BatchScanOptions opts)
      throws TException {
    try {
      Connector connector = getConnector(userpass);
      
            
      int threads = 10;
      Authorizations auth;
      if (opts != null && opts.isSetAuthorizations()) {
        auth = getAuthorizations(opts.authorizations);
      } else {
        auth = connector.securityOperations().getUserAuthorizations(userpass.getUsername());
      }
      if (opts != null && opts.threads > 0)
        threads = opts.threads;

      BatchScanner scanner = connector.createBatchScanner(tableName, auth, threads);
      
      if (opts != null) {
        if (opts.iterators != null) {
          for (org.apache.accumulo.proxy.thrift.IteratorSetting iter : opts.iterators) {
            IteratorSetting is = new IteratorSetting(iter.getPriority(), iter.getName(), iter.getIteratorClass(), iter.getProperties());
            scanner.addScanIterator(is);
          }
        }
        
        ArrayList<Range> ranges = new ArrayList<Range>();
        
        if (opts.ranges == null) {
          ranges.add(new Range());
        } else {
          for (org.apache.accumulo.proxy.thrift.Range range : opts.ranges) {
            Range aRange = new Range(range.getStart() == null ? null : Util.fromThrift(range.getStart()), true, range.getStop() == null ? null
                : Util.fromThrift(range.getStop()), false);
            ranges.add(aRange);
          }
        }
        scanner.setRanges(ranges);
      }
      UUID uuid = UUID.randomUUID();
      
      ScannerPlusIterator spi = new ScannerPlusIterator();
      spi.scanner = scanner;
      spi.iterator = scanner.iterator();
      scannerCache.put(uuid, spi);
      return uuid.toString();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean hasNext(String scanner) throws TException {
    ScannerPlusIterator spi = scannerCache.getIfPresent(UUID.fromString(scanner));
    if (spi == null) {
      throw new TException("Scanner never existed or no longer exists");
    }
    
    return (spi.iterator.hasNext());
  }
  
  @Override
  public KeyValueAndPeek nextEntry(String scanner) throws TException {
    
    ScanResult scanResult = nextK(scanner, 1);
    if (scanResult.results.size() > 0) {
      return new KeyValueAndPeek(scanResult.results.get(0), scanResult.isMore());
    } else {
      throw new NoMoreEntriesException();
    }
    
  }
  
  @Override
  public ScanResult nextK(String scanner, int k) throws TException {
    
    // fetch the scanner
    ScannerPlusIterator spi = scannerCache.getIfPresent(UUID.fromString(scanner));
    if (spi == null) {
      throw new UnknownScanner("Scanner never existed or no longer exists");
    }
    Iterator<Map.Entry<Key,Value>> batchScanner = spi.iterator;
    // synchronized to prevent race conditions
    synchronized (batchScanner) {
      ScanResult ret = new ScanResult();
      ret.setResults(new ArrayList<KeyValue>());
      int numRead = 0;
      try {
        while (batchScanner.hasNext() && numRead < k) {
          Map.Entry<Key,Value> next = batchScanner.next();
          ret.addToResults(new KeyValue(Util.toThrift(next.getKey()), ByteBuffer.wrap(next.getValue().get())));
          numRead++;
        }
        ret.setMore(numRead == k);
      } catch (Exception ex) {
        closeScanner(scanner);
        throw translateException(ex);
      }
      return ret;
    }
  }
  
  @Override
  public void closeScanner(String uuid) throws TException {
    scannerCache.invalidate(uuid);
  }
  
  @Override
  public void updateAndFlush(UserPass userpass, String tableName, Map<ByteBuffer,List<ColumnUpdate>> cells) throws TException {
    try {
      BatchWriter writer = getWriter(userpass, tableName, null);
      addCellsToWriter(cells, writer);
      writer.flush();
      writer.close();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();
  
  private void addCellsToWriter(Map<ByteBuffer,List<ColumnUpdate>> cells, BatchWriter writer) throws MutationsRejectedException {
    HashMap<Text,ColumnVisibility> vizMap = new HashMap<Text,ColumnVisibility>();
    
    for (Entry<ByteBuffer,List<ColumnUpdate>> entry : cells.entrySet()) {
      Mutation m = new Mutation(ByteBufferUtil.toBytes(entry.getKey()));
      
      for (ColumnUpdate update : entry.getValue()) {
        ColumnVisibility viz = EMPTY_VIS;
        if (update.isSetColVisibility()) {
          Text vizText = new Text(update.getColVisibility());
          viz = vizMap.get(vizText);
          if (viz == null) {
            vizMap.put(vizText, viz = new ColumnVisibility(vizText));
          }
        }
        byte[] value = new byte[0];
        if (update.isSetValue())
          value = update.getValue();
        if (update.isSetTimestamp()) {
          if (update.isSetDeleteCell()) {
            m.putDelete(update.getColFamily(), update.getColQualifier(), viz, update.getTimestamp());
          } else {
            if (update.isSetDeleteCell()) {
              m.putDelete(update.getColFamily(), update.getColQualifier(), viz, update.getTimestamp());
            } else {
              m.put(update.getColFamily(), update.getColQualifier(), viz, update.getTimestamp(), value);
            }
          }
        } else {
          m.put(update.getColFamily(), update.getColQualifier(), viz, value);
        }
      }
      writer.addMutation(m);
    }
  }
  
  @Override
  public String createWriter(UserPass userpass, String tableName, WriterOptions opts) throws TException {
    try {
      BatchWriter writer = getWriter(userpass, tableName, opts);
      UUID uuid = UUID.randomUUID();
      writerCache.put(uuid, writer);
      return uuid.toString();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void update(String writer, Map<ByteBuffer,List<ColumnUpdate>> cells) throws TException {
    try {
      BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
      if (batchwriter == null) {
        throw new UnknownWriter("Writer never existed or no longer exists");
      }
      addCellsToWriter(cells, batchwriter);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void flush(String writer) throws TException {
    try {
      BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
      if (batchwriter == null) {
        throw new UnknownWriter("Writer never existed or no longer exists");
      }
      batchwriter.flush();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void closeWriter(String writer) throws TException {
    try {
      BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
      if (batchwriter == null) {
        throw new UnknownWriter("Writer never existed or no longer exists");
      }
      batchwriter.close();
      writerCache.invalidate(UUID.fromString(writer));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  private BatchWriter getWriter(UserPass userpass, String tableName, WriterOptions opts) throws Exception {
    BatchWriterConfig cfg = new BatchWriterConfig();
    if (opts != null) {
      if (opts.maxMemory != 0)
        cfg.setMaxMemory(opts.maxMemory);
      if (opts.threads != 0)
        cfg.setMaxWriteThreads(opts.threads);
      if (opts.timeoutMs != 0)
        cfg.setTimeout(opts.timeoutMs, TimeUnit.MILLISECONDS);
      if (opts.latencyMs != 0)
        cfg.setMaxLatency(opts.latencyMs, TimeUnit.MILLISECONDS);
    }
    return getConnector(userpass).createBatchWriter(tableName, cfg);
  }
  
  private IteratorSetting getIteratorSetting(org.apache.accumulo.proxy.thrift.IteratorSetting setting) {
    return new IteratorSetting(setting.priority, setting.name, setting.iteratorClass, setting.getProperties());
  }
  
  private IteratorScope getIteratorScope(org.apache.accumulo.proxy.thrift.IteratorScope scope) {
    return IteratorScope.valueOf(scope.toString().toLowerCase());
  }
  
  private EnumSet<IteratorScope> getIteratorScopes(Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes) {
    EnumSet<IteratorScope> scopes_ = EnumSet.noneOf(IteratorScope.class);
    for (org.apache.accumulo.proxy.thrift.IteratorScope scope : scopes) {
      scopes_.add(getIteratorScope(scope));
    }
    return scopes_;
  }
  
  private EnumSet<org.apache.accumulo.proxy.thrift.IteratorScope> getProxyIteratorScopes(Set<IteratorScope> scopes) {
    EnumSet<org.apache.accumulo.proxy.thrift.IteratorScope> scopes_ = EnumSet.noneOf(org.apache.accumulo.proxy.thrift.IteratorScope.class);
    for (IteratorScope scope : scopes) {
      scopes_.add(org.apache.accumulo.proxy.thrift.IteratorScope.valueOf(scope.toString().toUpperCase()));
    }
    return scopes_;
  }
  
  @Override
  public void attachIterator(UserPass userpass, String tableName, org.apache.accumulo.proxy.thrift.IteratorSetting setting, Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes) throws TException {
    try {
      getConnector(userpass).tableOperations().attachIterator(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void checkIteratorConflicts(UserPass userpass, String tableName, org.apache.accumulo.proxy.thrift.IteratorSetting setting, Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes)
      throws TException {
    try {
      getConnector(userpass).tableOperations().checkIteratorConflicts(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void cloneTable(UserPass userpass, String tableName, String newTableName, boolean flush, Map<String,String> propertiesToSet,
      Set<String> propertiesToExclude) throws TException {
    try {
      getConnector(userpass).tableOperations().clone(tableName, newTableName, flush, propertiesToSet, propertiesToExclude);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void exportTable(UserPass userpass, String tableName, String exportDir) throws TException {
    try {
      getConnector(userpass).tableOperations().exportTable(tableName, exportDir);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void importTable(UserPass userpass, String tableName, String importDir) throws TException {
    try {
      getConnector(userpass).tableOperations().importTable(tableName, importDir);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public org.apache.accumulo.proxy.thrift.IteratorSetting getIteratorSetting(UserPass userpass, String tableName, String iteratorName, org.apache.accumulo.proxy.thrift.IteratorScope scope) throws TException {
    try {
      IteratorSetting is = getConnector(userpass).tableOperations().getIteratorSetting(tableName, iteratorName, getIteratorScope(scope));
      return new org.apache.accumulo.proxy.thrift.IteratorSetting(is.getPriority(), is.getName(), is.getIteratorClass(), is.getOptions());
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>> listIterators(UserPass userpass, String tableName) throws TException {
    try {
      Map<String,EnumSet<IteratorScope>> iterMap = getConnector(userpass).tableOperations().listIterators(tableName);
      Map<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>> result = new HashMap<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>>();
      for (Entry<String,EnumSet<IteratorScope>> entry : iterMap.entrySet()) {
        result.put(entry.getKey(), getProxyIteratorScopes(entry.getValue()));
      }
      return result;
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void removeIterator(UserPass userpass, String tableName, String iterName, Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes) throws TException {
    try {
      getConnector(userpass).tableOperations().removeIterator(tableName, iterName, getIteratorScopes(scopes));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Set<org.apache.accumulo.proxy.thrift.Range> splitRangeByTablets(UserPass userpass, String tableName, org.apache.accumulo.proxy.thrift.Range range, int maxSplits) throws TException {
    try {
      Set<Range> ranges = getConnector(userpass).tableOperations().splitRangeByTablets(tableName, getRange(range), maxSplits);
      Set<org.apache.accumulo.proxy.thrift.Range> result = new HashSet<org.apache.accumulo.proxy.thrift.Range>();
      for (Range r : ranges) {
        result.add(getRange(r));
      }
      return result;
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  private org.apache.accumulo.proxy.thrift.Range getRange(Range r) {
    return new org.apache.accumulo.proxy.thrift.Range(getProxyKey(r.getStartKey()), r.isStartKeyInclusive(), getProxyKey(r.getEndKey()), r.isEndKeyInclusive());
  }
  
  private org.apache.accumulo.proxy.thrift.Key getProxyKey(Key k) {
    if (k == null)
      return null;
    org.apache.accumulo.proxy.thrift.Key result = new org.apache.accumulo.proxy.thrift.Key(TextUtil.getByteBuffer(k.getRow()), TextUtil.getByteBuffer(k.getColumnFamily()), TextUtil.getByteBuffer(k.getColumnQualifier()),
        TextUtil.getByteBuffer(k.getColumnVisibility()));
    return result;
  }
  
  private Range getRange(org.apache.accumulo.proxy.thrift.Range range) {
    return new Range(Util.fromThrift(range.start), Util.fromThrift(range.stop));
  }
  
  @Override
  public void importDirectory(UserPass userpass, String tableName, String importDir, String failureDir, boolean setTime) throws TException {
    try {
      getConnector(userpass).tableOperations().importDirectory(tableName, importDir, failureDir, setTime);
    } catch (Exception e) {
      throw translateException(e);
    }
    
  }
  
  static private final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[] {});
  
  @Override
  public org.apache.accumulo.proxy.thrift.Range getRowRange(ByteBuffer row) throws TException {
    return new org.apache.accumulo.proxy.thrift.Range(new org.apache.accumulo.proxy.thrift.Key(row, EMPTY, EMPTY, EMPTY), true, new org.apache.accumulo.proxy.thrift.Key(row, EMPTY, EMPTY, EMPTY), true);
  }
  
  @Override
  public org.apache.accumulo.proxy.thrift.Key getFollowing(org.apache.accumulo.proxy.thrift.Key key, org.apache.accumulo.proxy.thrift.PartialKey part) throws TException {
    Key key_ = Util.fromThrift(key);
    PartialKey part_ = PartialKey.valueOf(part.toString());
    Key followingKey = key_.followingKey(part_);
    return getProxyKey(followingKey);
  }
  
  @Override
  public void pingTabletServer(UserPass userpass, String tserver) throws TException {
    try {
      getConnector(userpass).instanceOperations().ping(tserver);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
}

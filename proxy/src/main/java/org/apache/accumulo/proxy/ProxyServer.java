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
import org.apache.accumulo.proxy.thrift.PrincipalToken;
import org.apache.accumulo.proxy.thrift.ScanColumn;
import org.apache.accumulo.proxy.thrift.ScanOptions;
import org.apache.accumulo.proxy.thrift.ScanResult;
import org.apache.accumulo.proxy.thrift.ScanState;
import org.apache.accumulo.proxy.thrift.ScanType;
import org.apache.accumulo.proxy.thrift.UnknownScanner;
import org.apache.accumulo.proxy.thrift.UnknownWriter;
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
  
  protected Connector getConnector(PrincipalToken PrincipalToken) throws Exception {
    Connector connector = instance.getConnector(PrincipalToken.getPrincipal(), PrincipalToken.bufferForToken());
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
  public boolean ping(PrincipalToken PrincipalToken) throws TException {
    return true;
  }
  
  @Override
  public int addConstraint(PrincipalToken PrincipalToken, String tableName, String constraintClassName) throws TException {
    try {
      return getConnector(PrincipalToken).tableOperations().addConstraint(tableName, constraintClassName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void addSplits(PrincipalToken PrincipalToken, String tableName, Set<ByteBuffer> splits) throws TException {
    try {
      SortedSet<Text> sorted = new TreeSet<Text>();
      for (ByteBuffer split : splits) {
        sorted.add(ByteBufferUtil.toText(split));
      }
      getConnector(PrincipalToken).tableOperations().addSplits(tableName, sorted);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void clearLocatorCache(PrincipalToken PrincipalToken, String tableName) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().clearLocatorCache(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void compactTable(PrincipalToken PrincipalToken, String tableName, ByteBuffer start, ByteBuffer end, List<org.apache.accumulo.proxy.thrift.IteratorSetting> iterators, boolean flush,
      boolean wait) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().compact(tableName, ByteBufferUtil.toText(start), ByteBufferUtil.toText(end), getIteratorSettings(iterators),
          flush, wait);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void cancelCompaction(PrincipalToken PrincipalToken, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, org.apache.accumulo.proxy.thrift.AccumuloException, TException {
    try {
      getConnector(PrincipalToken).tableOperations().cancelCompaction(tableName);
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
  public void createTable(PrincipalToken PrincipalToken, String tableName, boolean versioningIter, org.apache.accumulo.proxy.thrift.TimeType timeType) throws TException {
    try {
      if (timeType == null)
        timeType = org.apache.accumulo.proxy.thrift.TimeType.MILLIS;
      
      getConnector(PrincipalToken).tableOperations().create(tableName, versioningIter, TimeType.valueOf(timeType.toString()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void deleteTable(PrincipalToken PrincipalToken, String tableName) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().delete(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void deleteRows(PrincipalToken PrincipalToken, String tableName, ByteBuffer start, ByteBuffer end) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().deleteRows(tableName, ByteBufferUtil.toText(start), ByteBufferUtil.toText(end));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean tableExists(PrincipalToken PrincipalToken, String tableName) throws TException {
    try {
      return getConnector(PrincipalToken).tableOperations().exists(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void flushTable(PrincipalToken PrincipalToken, String tableName, ByteBuffer startRow, ByteBuffer endRow, boolean wait) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().flush(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow), wait);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,Set<String>> getLocalityGroups(PrincipalToken PrincipalToken, String tableName) throws TException {
    try {
      Map<String,Set<Text>> groups = getConnector(PrincipalToken).tableOperations().getLocalityGroups(tableName);
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
  public ByteBuffer getMaxRow(PrincipalToken PrincipalToken, String tableName, Set<ByteBuffer> auths, ByteBuffer startRow, boolean startinclusive,
      ByteBuffer endRow, boolean endinclusive) throws TException {
    try {
      Connector connector = getConnector(PrincipalToken);
      Text startText = ByteBufferUtil.toText(startRow);
      Text endText = ByteBufferUtil.toText(endRow);
      Authorizations auth;
      if (auths != null) {
        auth = getAuthorizations(auths);
      } else {
        auth = connector.securityOperations().getUserAuthorizations(PrincipalToken.getPrincipal());
      }
      Text max = connector.tableOperations().getMaxRow(tableName, auth, startText, startinclusive, endText, endinclusive);
      return TextUtil.getByteBuffer(max);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,String> getTableProperties(PrincipalToken PrincipalToken, String tableName) throws TException {
    try {
      Map<String,String> ret = new HashMap<String,String>();
      
      for (Map.Entry<String,String> entry : getConnector(PrincipalToken).tableOperations().getProperties(tableName)) {
        ret.put(entry.getKey(), entry.getValue());
      }
      return ret;
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public List<ByteBuffer> getSplits(PrincipalToken PrincipalToken, String tableName, int maxSplits) throws TException {
    try {
      Collection<Text> splits = getConnector(PrincipalToken).tableOperations().getSplits(tableName, maxSplits);
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
  public Set<String> listTables(PrincipalToken PrincipalToken) throws TException {
    try {
      return getConnector(PrincipalToken).tableOperations().list();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,Integer> listConstraints(PrincipalToken PrincipalToken, String arg2) throws TException {
    try {
      return getConnector(PrincipalToken).tableOperations().listConstraints(arg2);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void mergeTablets(PrincipalToken PrincipalToken, String tableName, ByteBuffer start, ByteBuffer end) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().merge(tableName, ByteBufferUtil.toText(start), ByteBufferUtil.toText(end));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void offlineTable(PrincipalToken PrincipalToken, String tableName) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().offline(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void onlineTable(PrincipalToken PrincipalToken, String tableName) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().online(tableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void removeConstraint(PrincipalToken PrincipalToken, String tableName, int number) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().removeConstraint(tableName, number);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void removeTableProperty(PrincipalToken PrincipalToken, String tableName, String property) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().removeProperty(tableName, property);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void renameTable(PrincipalToken PrincipalToken, String oldTableName, String newTableName) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().rename(oldTableName, newTableName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void setLocalityGroups(PrincipalToken PrincipalToken, String tableName, Map<String,Set<String>> groupStrings) throws TException {
    try {
      Map<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
      for (String key : groupStrings.keySet()) {
        groups.put(key, new HashSet<Text>());
        for (String val : groupStrings.get(key)) {
          groups.get(key).add(new Text(val));
        }
      }
      getConnector(PrincipalToken).tableOperations().setLocalityGroups(tableName, groups);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void setTableProperty(PrincipalToken PrincipalToken, String tableName, String property, String value) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().setProperty(tableName, property, value);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,String> tableIdMap(PrincipalToken PrincipalToken) throws TException {
    try {
      return getConnector(PrincipalToken).tableOperations().tableIdMap();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,String> getSiteConfiguration(PrincipalToken PrincipalToken) throws TException {
    try {
      return getConnector(PrincipalToken).instanceOperations().getSiteConfiguration();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,String> getSystemConfiguration(PrincipalToken PrincipalToken) throws TException {
    try {
      return getConnector(PrincipalToken).instanceOperations().getSystemConfiguration();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public List<String> getTabletServers(PrincipalToken PrincipalToken) throws TException {
    try {
      return getConnector(PrincipalToken).instanceOperations().getTabletServers();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public List<org.apache.accumulo.proxy.thrift.ActiveScan> getActiveScans(PrincipalToken PrincipalToken, String tserver) throws TException {
    List<org.apache.accumulo.proxy.thrift.ActiveScan> result = new ArrayList<org.apache.accumulo.proxy.thrift.ActiveScan>();
    try {
      List<ActiveScan> activeScans = getConnector(PrincipalToken).instanceOperations().getActiveScans(tserver);
      for (ActiveScan scan : activeScans) {
        org.apache.accumulo.proxy.thrift.ActiveScan pscan = new org.apache.accumulo.proxy.thrift.ActiveScan();
        pscan.client = scan.getClient();
        pscan.principal = scan.getUser();
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
  public List<org.apache.accumulo.proxy.thrift.ActiveCompaction> getActiveCompactions(PrincipalToken PrincipalToken, String tserver) throws TException {
    try {
      List<org.apache.accumulo.proxy.thrift.ActiveCompaction> result = new ArrayList<org.apache.accumulo.proxy.thrift.ActiveCompaction>();
      List<ActiveCompaction> active = getConnector(PrincipalToken).instanceOperations().getActiveCompactions(tserver);
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
  public void removeProperty(PrincipalToken PrincipalToken, String property) throws TException {
    try {
      getConnector(PrincipalToken).instanceOperations().removeProperty(property);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void setProperty(PrincipalToken PrincipalToken, String property, String value) throws TException {
    try {
      getConnector(PrincipalToken).instanceOperations().setProperty(property, value);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean testClassLoad(PrincipalToken PrincipalToken, String className, String asTypeName) throws TException {
    try {
      return getConnector(PrincipalToken).instanceOperations().testClassLoad(className, asTypeName);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean authenticateUser(PrincipalToken PrincipalToken, String user, ByteBuffer password) throws TException {
    try {
      return getConnector(PrincipalToken).securityOperations().authenticateUser(user, password.array());
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void changeUserAuthorizations(PrincipalToken PrincipalToken, String user, Set<ByteBuffer> authorizations) throws TException {
    try {
      Set<String> auths = new HashSet<String>();
      for (ByteBuffer auth : authorizations) {
        auths.add(ByteBufferUtil.toString(auth));
      }
      getConnector(PrincipalToken).securityOperations().changeUserAuthorizations(user, new Authorizations(auths.toArray(new String[0])));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void changePrincipalTokenword(PrincipalToken PrincipalToken, String user, ByteBuffer password) throws TException {
    try {
      getConnector(PrincipalToken).securityOperations().changeUserPassword(user, password.array());
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void createUser(PrincipalToken PrincipalToken, String user, ByteBuffer password) throws TException {
    try {
      getConnector(PrincipalToken).securityOperations().createUser(user, password.array());
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void dropUser(PrincipalToken PrincipalToken, String user) throws TException {
    try {
      getConnector(PrincipalToken).securityOperations().dropUser(user);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public List<ByteBuffer> getUserAuthorizations(PrincipalToken PrincipalToken, String user) throws TException {
    try {
      return getConnector(PrincipalToken).securityOperations().getUserAuthorizations(user).getAuthorizationsBB();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void grantSystemPermission(PrincipalToken PrincipalToken, String user, org.apache.accumulo.proxy.thrift.SystemPermission perm) throws TException {
    try {
      getConnector(PrincipalToken).securityOperations().grantSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void grantTablePermission(PrincipalToken PrincipalToken, String user, String table, org.apache.accumulo.proxy.thrift.TablePermission perm) throws TException {
    try {
      getConnector(PrincipalToken).securityOperations().grantTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean hasSystemPermission(PrincipalToken PrincipalToken, String user, org.apache.accumulo.proxy.thrift.SystemPermission perm) throws TException {
    try {
      return getConnector(PrincipalToken).securityOperations().hasSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public boolean hasTablePermission(PrincipalToken PrincipalToken, String user, String table, org.apache.accumulo.proxy.thrift.TablePermission perm) throws TException {
    try {
      return getConnector(PrincipalToken).securityOperations().hasTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Set<String> listUsers(PrincipalToken PrincipalToken) throws TException {
    try {
      return getConnector(PrincipalToken).securityOperations().listUsers();
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void revokeSystemPermission(PrincipalToken PrincipalToken, String user, org.apache.accumulo.proxy.thrift.SystemPermission perm) throws TException {
    try {
      getConnector(PrincipalToken).securityOperations().revokeSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void revokeTablePermission(PrincipalToken PrincipalToken, String user, String table, org.apache.accumulo.proxy.thrift.TablePermission perm) throws TException {
    try {
      getConnector(PrincipalToken).securityOperations().revokeTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
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
  public String createScanner(PrincipalToken PrincipalToken, String tableName, ScanOptions opts)
      throws TException {
    try {
      Connector connector = getConnector(PrincipalToken);
      
      Authorizations auth;
      if (opts != null && opts.isSetAuthorizations()) {
        auth = getAuthorizations(opts.authorizations);
      } else {
        auth = connector.securityOperations().getUserAuthorizations(PrincipalToken.getPrincipal());
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
  public String createBatchScanner(PrincipalToken PrincipalToken, String tableName, BatchScanOptions opts)
      throws TException {
    try {
      Connector connector = getConnector(PrincipalToken);
      
            
      int threads = 10;
      Authorizations auth;
      if (opts != null && opts.isSetAuthorizations()) {
        auth = getAuthorizations(opts.authorizations);
      } else {
        auth = connector.securityOperations().getUserAuthorizations(PrincipalToken.getPrincipal());
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
  public void updateAndFlush(PrincipalToken PrincipalToken, String tableName, Map<ByteBuffer,List<ColumnUpdate>> cells) throws TException {
    try {
      BatchWriter writer = getWriter(PrincipalToken, tableName, null);
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
  public String createWriter(PrincipalToken PrincipalToken, String tableName, WriterOptions opts) throws TException {
    try {
      BatchWriter writer = getWriter(PrincipalToken, tableName, opts);
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
  
  private BatchWriter getWriter(PrincipalToken PrincipalToken, String tableName, WriterOptions opts) throws Exception {
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
    return getConnector(PrincipalToken).createBatchWriter(tableName, cfg);
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
  public void attachIterator(PrincipalToken PrincipalToken, String tableName, org.apache.accumulo.proxy.thrift.IteratorSetting setting, Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().attachIterator(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void checkIteratorConflicts(PrincipalToken PrincipalToken, String tableName, org.apache.accumulo.proxy.thrift.IteratorSetting setting, Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes)
      throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().checkIteratorConflicts(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void cloneTable(PrincipalToken PrincipalToken, String tableName, String newTableName, boolean flush, Map<String,String> propertiesToSet,
      Set<String> propertiesToExclude) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().clone(tableName, newTableName, flush, propertiesToSet, propertiesToExclude);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void exportTable(PrincipalToken PrincipalToken, String tableName, String exportDir) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().exportTable(tableName, exportDir);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public void importTable(PrincipalToken PrincipalToken, String tableName, String importDir) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().importTable(tableName, importDir);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public org.apache.accumulo.proxy.thrift.IteratorSetting getIteratorSetting(PrincipalToken PrincipalToken, String tableName, String iteratorName, org.apache.accumulo.proxy.thrift.IteratorScope scope) throws TException {
    try {
      IteratorSetting is = getConnector(PrincipalToken).tableOperations().getIteratorSetting(tableName, iteratorName, getIteratorScope(scope));
      return new org.apache.accumulo.proxy.thrift.IteratorSetting(is.getPriority(), is.getName(), is.getIteratorClass(), is.getOptions());
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Map<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>> listIterators(PrincipalToken PrincipalToken, String tableName) throws TException {
    try {
      Map<String,EnumSet<IteratorScope>> iterMap = getConnector(PrincipalToken).tableOperations().listIterators(tableName);
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
  public void removeIterator(PrincipalToken PrincipalToken, String tableName, String iterName, Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().removeIterator(tableName, iterName, getIteratorScopes(scopes));
    } catch (Exception e) {
      throw translateException(e);
    }
  }
  
  @Override
  public Set<org.apache.accumulo.proxy.thrift.Range> splitRangeByTablets(PrincipalToken PrincipalToken, String tableName, org.apache.accumulo.proxy.thrift.Range range, int maxSplits) throws TException {
    try {
      Set<Range> ranges = getConnector(PrincipalToken).tableOperations().splitRangeByTablets(tableName, getRange(range), maxSplits);
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
  public void importDirectory(PrincipalToken PrincipalToken, String tableName, String importDir, String failureDir, boolean setTime) throws TException {
    try {
      getConnector(PrincipalToken).tableOperations().importDirectory(tableName, importDir, failureDir, setTime);
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
  public void pingTabletServer(PrincipalToken PrincipalToken, String tserver) throws TException {
    try {
      getConnector(PrincipalToken).instanceOperations().ping(tserver);
    } catch (Exception e) {
      throw translateException(e);
    }
  }
}

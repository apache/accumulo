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

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
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
import org.apache.accumulo.proxy.thrift.AccumuloException;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.AccumuloSecurityException;
import org.apache.accumulo.proxy.thrift.IOException;
import org.apache.accumulo.proxy.thrift.KeyValueAndPeek;
import org.apache.accumulo.proxy.thrift.NoMoreEntriesException;
import org.apache.accumulo.proxy.thrift.PActiveCompaction;
import org.apache.accumulo.proxy.thrift.PActiveScan;
import org.apache.accumulo.proxy.thrift.PColumn;
import org.apache.accumulo.proxy.thrift.PColumnUpdate;
import org.apache.accumulo.proxy.thrift.PCompactionReason;
import org.apache.accumulo.proxy.thrift.PCompactionType;
import org.apache.accumulo.proxy.thrift.PIteratorScope;
import org.apache.accumulo.proxy.thrift.PIteratorSetting;
import org.apache.accumulo.proxy.thrift.PKey;
import org.apache.accumulo.proxy.thrift.PKeyExtent;
import org.apache.accumulo.proxy.thrift.PKeyValue;
import org.apache.accumulo.proxy.thrift.PPartialKey;
import org.apache.accumulo.proxy.thrift.PRange;
import org.apache.accumulo.proxy.thrift.PScanResult;
import org.apache.accumulo.proxy.thrift.PScanState;
import org.apache.accumulo.proxy.thrift.PScanType;
import org.apache.accumulo.proxy.thrift.PSystemPermission;
import org.apache.accumulo.proxy.thrift.PTablePermission;
import org.apache.accumulo.proxy.thrift.PTimeType;
import org.apache.accumulo.proxy.thrift.TableExistsException;
import org.apache.accumulo.proxy.thrift.TableNotFoundException;
import org.apache.accumulo.proxy.thrift.UserPass;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ProxyServer implements AccumuloProxy.Iface {
  
  protected Instance instance;
  
  protected class ScannerPlusIterator {
    public ScannerBase scanner;
    public Iterator<Map.Entry<Key,Value>> iterator;
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
    
    scannerCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000).build();
    
    writerCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000).build();
  }
  
  protected Connector getConnector(UserPass userpass) throws Exception {
    Connector connector = instance.getConnector(userpass.getUsername(), userpass.bufferForPassword());
    return connector;
  }
  
  @Override
  public boolean ping(UserPass userpass) throws TException {
    return true;
  }
  
  @Override
  public int tableOperations_addConstraint(UserPass userpass, String tableName, String constraintClassName) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, TException {
    try {
      return getConnector(userpass).tableOperations().addConstraint(tableName, constraintClassName);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_addSplits(UserPass userpass, String tableName, Set<ByteBuffer> splits) throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException, TException {
    try {
      SortedSet<Text> sorted = new TreeSet<Text>();
      for (ByteBuffer split : splits) {
        sorted.add(ByteBufferUtil.toText(split));
      }
      getConnector(userpass).tableOperations().addSplits(tableName, sorted);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_clearLocatorCache(UserPass userpass, String tableName) throws TableNotFoundException, TException {
    try {
      getConnector(userpass).tableOperations().clearLocatorCache(tableName);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_compact(UserPass userpass, String tableName, ByteBuffer start, ByteBuffer end, List<PIteratorSetting> iterators, boolean flush, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException, TException {
    try {
      getConnector(userpass).tableOperations().compact(tableName, ByteBufferUtil.toText(start), ByteBufferUtil.toText(end), getPIteratorSettings(iterators), flush, wait);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  private List<IteratorSetting> getPIteratorSettings(List<PIteratorSetting> iterators) {
    List<IteratorSetting> result = new ArrayList<IteratorSetting>();
    if (iterators != null) {
      for (PIteratorSetting is : iterators) {
        result.add(getIteratorSetting(is));
      }
    }
    return result;
  }

  @Override
  public void tableOperations_create(UserPass userpass, String tableName, boolean versioningIter, PTimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException, TException {
    try {
      getConnector(userpass).tableOperations().create(tableName, versioningIter, TimeType.valueOf(timeType.toString()));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_delete(UserPass userpass, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TException {
    try {
      getConnector(userpass).tableOperations().delete(tableName);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_deleteRows(UserPass userpass, String tableName, ByteBuffer start, ByteBuffer end) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, TException {
    try {
      getConnector(userpass).tableOperations().deleteRows(tableName, ByteBufferUtil.toText(start), ByteBufferUtil.toText(end));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public boolean tableOperations_exists(UserPass userpass, String tableName) throws TException {
    try {
      return getConnector(userpass).tableOperations().exists(tableName);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_flush(UserPass userpass, String tableName, ByteBuffer startRow, ByteBuffer endRow, boolean wait) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      getConnector(userpass).tableOperations().flush(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow), wait);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public Map<String,Set<String>> tableOperations_getLocalityGroups(UserPass userpass, String tableName) throws AccumuloException, TableNotFoundException,
      TException {
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
      throw new TException(e);
    }
  }
  
  @Override
  public ByteBuffer tableOperations_getMaxRow(UserPass userpass, String tableName, List<ByteBuffer> auths, ByteBuffer startRow, boolean startinclusive, ByteBuffer endRow,
      boolean endinclusive) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, TException {
    try {
      
      Text startText = ByteBufferUtil.toText(startRow);
      Text endText = ByteBufferUtil.toText(endRow);
      Text max = getConnector(userpass).tableOperations().getMaxRow(tableName, new Authorizations(auths), startText, startinclusive, endText, endinclusive);
      return TextUtil.getByteBuffer(max); 
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public Map<String,String> tableOperations_getProperties(UserPass userpass, String tableName) throws AccumuloException, TableNotFoundException, TException {
    try {
      Map<String,String> ret = new HashMap<String,String>();
      
      for (Map.Entry<String,String> entry : getConnector(userpass).tableOperations().getProperties(tableName)) {
        ret.put(entry.getKey(), entry.getValue());
      }
      return ret;
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public List<ByteBuffer> tableOperations_getSplits(UserPass userpass, String tableName, int maxSplits) throws TableNotFoundException, TException {
    try {
      Collection<Text> splits = getConnector(userpass).tableOperations().getSplits(tableName, maxSplits);
      List<ByteBuffer> ret = new ArrayList<ByteBuffer>();
      for (Text split : splits) {
        ret.add(TextUtil.getByteBuffer(split));
      }
      return ret;
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public Set<String> tableOperations_list(UserPass userpass) throws TException {
    try {
      return getConnector(userpass).tableOperations().list();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public Map<String,Integer> tableOperations_listConstraints(UserPass userpass, String arg2) throws AccumuloException, TableNotFoundException, TException {
    try {
      return getConnector(userpass).tableOperations().listConstraints(arg2);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_merge(UserPass userpass, String tableName, ByteBuffer start, ByteBuffer end) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, TException {
    try {
      getConnector(userpass).tableOperations().merge(tableName, ByteBufferUtil.toText(start), ByteBufferUtil.toText(end));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_offline(UserPass userpass, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException,
      TException {
    try {
      getConnector(userpass).tableOperations().offline(tableName);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_online(UserPass userpass, String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException,
      TException {
    try {
      getConnector(userpass).tableOperations().online(tableName);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_removeConstraint(UserPass userpass, String tableName, int number) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      getConnector(userpass).tableOperations().removeConstraint(tableName, number);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_removeProperty(UserPass userpass, String tableName, String property) throws AccumuloException, AccumuloSecurityException,
      TException {
    try {
      getConnector(userpass).tableOperations().removeProperty(tableName, property);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_rename(UserPass userpass, String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException,
      AccumuloException, TableExistsException, TException {
    try {
      getConnector(userpass).tableOperations().rename(oldTableName, newTableName);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_setLocalityGroups(UserPass userpass, String tableName, Map<String,Set<String>> groupStrings) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, TException {
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
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_setProperty(UserPass userpass, String tableName, String property, String value) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).tableOperations().setProperty(tableName, property, value);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public Map<String,String> tableOperations_tableIdMap(UserPass userpass) throws TException {
    try {
      return getConnector(userpass).tableOperations().tableIdMap();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public Map<String,String> instanceOperations_getSiteConfiguration(UserPass userpass) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      return getConnector(userpass).instanceOperations().getSiteConfiguration();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public Map<String,String> instanceOperations_getSystemConfiguration(UserPass userpass) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      return getConnector(userpass).instanceOperations().getSystemConfiguration();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public List<String> instanceOperations_getTabletServers(UserPass userpass) throws TException {
    try {
      return getConnector(userpass).instanceOperations().getTabletServers();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public List<PActiveScan> instanceOperations_getActiveScans(UserPass userpass, String tserver) throws AccumuloException, AccumuloSecurityException, TException {
    List<PActiveScan> result = new ArrayList<PActiveScan>();
    try {
      List<ActiveScan> activeScans = getConnector(userpass).instanceOperations().getActiveScans(tserver);
      for (ActiveScan scan : activeScans) {
        PActiveScan pscan = new PActiveScan();
        pscan.client = scan.getClient();
        pscan.user = scan.getUser();
        pscan.table = scan.getTable();
        pscan.age = scan.getAge();
        pscan.idleTime = scan.getIdleTime();
        pscan.type = PScanType.valueOf(scan.getType().toString());
        pscan.state = PScanState.valueOf(scan.getState().toString());
        KeyExtent e = scan.getExtent();
        pscan.extent = new PKeyExtent(
            e.getTableId().toString(), 
            TextUtil.getByteBuffer(e.getEndRow()), 
            TextUtil.getByteBuffer(e.getPrevEndRow()));
        pscan.columns = new ArrayList<PColumn>();
        if (scan.getColumns() != null) {
          for (Column c : scan.getColumns()) {
            PColumn column = new PColumn();
            column.setColFamily(c.getColumnFamily());
            column.setColQualifier(c.getColumnQualifier());
            column.setColVisibility(c.getColumnVisibility());
            pscan.columns.add(column);
          }
        }
        pscan.iterators = new ArrayList<PIteratorSetting>();
        for (String iteratorString : scan.getSsiList()) {
          String[] parts = iteratorString.split("[=,]");
          if (parts.length == 3) {
            String name = parts[0];
            int priority = Integer.parseInt(parts[1]);
            String classname = parts[2];
            PIteratorSetting settings = new PIteratorSetting(priority, name, classname, scan.getSsio().get(name));
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
      throw new TException(e);
    }
  }
  
  @Override
  public List<PActiveCompaction> instanceOperations_getActiveCompactions(UserPass userpass, String tserver) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      List<PActiveCompaction> result = new ArrayList<PActiveCompaction>();
      List<ActiveCompaction> active = getConnector(userpass).instanceOperations().getActiveCompactions(tserver);
      for (ActiveCompaction comp : active) {
        PActiveCompaction pcomp = new PActiveCompaction();
        pcomp.age = comp.getAge();
        pcomp.entriesRead = comp.getEntriesRead();
        pcomp.entriesWritten = comp.getEntriesWritten();
        KeyExtent e = comp.getExtent();
        pcomp.extent = new PKeyExtent(
            e.getTableId().toString(),
            TextUtil.getByteBuffer(e.getEndRow()),
            TextUtil.getByteBuffer(e.getPrevEndRow())
            );
        pcomp.inputFiles = new ArrayList<String>();
        if (comp.getInputFiles() != null) {
          pcomp.inputFiles.addAll(comp.getInputFiles());
        }
        pcomp.localityGroup = comp.getLocalityGroup();
        pcomp.outputFile = comp.getOutputFile();
        pcomp.reason = PCompactionReason.valueOf(comp.getReason().toString());
        pcomp.type = PCompactionType.valueOf(comp.getType().toString());
        
        pcomp.iterators = new ArrayList<PIteratorSetting>();
        if (comp.getIterators() != null) {
          for (IteratorSetting setting : comp.getIterators()) {
            PIteratorSetting psetting = 
                new PIteratorSetting(
                    setting.getPriority(),
                    setting.getName(),
                    setting.getIteratorClass(),
                    setting.getOptions());
            pcomp.iterators.add(psetting);
          }
        }
        result.add(pcomp);
      }
      return result;
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void instanceOperations_removeProperty(UserPass userpass, String property) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      getConnector(userpass).instanceOperations().removeProperty(property);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void instanceOperations_setProperty(UserPass userpass, String property, String value) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      getConnector(userpass).instanceOperations().setProperty(property, value);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public boolean instanceOperations_testClassLoad(UserPass userpass, String className, String asTypeName) throws AccumuloException, AccumuloSecurityException,
      TException {
    try {
      return getConnector(userpass).instanceOperations().testClassLoad(className, asTypeName);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public boolean securityOperations_authenticateUser(UserPass userpass, String user, ByteBuffer password) throws AccumuloException, AccumuloSecurityException,
      TException {
    try {
      return getConnector(userpass).securityOperations().authenticateUser(user, password.array());
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void securityOperations_changeUserAuthorizations(UserPass userpass, String user, Set<ByteBuffer> authorizations) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      Set<String> auths = new HashSet<String>();
      for (ByteBuffer auth : authorizations) {
        auths.add(ByteBufferUtil.toString(auth));
      }
      getConnector(userpass).securityOperations().changeUserAuthorizations(user, new Authorizations(auths.toArray(new String[0])));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void securityOperations_changeUserPassword(UserPass userpass, String user, ByteBuffer password) throws AccumuloException, AccumuloSecurityException,
      TException {
    try {
      getConnector(userpass).securityOperations().changeUserPassword(user, password.array());
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void securityOperations_createUser(UserPass userpass, String user, ByteBuffer password) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).securityOperations().createUser(user, password.array());
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void securityOperations_dropUser(UserPass userpass, String user) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      getConnector(userpass).securityOperations().dropUser(user);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public List<ByteBuffer> securityOperations_getUserAuthorizations(UserPass userpass, String user) throws AccumuloException, AccumuloSecurityException,
      TException {
    try {
      return getConnector(userpass).securityOperations().getUserAuthorizations(user).getAuthorizationsBB();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void securityOperations_grantSystemPermission(UserPass userpass, String user, PSystemPermission perm) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).securityOperations().grantSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void securityOperations_grantTablePermission(UserPass userpass, String user, String table, PTablePermission perm) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).securityOperations().grantTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public boolean securityOperations_hasSystemPermission(UserPass userpass, String user, PSystemPermission perm) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      return getConnector(userpass).securityOperations().hasSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public boolean securityOperations_hasTablePermission(UserPass userpass, String user, String table, PTablePermission perm) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      return getConnector(userpass).securityOperations().hasTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public Set<String> securityOperations_listUsers(UserPass userpass) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      return getConnector(userpass).securityOperations().listUsers();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void securityOperations_revokeSystemPermission(UserPass userpass, String user, PSystemPermission perm) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).securityOperations().revokeSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void securityOperations_revokeTablePermission(UserPass userpass, String user, String table, PTablePermission perm) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).securityOperations().revokeTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public String createScanner(UserPass userpass, String tableName, Set<ByteBuffer> authorizations, List<PIteratorSetting> iterators, PRange prange) throws TException {
    try {
      Connector connector = getConnector(userpass);
      
      Authorizations auth;
      if (authorizations != null)
        auth = new Authorizations(authorizations.toArray(new String[0]));
      else
        auth = connector.securityOperations().getUserAuthorizations(userpass.getUsername());
      
      Scanner scanner = connector.createScanner(tableName, auth);
      
      if (iterators != null) {
        for (PIteratorSetting iter : iterators) {
          IteratorSetting is = new IteratorSetting(iter.getPriority(), iter.getName(), iter.getIteratorClass(), iter.getProperties());
          scanner.addScanIterator(is);
        }
      }
      
      Range range = prange == null ? new Range() : (new Range(prange.getStart() == null ? null : Util.fromThrift(prange.getStart()), true,
          prange.getStop() == null ? null : Util.fromThrift(prange.getStop()), false));
      
      scanner.setRange(range);
      UUID uuid = UUID.randomUUID();
      
      ScannerPlusIterator spi = new ScannerPlusIterator();
      spi.scanner = scanner;
      spi.iterator = scanner.iterator();
      scannerCache.put(uuid, spi);
      return uuid.toString();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public String createBatchScanner(UserPass userpass, String tableName, Set<ByteBuffer> authorizations, List<PIteratorSetting> iterators, List<PRange> pranges)
      throws TException {
    try {
      Connector connector = getConnector(userpass);
      
      Authorizations auth;
      if (authorizations != null)
        auth = new Authorizations(authorizations.toArray(new String[0]));
      else
        auth = connector.securityOperations().getUserAuthorizations(userpass.getUsername());
      
      BatchScanner scanner = connector.createBatchScanner(tableName, auth, 10);
      
      if (iterators != null) {
        for (PIteratorSetting iter: iterators) {
          IteratorSetting is = new IteratorSetting(iter.getPriority(), iter.getName(), iter.getIteratorClass(), iter.getProperties());
          scanner.addScanIterator(is);
        }
      }
      
      ArrayList<Range> ranges = new ArrayList<Range>();
      
      if (pranges == null) {
        ranges.add(new Range());
      } else {
        for (PRange range : pranges) {
          Range aRange = new Range(range.getStart() == null ? null : Util.fromThrift(range.getStart()), true, range.getStop() == null ? null
              : Util.fromThrift(range.getStop()), false);
          ranges.add(aRange);
        }
      }
      scanner.setRanges(ranges);
      UUID uuid = UUID.randomUUID();
      
      ScannerPlusIterator spi = new ScannerPlusIterator();
      spi.scanner = scanner;
      spi.iterator = scanner.iterator();
      scannerCache.put(uuid, spi);
      return uuid.toString();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public boolean scanner_hasnext(String scanner) throws TException {
    ScannerPlusIterator spi = scannerCache.getIfPresent(UUID.fromString(scanner));
    if (spi == null) {
      throw new TException("Scanner never existed or no longer exists");
    }
    
    return (spi.iterator.hasNext());
  }
  
  @Override
  public KeyValueAndPeek scanner_next(String scanner) throws TException, NoMoreEntriesException {
    
    PScanResult scanResult = scanner_next_k(scanner, 1);
    if (scanResult.results.size() > 0) {
      return new KeyValueAndPeek(scanResult.results.get(0), scanResult.isMore());
    } else {
      throw new NoMoreEntriesException();
    }
    
  }
  
  @Override
  public PScanResult scanner_next_k(String scanner, int k) throws TException {
    
    // fetch the scanner
    ScannerPlusIterator spi = scannerCache.getIfPresent(UUID.fromString(scanner));
    if (spi == null) {
      throw new TException("Scanner never existed or no longer exists");
    }
    Iterator<Map.Entry<Key,Value>> batchScanner = spi.iterator;
    // synchronized to prevent race conditions
    synchronized (batchScanner) {
      PScanResult ret = new PScanResult();
      ret.setResults(new ArrayList<PKeyValue>());
      int numRead = 0;
      try {
        while (batchScanner.hasNext() && numRead < k) {
          Map.Entry<Key,Value> next = batchScanner.next();
          ret.addToResults(new PKeyValue(Util.toThrift(next.getKey()), ByteBuffer.wrap(next.getValue().get())));
          numRead++;
        }
        ret.setMore(numRead == k);
      } catch (Exception ex) {
        close_scanner(scanner);
        throw new TException(ex);
      }
      return ret;
    }
  }
  
  @Override
  public void close_scanner(String uuid) throws TException {
    scannerCache.invalidate(uuid);
  }
  
  @Override
  public void updateAndFlush(UserPass userpass, String tableName, Map<ByteBuffer,List<PColumnUpdate>> cells)
      throws TException {
    try {
      BatchWriter writer = getWriter(userpass, tableName);
      addCellsToWriter(cells, writer);
      writer.flush();
      writer.close();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();
  
  private void addCellsToWriter(Map<ByteBuffer,List<PColumnUpdate>> cells, BatchWriter writer)
      throws MutationsRejectedException {
    HashMap<Text,ColumnVisibility> vizMap = new HashMap<Text,ColumnVisibility>();
    
    for (Entry<ByteBuffer,List<PColumnUpdate>> entry : cells.entrySet()) {
      Mutation m = new Mutation(ByteBufferUtil.toBytes(entry.getKey()));
      
      for (PColumnUpdate update : entry.getValue()) {
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
  public String createWriter(UserPass userpass, String tableName) throws TException {
    try {
      BatchWriter writer = getWriter(userpass, tableName);
      UUID uuid = UUID.randomUUID();
      writerCache.put(uuid, writer);
      return uuid.toString();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void writer_update(String writer, Map<ByteBuffer,List<PColumnUpdate>> cells) throws TException {
    try {
      BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
      if (batchwriter == null) {
        throw new TException("Writer never existed or no longer exists");
      }
      addCellsToWriter(cells, batchwriter);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void writer_flush(String writer) throws TException {
    try {
      BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
      if (batchwriter == null) {
        throw new TException("Writer never existed or no longer exists");
      }
      batchwriter.flush();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void writer_close(String writer) throws TException {
    try {
      BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
      if (batchwriter == null) {
        throw new TException("Writer never existed or no longer exists");
      }
      batchwriter.close();
      writerCache.invalidate(UUID.fromString(writer));
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  private BatchWriter getWriter(UserPass userpass, String tableName) throws Exception {
    return getConnector(userpass).createBatchWriter(tableName, new BatchWriterConfig());
  }
  
  private IteratorSetting getIteratorSetting(PIteratorSetting setting) {
    return new IteratorSetting(setting.priority, setting.name, setting.iteratorClass, setting.getProperties());
  }
  
  private IteratorScope getIteratorScope(PIteratorScope scope) {
    return IteratorScope.valueOf(scope.toString().toLowerCase());
  }
  
  private EnumSet<IteratorScope> getIteratorScopes(Set<PIteratorScope> scopes) {
    EnumSet<IteratorScope> scopes_ = EnumSet.noneOf(IteratorScope.class);
    for (PIteratorScope scope: scopes) {
      scopes_.add(getIteratorScope(scope));
    }
    return scopes_;
  }

  private EnumSet<PIteratorScope> getPIteratorScopes(Set<IteratorScope> scopes) {
    EnumSet<PIteratorScope> scopes_ = EnumSet.noneOf(PIteratorScope.class);
    for (IteratorScope scope: scopes) {
      scopes_.add(PIteratorScope.valueOf(scope.toString().toUpperCase()));
    }
    return scopes_;
  }


  @Override
  public void tableOperations_attachIterator(UserPass userpass, String tableName, PIteratorSetting setting, Set<PIteratorScope> scopes)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TException {
    try {
      getConnector(userpass).tableOperations().attachIterator(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void tableOperations_checkIteratorConflicts(UserPass userpass, String tableName, PIteratorSetting setting, Set<PIteratorScope> scopes)
      throws AccumuloException, TableNotFoundException, TException {
    try {
      getConnector(userpass).tableOperations().checkIteratorConflicts(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void tableOperations_clone(UserPass userpass, String tableName, String newTableName, boolean flush, Map<String,String> propertiesToSet,
      Set<String> propertiesToExclude) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException, TException {
    try {
      getConnector(userpass).tableOperations().clone(tableName, newTableName, flush, propertiesToSet, propertiesToExclude);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void tableOperations_exportTable(UserPass userpass, String tableName, String exportDir) throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).tableOperations().exportTable(tableName, exportDir);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void tableOperations_importTable(UserPass userpass, String tableName, String importDir) throws TableExistsException, AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).tableOperations().importTable(tableName, importDir);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public PIteratorSetting tableOperations_getIteratorSetting(UserPass userpass, String tableName, String iteratorName, PIteratorScope scope)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TException {
    try {
      IteratorSetting is = getConnector(userpass).tableOperations().getIteratorSetting(tableName, iteratorName, getIteratorScope(scope));
      return new PIteratorSetting(is.getPriority(), is.getName(), is.getIteratorClass(), is.getOptions());
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public Map<String,Set<PIteratorScope>> tableOperations_listIterators(UserPass userpass, String tableName) throws AccumuloSecurityException,
      AccumuloException, TableNotFoundException, TException {
    try {
      Map<String,EnumSet<IteratorScope>> iterMap = getConnector(userpass).tableOperations().listIterators(tableName);
      Map<String, Set<PIteratorScope>> result = new HashMap<String, Set<PIteratorScope>>();
      for (Entry<String,EnumSet<IteratorScope>> entry : iterMap.entrySet()) {
        result.put(entry.getKey(), getPIteratorScopes(entry.getValue()));
      }
      return result;
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void tableOperations_removeIterator(UserPass userpass, String tableName, String iterName, Set<PIteratorScope> scopes)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TException {
    try {
      getConnector(userpass).tableOperations().removeIterator(tableName, iterName, getIteratorScopes(scopes));
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public Set<PRange> tableOperations_splitRangeByTablets(UserPass userpass, String tableName, PRange range, int maxSplits) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException, TException {
    try {
      Set<Range> ranges = getConnector(userpass).tableOperations().splitRangeByTablets(tableName, getRange(range), maxSplits);
      Set<PRange> result = new HashSet<PRange>();
      for (Range r: ranges) {
        result.add(getPRange(r));
      }
      return result;
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  private PRange getPRange(Range r) {
    return new PRange(getPKey(r.getStartKey()), r.isStartKeyInclusive(), getPKey(r.getEndKey()), r.isEndKeyInclusive());
  }

  private PKey getPKey(Key k) {
    if (k == null)
      return null;
    PKey result = new PKey(
        TextUtil.getByteBuffer(k.getRow()),
        TextUtil.getByteBuffer(k.getColumnFamily()),
        TextUtil.getByteBuffer(k.getColumnQualifier()),
        TextUtil.getByteBuffer(k.getColumnVisibility())
        );
    return result;
  }

  private Range getRange(PRange range) {
    return new Range(getKey(range.start), getKey(range.stop));
  }

  private Key getKey(PKey start) {
    if (start == null)
      return null;
    return new Key(start.getRow(), start.getColFamily(), start.getColQualifier(), start.getColVisibility(), 0);
  }

  @Override
  public void tableOperations_importDirectory(UserPass userpass, String tableName, String importDir, String failureDir, boolean setTime)
      throws TableNotFoundException, IOException, AccumuloException, AccumuloSecurityException, TException {
    try {
      getConnector(userpass).tableOperations().importDirectory(tableName, importDir, failureDir, setTime);
    } catch (Exception e) {
      throw new TException(e);
    }
    
  }
  
  static private final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[]{});

  @Override
  public PRange getRowRange(ByteBuffer row) throws TException {
    return new PRange(new PKey(row, EMPTY, EMPTY, EMPTY), true, new PKey(row, EMPTY, EMPTY, EMPTY), true);
  }

  @Override
  public PKey getFollowing(PKey key, PPartialKey part) throws TException {
    Key key_ = getKey(key);
    PartialKey part_ = PartialKey.valueOf(part.toString());
    Key followingKey = key_.followingKey(part_);
    return getPKey(followingKey);
  }

  @Override
  public void instanceOperations_pingTabletServer(UserPass userpass, String tserver) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      getConnector(userpass).instanceOperations().ping(tserver);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
}

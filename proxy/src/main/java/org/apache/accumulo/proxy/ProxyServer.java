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
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.proxy.thrift.AccumuloException;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.AccumuloSecurityException;
import org.apache.accumulo.proxy.thrift.KeyValueAndPeek;
import org.apache.accumulo.proxy.thrift.NoMoreEntriesException;
import org.apache.accumulo.proxy.thrift.PColumn;
import org.apache.accumulo.proxy.thrift.PColumnUpdate;
import org.apache.accumulo.proxy.thrift.PIteratorSetting;
import org.apache.accumulo.proxy.thrift.PKeyValue;
import org.apache.accumulo.proxy.thrift.PRange;
import org.apache.accumulo.proxy.thrift.PScanResult;
import org.apache.accumulo.proxy.thrift.PSystemPermission;
import org.apache.accumulo.proxy.thrift.PTablePermission;
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
  public void tableOperations_addSplits(UserPass userpass, String tableName, Set<String> splits) throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException, TException {
    try {
      SortedSet<Text> sorted = new TreeSet<Text>();
      for (String split : splits) {
        sorted.add(new Text(split));
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
  public void tableOperations_compact(UserPass userpass, String tableName, String start, String end, boolean flush, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException, TException {
    try {
      getConnector(userpass).tableOperations().compact(tableName, new Text(start), new Text(end), flush, wait);
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void tableOperations_create(UserPass userpass, String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException, TException {
    try {
      getConnector(userpass).tableOperations().create(tableName);
    } catch (Exception e) {
      e.printStackTrace();
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
  public void tableOperations_deleteRows(UserPass userpass, String tableName, String start, String end) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, TException {
    try {
      getConnector(userpass).tableOperations().deleteRows(tableName, new Text(start), new Text(end));
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
  public void tableOperations_flush(UserPass userpass, String tableName) throws AccumuloException, AccumuloSecurityException, TException {
    try {
      getConnector(userpass).tableOperations().flush(tableName, null, null, true);
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
  public String tableOperations_getMaxRow(UserPass userpass, String tableName, List<ByteBuffer> auths, String start, boolean startinclusive, String end,
      boolean endinclusive) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, TException {
    try {
      
      Text startText = start == null ? null : new Text(start);
      Text endText = end == null ? null : new Text(end);
      return getConnector(userpass).tableOperations().getMaxRow(tableName, new Authorizations(auths), startText, startinclusive, endText, endinclusive)
          .toString();
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
  public List<String> tableOperations_getSplits(UserPass userpass, String tableName, int maxSplits) throws TableNotFoundException, TException {
    try {
      Collection<Text> splits = getConnector(userpass).tableOperations().getSplits(tableName, maxSplits);
      List<String> ret = new ArrayList<String>();
      for (Text split : splits) {
        ret.add(split.toString());
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
  public void tableOperations_merge(UserPass userpass, String tableName, String start, String end) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, TException {
    try {
      getConnector(userpass).tableOperations().merge(tableName, new Text(start), new Text(end));
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
  public void securityOperations_changeUserAuthorizations(UserPass userpass, String user, Set<String> authorizations) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).securityOperations().changeUserAuthorizations(user, new Authorizations(authorizations.toArray(new String[0])));
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
  public void securityOperations_createUser(UserPass userpass, String user, ByteBuffer password, Set<String> authorizations) throws AccumuloException,
      AccumuloSecurityException, TException {
    try {
      getConnector(userpass).securityOperations().createUser(user, password.array());
      getConnector(userpass).securityOperations().changeUserAuthorizations(user, new Authorizations(authorizations.toArray(new String[] {})));
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
  public String createScanner(UserPass userpass, String tableName, Set<String> authorizations, PIteratorSetting in_is, PRange prange) throws TException {
    try {
      Connector connector = getConnector(userpass);
      
      Authorizations auth;
      if (authorizations != null)
        auth = new Authorizations(authorizations.toArray(new String[0]));
      else
        auth = connector.securityOperations().getUserAuthorizations(userpass.getUsername());
      
      Scanner scanner = connector.createScanner(tableName, auth);
      
      if (in_is != null) {
        IteratorSetting is = new IteratorSetting(in_is.getPriority(), in_is.getName(), in_is.getIteratorClass(), in_is.getProperties());
        scanner.addScanIterator(is);
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
  public String createBatchScanner(UserPass userpass, String tableName, Set<String> authorizations, PIteratorSetting in_is, List<PRange> pranges)
      throws TException {
    try {
      Connector connector = getConnector(userpass);
      
      Authorizations auth;
      if (authorizations != null)
        auth = new Authorizations(authorizations.toArray(new String[0]));
      else
        auth = connector.securityOperations().getUserAuthorizations(userpass.getUsername());
      
      BatchScanner scanner = connector.createBatchScanner(tableName, auth, 10);
      
      if (in_is != null) {
        IteratorSetting is = new IteratorSetting(in_is.getPriority(), in_is.getName(), in_is.getIteratorClass(), in_is.getProperties());
        scanner.addScanIterator(is);
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
      while (batchScanner.hasNext() && numRead < k) {
        Map.Entry<Key,Value> next = batchScanner.next();
        ret.addToResults(new PKeyValue(Util.toThrift(next.getKey()), ByteBuffer.wrap(next.getValue().get())));
        numRead++;
      }
      ret.setMore(numRead == k);
      return ret;
    }
  }
  
  @Override
  public void close_scanner(String uuid) throws TException {
    scannerCache.invalidate(uuid);
  }
  
  @Override
  public void updateAndFlush(UserPass userpass, String tableName, Map<ByteBuffer,List<PColumnUpdate>> cells, Map<ByteBuffer,List<PColumn>> deletedCells)
      throws TException {
    try {
      BatchWriter writer = getWriter(userpass, tableName);
      addCellsToWriter(cells, deletedCells, writer);
      writer.flush();
      writer.close();
    } catch (Exception e) {
      throw new TException(e);
    }
  }
  
  private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();
  
  private void addCellsToWriter(Map<ByteBuffer,List<PColumnUpdate>> cells, Map<ByteBuffer,List<PColumn>> deletedCells, BatchWriter writer)
      throws MutationsRejectedException {
    HashMap<Text,ColumnVisibility> vizMap = new HashMap<Text,ColumnVisibility>();
    if (cells != null) {
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
          if (update.isSetTimestamp())
            m.put(update.getColFamily(), update.getColQualifier(), viz, update.getTimestamp(), value);
          else
            m.put(update.getColFamily(), update.getColQualifier(), viz, value);
        }
        writer.addMutation(m);
      }
    }
    
    if (deletedCells != null) {
      for (Entry<ByteBuffer,List<PColumn>> entry : deletedCells.entrySet()) {
        Mutation m = new Mutation(ByteBufferUtil.toBytes(entry.getKey()));
        for (PColumn col : entry.getValue()) {
          ColumnVisibility viz = EMPTY_VIS;
          long timestamp = 0;
          if (col.isSetColVisibility()) {
            Text vizText = new Text(col.getColVisibility());
            viz = vizMap.get(vizText);
            if (viz == null) {
              vizMap.put(vizText, viz = new ColumnVisibility(vizText));
            }
          }
          if (col.isSetTimestamp())
            timestamp = col.getTimestamp();
          m.putDelete(col.getColFamily(), col.getColQualifier(), viz, timestamp);
        }
        writer.addMutation(m);
      }
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
  public void writer_update(String writer, Map<ByteBuffer,List<PColumnUpdate>> cells, Map<ByteBuffer,List<PColumn>> deletedCells) throws TException {
    try {
      BatchWriter batchwriter = writerCache.getIfPresent(UUID.fromString(writer));
      if (batchwriter == null) {
        throw new TException("Writer never existed or no longer exists");
      }
      addCellsToWriter(cells, deletedCells, batchwriter);
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
  
}

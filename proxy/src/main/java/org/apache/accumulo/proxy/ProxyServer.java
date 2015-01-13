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

import static com.google.common.base.Charsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
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
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.SecurityErrorCode;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.proxy.thrift.AccumuloProxy;
import org.apache.accumulo.proxy.thrift.BatchScanOptions;
import org.apache.accumulo.proxy.thrift.ColumnUpdate;
import org.apache.accumulo.proxy.thrift.CompactionReason;
import org.apache.accumulo.proxy.thrift.CompactionType;
import org.apache.accumulo.proxy.thrift.Condition;
import org.apache.accumulo.proxy.thrift.ConditionalStatus;
import org.apache.accumulo.proxy.thrift.ConditionalUpdates;
import org.apache.accumulo.proxy.thrift.ConditionalWriterOptions;
import org.apache.accumulo.proxy.thrift.DiskUsage;
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
import org.apache.accumulo.proxy.thrift.WriterOptions;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Proxy Server exposing the Accumulo API via Thrift..
 *
 * @since 1.5
 */
public class ProxyServer implements AccumuloProxy.Iface {

  public static final Logger logger = Logger.getLogger(ProxyServer.class);
  protected Instance instance;

  protected Class<? extends AuthenticationToken> tokenClass;

  static protected class ScannerPlusIterator {
    public ScannerBase scanner;
    public Iterator<Map.Entry<Key,Value>> iterator;
  }

  static protected class BatchWriterPlusException {
    public BatchWriter writer;
    public MutationsRejectedException exception = null;
  }

  static class CloseWriter implements RemovalListener<UUID,BatchWriterPlusException> {
    @Override
    public void onRemoval(RemovalNotification<UUID,BatchWriterPlusException> notification) {
      try {
        BatchWriterPlusException value = notification.getValue();
        if (value.exception != null)
          throw value.exception;
        notification.getValue().writer.close();
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
        final BatchScanner scanner = (BatchScanner) base;
        scanner.close();
      }
    }

    public CloseScanner() {}
  }

  public static class CloseConditionalWriter implements RemovalListener<UUID,ConditionalWriter> {
    @Override
    public void onRemoval(RemovalNotification<UUID,ConditionalWriter> notification) {
      notification.getValue().close();
    }
  }

  protected Cache<UUID,ScannerPlusIterator> scannerCache;
  protected Cache<UUID,BatchWriterPlusException> writerCache;
  protected Cache<UUID,ConditionalWriter> conditionalWriterCache;

  public ProxyServer(Properties props) {

    String useMock = props.getProperty("useMockInstance");
    if (useMock != null && Boolean.parseBoolean(useMock))
      instance = new MockInstance();
    else
      instance = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(props.getProperty("instance"))
          .withZkHosts(props.getProperty("zookeepers")));

    try {
      String tokenProp = props.getProperty("tokenClass", PasswordToken.class.getName());
      tokenClass = Class.forName(tokenProp).asSubclass(AuthenticationToken.class);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    scannerCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000).removalListener(new CloseScanner()).build();

    writerCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000).removalListener(new CloseWriter()).build();

    conditionalWriterCache = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).maximumSize(1000).removalListener(new CloseConditionalWriter())
        .build();
  }

  protected Connector getConnector(ByteBuffer login) throws Exception {
    String[] pair = new String(login.array(), login.position(), login.remaining(), UTF_8).split(",", 2);
    if (instance.getInstanceID().equals(pair[0])) {
      Credentials creds = Credentials.deserialize(pair[1]);
      return instance.getConnector(creds.getPrincipal(), creds.getToken());
    } else {
      throw new org.apache.accumulo.core.client.AccumuloSecurityException(pair[0],
          org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode.INVALID_INSTANCEID);
    }
  }

  private void handleAccumuloException(AccumuloException e) throws org.apache.accumulo.proxy.thrift.TableNotFoundException,
      org.apache.accumulo.proxy.thrift.AccumuloException {
    if (e.getCause() instanceof ThriftTableOperationException) {
      ThriftTableOperationException ttoe = (ThriftTableOperationException) e.getCause();
      if (ttoe.type == TableOperationExceptionType.NOTFOUND) {
        throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
      }
    }
    throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
  }

  private void handleAccumuloSecurityException(AccumuloSecurityException e) throws org.apache.accumulo.proxy.thrift.TableNotFoundException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException {
    if (e.getSecurityErrorCode().equals(SecurityErrorCode.TABLE_DOESNT_EXIST))
      throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
    throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
  }

  private void handleExceptionTNF(Exception ex) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      throw ex;
    } catch (AccumuloException e) {
      Throwable cause = e.getCause();
      if (null != cause && TableNotFoundException.class.equals(cause.getClass())) {
        throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(cause.toString());
      }
      handleAccumuloException(e);
    } catch (AccumuloSecurityException e) {
      handleAccumuloSecurityException(e);
    } catch (TableNotFoundException e) {
      throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(ex.toString());
    } catch (Exception e) {
      throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
    }
  }

  private void handleExceptionTEE(Exception ex) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException,
      org.apache.accumulo.proxy.thrift.TableExistsException, TException {
    try {
      throw ex;
    } catch (AccumuloException e) {
      handleAccumuloException(e);
    } catch (AccumuloSecurityException e) {
      handleAccumuloSecurityException(e);
    } catch (TableNotFoundException e) {
      throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(ex.toString());
    } catch (TableExistsException e) {
      throw new org.apache.accumulo.proxy.thrift.TableExistsException(e.toString());
    } catch (Exception e) {
      throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
    }
  }

  private void handleExceptionMRE(Exception ex) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException,
      org.apache.accumulo.proxy.thrift.MutationsRejectedException, TException {
    try {
      throw ex;
    } catch (MutationsRejectedException e) {
      throw new org.apache.accumulo.proxy.thrift.MutationsRejectedException(ex.toString());
    } catch (AccumuloException e) {
      handleAccumuloException(e);
    } catch (AccumuloSecurityException e) {
      handleAccumuloSecurityException(e);
    } catch (TableNotFoundException e) {
      throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(ex.toString());
    } catch (Exception e) {
      throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
    }
  }

  private void handleException(Exception ex) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      throw ex;
    } catch (AccumuloException e) {
      throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
    } catch (AccumuloSecurityException e) {
      throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
    } catch (Exception e) {
      throw new org.apache.accumulo.proxy.thrift.AccumuloException(e.toString());
    }
  }

  @Override
  public int addConstraint(ByteBuffer login, String tableName, String constraintClassName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

    try {
      return getConnector(login).tableOperations().addConstraint(tableName, constraintClassName);
    } catch (Exception e) {
      handleExceptionTNF(e);
      return -1;
    }
  }

  @Override
  public void addSplits(ByteBuffer login, String tableName, Set<ByteBuffer> splits) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

    try {
      SortedSet<Text> sorted = new TreeSet<Text>();
      for (ByteBuffer split : splits) {
        sorted.add(ByteBufferUtil.toText(split));
      }
      getConnector(login).tableOperations().addSplits(tableName, sorted);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void clearLocatorCache(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().clearLocatorCache(tableName);
    } catch (TableNotFoundException e) {
      throw new org.apache.accumulo.proxy.thrift.TableNotFoundException(e.toString());
    } catch (Exception e) {
      throw new TException(e.toString());
    }
  }

  @Override
  public void compactTable(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow,
      List<org.apache.accumulo.proxy.thrift.IteratorSetting> iterators, boolean flush, boolean wait)
      throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException,
      org.apache.accumulo.proxy.thrift.AccumuloException, TException {
    try {
      getConnector(login).tableOperations().compact(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow), getIteratorSettings(iterators),
          flush, wait);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void cancelCompaction(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, org.apache.accumulo.proxy.thrift.AccumuloException, TException {

    try {
      getConnector(login).tableOperations().cancelCompaction(tableName);
    } catch (Exception e) {
      handleExceptionTNF(e);
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
  public void createTable(ByteBuffer login, String tableName, boolean versioningIter, org.apache.accumulo.proxy.thrift.TimeType type)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableExistsException, TException {
    try {
      if (type == null)
        type = org.apache.accumulo.proxy.thrift.TimeType.MILLIS;

      getConnector(login).tableOperations().create(tableName, versioningIter, TimeType.valueOf(type.toString()));
    } catch (TableExistsException e) {
      throw new org.apache.accumulo.proxy.thrift.TableExistsException(e.toString());
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void deleteTable(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().delete(tableName);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void deleteRows(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().deleteRows(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow));
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public boolean tableExists(ByteBuffer login, String tableName) throws TException {
    try {
      return getConnector(login).tableOperations().exists(tableName);
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void flushTable(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow, boolean wait)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().flush(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow), wait);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public Map<String,Set<String>> getLocalityGroups(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      Map<String,Set<Text>> groups = getConnector(login).tableOperations().getLocalityGroups(tableName);
      Map<String,Set<String>> ret = new HashMap<String,Set<String>>();
      for (Entry<String,Set<Text>> entry : groups.entrySet()) {
        Set<String> value = new HashSet<String>();
        ret.put(entry.getKey(), value);
        for (Text val : entry.getValue()) {
          value.add(val.toString());
        }
      }
      return ret;
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public ByteBuffer getMaxRow(ByteBuffer login, String tableName, Set<ByteBuffer> auths, ByteBuffer startRow, boolean startInclusive, ByteBuffer endRow,
      boolean endInclusive) throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      Connector connector = getConnector(login);
      Text startText = ByteBufferUtil.toText(startRow);
      Text endText = ByteBufferUtil.toText(endRow);
      Authorizations auth;
      if (auths != null) {
        auth = getAuthorizations(auths);
      } else {
        auth = connector.securityOperations().getUserAuthorizations(connector.whoami());
      }
      Text max = connector.tableOperations().getMaxRow(tableName, auth, startText, startInclusive, endText, endInclusive);
      return TextUtil.getByteBuffer(max);
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public Map<String,String> getTableProperties(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      Map<String,String> ret = new HashMap<String,String>();

      for (Map.Entry<String,String> entry : getConnector(login).tableOperations().getProperties(tableName)) {
        ret.put(entry.getKey(), entry.getValue());
      }
      return ret;
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public List<ByteBuffer> listSplits(ByteBuffer login, String tableName, int maxSplits) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      Collection<Text> splits = getConnector(login).tableOperations().listSplits(tableName, maxSplits);
      List<ByteBuffer> ret = new ArrayList<ByteBuffer>();
      for (Text split : splits) {
        ret.add(TextUtil.getByteBuffer(split));
      }
      return ret;
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public Set<String> listTables(ByteBuffer login) throws TException {
    try {
      return getConnector(login).tableOperations().list();
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public Map<String,Integer> listConstraints(ByteBuffer login, String tableName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

    try {
      return getConnector(login).tableOperations().listConstraints(tableName);
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public void mergeTablets(ByteBuffer login, String tableName, ByteBuffer startRow, ByteBuffer endRow)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().merge(tableName, ByteBufferUtil.toText(startRow), ByteBufferUtil.toText(endRow));
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void offlineTable(ByteBuffer login, String tableName, boolean wait) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().offline(tableName, wait);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void onlineTable(ByteBuffer login, String tableName, boolean wait) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().online(tableName, wait);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void removeConstraint(ByteBuffer login, String tableName, int constraint) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

    try {
      getConnector(login).tableOperations().removeConstraint(tableName, constraint);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void removeTableProperty(ByteBuffer login, String tableName, String property) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().removeProperty(tableName, property);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void renameTable(ByteBuffer login, String oldTableName, String newTableName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException,
      org.apache.accumulo.proxy.thrift.TableExistsException, TException {
    try {
      getConnector(login).tableOperations().rename(oldTableName, newTableName);
    } catch (Exception e) {
      handleExceptionTEE(e);
    }
  }

  @Override
  public void setLocalityGroups(ByteBuffer login, String tableName, Map<String,Set<String>> groupStrings)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      Map<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
      for (Entry<String,Set<String>> groupEntry : groupStrings.entrySet()) {
        groups.put(groupEntry.getKey(), new HashSet<Text>());
        for (String val : groupEntry.getValue()) {
          groups.get(groupEntry.getKey()).add(new Text(val));
        }
      }
      getConnector(login).tableOperations().setLocalityGroups(tableName, groups);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void setTableProperty(ByteBuffer login, String tableName, String property, String value) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().setProperty(tableName, property, value);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public Map<String,String> tableIdMap(ByteBuffer login) throws TException {
    try {
      return getConnector(login).tableOperations().tableIdMap();
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public List<DiskUsage> getDiskUsage(ByteBuffer login, Set<String> tables) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      List<org.apache.accumulo.core.client.admin.DiskUsage> diskUsages = getConnector(login).tableOperations().getDiskUsage(tables);
      List<DiskUsage> retUsages = new ArrayList<DiskUsage>();
      for (org.apache.accumulo.core.client.admin.DiskUsage diskUsage : diskUsages) {
        DiskUsage usage = new DiskUsage();
        usage.setTables(new ArrayList<String>(diskUsage.getTables()));
        usage.setUsage(diskUsage.getUsage());
        retUsages.add(usage);
      }
      return retUsages;
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public Map<String,String> getSiteConfiguration(ByteBuffer login) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      return getConnector(login).instanceOperations().getSiteConfiguration();
    } catch (Exception e) {
      handleException(e);
      return null;
    }
  }

  @Override
  public Map<String,String> getSystemConfiguration(ByteBuffer login) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      return getConnector(login).instanceOperations().getSystemConfiguration();
    } catch (Exception e) {
      handleException(e);
      return null;
    }
  }

  @Override
  public List<String> getTabletServers(ByteBuffer login) throws TException {
    try {
      return getConnector(login).instanceOperations().getTabletServers();
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public List<org.apache.accumulo.proxy.thrift.ActiveScan> getActiveScans(ByteBuffer login, String tserver)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    List<org.apache.accumulo.proxy.thrift.ActiveScan> result = new ArrayList<org.apache.accumulo.proxy.thrift.ActiveScan>();
    try {
      List<ActiveScan> activeScans = getConnector(login).instanceOperations().getActiveScans(tserver);
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
        pscan.extent = new org.apache.accumulo.proxy.thrift.KeyExtent(e.getTableId().toString(), TextUtil.getByteBuffer(e.getEndRow()),
            TextUtil.getByteBuffer(e.getPrevEndRow()));
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
            org.apache.accumulo.proxy.thrift.IteratorSetting settings = new org.apache.accumulo.proxy.thrift.IteratorSetting(priority, name, classname, scan
                .getSsio().get(name));
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
      handleException(e);
      return null;
    }
  }

  @Override
  public List<org.apache.accumulo.proxy.thrift.ActiveCompaction> getActiveCompactions(ByteBuffer login, String tserver)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {

    try {
      List<org.apache.accumulo.proxy.thrift.ActiveCompaction> result = new ArrayList<org.apache.accumulo.proxy.thrift.ActiveCompaction>();
      List<ActiveCompaction> active = getConnector(login).instanceOperations().getActiveCompactions(tserver);
      for (ActiveCompaction comp : active) {
        org.apache.accumulo.proxy.thrift.ActiveCompaction pcomp = new org.apache.accumulo.proxy.thrift.ActiveCompaction();
        pcomp.age = comp.getAge();
        pcomp.entriesRead = comp.getEntriesRead();
        pcomp.entriesWritten = comp.getEntriesWritten();
        KeyExtent e = comp.getExtent();
        pcomp.extent = new org.apache.accumulo.proxy.thrift.KeyExtent(e.getTableId().toString(), TextUtil.getByteBuffer(e.getEndRow()),
            TextUtil.getByteBuffer(e.getPrevEndRow()));
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
            org.apache.accumulo.proxy.thrift.IteratorSetting psetting = new org.apache.accumulo.proxy.thrift.IteratorSetting(setting.getPriority(),
                setting.getName(), setting.getIteratorClass(), setting.getOptions());
            pcomp.iterators.add(psetting);
          }
        }
        result.add(pcomp);
      }
      return result;
    } catch (Exception e) {
      handleException(e);
      return null;
    }
  }

  @Override
  public void removeProperty(ByteBuffer login, String property) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      getConnector(login).instanceOperations().removeProperty(property);
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void setProperty(ByteBuffer login, String property, String value) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      getConnector(login).instanceOperations().setProperty(property, value);
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public boolean testClassLoad(ByteBuffer login, String className, String asTypeName) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      return getConnector(login).instanceOperations().testClassLoad(className, asTypeName);
    } catch (Exception e) {
      handleException(e);
      return false;
    }
  }

  @Override
  public boolean authenticateUser(ByteBuffer login, String user, Map<String,String> properties) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      return getConnector(login).securityOperations().authenticateUser(user, getToken(user, properties));
    } catch (Exception e) {
      handleException(e);
      return false;
    }
  }

  @Override
  public void changeUserAuthorizations(ByteBuffer login, String user, Set<ByteBuffer> authorizations)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      Set<String> auths = new HashSet<String>();
      for (ByteBuffer auth : authorizations) {
        auths.add(ByteBufferUtil.toString(auth));
      }
      getConnector(login).securityOperations().changeUserAuthorizations(user, new Authorizations(auths.toArray(new String[0])));
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void changeLocalUserPassword(ByteBuffer login, String user, ByteBuffer password) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      getConnector(login).securityOperations().changeLocalUserPassword(user, new PasswordToken(password));
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void createLocalUser(ByteBuffer login, String user, ByteBuffer password) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      getConnector(login).securityOperations().createLocalUser(user, new PasswordToken(password));
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void dropLocalUser(ByteBuffer login, String user) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      getConnector(login).securityOperations().dropLocalUser(user);
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public List<ByteBuffer> getUserAuthorizations(ByteBuffer login, String user) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      return getConnector(login).securityOperations().getUserAuthorizations(user).getAuthorizationsBB();
    } catch (Exception e) {
      handleException(e);
      return null;
    }
  }

  @Override
  public void grantSystemPermission(ByteBuffer login, String user, org.apache.accumulo.proxy.thrift.SystemPermission perm)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      getConnector(login).securityOperations().grantSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void grantTablePermission(ByteBuffer login, String user, String table, org.apache.accumulo.proxy.thrift.TablePermission perm)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).securityOperations().grantTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public boolean hasSystemPermission(ByteBuffer login, String user, org.apache.accumulo.proxy.thrift.SystemPermission perm)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      return getConnector(login).securityOperations().hasSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      handleException(e);
      return false;
    }
  }

  @Override
  public boolean hasTablePermission(ByteBuffer login, String user, String table, org.apache.accumulo.proxy.thrift.TablePermission perm)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      return getConnector(login).securityOperations().hasTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      handleExceptionTNF(e);
      return false;
    }
  }

  @Override
  public Set<String> listLocalUsers(ByteBuffer login) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      return getConnector(login).securityOperations().listLocalUsers();
    } catch (Exception e) {
      handleException(e);
      return null;
    }
  }

  @Override
  public void revokeSystemPermission(ByteBuffer login, String user, org.apache.accumulo.proxy.thrift.SystemPermission perm)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      getConnector(login).securityOperations().revokeSystemPermission(user, SystemPermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public void revokeTablePermission(ByteBuffer login, String user, String table, org.apache.accumulo.proxy.thrift.TablePermission perm)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).securityOperations().revokeTablePermission(user, table, TablePermission.getPermissionById((byte) perm.getValue()));
    } catch (Exception e) {
      handleExceptionTNF(e);
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
  public String createScanner(ByteBuffer login, String tableName, ScanOptions opts) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      Connector connector = getConnector(login);

      Authorizations auth;
      if (opts != null && opts.isSetAuthorizations()) {
        auth = getAuthorizations(opts.authorizations);
      } else {
        auth = connector.securityOperations().getUserAuthorizations(connector.whoami());
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
          Range range = new Range(Util.fromThrift(prange.getStart()), prange.startInclusive, Util.fromThrift(prange.getStop()), prange.stopInclusive);
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
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public String createBatchScanner(ByteBuffer login, String tableName, BatchScanOptions opts) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      Connector connector = getConnector(login);

      int threads = 10;
      Authorizations auth;
      if (opts != null && opts.isSetAuthorizations()) {
        auth = getAuthorizations(opts.authorizations);
      } else {
        auth = connector.securityOperations().getUserAuthorizations(connector.whoami());
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
      handleExceptionTNF(e);
      return null;
    }
  }

  private ScannerPlusIterator getScanner(String scanner) throws UnknownScanner {

    UUID uuid = null;
    try {
      uuid = UUID.fromString(scanner);
    } catch (IllegalArgumentException e) {
      throw new UnknownScanner(e.getMessage());
    }

    ScannerPlusIterator spi = scannerCache.getIfPresent(uuid);
    if (spi == null) {
      throw new UnknownScanner("Scanner never existed or no longer exists");
    }
    return spi;
  }

  @Override
  public boolean hasNext(String scanner) throws UnknownScanner, TException {
    ScannerPlusIterator spi = getScanner(scanner);

    return (spi.iterator.hasNext());
  }

  @Override
  public KeyValueAndPeek nextEntry(String scanner) throws NoMoreEntriesException, UnknownScanner, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      TException {

    ScanResult scanResult = nextK(scanner, 1);
    if (scanResult.results.size() > 0) {
      return new KeyValueAndPeek(scanResult.results.get(0), scanResult.isMore());
    } else {
      throw new NoMoreEntriesException();
    }
  }

  @Override
  public ScanResult nextK(String scanner, int k) throws NoMoreEntriesException, UnknownScanner, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      TException {

    // fetch the scanner
    ScannerPlusIterator spi = getScanner(scanner);
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
        throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(ex.toString());
      }
      return ret;
    }
  }

  @Override
  public void closeScanner(String scanner) throws UnknownScanner, TException {
    UUID uuid = null;
    try {
      uuid = UUID.fromString(scanner);
    } catch (IllegalArgumentException e) {
      throw new UnknownScanner(e.getMessage());
    }

    try {
      if (scannerCache.asMap().remove(uuid) == null) {
        throw new UnknownScanner("Scanner never existed or no longer exists");
      }
    } catch (UnknownScanner e) {
      throw e;
    } catch (Exception e) {
      throw new TException(e.toString());
    }
  }

  @Override
  public void updateAndFlush(ByteBuffer login, String tableName, Map<ByteBuffer,List<ColumnUpdate>> cells)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, org.apache.accumulo.proxy.thrift.MutationsRejectedException, TException {
    try {
      BatchWriterPlusException bwpe = getWriter(login, tableName, null);
      addCellsToWriter(cells, bwpe);
      if (bwpe.exception != null)
        throw bwpe.exception;
      bwpe.writer.flush();
      bwpe.writer.close();
    } catch (Exception e) {
      handleExceptionMRE(e);
    }
  }

  private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();

  private void addCellsToWriter(Map<ByteBuffer,List<ColumnUpdate>> cells, BatchWriterPlusException bwpe) {
    if (bwpe.exception != null)
      return;

    HashMap<Text,ColumnVisibility> vizMap = new HashMap<Text,ColumnVisibility>();

    for (Map.Entry<ByteBuffer,List<ColumnUpdate>> entry : cells.entrySet()) {
      Mutation m = new Mutation(ByteBufferUtil.toBytes(entry.getKey()));
      addUpdatesToMutation(vizMap, m, entry.getValue());
      try {
        bwpe.writer.addMutation(m);
      } catch (MutationsRejectedException mre) {
        bwpe.exception = mre;
      }
    }
  }

  private void addUpdatesToMutation(HashMap<Text,ColumnVisibility> vizMap, Mutation m, List<ColumnUpdate> cu) {
    for (ColumnUpdate update : cu) {
      ColumnVisibility viz = EMPTY_VIS;
      if (update.isSetColVisibility()) {
        viz = getCahcedCV(vizMap, update.getColVisibility());
      }
      byte[] value = new byte[0];
      if (update.isSetValue())
        value = update.getValue();
      if (update.isSetTimestamp()) {
        if (update.isSetDeleteCell() && update.isDeleteCell()) {
          m.putDelete(update.getColFamily(), update.getColQualifier(), viz, update.getTimestamp());
        } else {
          m.put(update.getColFamily(), update.getColQualifier(), viz, update.getTimestamp(), value);
        }
      } else {
        if (update.isSetDeleteCell() && update.isDeleteCell()) {
          m.putDelete(new Text(update.getColFamily()), new Text(update.getColQualifier()), viz);
        } else {
          m.put(new Text(update.getColFamily()), new Text(update.getColQualifier()), viz, new Value(value));
        }
      }
    }
  }

  private static ColumnVisibility getCahcedCV(HashMap<Text,ColumnVisibility> vizMap, byte[] cv) {
    ColumnVisibility viz;
    Text vizText = new Text(cv);
    viz = vizMap.get(vizText);
    if (viz == null) {
      vizMap.put(vizText, viz = new ColumnVisibility(vizText));
    }
    return viz;
  }

  @Override
  public String createWriter(ByteBuffer login, String tableName, WriterOptions opts) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      BatchWriterPlusException writer = getWriter(login, tableName, opts);
      UUID uuid = UUID.randomUUID();
      writerCache.put(uuid, writer);
      return uuid.toString();
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public void update(String writer, Map<ByteBuffer,List<ColumnUpdate>> cells) throws TException {
    try {
      BatchWriterPlusException bwpe = getWriter(writer);
      addCellsToWriter(cells, bwpe);
    } catch (UnknownWriter e) {
      // just drop it, this is a oneway thrift call and throwing a TException seems to make all subsequent thrift calls fail
    }
  }

  @Override
  public void flush(String writer) throws UnknownWriter, org.apache.accumulo.proxy.thrift.MutationsRejectedException, TException {
    try {
      BatchWriterPlusException bwpe = getWriter(writer);
      if (bwpe.exception != null)
        throw bwpe.exception;
      bwpe.writer.flush();
    } catch (MutationsRejectedException e) {
      throw new org.apache.accumulo.proxy.thrift.MutationsRejectedException(e.toString());
    } catch (UnknownWriter uw) {
      throw uw;
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public void closeWriter(String writer) throws UnknownWriter, org.apache.accumulo.proxy.thrift.MutationsRejectedException, TException {
    try {
      BatchWriterPlusException bwpe = getWriter(writer);
      if (bwpe.exception != null)
        throw bwpe.exception;
      bwpe.writer.close();
      writerCache.invalidate(UUID.fromString(writer));
    } catch (UnknownWriter uw) {
      throw uw;
    } catch (MutationsRejectedException e) {
      throw new org.apache.accumulo.proxy.thrift.MutationsRejectedException(e.toString());
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  private BatchWriterPlusException getWriter(String writer) throws UnknownWriter {
    UUID uuid = null;
    try {
      uuid = UUID.fromString(writer);
    } catch (IllegalArgumentException iae) {
      throw new UnknownWriter(iae.getMessage());
    }

    BatchWriterPlusException bwpe = writerCache.getIfPresent(uuid);
    if (bwpe == null) {
      throw new UnknownWriter("Writer never existed or no longer exists");
    }
    return bwpe;
  }

  private BatchWriterPlusException getWriter(ByteBuffer login, String tableName, WriterOptions opts) throws Exception {
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
    BatchWriterPlusException result = new BatchWriterPlusException();
    result.writer = getConnector(login).createBatchWriter(tableName, cfg);
    return result;
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
  public void attachIterator(ByteBuffer login, String tableName, org.apache.accumulo.proxy.thrift.IteratorSetting setting,
      Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes) throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().attachIterator(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void checkIteratorConflicts(ByteBuffer login, String tableName, org.apache.accumulo.proxy.thrift.IteratorSetting setting,
      Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().checkIteratorConflicts(tableName, getIteratorSetting(setting), getIteratorScopes(scopes));
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void cloneTable(ByteBuffer login, String tableName, String newTableName, boolean flush, Map<String,String> propertiesToSet,
      Set<String> propertiesToExclude) throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, org.apache.accumulo.proxy.thrift.TableExistsException, TException {
    try {
      propertiesToExclude = propertiesToExclude == null ? new HashSet<String>() : propertiesToExclude;
      propertiesToSet = propertiesToSet == null ? new HashMap<String,String>() : propertiesToSet;

      getConnector(login).tableOperations().clone(tableName, newTableName, flush, propertiesToSet, propertiesToExclude);
    } catch (Exception e) {
      handleExceptionTEE(e);
    }
  }

  @Override
  public void exportTable(ByteBuffer login, String tableName, String exportDir) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

    try {
      getConnector(login).tableOperations().exportTable(tableName, exportDir);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public void importTable(ByteBuffer login, String tableName, String importDir) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableExistsException, TException {

    try {
      getConnector(login).tableOperations().importTable(tableName, importDir);
    } catch (TableExistsException e) {
      throw new org.apache.accumulo.proxy.thrift.TableExistsException(e.toString());
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public org.apache.accumulo.proxy.thrift.IteratorSetting getIteratorSetting(ByteBuffer login, String tableName, String iteratorName,
      org.apache.accumulo.proxy.thrift.IteratorScope scope) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      IteratorSetting is = getConnector(login).tableOperations().getIteratorSetting(tableName, iteratorName, getIteratorScope(scope));
      return new org.apache.accumulo.proxy.thrift.IteratorSetting(is.getPriority(), is.getName(), is.getIteratorClass(), is.getOptions());
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public Map<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>> listIterators(ByteBuffer login, String tableName)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      Map<String,EnumSet<IteratorScope>> iterMap = getConnector(login).tableOperations().listIterators(tableName);
      Map<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>> result = new HashMap<String,Set<org.apache.accumulo.proxy.thrift.IteratorScope>>();
      for (Map.Entry<String,EnumSet<IteratorScope>> entry : iterMap.entrySet()) {
        result.put(entry.getKey(), getProxyIteratorScopes(entry.getValue()));
      }
      return result;
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public void removeIterator(ByteBuffer login, String tableName, String iterName, Set<org.apache.accumulo.proxy.thrift.IteratorScope> scopes)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      getConnector(login).tableOperations().removeIterator(tableName, iterName, getIteratorScopes(scopes));
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public Set<org.apache.accumulo.proxy.thrift.Range> splitRangeByTablets(ByteBuffer login, String tableName, org.apache.accumulo.proxy.thrift.Range range,
      int maxSplits) throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      Set<Range> ranges = getConnector(login).tableOperations().splitRangeByTablets(tableName, getRange(range), maxSplits);
      Set<org.apache.accumulo.proxy.thrift.Range> result = new HashSet<org.apache.accumulo.proxy.thrift.Range>();
      for (Range r : ranges) {
        result.add(getRange(r));
      }
      return result;
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  private org.apache.accumulo.proxy.thrift.Range getRange(Range r) {
    return new org.apache.accumulo.proxy.thrift.Range(getProxyKey(r.getStartKey()), r.isStartKeyInclusive(), getProxyKey(r.getEndKey()), r.isEndKeyInclusive());
  }

  private org.apache.accumulo.proxy.thrift.Key getProxyKey(Key k) {
    if (k == null)
      return null;
    org.apache.accumulo.proxy.thrift.Key result = new org.apache.accumulo.proxy.thrift.Key(TextUtil.getByteBuffer(k.getRow()), TextUtil.getByteBuffer(k
        .getColumnFamily()), TextUtil.getByteBuffer(k.getColumnQualifier()), TextUtil.getByteBuffer(k.getColumnVisibility()));
    result.setTimestamp(k.getTimestamp());
    return result;
  }

  private Range getRange(org.apache.accumulo.proxy.thrift.Range range) {
    return new Range(Util.fromThrift(range.start), Util.fromThrift(range.stop));
  }

  @Override
  public void importDirectory(ByteBuffer login, String tableName, String importDir, String failureDir, boolean setTime)
      throws org.apache.accumulo.proxy.thrift.TableNotFoundException, org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      getConnector(login).tableOperations().importDirectory(tableName, importDir, failureDir, setTime);
    } catch (Exception e) {
      handleExceptionTNF(e);
    }
  }

  @Override
  public org.apache.accumulo.proxy.thrift.Range getRowRange(ByteBuffer row) throws TException {
    return getRange(new Range(ByteBufferUtil.toText(row)));
  }

  @Override
  public org.apache.accumulo.proxy.thrift.Key getFollowing(org.apache.accumulo.proxy.thrift.Key key, org.apache.accumulo.proxy.thrift.PartialKey part)
      throws TException {
    Key key_ = Util.fromThrift(key);
    PartialKey part_ = PartialKey.valueOf(part.toString());
    Key followingKey = key_.followingKey(part_);
    return getProxyKey(followingKey);
  }

  @Override
  public void pingTabletServer(ByteBuffer login, String tserver) throws org.apache.accumulo.proxy.thrift.AccumuloException,
      org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      getConnector(login).instanceOperations().ping(tserver);
    } catch (Exception e) {
      handleException(e);
    }
  }

  @Override
  public ByteBuffer login(String principal, Map<String,String> loginProperties) throws org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {
    try {
      AuthenticationToken token = getToken(principal, loginProperties);
      ByteBuffer login = ByteBuffer.wrap((instance.getInstanceID() + "," + new Credentials(principal, token).serialize()).getBytes(UTF_8));
      getConnector(login); // check to make sure user exists
      return login;
    } catch (AccumuloSecurityException e) {
      throw new org.apache.accumulo.proxy.thrift.AccumuloSecurityException(e.toString());
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  private AuthenticationToken getToken(String principal, Map<String,String> properties) throws AccumuloSecurityException, AccumuloException {
    AuthenticationToken.Properties props = new AuthenticationToken.Properties();
    props.putAllStrings(properties);
    AuthenticationToken token;
    try {
      token = tokenClass.newInstance();
    } catch (InstantiationException e) {
      throw new AccumuloException(e);
    } catch (IllegalAccessException e) {
      throw new AccumuloException(e);
    }
    token.init(props);
    return token;
  }

  @Override
  public boolean testTableClassLoad(ByteBuffer login, String tableName, String className, String asTypeName)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      return getConnector(login).tableOperations().testClassLoad(tableName, className, asTypeName);
    } catch (Exception e) {
      handleExceptionTNF(e);
      return false;
    }
  }

  @Override
  public String createConditionalWriter(ByteBuffer login, String tableName, ConditionalWriterOptions options)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {
    try {
      ConditionalWriterConfig cwc = new ConditionalWriterConfig();
      if (options.getMaxMemory() != 0) {
        // TODO
      }
      if (options.isSetThreads() && options.getThreads() != 0)
        cwc.setMaxWriteThreads(options.getThreads());
      if (options.isSetTimeoutMs() && options.getTimeoutMs() != 0)
        cwc.setTimeout(options.getTimeoutMs(), TimeUnit.MILLISECONDS);
      if (options.isSetAuthorizations() && options.getAuthorizations() != null)
        cwc.setAuthorizations(getAuthorizations(options.getAuthorizations()));

      ConditionalWriter cw = getConnector(login).createConditionalWriter(tableName, cwc);

      UUID id = UUID.randomUUID();

      conditionalWriterCache.put(id, cw);

      return id.toString();
    } catch (Exception e) {
      handleExceptionTNF(e);
      return null;
    }
  }

  @Override
  public Map<ByteBuffer,ConditionalStatus> updateRowsConditionally(String conditionalWriter, Map<ByteBuffer,ConditionalUpdates> updates) throws UnknownWriter,
      org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException, TException {

    ConditionalWriter cw = conditionalWriterCache.getIfPresent(UUID.fromString(conditionalWriter));

    if (cw == null) {
      throw new UnknownWriter();
    }

    try {
      HashMap<Text,ColumnVisibility> vizMap = new HashMap<Text,ColumnVisibility>();

      ArrayList<ConditionalMutation> cmuts = new ArrayList<ConditionalMutation>(updates.size());
      for (Entry<ByteBuffer,ConditionalUpdates> cu : updates.entrySet()) {
        ConditionalMutation cmut = new ConditionalMutation(ByteBufferUtil.toBytes(cu.getKey()));

        for (Condition tcond : cu.getValue().conditions) {
          org.apache.accumulo.core.data.Condition cond = new org.apache.accumulo.core.data.Condition(tcond.column.getColFamily(),
              tcond.column.getColQualifier());

          if (tcond.getColumn().getColVisibility() != null && tcond.getColumn().getColVisibility().length > 0) {
            cond.setVisibility(getCahcedCV(vizMap, tcond.getColumn().getColVisibility()));
          }

          if (tcond.isSetValue())
            cond.setValue(tcond.getValue());

          if (tcond.isSetTimestamp())
            cond.setTimestamp(tcond.getTimestamp());

          if (tcond.isSetIterators()) {
            cond.setIterators(getIteratorSettings(tcond.getIterators()).toArray(new IteratorSetting[tcond.getIterators().size()]));
          }

          cmut.addCondition(cond);
        }

        addUpdatesToMutation(vizMap, cmut, cu.getValue().updates);

        cmuts.add(cmut);
      }

      Iterator<Result> results = cw.write(cmuts.iterator());

      HashMap<ByteBuffer,ConditionalStatus> resultMap = new HashMap<ByteBuffer,ConditionalStatus>();

      while (results.hasNext()) {
        Result result = results.next();
        ByteBuffer row = ByteBuffer.wrap(result.getMutation().getRow());
        ConditionalStatus status = ConditionalStatus.valueOf(result.getStatus().name());
        resultMap.put(row, status);
      }

      return resultMap;
    } catch (Exception e) {
      handleException(e);
      return null;
    }
  }

  @Override
  public void closeConditionalWriter(String conditionalWriter) throws TException {
    ConditionalWriter cw = conditionalWriterCache.getIfPresent(UUID.fromString(conditionalWriter));
    if (cw != null) {
      cw.close();
      conditionalWriterCache.invalidate(UUID.fromString(conditionalWriter));
    }
  }

  @Override
  public ConditionalStatus updateRowConditionally(ByteBuffer login, String tableName, ByteBuffer row, ConditionalUpdates updates)
      throws org.apache.accumulo.proxy.thrift.AccumuloException, org.apache.accumulo.proxy.thrift.AccumuloSecurityException,
      org.apache.accumulo.proxy.thrift.TableNotFoundException, TException {

    String cwid = createConditionalWriter(login, tableName, new ConditionalWriterOptions());
    try {
      return updateRowsConditionally(cwid, Collections.singletonMap(row, updates)).get(row);
    } finally {
      closeConditionalWriter(cwid);
    }
  }
}

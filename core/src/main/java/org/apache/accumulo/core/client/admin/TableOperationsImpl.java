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
package org.apache.accumulo.core.client.admin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.accumulo.cloudtrace.instrument.Tracer;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.AccumuloServerException;
import org.apache.accumulo.core.client.impl.ClientExec;
import org.apache.accumulo.core.client.impl.ClientExecReturn;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.client.impl.ServerClient;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.TableOperation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.security.thrift.ThriftSecurityException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

/**
 * Provides a class for administering tables
 * 
 */
public class TableOperationsImpl extends TableOperationsHelper {
  private Instance instance;
  private AuthInfo credentials;
  
  private static final Logger log = Logger.getLogger(TableOperations.class);
  
  /**
   * @param instance
   *          the connection information for this instance
   * @param credentials
   *          the username/password for this connection
   */
  public TableOperationsImpl(Instance instance, AuthInfo credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }
  
  /**
   * Retrieve a list of tables in Accumulo.
   * 
   * @return List of tables in accumulo
   */
  public SortedSet<String> list() {
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Fetching list of tables...");
    TreeSet<String> tableNames = new TreeSet<String>(Tables.getNameToIdMap(instance).keySet());
    opTimer.stop("Fetched " + tableNames.size() + " table names in %DURATION%");
    return tableNames;
  }
  
  /**
   * A method to check if a table exists in Accumulo.
   * 
   * @param tableName
   *          the name of the table
   * @return true if the table exists
   */
  public boolean exists(String tableName) {
    ArgumentChecker.notNull(tableName);
    if (tableName.equals(Constants.METADATA_TABLE_NAME))
      return true;
    
    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Checking if table " + tableName + "exists...");
    boolean exists = Tables.getNameToIdMap(instance).containsKey(tableName);
    opTimer.stop("Checked existance of " + exists + " in %DURATION%");
    return exists;
  }
  
  /**
   * Create a table with no special configuration
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableExistsException
   *           if the table already exists
   */
  public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    create(tableName, true, TimeType.MILLIS);
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   */
  public void create(String tableName, boolean limitVersion) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    create(tableName, limitVersion, TimeType.MILLIS);
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param timeType
   *          specifies logical or real-time based time recording for entries in the table
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   */
  public void create(String tableName, boolean limitVersion, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    ArgumentChecker.notNull(tableName, timeType);
    
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes()), ByteBuffer.wrap(timeType.name().getBytes()));
    
    Map<String,String> opts;
    if (limitVersion) {
      opts = IteratorUtil.generateInitialTableProperties();
    } else
      opts = Collections.emptyMap();
    
    try {
      doTableOperation(TableOperation.CREATE, args, opts);
    } catch (TableNotFoundException e1) {
      // should not happen
      throw new RuntimeException(e1);
    }
  }
  
  private long beginTableOperation() throws ThriftSecurityException, TException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        return client.beginTableOperation(Tracer.traceInfo(), credentials);
      } catch (TTransportException tte) {
        log.debug("Failed to call beginTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }
  
  private void executeTableOperation(long opid, TableOperation op, List<ByteBuffer> args, Map<String,String> opts, boolean autoCleanUp)
      throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        client.executeTableOperation(Tracer.traceInfo(), credentials, opid, op, args, opts, autoCleanUp);
        break;
      } catch (TTransportException tte) {
        log.debug("Failed to call executeTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }
  
  private String waitForTableOperation(long opid) throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        return client.waitForTableOperation(Tracer.traceInfo(), credentials, opid);
      } catch (TTransportException tte) {
        log.debug("Failed to call waitForTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }
  
  private void finishTableOperation(long opid) throws ThriftSecurityException, TException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        client.finishTableOperation(Tracer.traceInfo(), credentials, opid);
        break;
      } catch (TTransportException tte) {
        log.debug("Failed to call finishTableOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }
  
  private String doTableOperation(TableOperation op, List<ByteBuffer> args, Map<String,String> opts) throws AccumuloSecurityException, TableExistsException,
      TableNotFoundException, AccumuloException {
    return doTableOperation(op, args, opts, true);
  }
  
  private String doTableOperation(TableOperation op, List<ByteBuffer> args, Map<String,String> opts, boolean wait) throws AccumuloSecurityException,
      TableExistsException, TableNotFoundException, AccumuloException {
    Long opid = null;
    
    try {
      opid = beginTableOperation();
      executeTableOperation(opid, op, args, opts, !wait);
      if (!wait) {
        opid = null;
        return null;
      }
      String ret = waitForTableOperation(opid);
      Tables.clearCache(instance);
      return ret;
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case EXISTS:
          throw new TableExistsException(e);
        case NOTFOUND:
          throw new TableNotFoundException(e);
        case OFFLINE:
          throw new TableOfflineException(instance, null);
        case OTHER:
        default:
          throw new AccumuloException(e.description, e);
      }
    } catch (Exception e) {
      throw new AccumuloException(e.getMessage(), e);
    } finally {
      // always finish table op, even when exception
      if (opid != null)
        try {
          finishTableOperation(opid);
        } catch (Exception e) {
          log.warn(e.getMessage(), e);
        }
    }
  }
  
  private static class SplitEnv {
    private String tableName;
    private String tableId;
    private ExecutorService executor;
    private CountDownLatch latch;
    private AtomicReference<Exception> exception;
    
    SplitEnv(String tableName, String tableId, ExecutorService executor, CountDownLatch latch, AtomicReference<Exception> exception) {
      this.tableName = tableName;
      this.tableId = tableId;
      this.executor = executor;
      this.latch = latch;
      this.exception = exception;
    }
  }
  
  private class SplitTask implements Runnable {
    
    private List<Text> splits;
    private SplitEnv env;
    
    SplitTask(SplitEnv env, List<Text> splits) {
      this.env = env;
      this.splits = splits;
    }
    
    @Override
    public void run() {
      try {
        if (env.exception.get() != null)
          return;
        
        if (splits.size() <= 2) {
          addSplits(env.tableName, new TreeSet<Text>(splits), env.tableId);
          for (int i = 0; i < splits.size(); i++)
            env.latch.countDown();
          return;
        }
        
        int mid = splits.size() / 2;
        
        // split the middle split point to ensure that child task split different tablets and can therefore
        // run in parallel
        addSplits(env.tableName, new TreeSet<Text>(splits.subList(mid, mid + 1)), env.tableId);
        env.latch.countDown();
        
        env.executor.submit(new SplitTask(env, splits.subList(0, mid)));
        env.executor.submit(new SplitTask(env, splits.subList(mid + 1, splits.size())));
        
      } catch (Exception e) {
        env.exception.compareAndSet(null, e);
      }
    }
    
  }

  /**
   * @param tableName
   *          the name of the table
   * @param partitionKeys
   *          a sorted set of row key values to pre-split the table on
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public void addSplits(String tableName, SortedSet<Text> partitionKeys) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    String tableId = Tables.getTableId(instance, tableName);
    
    List<Text> splits = new ArrayList<Text>(partitionKeys);
    // should be sorted because we copied from a sorted set, but that makes assumptions about
    // how the copy was done so resort to be sure.
    Collections.sort(splits);

    CountDownLatch latch = new CountDownLatch(splits.size());
    AtomicReference<Exception> exception = new AtomicReference<Exception>(null);
    
    ExecutorService executor = Executors.newFixedThreadPool(16, new NamingThreadFactory("addSplits"));
    try {
      executor.submit(new SplitTask(new SplitEnv(tableName, tableId, executor, latch, exception), splits));

      while (!latch.await(100, TimeUnit.MILLISECONDS)) {
        if (exception.get() != null) {
          executor.shutdownNow();
          Exception excep = exception.get();
          if (excep instanceof TableNotFoundException)
            throw (TableNotFoundException) excep;
          else if (excep instanceof AccumuloException)
            throw (AccumuloException) excep;
          else if (excep instanceof AccumuloSecurityException)
            throw (AccumuloSecurityException) excep;
          else if (excep instanceof RuntimeException)
            throw (RuntimeException) excep;
          else
            throw new RuntimeException(excep);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      executor.shutdown();
    }
  }
  
  private void addSplits(String tableName, SortedSet<Text> partitionKeys, String tableId) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, AccumuloServerException {
    TabletLocator tabLocator = TabletLocator.getInstance(instance, credentials, new Text(tableId));
    
    for (Text split : partitionKeys) {
      boolean successful = false;
      int attempt = 0;
      
      while (!successful) {
        
        if (attempt > 0)
          UtilWaitThread.sleep(100);
        
        attempt++;
        
        TabletLocation tl = tabLocator.locateTablet(split, false, false);
        
        if (tl == null) {
          if (!Tables.exists(instance, tableId))
            throw new TableNotFoundException(tableId, tableName, null);
          else if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
            throw new TableOfflineException(instance, tableId);
          continue;
        }
        
        try {
          TabletClientService.Client client = ThriftUtil.getTServerClient(tl.tablet_location, instance.getConfiguration());
          try {
            OpTimer opTimer = null;
            if (log.isTraceEnabled())
              opTimer = new OpTimer(log, Level.TRACE).start("Splitting tablet " + tl.tablet_extent + " on " + tl.tablet_location + " at " + split);
            
            client.splitTablet(Tracer.traceInfo(), credentials, tl.tablet_extent.toThrift(), TextUtil.getByteBuffer(split));
            
            // just split it, might as well invalidate it in the cache
            tabLocator.invalidateCache(tl.tablet_extent);
            
            if (opTimer != null)
              opTimer.stop("Split tablet in %DURATION%");
          } finally {
            ThriftUtil.returnClient(client);
          }
          
        } catch (TApplicationException tae) {
          throw new AccumuloServerException(tl.tablet_location, tae);
        } catch (TTransportException e) {
          tabLocator.invalidateCache(tl.tablet_location);
          continue;
        } catch (ThriftSecurityException e) {
          Tables.clearCache(instance);
          if (!Tables.exists(instance, tableId))
            throw new TableNotFoundException(tableId, tableName, null);
          throw new AccumuloSecurityException(e.user, e.code, e);
        } catch (NotServingTabletException e) {
          tabLocator.invalidateCache(tl.tablet_extent);
          continue;
        } catch (TException e) {
          tabLocator.invalidateCache(tl.tablet_location);
          continue;
        }
        
        successful = true;
      }
    }
  }
  
  public void merge(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    
    ArgumentChecker.notNull(tableName);
    ByteBuffer EMPTY = ByteBuffer.allocate(0);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes()), start == null ? EMPTY : TextUtil.getByteBuffer(start), end == null ? EMPTY
        : TextUtil.getByteBuffer(end));
    Map<String,String> opts = new HashMap<String,String>();
    try {
      doTableOperation(TableOperation.MERGE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new RuntimeException(e);
    }
  }
  
  public void deleteRows(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    
    ArgumentChecker.notNull(tableName);
    ByteBuffer EMPTY = ByteBuffer.allocate(0);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes()), start == null ? EMPTY : TextUtil.getByteBuffer(start), end == null ? EMPTY
        : TextUtil.getByteBuffer(end));
    Map<String,String> opts = new HashMap<String,String>();
    try {
      doTableOperation(TableOperation.DELETE_RANGE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new RuntimeException(e);
    }
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @return the split points (end-row names) for the table's current split profile
   */
  @Override
  public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
    
    ArgumentChecker.notNull(tableName);
    
    if (!exists(tableName)) {
      throw new TableNotFoundException(null, tableName, "Unknown table for getSplits");
    }
    
    SortedSet<KeyExtent> tablets = new TreeSet<KeyExtent>();
    Map<KeyExtent,String> locations = new TreeMap<KeyExtent,String>();
    
    while (true) {
      try {
        tablets.clear();
        locations.clear();
        MetadataTable.getEntries(instance, credentials, tableName, false, locations, tablets);
        break;
      } catch (Throwable t) {
        log.info(t.getMessage() + " ... retrying ...");
        UtilWaitThread.sleep(3000);
      }
    }
    
    ArrayList<Text> endRows = new ArrayList<Text>(tablets.size());
    
    for (KeyExtent ke : tablets)
      if (ke.getEndRow() != null)
        endRows.add(ke.getEndRow());
    
    return endRows;
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param maxSplits
   *          specifies the maximum number of splits to return
   * @return the split points (end-row names) for the table's current split profile, grouped into fewer splits so as not to exceed maxSplits
   * @throws TableNotFoundException
   */
  @Override
  public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException {
    Collection<Text> endRows = getSplits(tableName);
    
    if (endRows.size() <= maxSplits)
      return endRows;
    
    double r = (maxSplits + 1) / (double) (endRows.size());
    double pos = 0;
    
    ArrayList<Text> subset = new ArrayList<Text>(maxSplits);
    
    int j = 0;
    for (int i = 0; i < endRows.size() && j < maxSplits; i++) {
      pos += r;
      while (pos > 1) {
        subset.add(((ArrayList<Text>) endRows).get(i));
        j++;
        pos -= 1;
      }
    }
    
    return subset;
  }
  
  /**
   * Delete a table
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    
    try {
      doTableOperation(TableOperation.DELETE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new RuntimeException(e);
    }
    
  }
  
  @Override
  public void clone(String srcTableName, String newTableName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException, TableExistsException {
    
    ArgumentChecker.notNull(srcTableName, newTableName);
    
    String srcTableId = Tables.getTableId(instance, srcTableName);
    
    if (flush)
      _flush(srcTableId, null, null, true);
    
    if (!Collections.disjoint(propertiesToExclude, propertiesToSet.keySet()))
      throw new IllegalArgumentException("propertiesToSet and propertiesToExclude not disjoint");
    
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(srcTableId.getBytes()), ByteBuffer.wrap(newTableName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    opts.putAll(propertiesToSet);
    for (String prop : propertiesToExclude)
      opts.put(prop, null);
    
    doTableOperation(TableOperation.CLONE, args, opts);
  }
  
  /**
   * Rename a table
   * 
   * @param oldTableName
   *          the old table name
   * @param newTableName
   *          the new table name
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the old table name does not exist
   * @throws TableExistsException
   *           if the new table name already exists
   */
  public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      TableExistsException {
    
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(oldTableName.getBytes()), ByteBuffer.wrap(newTableName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    doTableOperation(TableOperation.RENAME, args, opts);
  }
  
  /**
   * @deprecated since 1.4 {@link #flush(String, Text, Text, boolean)}
   */
  public void flush(String tableName) throws AccumuloException, AccumuloSecurityException {
    try {
      flush(tableName, null, null, false);
    } catch (TableNotFoundException e) {
      throw new AccumuloException(e.getMessage(), e);
    }
  }
  
  /**
   * Flush a table
   * 
   * @param tableName
   *          the name of the table
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   */
  public void flush(String tableName, Text start, Text end, boolean wait) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    
    String tableId = Tables.getTableId(instance, tableName);
    _flush(tableId, start, end, wait);
  }
  
  public void compact(String tableName, Text start, Text end, boolean flush, boolean wait) throws AccumuloSecurityException, TableNotFoundException,
      AccumuloException {
    compact(tableName, start, end, new ArrayList<IteratorSetting>(), flush, wait);
  }
  
  public void compact(String tableName, Text start, Text end, List<IteratorSetting> iterators, boolean flush, boolean wait) throws AccumuloSecurityException,
      TableNotFoundException, AccumuloException {
    ArgumentChecker.notNull(tableName);
    ByteBuffer EMPTY = ByteBuffer.allocate(0);
    
    String tableId = Tables.getTableId(instance, tableName);
    
    if (flush)
      _flush(tableId, start, end, true);
    
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.getBytes()), start == null ? EMPTY : TextUtil.getByteBuffer(start), end == null ? EMPTY
        : TextUtil.getByteBuffer(end), ByteBuffer.wrap(IteratorUtil.encodeIteratorSettings(iterators)));

    Map<String,String> opts = new HashMap<String,String>();
    try {
      doTableOperation(TableOperation.COMPACT, args, opts, wait);
    } catch (TableExistsException e) {
      // should not happen
      throw new RuntimeException(e);
    }
  }
  
  private void _flush(String tableId, Text start, Text end, boolean wait) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    
    try {
      long flushID;
      
      // used to pass the table name. but the tableid associated with a table name could change between calls.
      // so pass the tableid to both calls
      
      while (true) {
        MasterClientService.Iface client = null;
        try {
          client = MasterClient.getConnectionWithRetry(instance);
          flushID = client.initiateFlush(Tracer.traceInfo(), credentials, tableId);
          break;
        } catch (TTransportException tte) {
          log.debug("Failed to call initiateFlush, retrying ... ", tte);
          UtilWaitThread.sleep(100);
        } finally {
          MasterClient.close(client);
        }
      }
      
      while (true) {
        MasterClientService.Iface client = null;
        try {
          client = MasterClient.getConnectionWithRetry(instance);
          client.waitForFlush(Tracer.traceInfo(), credentials, tableId, TextUtil.getByteBuffer(start), TextUtil.getByteBuffer(end), flushID, wait ? Long.MAX_VALUE : 1);
          break;
        } catch (TTransportException tte) {
          log.debug("Failed to call initiateFlush, retrying ... ", tte);
          UtilWaitThread.sleep(100);
        } finally {
          MasterClient.close(client);
        }
      }
    } catch (ThriftSecurityException e) {
      log.debug("flush security exception on table id " + tableId);
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new TableNotFoundException(e);
        case OTHER:
        default:
          throw new AccumuloException(e.description, e);
      }
    } catch (Exception e) {
      throw new AccumuloException(e);
    }
  }
  
  /**
   * Sets a property on a table
   * 
   * @param tableName
   *          the name of the table
   * @param property
   *          the name of a per-table property
   * @param value
   *          the value to set a per-table property to
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void setProperty(final String tableName, final String property, final String value) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, property, value);
    MasterClient.execute(instance, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.setTableProperty(Tracer.traceInfo(), credentials, tableName, property, value);
      }
    });
  }
  
  /**
   * Removes a property from a table
   * 
   * @param tableName
   *          the name of the table
   * @param property
   *          the name of a per-table property
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   */
  public void removeProperty(final String tableName, final String property) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, property);
    MasterClient.execute(instance, new ClientExec<MasterClientService.Client>() {
      @Override
      public void execute(MasterClientService.Client client) throws Exception {
        client.removeTableProperty(Tracer.traceInfo(), credentials, tableName, property);
      }
    });
  }
  
  /**
   * Gets properties of a table
   * 
   * @param tableName
   *          the name of the table
   * @return all properties visible by this table (system and per-table properties)
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Iterable<Entry<String,String>> getProperties(final String tableName) throws AccumuloException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    try {
      return ServerClient.executeRaw(instance, new ClientExecReturn<Map<String,String>,ClientService.Client>() {
        @Override
        public Map<String,String> execute(ClientService.Client client) throws Exception {
          return client.getTableConfiguration(tableName);
        }
      }).entrySet();
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new TableNotFoundException(e);
        case OTHER:
        default:
          throw new AccumuloException(e.description, e);
      }
    } catch (AccumuloException e) {
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    }
    
  }
  
  /**
   * Sets a tables locality groups. A tables locality groups can be changed at any time.
   * 
   * @param tableName
   *          the name of the table
   * @param groups
   *          mapping of locality group names to column families in the locality group
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    // ensure locality groups do not overlap
    HashSet<Text> all = new HashSet<Text>();
    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      
      if (!Collections.disjoint(all, entry.getValue())) {
        throw new IllegalArgumentException("Group " + entry.getKey() + " overlaps with another group");
      }
      
      all.addAll(entry.getValue());
    }
    
    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      Set<Text> colFams = entry.getValue();
      String value = LocalityGroupUtil.encodeColumnFamilies(colFams);
      setProperty(tableName, Property.TABLE_LOCALITY_GROUP_PREFIX + entry.getKey(), value);
    }
    
    setProperty(tableName, Property.TABLE_LOCALITY_GROUPS.getKey(), StringUtil.join(groups.keySet(), ","));
    
    // remove anything extraneous
    String prefix = Property.TABLE_LOCALITY_GROUP_PREFIX.getKey();
    for (Entry<String,String> entry : getProperties(tableName)) {
      String property = entry.getKey();
      if (property.startsWith(prefix)) {
        // this property configures a locality group, find out which
        // one:
        String[] parts = property.split("\\.");
        String group = parts[parts.length - 1];
        
        if (!groups.containsKey(group)) {
          removeProperty(tableName, property);
        }
      }
    }
  }
  
  /**
   * 
   * Gets the locality groups currently set for a table.
   * 
   * @param tableName
   *          the name of the table
   * @return mapping of locality group names to column families in the locality group
   * @throws AccumuloException
   *           if a general error occurs
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Map<String,Set<Text>> getLocalityGroups(String tableName) throws AccumuloException, TableNotFoundException {
    AccumuloConfiguration conf = new ConfigurationCopy(this.getProperties(tableName));
    Map<String,Set<ByteSequence>> groups = LocalityGroupUtil.getLocalityGroups(conf);
    
    Map<String,Set<Text>> groups2 = new HashMap<String,Set<Text>>();
    for (Entry<String,Set<ByteSequence>> entry : groups.entrySet()) {
      
      HashSet<Text> colFams = new HashSet<Text>();
      
      for (ByteSequence bs : entry.getValue()) {
        colFams.add(new Text(bs.toArray()));
      }
      
      groups2.put(entry.getKey(), colFams);
    }
    
    return groups2;
  }
  
  /**
   * @param tableName
   *          the name of the table
   * @param range
   *          a range to split
   * @param maxSplits
   *          the maximum number of splits
   * @return the range, split into smaller ranges that fall on boundaries of the table's split points as evenly as possible
   * @throws AccumuloException
   *           if a general error occurs
   * @throws AccumuloSecurityException
   *           if the user does not have permission
   * @throws TableNotFoundException
   *           if the table does not exist
   */
  public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    ArgumentChecker.notNull(tableName, range);
    if (maxSplits < 1)
      throw new IllegalArgumentException("maximum splits must be >= 1");
    if (maxSplits == 1)
      return Collections.singleton(range);
    
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    String tableId = Tables.getTableId(instance, tableName);
    TabletLocator tl = TabletLocator.getInstance(instance, credentials, new Text(tableId));
    // its possible that the cache could contain complete, but old information about a tables tablets... so clear it
    tl.invalidateCache();
    while (!tl.binRanges(Collections.singletonList(range), binnedRanges).isEmpty()) {
      if (!Tables.exists(instance, tableId))
        throw new TableDeletedException(tableId);
      if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
        throw new TableOfflineException(instance, tableId);

      log.warn("Unable to locate bins for specified range. Retrying.");
      // sleep randomly between 100 and 200ms
      UtilWaitThread.sleep(100 + (int) (Math.random() * 100));
      binnedRanges.clear();
      tl.invalidateCache();
    }
    
    // group key extents to get <= maxSplits
    LinkedList<KeyExtent> unmergedExtents = new LinkedList<KeyExtent>();
    List<KeyExtent> mergedExtents = new ArrayList<KeyExtent>();
    
    for (Map<KeyExtent,List<Range>> map : binnedRanges.values())
      unmergedExtents.addAll(map.keySet());
    
    // the sort method is efficient for linked list
    Collections.sort(unmergedExtents);
    
    while (unmergedExtents.size() + mergedExtents.size() > maxSplits) {
      if (unmergedExtents.size() >= 2) {
        KeyExtent first = unmergedExtents.removeFirst();
        KeyExtent second = unmergedExtents.removeFirst();
        first.setEndRow(second.getEndRow());
        mergedExtents.add(first);
      } else {
        mergedExtents.addAll(unmergedExtents);
        unmergedExtents.clear();
        unmergedExtents.addAll(mergedExtents);
        mergedExtents.clear();
      }
      
    }
    
    mergedExtents.addAll(unmergedExtents);
    
    Set<Range> ranges = new HashSet<Range>();
    for (KeyExtent k : mergedExtents)
      ranges.add(k.toDataRange().clip(range));
    
    return ranges;
  }
  
  @Override
  public void importDirectory(String tableName, String dir, String failureDir, boolean setTime) throws IOException, AccumuloSecurityException,
      TableNotFoundException, AccumuloException {
    ArgumentChecker.notNull(tableName, dir, failureDir);
    FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), instance.getConfiguration());
    Path failPath = fs.makeQualified(new Path(failureDir));
    if (!fs.exists(new Path(dir)))
      throw new AccumuloException("Bulk import directory " + dir + " does not exist!");
    if (!fs.exists(failPath))
      throw new AccumuloException("Bulk import failure directory " + failureDir + " does not exist!");
    FileStatus[] listStatus = fs.listStatus(failPath);
    if (listStatus != null && listStatus.length != 0) {
      if (listStatus.length == 1 && listStatus[0].isDir())
        throw new AccumuloException("Bulk import directory " + failPath + " is a file");
      throw new AccumuloException("Bulk import failure directory " + failPath + " is not empty");
    }
    
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes()), ByteBuffer.wrap(dir.getBytes()), ByteBuffer.wrap(failureDir.getBytes()),
        ByteBuffer.wrap((setTime + "").getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    
    try {
      doTableOperation(TableOperation.BULK_IMPORT, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new RuntimeException(e);
    }
    // return new BulkImportHelper(instance, credentials, tableName).importDirectory(new Path(dir), new Path(failureDir), numThreads, numAssignThreads,
    // disableGC);
  }
  
  /**
   * 
   * @param tableName
   *          the table to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws TableNotFoundException
   */
  public void offline(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    
    ArgumentChecker.notNull(tableName);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    
    try {
      doTableOperation(TableOperation.OFFLINE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new RuntimeException(e);
    }
  }
  
  /**
   * 
   * @param tableName
   *          the table to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   * @throws TableNotFoundException
   */
  public void online(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes()));
    Map<String,String> opts = new HashMap<String,String>();
    
    try {
      doTableOperation(TableOperation.ONLINE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Clears the tablet locator cache for a specified table
   * 
   * @param tableName
   *          the name of the table
   * @throws TableNotFoundException
   *           if table does not exist
   */
  public void clearLocatorCache(String tableName) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    TabletLocator tabLocator = TabletLocator.getInstance(instance, credentials, new Text(Tables.getTableId(instance, tableName)));
    tabLocator.invalidateCache();
  }
  
  /**
   * Get a mapping of table name to internal table id.
   * 
   * @return the map from table name to internal table id
   */
  public Map<String,String> tableIdMap() {
    return Tables.getNameToIdMap(instance);
  }
  
  @Override
  public Text getMaxRow(String tableName, Authorizations auths, Text startRow, boolean startInclusive, Text endRow, boolean endInclusive)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, auths);
    Scanner scanner = instance.getConnector(credentials).createScanner(tableName, auths);
    return FindMax.findMax(scanner, startRow, startInclusive, endRow, endInclusive);
  }
  
  public static Map<String,String> getExportedProps(FileSystem fs, Path path) throws IOException {
    HashMap<String,String> props = new HashMap<String,String>();
    
    ZipInputStream zis = new ZipInputStream(fs.open(path));
    try {
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().equals(Constants.EXPORT_TABLE_CONFIG_FILE)) {
          BufferedReader in = new BufferedReader(new InputStreamReader(zis));
          String line;
          while ((line = in.readLine()) != null) {
            String sa[] = line.split("=", 2);
            props.put(sa[0], sa[1]);
          }
          
          break;
        }
      }
    } finally {
      zis.close();
    }
    return props;
  }

  @Override
  public void importTable(String tableName, String importDir) throws TableExistsException, AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, importDir);
    
    try{
      FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), instance.getConfiguration());;
      Map<String,String> props = getExportedProps(fs, new Path(importDir, Constants.EXPORT_FILE));
      
      for(String propKey : props.keySet()){
        if (Property.isClassProperty(propKey) && !props.get(propKey).contains(Constants.CORE_PACKAGE_NAME)) {
          Logger.getLogger(this.getClass()).info(
              "Imported table sets '" + propKey + "' to '" + props.get(propKey) + "'.  Ensure this class is on Accumulo classpath.");
        }
      }
      
    }catch(IOException ioe){
      Logger.getLogger(this.getClass()).warn("Failed to check if imported table references external java classes : " + ioe.getMessage());
    }
    
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes()), ByteBuffer.wrap(importDir.getBytes()));
    
    Map<String,String> opts = Collections.emptyMap();
    
    try {
      doTableOperation(TableOperation.IMPORT, args, opts);
    } catch (TableNotFoundException e1) {
      // should not happen
      throw new RuntimeException(e1);
    }
    
  }
  
  @Override
  public void exportTable(String tableName, String exportDir) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, exportDir);
    
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes()), ByteBuffer.wrap(exportDir.getBytes()));
    
    Map<String,String> opts = Collections.emptyMap();
    
    try {
      doTableOperation(TableOperation.EXPORT, args, opts);
    } catch (TableExistsException e1) {
      // should not happen
      throw new RuntimeException(e1);
    }
  }
}

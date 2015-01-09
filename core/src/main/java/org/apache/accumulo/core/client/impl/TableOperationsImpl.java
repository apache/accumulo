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
package org.apache.accumulo.core.client.impl;

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IsolatedScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.FindMax;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.impl.thrift.ClientService;
import org.apache.accumulo.core.client.impl.thrift.ClientService.Client;
import org.apache.accumulo.core.client.impl.thrift.TDiskUsage;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.FateOperation;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.metadata.MetadataServicer;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.NamingThreadFactory;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.StringUtil;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class TableOperationsImpl extends TableOperationsHelper {
  private Instance instance;
  private Credentials credentials;

  public static final String CLONE_EXCLUDE_PREFIX = "!";

  private static final Logger log = Logger.getLogger(TableOperations.class);

  /**
   * @param instance
   *          the connection information for this instance
   * @param credentials
   *          the username/password for this connection
   */
  public TableOperationsImpl(Instance instance, Credentials credentials) {
    ArgumentChecker.notNull(instance, credentials);
    this.instance = instance;
    this.credentials = credentials;
  }

  /**
   * Retrieve a list of tables in Accumulo.
   *
   * @return List of tables in accumulo
   */
  @Override
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
  @Override
  public boolean exists(String tableName) {
    ArgumentChecker.notNull(tableName);
    if (tableName.equals(MetadataTable.NAME) || tableName.equals(RootTable.NAME))
      return true;

    OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Checking if table " + tableName + " exists...");
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
  @Override
  public void create(String tableName) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    create(tableName, true, TimeType.MILLIS);
  }

  /**
   * @param tableName
   *          the name of the table
   * @param limitVersion
   *          Enables/disables the versioning iterator, which will limit the number of Key versions kept.
   */
  @Override
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
  @Override
  public void create(String tableName, boolean limitVersion, TimeType timeType) throws AccumuloException, AccumuloSecurityException, TableExistsException {
    ArgumentChecker.notNull(tableName, timeType);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)), ByteBuffer.wrap(timeType.name().getBytes(UTF_8)));

    Map<String,String> opts;
    if (limitVersion)
      opts = IteratorUtil.generateInitialTableProperties(limitVersion);
    else
      opts = Collections.emptyMap();

    try {
      doTableFateOperation(tableName, AccumuloException.class, FateOperation.TABLE_CREATE, args, opts);
    } catch (TableNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  private long beginFateOperation() throws ThriftSecurityException, TException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        return client.beginFateOperation(Tracer.traceInfo(), credentials.toThrift(instance));
      } catch (TTransportException tte) {
        log.debug("Failed to call beginFateOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  // This method is for retrying in the case of network failures; anything else it passes to the caller to deal with
  private void executeFateOperation(long opid, FateOperation op, List<ByteBuffer> args, Map<String,String> opts, boolean autoCleanUp)
      throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        client.executeFateOperation(Tracer.traceInfo(), credentials.toThrift(instance), opid, op, args, opts, autoCleanUp);
        break;
      } catch (TTransportException tte) {
        log.debug("Failed to call executeFateOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private String waitForFateOperation(long opid) throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        return client.waitForFateOperation(Tracer.traceInfo(), credentials.toThrift(instance), opid);
      } catch (TTransportException tte) {
        log.debug("Failed to call waitForFateOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  private void finishFateOperation(long opid) throws ThriftSecurityException, TException {
    while (true) {
      MasterClientService.Iface client = null;
      try {
        client = MasterClient.getConnectionWithRetry(instance);
        client.finishFateOperation(Tracer.traceInfo(), credentials.toThrift(instance), opid);
        break;
      } catch (TTransportException tte) {
        log.debug("Failed to call finishFateOperation(), retrying ... ", tte);
        UtilWaitThread.sleep(100);
      } finally {
        MasterClient.close(client);
      }
    }
  }

  String doFateOperation(FateOperation op, List<ByteBuffer> args, Map<String,String> opts) throws AccumuloSecurityException, TableExistsException,
      TableNotFoundException, AccumuloException, NamespaceExistsException, NamespaceNotFoundException {
    return doFateOperation(op, args, opts, true);
  }

  String doFateOperation(FateOperation op, List<ByteBuffer> args, Map<String,String> opts, boolean wait) throws AccumuloSecurityException,
      TableExistsException, TableNotFoundException, AccumuloException, NamespaceExistsException, NamespaceNotFoundException {
    Long opid = null;

    try {
      opid = beginFateOperation();
      executeFateOperation(opid, op, args, opts, !wait);
      if (!wait) {
        opid = null;
        return null;
      }
      String ret = waitForFateOperation(opid);
      return ret;
    } catch (ThriftSecurityException e) {
      String tableName = ByteBufferUtil.toString(args.get(0));
      switch (e.getCode()) {
        case TABLE_DOESNT_EXIST:
          throw new TableNotFoundException(null, tableName, "Target table does not exist");
        case NAMESPACE_DOESNT_EXIST:
          throw new NamespaceNotFoundException(null, tableName, "Target namespace does not exist");
        default:
          String tableInfo = Tables.getPrintableTableInfoFromName(instance, tableName);
          throw new AccumuloSecurityException(e.user, e.code, tableInfo, e);
      }
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case EXISTS:
          throw new TableExistsException(e);
        case NOTFOUND:
          throw new TableNotFoundException(e);
        case NAMESPACE_EXISTS:
          throw new NamespaceExistsException(e);
        case NAMESPACE_NOTFOUND:
          throw new NamespaceNotFoundException(e);
        case OFFLINE:
          throw new TableOfflineException(instance, null);
        default:
          throw new AccumuloException(e.description, e);
      }
    } catch (Exception e) {
      throw new AccumuloException(e.getMessage(), e);
    } finally {
      Tables.clearCache(instance);
      // always finish table op, even when exception
      if (opid != null)
        try {
          finishFateOperation(opid);
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
  @Override
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
    TabletLocator tabLocator = TabletLocator.getLocator(instance, new Text(tableId));

    for (Text split : partitionKeys) {
      boolean successful = false;
      int attempt = 0;
      long locationFailures = 0;

      while (!successful) {

        if (attempt > 0)
          UtilWaitThread.sleep(100);

        attempt++;

        TabletLocation tl = tabLocator.locateTablet(credentials, split, false, false);

        if (tl == null) {
          if (!Tables.exists(instance, tableId))
            throw new TableNotFoundException(tableId, tableName, null);
          else if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
            throw new TableOfflineException(instance, tableId);
          continue;
        }

        try {
          TabletClientService.Client client = ThriftUtil.getTServerClient(tl.tablet_location, ServerConfigurationUtil.getConfiguration(instance));
          try {
            OpTimer opTimer = null;
            if (log.isTraceEnabled())
              opTimer = new OpTimer(log, Level.TRACE).start("Splitting tablet " + tl.tablet_extent + " on " + tl.tablet_location + " at " + split);

            client.splitTablet(Tracer.traceInfo(), credentials.toThrift(instance), tl.tablet_extent.toThrift(), TextUtil.getByteBuffer(split));

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
          // Do not silently spin when we repeatedly fail to get the location for a tablet
          locationFailures++;
          if (5 == locationFailures || 0 == locationFailures % 50) {
            log.warn("Having difficulty locating hosting tabletserver for split " + split + " on table " + tableName + ". Seen " + locationFailures
                + " failures.");
          }

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

  @Override
  public void merge(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    ArgumentChecker.notNull(tableName);
    ByteBuffer EMPTY = ByteBuffer.allocate(0);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)), start == null ? EMPTY : TextUtil.getByteBuffer(start),
        end == null ? EMPTY : TextUtil.getByteBuffer(end));
    Map<String,String> opts = new HashMap<String,String>();
    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_MERGE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  @Override
  public void deleteRows(String tableName, Text start, Text end) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    ArgumentChecker.notNull(tableName);
    ByteBuffer EMPTY = ByteBuffer.allocate(0);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)), start == null ? EMPTY : TextUtil.getByteBuffer(start),
        end == null ? EMPTY : TextUtil.getByteBuffer(end));
    Map<String,String> opts = new HashMap<String,String>();
    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_DELETE_RANGE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  /**
   * @param tableName
   *          the name of the table
   * @return the split points (end-row names) for the table's current split profile
   */
  @Override
  public Collection<Text> listSplits(String tableName) throws TableNotFoundException, AccumuloSecurityException {

    ArgumentChecker.notNull(tableName);

    String tableId = Tables.getTableId(instance, tableName);

    TreeMap<KeyExtent,String> tabletLocations = new TreeMap<KeyExtent,String>();

    while (true) {
      try {
        tabletLocations.clear();
        // the following method throws AccumuloException for some conditions that should be retried
        MetadataServicer.forTableId(instance, credentials, tableId).getTabletLocations(tabletLocations);
        break;
      } catch (AccumuloSecurityException ase) {
        throw ase;
      } catch (Exception e) {
        if (!Tables.exists(instance, tableId)) {
          throw new TableNotFoundException(tableId, tableName, null);
        }

        if (e instanceof RuntimeException && e.getCause() instanceof AccumuloSecurityException) {
          throw (AccumuloSecurityException) e.getCause();
        }

        log.info(e.getMessage() + " ... retrying ...");
        UtilWaitThread.sleep(3000);
      }
    }

    ArrayList<Text> endRows = new ArrayList<Text>(tabletLocations.size());

    for (KeyExtent ke : tabletLocations.keySet())
      if (ke.getEndRow() != null)
        endRows.add(ke.getEndRow());

    return endRows;
  }

  @Deprecated
  @Override
  public Collection<Text> getSplits(String tableName) throws TableNotFoundException {
    try {
      return listSplits(tableName);
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @param tableName
   *          the name of the table
   * @param maxSplits
   *          specifies the maximum number of splits to return
   * @return the split points (end-row names) for the table's current split profile, grouped into fewer splits so as not to exceed maxSplits
   */
  @Override
  public Collection<Text> listSplits(String tableName, int maxSplits) throws TableNotFoundException, AccumuloSecurityException {
    Collection<Text> endRows = listSplits(tableName);

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

  @Deprecated
  @Override
  public Collection<Text> getSplits(String tableName, int maxSplits) throws TableNotFoundException {
    try {
      return listSplits(tableName, maxSplits);
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
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
  @Override
  public void delete(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<String,String>();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_DELETE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

  }

  @Override
  public void clone(String srcTableName, String newTableName, boolean flush, Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException, TableExistsException {

    ArgumentChecker.notNull(srcTableName, newTableName);

    String srcTableId = Tables.getTableId(instance, srcTableName);

    if (flush)
      _flush(srcTableId, null, null, true);

    if (propertiesToExclude == null)
      propertiesToExclude = Collections.emptySet();

    if (propertiesToSet == null)
      propertiesToSet = Collections.emptyMap();

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(srcTableId.getBytes(UTF_8)), ByteBuffer.wrap(newTableName.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<String,String>();
    for (Entry<String,String> entry : propertiesToSet.entrySet()) {
      if (entry.getKey().startsWith(CLONE_EXCLUDE_PREFIX))
        throw new IllegalArgumentException("Property can not start with " + CLONE_EXCLUDE_PREFIX);
      opts.put(entry.getKey(), entry.getValue());
    }

    for (String prop : propertiesToExclude) {
      opts.put(CLONE_EXCLUDE_PREFIX + prop, "");
    }

    doTableFateOperation(newTableName, AccumuloException.class, FateOperation.TABLE_CLONE, args, opts);
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
  @Override
  public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      TableExistsException {

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(oldTableName.getBytes(UTF_8)), ByteBuffer.wrap(newTableName.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<String,String>();
    doTableFateOperation(oldTableName, TableNotFoundException.class, FateOperation.TABLE_RENAME, args, opts);
  }

  /**
   * @deprecated since 1.4 {@link #flush(String, Text, Text, boolean)}
   */
  @Override
  @Deprecated
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
   */
  @Override
  public void flush(String tableName, Text start, Text end, boolean wait) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);

    String tableId = Tables.getTableId(instance, tableName);
    _flush(tableId, start, end, wait);
  }

  @Override
  public void compact(String tableName, Text start, Text end, boolean flush, boolean wait) throws AccumuloSecurityException, TableNotFoundException,
      AccumuloException {
    compact(tableName, start, end, new ArrayList<IteratorSetting>(), flush, wait);
  }

  @Override
  public void compact(String tableName, Text start, Text end, List<IteratorSetting> iterators, boolean flush, boolean wait) throws AccumuloSecurityException,
      TableNotFoundException, AccumuloException {
    ArgumentChecker.notNull(tableName);
    ByteBuffer EMPTY = ByteBuffer.allocate(0);

    String tableId = Tables.getTableId(instance, tableName);

    if (flush)
      _flush(tableId, start, end, true);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.getBytes(UTF_8)), start == null ? EMPTY : TextUtil.getByteBuffer(start), end == null ? EMPTY
        : TextUtil.getByteBuffer(end), ByteBuffer.wrap(IteratorUtil.encodeIteratorSettings(iterators)));

    Map<String,String> opts = new HashMap<String,String>();
    try {
      doFateOperation(FateOperation.TABLE_COMPACT, args, opts, wait);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    } catch (NamespaceExistsException e) {
      // should not happen
      throw new AssertionError(e);
    } catch (NamespaceNotFoundException e) {
      throw new TableNotFoundException(null, tableName, "Namespace not found", e);
    }
  }

  @Override
  public void cancelCompaction(String tableName) throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    String tableId = Tables.getTableId(instance, tableName);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.getBytes(UTF_8)));

    Map<String,String> opts = new HashMap<String,String>();
    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_CANCEL_COMPACT, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
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
          flushID = client.initiateFlush(Tracer.traceInfo(), credentials.toThrift(instance), tableId);
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
          client.waitForFlush(Tracer.traceInfo(), credentials.toThrift(instance), tableId, TextUtil.getByteBuffer(start), TextUtil.getByteBuffer(end), flushID,
              wait ? Long.MAX_VALUE : 1);
          break;
        } catch (TTransportException tte) {
          log.debug("Failed to call initiateFlush, retrying ... ", tte);
          UtilWaitThread.sleep(100);
        } finally {
          MasterClient.close(client);
        }
      }
    } catch (ThriftSecurityException e) {
      switch (e.getCode()) {
        case TABLE_DOESNT_EXIST:
          throw new TableNotFoundException(tableId, null, e.getMessage(), e);
        default:
          log.debug("flush security exception on table id " + tableId);
          throw new AccumuloSecurityException(e.user, e.code, e);
      }
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new TableNotFoundException(e);
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
  @Override
  public void setProperty(final String tableName, final String property, final String value) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, property, value);
    try {
      MasterClient.executeTable(instance, new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          client.setTableProperty(Tracer.traceInfo(), credentials.toThrift(instance), tableName, property, value);
        }
      });
    } catch (TableNotFoundException e) {
      throw new AccumuloException(e);
    }
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
  @Override
  public void removeProperty(final String tableName, final String property) throws AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, property);
    try {
      MasterClient.executeTable(instance, new ClientExec<MasterClientService.Client>() {
        @Override
        public void execute(MasterClientService.Client client) throws Exception {
          client.removeTableProperty(Tracer.traceInfo(), credentials.toThrift(instance), tableName, property);
        }
      });
    } catch (TableNotFoundException e) {
      throw new AccumuloException(e);
    }
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
  @Override
  public Iterable<Entry<String,String>> getProperties(final String tableName) throws AccumuloException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    try {
      return ServerClient.executeRaw(instance, new ClientExecReturn<Map<String,String>,ClientService.Client>() {
        @Override
        public Map<String,String> execute(ClientService.Client client) throws Exception {
          return client.getTableConfiguration(Tracer.traceInfo(), credentials.toThrift(instance), tableName);
        }
      }).entrySet();
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new TableNotFoundException(e);
        case NAMESPACE_NOTFOUND:
          throw new TableNotFoundException(tableName, new NamespaceNotFoundException(e));
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
  @Override
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

    try {
      setProperty(tableName, Property.TABLE_LOCALITY_GROUPS.getKey(), StringUtil.join(groups.keySet(), ","));
    } catch (AccumuloException e) {
      if (e.getCause() instanceof TableNotFoundException)
        throw (TableNotFoundException) e.getCause();
      throw e;
    }

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
  @Override
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
  @Override
  public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    ArgumentChecker.notNull(tableName, range);
    if (maxSplits < 1)
      throw new IllegalArgumentException("maximum splits must be >= 1");
    if (maxSplits == 1)
      return Collections.singleton(range);

    Random random = new Random();
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    String tableId = Tables.getTableId(instance, tableName);
    TabletLocator tl = TabletLocator.getLocator(instance, new Text(tableId));
    // its possible that the cache could contain complete, but old information about a tables tablets... so clear it
    tl.invalidateCache();
    while (!tl.binRanges(credentials, Collections.singletonList(range), binnedRanges).isEmpty()) {
      if (!Tables.exists(instance, tableId))
        throw new TableDeletedException(tableId);
      if (Tables.getTableState(instance, tableId) == TableState.OFFLINE)
        throw new TableOfflineException(instance, tableId);

      log.warn("Unable to locate bins for specified range. Retrying.");
      // sleep randomly between 100 and 200ms
      UtilWaitThread.sleep(100 + random.nextInt(100));
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

  // TODO Remove deprecation warning surppression when Hadoop1 support is dropped
  @SuppressWarnings("deprecation")
  private Path checkPath(String dir, String kind, String type) throws IOException, AccumuloException {
    Path ret;
    FileSystem fs = VolumeConfiguration.getVolume(dir, CachedConfiguration.getInstance(), ServerConfigurationUtil.getConfiguration(instance)).getFileSystem();

    if (dir.contains(":")) {
      ret = new Path(dir);
    } else {
      ret = fs.makeQualified(new Path(dir));
    }

    if (!fs.exists(ret))
      throw new AccumuloException(kind + " import " + type + " directory " + dir + " does not exist!");

    if (!fs.getFileStatus(ret).isDir()) {
      throw new AccumuloException(kind + " import " + type + " directory " + dir + " is not a directory!");
    }

    if (type.equals("failure")) {
      FileStatus[] listStatus = fs.listStatus(ret);
      if (listStatus != null && listStatus.length != 0) {
        throw new AccumuloException("Bulk import failure directory " + ret + " is not empty");
      }
    }

    return ret;
  }

  @Override
  public void importDirectory(String tableName, String dir, String failureDir, boolean setTime) throws IOException, AccumuloSecurityException,
      TableNotFoundException, AccumuloException {
    ArgumentChecker.notNull(tableName, dir, failureDir);
    // check for table existance
    Tables.getTableId(instance, tableName);

    Path dirPath = checkPath(dir, "Bulk", "");
    Path failPath = checkPath(failureDir, "Bulk", "failure");

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)), ByteBuffer.wrap(dirPath.toString().getBytes(UTF_8)),
        ByteBuffer.wrap(failPath.toString().getBytes(UTF_8)), ByteBuffer.wrap((setTime + "").getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<String,String>();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_BULK_IMPORT, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  private void waitForTableStateTransition(String tableId, TableState expectedState) throws AccumuloException, TableNotFoundException,
      AccumuloSecurityException {

    Text startRow = null;
    Text lastRow = null;

    while (true) {

      if (Tables.getTableState(instance, tableId) != expectedState) {
        Tables.clearCache(instance);
        if (Tables.getTableState(instance, tableId) != expectedState) {
          if (!Tables.exists(instance, tableId))
            throw new TableDeletedException(tableId);
          throw new AccumuloException("Unexpected table state " + tableId + " " + Tables.getTableState(instance, tableId) + " != " + expectedState);
        }
      }

      Range range = new KeyExtent(new Text(tableId), null, null).toMetadataRange();
      if (startRow == null || lastRow == null)
        range = new KeyExtent(new Text(tableId), null, null).toMetadataRange();
      else
        range = new Range(startRow, lastRow);

      String metaTable = MetadataTable.NAME;
      if (tableId.equals(MetadataTable.ID))
        metaTable = RootTable.NAME;

      Scanner scanner = createMetadataScanner(instance, credentials, metaTable, range);

      RowIterator rowIter = new RowIterator(scanner);

      KeyExtent lastExtent = null;

      int total = 0;
      int waitFor = 0;
      int holes = 0;
      Text continueRow = null;
      MapCounter<String> serverCounts = new MapCounter<String>();

      while (rowIter.hasNext()) {
        Iterator<Entry<Key,Value>> row = rowIter.next();

        total++;

        KeyExtent extent = null;
        String future = null;
        String current = null;

        while (row.hasNext()) {
          Entry<Key,Value> entry = row.next();
          Key key = entry.getKey();

          if (key.getColumnFamily().equals(TabletsSection.FutureLocationColumnFamily.NAME))
            future = entry.getValue().toString();

          if (key.getColumnFamily().equals(TabletsSection.CurrentLocationColumnFamily.NAME))
            current = entry.getValue().toString();

          if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key))
            extent = new KeyExtent(key.getRow(), entry.getValue());
        }

        if ((expectedState == TableState.ONLINE && current == null) || (expectedState == TableState.OFFLINE && (future != null || current != null))) {
          if (continueRow == null)
            continueRow = extent.getMetadataEntry();
          waitFor++;
          lastRow = extent.getMetadataEntry();

          if (current != null)
            serverCounts.increment(current, 1);
          if (future != null)
            serverCounts.increment(future, 1);
        }

        if (!extent.getTableId().toString().equals(tableId)) {
          throw new AccumuloException("Saw unexpected table Id " + tableId + " " + extent);
        }

        if (lastExtent != null && !extent.isPreviousExtent(lastExtent)) {
          holes++;
        }

        lastExtent = extent;
      }

      if (continueRow != null) {
        startRow = continueRow;
      }

      if (holes > 0 || total == 0) {
        startRow = null;
        lastRow = null;
      }

      if (waitFor > 0 || holes > 0 || total == 0) {
        long waitTime;
        long maxPerServer = 0;
        if (serverCounts.size() > 0) {
          maxPerServer = Collections.max(serverCounts.values());
          waitTime = maxPerServer * 10;
        } else
          waitTime = waitFor * 10;
        waitTime = Math.max(100, waitTime);
        waitTime = Math.min(5000, waitTime);
        log.trace("Waiting for " + waitFor + "(" + maxPerServer + ") tablets, startRow = " + startRow + " lastRow = " + lastRow + ", holes=" + holes
            + " sleeping:" + waitTime + "ms");
        UtilWaitThread.sleep(waitTime);
      } else {
        break;
      }

    }
  }

  /**
   * Create an IsolatedScanner over the given table, fetching the columns necessary to determine when a table has transitioned to online or offline.
   */
  protected IsolatedScanner createMetadataScanner(Instance inst, Credentials creds, String metaTable, Range range) throws TableNotFoundException,
      AccumuloException, AccumuloSecurityException {
    Scanner scanner = inst.getConnector(creds.getPrincipal(), creds.getToken()).createScanner(metaTable, Authorizations.EMPTY);
    TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
    scanner.fetchColumnFamily(TabletsSection.FutureLocationColumnFamily.NAME);
    scanner.fetchColumnFamily(TabletsSection.CurrentLocationColumnFamily.NAME);
    scanner.setRange(range);
    return new IsolatedScanner(scanner);
  }

  @Override
  public void offline(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    offline(tableName, false);
  }

  /**
   *
   * @param tableName
   *          the table to take offline
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  @Override
  public void offline(String tableName, boolean wait) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

    ArgumentChecker.notNull(tableName);
    String tableId = Tables.getTableId(instance, tableName);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<String,String>();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_OFFLINE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

    if (wait)
      waitForTableStateTransition(tableId, TableState.OFFLINE);
  }

  @Override
  public void online(String tableName) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    online(tableName, false);
  }

  /**
   *
   * @param tableName
   *          the table to take online
   * @throws AccumuloException
   *           when there is a general accumulo error
   * @throws AccumuloSecurityException
   *           when the user does not have the proper permissions
   */
  @Override
  public void online(String tableName, boolean wait) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    String tableId = Tables.getTableId(instance, tableName);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<String,String>();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_ONLINE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

    if (wait)
      waitForTableStateTransition(tableId, TableState.ONLINE);
  }

  /**
   * Clears the tablet locator cache for a specified table
   *
   * @param tableName
   *          the name of the table
   * @throws TableNotFoundException
   *           if table does not exist
   */
  @Override
  public void clearLocatorCache(String tableName) throws TableNotFoundException {
    ArgumentChecker.notNull(tableName);
    TabletLocator tabLocator = TabletLocator.getLocator(instance, new Text(Tables.getTableId(instance, tableName)));
    tabLocator.invalidateCache();
  }

  /**
   * Get a mapping of table name to internal table id.
   *
   * @return the map from table name to internal table id
   */
  @Override
  public Map<String,String> tableIdMap() {
    return Tables.getNameToIdMap(instance);
  }

  @Override
  public Text getMaxRow(String tableName, Authorizations auths, Text startRow, boolean startInclusive, Text endRow, boolean endInclusive)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, auths);
    Scanner scanner = instance.getConnector(credentials.getPrincipal(), credentials.getToken()).createScanner(tableName, auths);
    return FindMax.findMax(scanner, startRow, startInclusive, endRow, endInclusive);
  }

  @Override
  public List<DiskUsage> getDiskUsage(Set<String> tableNames) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    List<TDiskUsage> diskUsages = null;
    while (diskUsages == null) {
      Pair<String,Client> pair = null;
      try {
        // this operation may us a lot of memory... its likely that connections to tabletservers hosting metadata tablets will be cached, so do not use cached
        // connections
        pair = ServerClient.getConnection(instance, false);
        diskUsages = pair.getSecond().getDiskUsage(tableNames, credentials.toThrift(instance));
      } catch (ThriftTableOperationException e) {
        switch (e.getType()) {
          case NOTFOUND:
            throw new TableNotFoundException(e);
          case NAMESPACE_NOTFOUND:
            throw new TableNotFoundException(e.getTableName(), new NamespaceNotFoundException(e));
          default:
            throw new AccumuloException(e.description, e);
        }
      } catch (ThriftSecurityException e) {
        throw new AccumuloSecurityException(e.getUser(), e.getCode());
      } catch (TTransportException e) {
        // some sort of communication error occurred, retry
        if (pair == null) {
          log.debug("Disk usage request failed.  Pair is null.  Retrying request...", e);
        } else {
          log.debug("Disk usage request failed " + pair.getFirst() + ", retrying ... ", e);
        }
        UtilWaitThread.sleep(100);
      } catch (TException e) {
        // may be a TApplicationException which indicates error on the server side
        throw new AccumuloException(e);
      } finally {
        // must always return thrift connection
        if (pair != null)
          ServerClient.close(pair.getSecond());
      }
    }

    List<DiskUsage> finalUsages = new ArrayList<DiskUsage>();
    for (TDiskUsage diskUsage : diskUsages) {
      finalUsages.add(new DiskUsage(new TreeSet<String>(diskUsage.getTables()), diskUsage.getUsage()));
    }

    return finalUsages;
  }

  public static Map<String,String> getExportedProps(FileSystem fs, Path path) throws IOException {
    HashMap<String,String> props = new HashMap<String,String>();

    ZipInputStream zis = new ZipInputStream(fs.open(path));
    try {
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().equals(Constants.EXPORT_TABLE_CONFIG_FILE)) {
          BufferedReader in = new BufferedReader(new InputStreamReader(zis, UTF_8));
          try {
            String line;
            while ((line = in.readLine()) != null) {
              String sa[] = line.split("=", 2);
              props.put(sa[0], sa[1]);
            }
          } finally {
            in.close();
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

    try {
      importDir = checkPath(importDir, "Table", "").toString();
    } catch (IOException e) {
      throw new AccumuloException(e);
    }

    try {
      FileSystem fs = new Path(importDir).getFileSystem(CachedConfiguration.getInstance());
      Map<String,String> props = getExportedProps(fs, new Path(importDir, Constants.EXPORT_FILE));

      for (Entry<String,String> entry : props.entrySet()) {
        if (Property.isClassProperty(entry.getKey()) && !entry.getValue().contains(Constants.CORE_PACKAGE_NAME)) {
          Logger.getLogger(this.getClass()).info(
              "Imported table sets '" + entry.getKey() + "' to '" + entry.getValue() + "'.  Ensure this class is on Accumulo classpath.");
        }
      }

    } catch (IOException ioe) {
      Logger.getLogger(this.getClass()).warn("Failed to check if imported table references external java classes : " + ioe.getMessage());
    }

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)), ByteBuffer.wrap(importDir.getBytes(UTF_8)));

    Map<String,String> opts = Collections.emptyMap();

    try {
      doTableFateOperation(tableName, AccumuloException.class, FateOperation.TABLE_IMPORT, args, opts);
    } catch (TableNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }

  }

  @Override
  public void exportTable(String tableName, String exportDir) throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, exportDir);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)), ByteBuffer.wrap(exportDir.getBytes(UTF_8)));

    Map<String,String> opts = Collections.emptyMap();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_EXPORT, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  @Override
  public boolean testClassLoad(final String tableName, final String className, final String asTypeName) throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException {
    ArgumentChecker.notNull(tableName, className, asTypeName);

    try {
      return ServerClient.executeRaw(instance, new ClientExecReturn<Boolean,ClientService.Client>() {
        @Override
        public Boolean execute(ClientService.Client client) throws Exception {
          return client.checkTableClass(Tracer.traceInfo(), credentials.toThrift(instance), tableName, className, asTypeName);
        }
      });
    } catch (ThriftTableOperationException e) {
      switch (e.getType()) {
        case NOTFOUND:
          throw new TableNotFoundException(e);
        case NAMESPACE_NOTFOUND:
          throw new TableNotFoundException(tableName, new NamespaceNotFoundException(e));
        default:
          throw new AccumuloException(e.description, e);
      }
    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (AccumuloException e) {
      throw e;
    } catch (Exception e) {
      throw new AccumuloException(e);
    }
  }

  @Override
  public void attachIterator(String tableName, IteratorSetting setting, EnumSet<IteratorScope> scopes) throws AccumuloSecurityException, AccumuloException,
      TableNotFoundException {
    testClassLoad(tableName, setting.getIteratorClass(), SortedKeyValueIterator.class.getName());
    super.attachIterator(tableName, setting, scopes);
  }

  @Override
  public int addConstraint(String tableName, String constraintClassName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    testClassLoad(tableName, constraintClassName, Constraint.class.getName());
    return super.addConstraint(tableName, constraintClassName);
  }

  private void doTableFateOperation(String tableName, Class<? extends Exception> namespaceNotFoundExceptionClass, FateOperation op, List<ByteBuffer> args,
      Map<String,String> opts) throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
    try {
      doFateOperation(op, args, opts);
    } catch (NamespaceExistsException e) {
      // should not happen
      throw new AssertionError(e);
    } catch (NamespaceNotFoundException e) {
      if (namespaceNotFoundExceptionClass == null) {
        // should not happen
        throw new AssertionError(e);
      } else if (AccumuloException.class.isAssignableFrom(namespaceNotFoundExceptionClass)) {
        throw new AccumuloException("Cannot create table in non-existent namespace", e);
      } else if (TableNotFoundException.class.isAssignableFrom(namespaceNotFoundExceptionClass)) {
        throw new TableNotFoundException(null, tableName, "Namespace not found", e);
      } else {
        // should not happen
        throw new AssertionError(e);
      }
    }
  }

}

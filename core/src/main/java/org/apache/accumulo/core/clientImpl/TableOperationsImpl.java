/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.accumulo.core.Constants.MAX_TABLE_NAME_LEN;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.admin.CloneConfiguration;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.FindMax;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.SummaryRetriever;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.admin.compaction.CompactionConfigurer;
import org.apache.accumulo.core.client.admin.compaction.CompactionSelector;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.core.clientImpl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.clientImpl.bulk.BulkImport;
import org.apache.accumulo.core.clientImpl.thrift.ClientService;
import org.apache.accumulo.core.clientImpl.thrift.ClientService.Client;
import org.apache.accumulo.core.clientImpl.thrift.TDiskUsage;
import org.apache.accumulo.core.clientImpl.thrift.ThriftNotActiveServiceException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.dataImpl.thrift.TRowRange;
import org.apache.accumulo.core.dataImpl.thrift.TSummaries;
import org.apache.accumulo.core.dataImpl.thrift.TSummarizerConfiguration;
import org.apache.accumulo.core.dataImpl.thrift.TSummaryRequest;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.manager.thrift.FateOperation;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.metadata.MetadataServicer;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.summary.SummarizerConfigurationUtil;
import org.apache.accumulo.core.summary.SummaryCollection;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.core.util.LocalityGroupUtil.LocalityGroupConfigurationError;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.volume.VolumeConfiguration;
import org.apache.accumulo.fate.util.Retry;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

public class TableOperationsImpl extends TableOperationsHelper {

  public static final String CLONE_EXCLUDE_PREFIX = "!";
  private static final Logger log = LoggerFactory.getLogger(TableOperations.class);
  private final ClientContext context;

  public TableOperationsImpl(ClientContext context) {
    checkArgument(context != null, "context is null");
    this.context = context;
  }

  @Override
  public SortedSet<String> list() {

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Fetching list of tables...", Thread.currentThread().getId());
      timer = new OpTimer().start();
    }

    TreeSet<String> tableNames = new TreeSet<>(Tables.getNameToIdMap(context).keySet());

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Fetched {} table names in {}", Thread.currentThread().getId(),
          tableNames.size(), String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    return tableNames;
  }

  @Override
  public boolean exists(String tableName) {
    checkArgument(tableName != null, "tableName is null");
    if (tableName.equals(MetadataTable.NAME) || tableName.equals(RootTable.NAME))
      return true;

    OpTimer timer = null;

    if (log.isTraceEnabled()) {
      log.trace("tid={} Checking if table {} exists...", Thread.currentThread().getId(), tableName);
      timer = new OpTimer().start();
    }

    boolean exists = Tables.getNameToIdMap(context).containsKey(tableName);

    if (timer != null) {
      timer.stop();
      log.trace("tid={} Checked existance of {} in {}", Thread.currentThread().getId(), exists,
          String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
    }

    return exists;
  }

  @Override
  public void create(String tableName)
      throws AccumuloException, AccumuloSecurityException, TableExistsException {
    create(tableName, new NewTableConfiguration());
  }

  @Override
  public void create(String tableName, NewTableConfiguration ntc)
      throws AccumuloException, AccumuloSecurityException, TableExistsException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(ntc != null, "ntc is null");
    checkArgument(tableName.length() <= MAX_TABLE_NAME_LEN,
        "Table name is longer than " + MAX_TABLE_NAME_LEN + " characters");

    List<ByteBuffer> args = new ArrayList<>();
    args.add(ByteBuffer.wrap(tableName.getBytes(UTF_8)));
    args.add(ByteBuffer.wrap(ntc.getTimeType().name().getBytes(UTF_8)));
    // Send info relating to initial table creation i.e, create online or offline
    args.add(ByteBuffer.wrap(ntc.getInitialTableState().name().getBytes(UTF_8)));
    // Check for possible initial splits to be added at table creation
    // Always send number of initial splits to be created, even if zero. If greater than zero,
    // add the splits to the argument List which will be used by the FATE operations.
    int numSplits = ntc.getSplits().size();
    args.add(ByteBuffer.wrap(String.valueOf(numSplits).getBytes(UTF_8)));
    if (numSplits > 0) {
      for (Text t : ntc.getSplits()) {
        args.add(TextUtil.getByteBuffer(t));
      }
    }

    Map<String,String> opts = ntc.getProperties();

    try {
      doTableFateOperation(tableName, AccumuloException.class, FateOperation.TABLE_CREATE, args,
          opts);
    } catch (TableNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  private long beginFateOperation() throws ThriftSecurityException, TException {
    while (true) {
      ManagerClientService.Iface client = null;
      try {
        client = ManagerClient.getConnectionWithRetry(context);
        return client.beginFateOperation(TraceUtil.traceInfo(), context.rpcCreds());
      } catch (TTransportException tte) {
        log.debug("Failed to call beginFateOperation(), retrying ... ", tte);
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        log.debug("Contacted a Manager which is no longer active, retrying");
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } finally {
        ManagerClient.close(client);
      }
    }
  }

  // This method is for retrying in the case of network failures; anything else it passes to the
  // caller to deal with
  private void executeFateOperation(long opid, FateOperation op, List<ByteBuffer> args,
      Map<String,String> opts, boolean autoCleanUp)
      throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      ManagerClientService.Iface client = null;
      try {
        client = ManagerClient.getConnectionWithRetry(context);
        client.executeFateOperation(TraceUtil.traceInfo(), context.rpcCreds(), opid, op, args, opts,
            autoCleanUp);
        return;
      } catch (TTransportException tte) {
        log.debug("Failed to call executeFateOperation(), retrying ... ", tte);
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        log.debug("Contacted a Manager which is no longer active, retrying");
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } finally {
        ManagerClient.close(client);
      }
    }
  }

  private String waitForFateOperation(long opid)
      throws ThriftSecurityException, TException, ThriftTableOperationException {
    while (true) {
      ManagerClientService.Iface client = null;
      try {
        client = ManagerClient.getConnectionWithRetry(context);
        return client.waitForFateOperation(TraceUtil.traceInfo(), context.rpcCreds(), opid);
      } catch (TTransportException tte) {
        log.debug("Failed to call waitForFateOperation(), retrying ... ", tte);
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        log.debug("Contacted a Manager which is no longer active, retrying");
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } finally {
        ManagerClient.close(client);
      }
    }
  }

  private void finishFateOperation(long opid) throws ThriftSecurityException, TException {
    while (true) {
      ManagerClientService.Iface client = null;
      try {
        client = ManagerClient.getConnectionWithRetry(context);
        client.finishFateOperation(TraceUtil.traceInfo(), context.rpcCreds(), opid);
        break;
      } catch (TTransportException tte) {
        log.debug("Failed to call finishFateOperation(), retrying ... ", tte);
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (ThriftNotActiveServiceException e) {
        // Let it loop, fetching a new location
        log.debug("Contacted a Manager which is no longer active, retrying");
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } finally {
        ManagerClient.close(client);
      }
    }
  }

  public String doBulkFateOperation(List<ByteBuffer> args, String tableName)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    try {
      return doFateOperation(FateOperation.TABLE_BULK_IMPORT2, args, Collections.emptyMap(),
          tableName);
    } catch (TableExistsException | NamespaceExistsException e) {
      // should not happen
      throw new AssertionError(e);
    } catch (NamespaceNotFoundException ne) {
      throw new TableNotFoundException(null, tableName, "Namespace not found", ne);
    }
  }

  String doFateOperation(FateOperation op, List<ByteBuffer> args, Map<String,String> opts,
      String tableOrNamespaceName)
      throws AccumuloSecurityException, TableExistsException, TableNotFoundException,
      AccumuloException, NamespaceExistsException, NamespaceNotFoundException {
    return doFateOperation(op, args, opts, tableOrNamespaceName, true);
  }

  String doFateOperation(FateOperation op, List<ByteBuffer> args, Map<String,String> opts,
      String tableOrNamespaceName, boolean wait)
      throws AccumuloSecurityException, TableExistsException, TableNotFoundException,
      AccumuloException, NamespaceExistsException, NamespaceNotFoundException {
    Long opid = null;

    try {
      opid = beginFateOperation();
      executeFateOperation(opid, op, args, opts, !wait);
      if (!wait) {
        opid = null;
        return null;
      }
      return waitForFateOperation(opid);
    } catch (ThriftSecurityException e) {
      switch (e.getCode()) {
        case TABLE_DOESNT_EXIST:
          throw new TableNotFoundException(null, tableOrNamespaceName,
              "Target table does not exist");
        case NAMESPACE_DOESNT_EXIST:
          throw new NamespaceNotFoundException(null, tableOrNamespaceName,
              "Target namespace does not exist");
        default:
          String tableInfo = Tables.getPrintableTableInfoFromName(context, tableOrNamespaceName);
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
          throw new TableOfflineException(
              Tables.getTableOfflineMsg(context, Tables.getTableId(context, tableOrNamespaceName)));
        case BULK_CONCURRENT_MERGE:
          throw new AccumuloBulkMergeException(e);
        default:
          throw new AccumuloException(e.description, e);
      }
    } catch (Exception e) {
      throw new AccumuloException(e.getMessage(), e);
    } finally {
      Tables.clearCache(context);
      // always finish table op, even when exception
      if (opid != null)
        try {
          finishFateOperation(opid);
        } catch (Exception e) {
          log.warn("Exception thrown while finishing fate table operation", e);
        }
    }
  }

  private static class SplitEnv {
    private String tableName;
    private TableId tableId;
    private ExecutorService executor;
    private CountDownLatch latch;
    private AtomicReference<Exception> exception;

    SplitEnv(String tableName, TableId tableId, ExecutorService executor, CountDownLatch latch,
        AtomicReference<Exception> exception) {
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
          addSplits(env.tableName, new TreeSet<>(splits), env.tableId);
          splits.forEach(s -> env.latch.countDown());
          return;
        }

        int mid = splits.size() / 2;

        // split the middle split point to ensure that child task split different tablets and can
        // therefore
        // run in parallel
        addSplits(env.tableName, new TreeSet<>(splits.subList(mid, mid + 1)), env.tableId);
        env.latch.countDown();

        env.executor.execute(new SplitTask(env, splits.subList(0, mid)));
        env.executor.execute(new SplitTask(env, splits.subList(mid + 1, splits.size())));

      } catch (Exception t) {
        env.exception.compareAndSet(null, t);
      }
    }

  }

  @Override
  public void addSplits(String tableName, SortedSet<Text> partitionKeys)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    TableId tableId = Tables.getTableId(context, tableName);

    List<Text> splits = new ArrayList<>(partitionKeys);
    // should be sorted because we copied from a sorted set, but that makes assumptions about
    // how the copy was done so resort to be sure.
    Collections.sort(splits);

    CountDownLatch latch = new CountDownLatch(splits.size());
    AtomicReference<Exception> exception = new AtomicReference<>(null);

    ExecutorService executor = ThreadPools.createFixedThreadPool(16, "addSplits", false);
    try {
      executor.execute(
          new SplitTask(new SplitEnv(tableName, tableId, executor, latch, exception), splits));

      while (!latch.await(100, TimeUnit.MILLISECONDS)) {
        if (exception.get() != null) {
          executor.shutdownNow();
          Throwable excep = exception.get();
          // Below all exceptions are wrapped and rethrown. This is done so that the user knows what
          // code path got them here. If the wrapping was not done, the
          // user would only have the stack trace for the background thread.
          if (excep instanceof TableNotFoundException) {
            TableNotFoundException tnfe = (TableNotFoundException) excep;
            throw new TableNotFoundException(tableId.canonical(), tableName,
                "Table not found by background thread", tnfe);
          } else if (excep instanceof TableOfflineException) {
            log.debug("TableOfflineException occurred in background thread. Throwing new exception",
                excep);
            throw new TableOfflineException(Tables.getTableOfflineMsg(context, tableId));
          } else if (excep instanceof AccumuloSecurityException) {
            // base == background accumulo security exception
            AccumuloSecurityException base = (AccumuloSecurityException) excep;
            throw new AccumuloSecurityException(base.getUser(), base.asThriftException().getCode(),
                base.getTableInfo(), excep);
          } else if (excep instanceof AccumuloServerException) {
            throw new AccumuloServerException((AccumuloServerException) excep);
          } else if (excep instanceof Error) {
            throw new Error(excep);
          } else {
            throw new AccumuloException(excep);
          }
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      executor.shutdown();
    }
  }

  private void addSplits(String tableName, SortedSet<Text> partitionKeys, TableId tableId)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      AccumuloServerException {
    TabletLocator tabLocator = TabletLocator.getLocator(context, tableId);

    for (Text split : partitionKeys) {
      boolean successful = false;
      int attempt = 0;
      long locationFailures = 0;

      while (!successful) {

        if (attempt > 0)
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

        attempt++;

        TabletLocation tl = tabLocator.locateTablet(context, split, false, false);

        if (tl == null) {
          if (!Tables.exists(context, tableId))
            throw new TableNotFoundException(tableId.canonical(), tableName, null);
          else if (Tables.getTableState(context, tableId) == TableState.OFFLINE)
            throw new TableOfflineException(Tables.getTableOfflineMsg(context, tableId));
          continue;
        }

        HostAndPort address = HostAndPort.fromString(tl.tablet_location);

        try {
          TabletClientService.Client client = ThriftUtil.getTServerClient(address, context);
          try {

            OpTimer timer = null;

            if (log.isTraceEnabled()) {
              log.trace("tid={} Splitting tablet {} on {} at {}", Thread.currentThread().getId(),
                  tl.tablet_extent, address, split);
              timer = new OpTimer().start();
            }

            client.splitTablet(TraceUtil.traceInfo(), context.rpcCreds(),
                tl.tablet_extent.toThrift(), TextUtil.getByteBuffer(split));

            // just split it, might as well invalidate it in the cache
            tabLocator.invalidateCache(tl.tablet_extent);

            if (timer != null) {
              timer.stop();
              log.trace("Split tablet in {}",
                  String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
            }

          } finally {
            ThriftUtil.returnClient(client);
          }

        } catch (TApplicationException tae) {
          throw new AccumuloServerException(address.toString(), tae);
        } catch (TTransportException e) {
          tabLocator.invalidateCache(context, tl.tablet_location);
          continue;
        } catch (ThriftSecurityException e) {
          Tables.clearCache(context);
          if (!Tables.exists(context, tableId))
            throw new TableNotFoundException(tableId.canonical(), tableName, null);
          throw new AccumuloSecurityException(e.user, e.code, e);
        } catch (NotServingTabletException e) {
          // Do not silently spin when we repeatedly fail to get the location for a tablet
          locationFailures++;
          if (locationFailures == 5 || locationFailures % 50 == 0) {
            log.warn("Having difficulty locating hosting tabletserver for split {} on table {}."
                + " Seen {} failures.", split, tableName, locationFailures);
          }

          tabLocator.invalidateCache(tl.tablet_extent);
          continue;
        } catch (TException e) {
          tabLocator.invalidateCache(context, tl.tablet_location);
          continue;
        }

        successful = true;
      }
    }
  }

  @Override
  public void merge(String tableName, Text start, Text end)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    checkArgument(tableName != null, "tableName is null");
    ByteBuffer EMPTY = ByteBuffer.allocate(0);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)),
        start == null ? EMPTY : TextUtil.getByteBuffer(start),
        end == null ? EMPTY : TextUtil.getByteBuffer(end));
    Map<String,String> opts = new HashMap<>();
    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_MERGE, args,
          opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  @Override
  public void deleteRows(String tableName, Text start, Text end)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    checkArgument(tableName != null, "tableName is null");
    ByteBuffer EMPTY = ByteBuffer.allocate(0);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)),
        start == null ? EMPTY : TextUtil.getByteBuffer(start),
        end == null ? EMPTY : TextUtil.getByteBuffer(end));
    Map<String,String> opts = new HashMap<>();
    try {
      doTableFateOperation(tableName, TableNotFoundException.class,
          FateOperation.TABLE_DELETE_RANGE, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  @Override
  public Collection<Text> listSplits(String tableName)
      throws TableNotFoundException, AccumuloSecurityException {
    return _listSplits(tableName);
  }

  private List<Text> _listSplits(String tableName)
      throws TableNotFoundException, AccumuloSecurityException {
    checkArgument(tableName != null, "tableName is null");

    TableId tableId = Tables.getTableId(context, tableName);

    TreeMap<KeyExtent,String> tabletLocations = new TreeMap<>();

    while (true) {
      try {
        tabletLocations.clear();
        // the following method throws AccumuloException for some conditions that should be retried
        MetadataServicer.forTableId(context, tableId).getTabletLocations(tabletLocations);
        break;
      } catch (AccumuloSecurityException ase) {
        throw ase;
      } catch (Exception e) {
        if (!Tables.exists(context, tableId)) {
          throw new TableNotFoundException(tableId.canonical(), tableName, null);
        }

        if (e instanceof RuntimeException && e.getCause() instanceof AccumuloSecurityException) {
          throw (AccumuloSecurityException) e.getCause();
        }

        log.info("{} ... retrying ...", e, e);
        sleepUninterruptibly(3, TimeUnit.SECONDS);
      }
    }

    ArrayList<Text> endRows = new ArrayList<>(tabletLocations.size());

    for (KeyExtent ke : tabletLocations.keySet())
      if (ke.endRow() != null)
        endRows.add(ke.endRow());

    return endRows;

  }

  @Override
  public Collection<Text> listSplits(String tableName, int maxSplits)
      throws TableNotFoundException, AccumuloSecurityException {
    List<Text> endRows = _listSplits(tableName);

    if (endRows.size() <= maxSplits)
      return endRows;

    double r = (maxSplits + 1) / (double) (endRows.size());
    double pos = 0;

    ArrayList<Text> subset = new ArrayList<>(maxSplits);

    int j = 0;
    for (int i = 0; i < endRows.size() && j < maxSplits; i++) {
      pos += r;
      while (pos > 1) {
        subset.add(endRows.get(i));
        j++;
        pos -= 1;
      }
    }

    return subset;
  }

  @Override
  public void delete(String tableName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_DELETE,
          args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

  }

  @Override
  public void clone(String srcTableName, String newTableName, boolean flush,
      Map<String,String> propertiesToSet, Set<String> propertiesToExclude)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      TableExistsException {
    clone(srcTableName, newTableName,
        CloneConfiguration.builder().setFlush(flush).setPropertiesToSet(propertiesToSet)
            .setPropertiesToExclude(propertiesToExclude).setKeepOffline(false).build());
  }

  @Override
  public void clone(String srcTableName, String newTableName, CloneConfiguration config)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      TableExistsException {

    checkArgument(srcTableName != null, "srcTableName is null");
    checkArgument(newTableName != null, "newTableName is null");
    checkArgument(newTableName.length() <= MAX_TABLE_NAME_LEN,
        "Table name is longer than " + MAX_TABLE_NAME_LEN + " characters");

    TableId srcTableId = Tables.getTableId(context, srcTableName);

    if (config.isFlush())
      _flush(srcTableId, null, null, true);

    Set<String> propertiesToExclude = config.getPropertiesToExclude();
    if (propertiesToExclude == null)
      propertiesToExclude = Collections.emptySet();

    Map<String,String> propertiesToSet = config.getPropertiesToSet();
    if (propertiesToSet == null)
      propertiesToSet = Collections.emptyMap();

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(srcTableId.canonical().getBytes(UTF_8)),
        ByteBuffer.wrap(newTableName.getBytes(UTF_8)),
        ByteBuffer.wrap(Boolean.toString(config.isKeepOffline()).getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();
    for (Entry<String,String> entry : propertiesToSet.entrySet()) {
      if (entry.getKey().startsWith(CLONE_EXCLUDE_PREFIX))
        throw new IllegalArgumentException("Property can not start with " + CLONE_EXCLUDE_PREFIX);
      opts.put(entry.getKey(), entry.getValue());
    }

    for (String prop : propertiesToExclude) {
      opts.put(CLONE_EXCLUDE_PREFIX + prop, "");
    }

    doTableFateOperation(newTableName, AccumuloException.class, FateOperation.TABLE_CLONE, args,
        opts);
  }

  @Override
  public void rename(String oldTableName, String newTableName) throws AccumuloSecurityException,
      TableNotFoundException, AccumuloException, TableExistsException {
    checkArgument(newTableName.length() <= MAX_TABLE_NAME_LEN,
        "Table name is longer than " + MAX_TABLE_NAME_LEN + " characters");
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(oldTableName.getBytes(UTF_8)),
        ByteBuffer.wrap(newTableName.getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();
    doTableFateOperation(oldTableName, TableNotFoundException.class, FateOperation.TABLE_RENAME,
        args, opts);
  }

  @Override
  public void flush(String tableName) throws AccumuloException, AccumuloSecurityException {
    try {
      flush(tableName, null, null, false);
    } catch (TableNotFoundException e) {
      throw new AccumuloException(e.getMessage(), e);
    }
  }

  @Override
  public void flush(String tableName, Text start, Text end, boolean wait)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");

    TableId tableId = Tables.getTableId(context, tableName);
    _flush(tableId, start, end, wait);
  }

  @Override
  public void compact(String tableName, Text start, Text end, boolean flush, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    compact(tableName, start, end, new ArrayList<>(), flush, wait);
  }

  @Override
  public void compact(String tableName, Text start, Text end, List<IteratorSetting> iterators,
      boolean flush, boolean wait)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    compact(tableName, new CompactionConfig().setStartRow(start).setEndRow(end)
        .setIterators(iterators).setFlush(flush).setWait(wait));
  }

  @Override
  public void compact(String tableName, CompactionConfig config)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    checkArgument(tableName != null, "tableName is null");

    // Ensure compaction iterators exist on a tabletserver
    final String skviName = SortedKeyValueIterator.class.getName();
    for (IteratorSetting setting : config.getIterators()) {
      String iteratorClass = setting.getIteratorClass();
      if (!testClassLoad(tableName, iteratorClass, skviName)) {
        throw new AccumuloException("TabletServer could not load iterator class " + iteratorClass);
      }
    }

    ensureStrategyCanLoad(tableName, config);

    if (!UserCompactionUtils.isDefault(config.getConfigurer())) {
      if (!testClassLoad(tableName, config.getConfigurer().getClassName(),
          CompactionConfigurer.class.getName())) {
        throw new AccumuloException(
            "TabletServer could not load " + CompactionConfigurer.class.getSimpleName() + " class "
                + config.getConfigurer().getClassName());
      }
    }

    if (!UserCompactionUtils.isDefault(config.getSelector())) {
      if (!testClassLoad(tableName, config.getSelector().getClassName(),
          CompactionSelector.class.getName())) {
        throw new AccumuloException(
            "TabletServer could not load " + CompactionSelector.class.getSimpleName() + " class "
                + config.getSelector().getClassName());
      }
    }

    TableId tableId = Tables.getTableId(context, tableName);

    Text start = config.getStartRow();
    Text end = config.getEndRow();

    if (config.getFlush())
      _flush(tableId, start, end, true);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.canonical().getBytes(UTF_8)),
        ByteBuffer.wrap(UserCompactionUtils.encode(config)));

    Map<String,String> opts = new HashMap<>();
    try {
      doFateOperation(FateOperation.TABLE_COMPACT, args, opts, tableName, config.getWait());
    } catch (TableExistsException | NamespaceExistsException e) {
      // should not happen
      throw new AssertionError(e);
    } catch (NamespaceNotFoundException e) {
      throw new TableNotFoundException(null, tableName, "Namespace not found", e);
    }
  }

  @SuppressWarnings("removal")
  private void ensureStrategyCanLoad(String tableName, CompactionConfig config)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    // Make sure the specified compaction strategy exists on a tabletserver
    if (!CompactionStrategyConfigUtil.isDefault(config.getCompactionStrategy())) {
      if (!testClassLoad(tableName, config.getCompactionStrategy().getClassName(),
          "org.apache.accumulo.tserver.compaction.CompactionStrategy")) {
        throw new AccumuloException("TabletServer could not load CompactionStrategy class "
            + config.getCompactionStrategy().getClassName());
      }
    }
  }

  @Override
  public void cancelCompaction(String tableName)
      throws AccumuloSecurityException, TableNotFoundException, AccumuloException {
    TableId tableId = Tables.getTableId(context, tableName);

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.canonical().getBytes(UTF_8)));

    Map<String,String> opts = new HashMap<>();
    try {
      doTableFateOperation(tableName, TableNotFoundException.class,
          FateOperation.TABLE_CANCEL_COMPACT, args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

  }

  private void _flush(TableId tableId, Text start, Text end, boolean wait)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    try {
      long flushID;

      // used to pass the table name. but the tableid associated with a table name could change
      // between calls.
      // so pass the tableid to both calls

      while (true) {
        ManagerClientService.Iface client = null;
        try {
          client = ManagerClient.getConnectionWithRetry(context);
          flushID =
              client.initiateFlush(TraceUtil.traceInfo(), context.rpcCreds(), tableId.canonical());
          break;
        } catch (TTransportException tte) {
          log.debug("Failed to call initiateFlush, retrying ... ", tte);
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } catch (ThriftNotActiveServiceException e) {
          // Let it loop, fetching a new location
          log.debug("Contacted a Manager which is no longer active, retrying");
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } finally {
          ManagerClient.close(client);
        }
      }

      while (true) {
        ManagerClientService.Iface client = null;
        try {
          client = ManagerClient.getConnectionWithRetry(context);
          client.waitForFlush(TraceUtil.traceInfo(), context.rpcCreds(), tableId.canonical(),
              TextUtil.getByteBuffer(start), TextUtil.getByteBuffer(end), flushID,
              wait ? Long.MAX_VALUE : 1);
          break;
        } catch (TTransportException tte) {
          log.debug("Failed to call initiateFlush, retrying ... ", tte);
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } catch (ThriftNotActiveServiceException e) {
          // Let it loop, fetching a new location
          log.debug("Contacted a Manager which is no longer active, retrying");
          sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } finally {
          ManagerClient.close(client);
        }
      }
    } catch (ThriftSecurityException e) {
      switch (e.getCode()) {
        case TABLE_DOESNT_EXIST:
          throw new TableNotFoundException(tableId.canonical(), null, e.getMessage(), e);
        default:
          log.debug("flush security exception on table id {}", tableId);
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

  @Override
  public void setProperty(final String tableName, final String property, final String value)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(property != null, "property is null");
    checkArgument(value != null, "value is null");
    try {
      setPropertyNoChecks(tableName, property, value);

      checkLocalityGroups(tableName, property);
    } catch (TableNotFoundException e) {
      throw new AccumuloException(e);
    }
  }

  private void setPropertyNoChecks(final String tableName, final String property,
      final String value)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ManagerClient.executeTable(context, client -> client.setTableProperty(TraceUtil.traceInfo(),
        context.rpcCreds(), tableName, property, value));
  }

  @Override
  public void removeProperty(final String tableName, final String property)
      throws AccumuloException, AccumuloSecurityException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(property != null, "property is null");
    try {
      removePropertyNoChecks(tableName, property);

      checkLocalityGroups(tableName, property);
    } catch (TableNotFoundException e) {
      throw new AccumuloException(e);
    }
  }

  private void removePropertyNoChecks(final String tableName, final String property)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    ManagerClient.executeTable(context, client -> client.removeTableProperty(TraceUtil.traceInfo(),
        context.rpcCreds(), tableName, property));
  }

  void checkLocalityGroups(String tableName, String propChanged)
      throws AccumuloException, TableNotFoundException {
    if (LocalityGroupUtil.isLocalityGroupProperty(propChanged)) {
      Iterable<Entry<String,String>> allProps = getProperties(tableName);
      try {
        LocalityGroupUtil.checkLocalityGroups(allProps);
      } catch (LocalityGroupConfigurationError | RuntimeException e) {
        LoggerFactory.getLogger(this.getClass()).warn("Changing '" + propChanged + "' for table '"
            + tableName
            + "' resulted in bad locality group config.  This may be a transient situation since "
            + "the config spreads over multiple properties.  Setting properties in a different "
            + "order may help.  Even though this warning was displayed, the property was updated. "
            + "Please check your config to ensure consistency.", e);
      }
    }
  }

  @Override
  public Iterable<Entry<String,String>> getProperties(final String tableName)
      throws AccumuloException, TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    try {
      return ServerClient.executeRaw(context, client -> client
          .getTableConfiguration(TraceUtil.traceInfo(), context.rpcCreds(), tableName)).entrySet();
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

  @Override
  public void setLocalityGroups(String tableName, Map<String,Set<Text>> groups)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    // ensure locality groups do not overlap
    LocalityGroupUtil.ensureNonOverlappingGroups(groups);

    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      Set<Text> colFams = entry.getValue();
      String value = LocalityGroupUtil.encodeColumnFamilies(colFams);
      setPropertyNoChecks(tableName, Property.TABLE_LOCALITY_GROUP_PREFIX + entry.getKey(), value);
    }

    try {
      setPropertyNoChecks(tableName, Property.TABLE_LOCALITY_GROUPS.getKey(),
          Joiner.on(",").join(groups.keySet()));
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
          removePropertyNoChecks(tableName, property);
        }
      }
    }
  }

  @Override
  public Map<String,Set<Text>> getLocalityGroups(String tableName)
      throws AccumuloException, TableNotFoundException {
    AccumuloConfiguration conf = new ConfigurationCopy(this.getProperties(tableName));
    Map<String,Set<ByteSequence>> groups = LocalityGroupUtil.getLocalityGroups(conf);

    Map<String,Set<Text>> groups2 = new HashMap<>();
    for (Entry<String,Set<ByteSequence>> entry : groups.entrySet()) {

      HashSet<Text> colFams = new HashSet<>();

      for (ByteSequence bs : entry.getValue()) {
        colFams.add(new Text(bs.toArray()));
      }

      groups2.put(entry.getKey(), colFams);
    }

    return groups2;
  }

  @Override
  public Set<Range> splitRangeByTablets(String tableName, Range range, int maxSplits)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(range != null, "range is null");
    if (maxSplits < 1)
      throw new IllegalArgumentException("maximum splits must be >= 1");
    if (maxSplits == 1)
      return Collections.singleton(range);

    Random random = new SecureRandom();
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();
    TableId tableId = Tables.getTableId(context, tableName);
    TabletLocator tl = TabletLocator.getLocator(context, tableId);
    // its possible that the cache could contain complete, but old information about a tables
    // tablets... so clear it
    tl.invalidateCache();
    while (!tl.binRanges(context, Collections.singletonList(range), binnedRanges).isEmpty()) {
      if (!Tables.exists(context, tableId))
        throw new TableDeletedException(tableId.canonical());
      if (Tables.getTableState(context, tableId) == TableState.OFFLINE)
        throw new TableOfflineException(Tables.getTableOfflineMsg(context, tableId));

      log.warn("Unable to locate bins for specified range. Retrying.");
      // sleep randomly between 100 and 200ms
      sleepUninterruptibly(100 + random.nextInt(100), TimeUnit.MILLISECONDS);
      binnedRanges.clear();
      tl.invalidateCache();
    }

    // group key extents to get <= maxSplits
    LinkedList<KeyExtent> unmergedExtents = new LinkedList<>();
    List<KeyExtent> mergedExtents = new ArrayList<>();

    for (Map<KeyExtent,List<Range>> map : binnedRanges.values())
      unmergedExtents.addAll(map.keySet());

    // the sort method is efficient for linked list
    Collections.sort(unmergedExtents);

    while (unmergedExtents.size() + mergedExtents.size() > maxSplits) {
      if (unmergedExtents.size() >= 2) {
        KeyExtent first = unmergedExtents.removeFirst();
        KeyExtent second = unmergedExtents.removeFirst();
        KeyExtent merged = new KeyExtent(first.tableId(), second.endRow(), first.prevEndRow());
        mergedExtents.add(merged);
      } else {
        mergedExtents.addAll(unmergedExtents);
        unmergedExtents.clear();
        unmergedExtents.addAll(mergedExtents);
        mergedExtents.clear();
      }

    }

    mergedExtents.addAll(unmergedExtents);

    Set<Range> ranges = new HashSet<>();
    for (KeyExtent k : mergedExtents)
      ranges.add(k.toDataRange().clip(range));

    return ranges;
  }

  private Path checkPath(String dir, String kind, String type)
      throws IOException, AccumuloException, AccumuloSecurityException {
    FileSystem fs = VolumeConfiguration.fileSystemForPath(dir, context.getHadoopConf());
    Path ret = dir.contains(":") ? new Path(dir) : fs.makeQualified(new Path(dir));

    try {
      if (!fs.getFileStatus(ret).isDirectory()) {
        throw new AccumuloException(
            kind + " import " + type + " directory " + ret + " is not a directory!");
      }
    } catch (FileNotFoundException fnf) {
      throw new AccumuloException(
          kind + " import " + type + " directory " + ret + " does not exist!");
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
  @Deprecated(since = "2.0.0")
  public void importDirectory(String tableName, String dir, String failureDir, boolean setTime)
      throws IOException, AccumuloSecurityException, TableNotFoundException, AccumuloException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(dir != null, "dir is null");
    checkArgument(failureDir != null, "failureDir is null");
    // check for table existence
    Tables.getTableId(context, tableName);

    Path dirPath = checkPath(dir, "Bulk", "");
    Path failPath = checkPath(failureDir, "Bulk", "failure");

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)),
        ByteBuffer.wrap(dirPath.toString().getBytes(UTF_8)),
        ByteBuffer.wrap(failPath.toString().getBytes(UTF_8)),
        ByteBuffer.wrap((setTime + "").getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_BULK_IMPORT,
          args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  private void waitForTableStateTransition(TableId tableId, TableState expectedState)
      throws AccumuloException, TableNotFoundException {

    Text startRow = null;
    Text lastRow = null;

    while (true) {

      if (Tables.getTableState(context, tableId) != expectedState) {
        Tables.clearCache(context);
        TableState currentState = Tables.getTableState(context, tableId);
        if (currentState != expectedState) {
          if (!Tables.exists(context, tableId))
            throw new TableDeletedException(tableId.canonical());
          if (currentState == TableState.DELETING)
            throw new TableNotFoundException(tableId.canonical(), "", "Table is being deleted.");
          throw new AccumuloException("Unexpected table state " + tableId + " "
              + Tables.getTableState(context, tableId) + " != " + expectedState);
        }
      }

      Range range;
      if (startRow == null || lastRow == null)
        range = new KeyExtent(tableId, null, null).toMetaRange();
      else
        range = new Range(startRow, lastRow);

      TabletsMetadata tablets = TabletsMetadata.builder(context).scanMetadataTable()
          .overRange(range).fetch(LOCATION, PREV_ROW).build();

      KeyExtent lastExtent = null;

      int total = 0;
      int waitFor = 0;
      int holes = 0;
      Text continueRow = null;
      MapCounter<String> serverCounts = new MapCounter<>();

      for (TabletMetadata tablet : tablets) {
        total++;

        Location loc = tablet.getLocation();

        if ((expectedState == TableState.ONLINE
            && (loc == null || loc.getType() == LocationType.FUTURE))
            || (expectedState == TableState.OFFLINE && loc != null)) {
          if (continueRow == null)
            continueRow = tablet.getExtent().toMetaRow();
          waitFor++;
          lastRow = tablet.getExtent().toMetaRow();

          if (loc != null) {
            serverCounts.increment(loc.getHostPortSession(), 1);
          }
        }

        if (!tablet.getExtent().tableId().equals(tableId)) {
          throw new AccumuloException(
              "Saw unexpected table Id " + tableId + " " + tablet.getExtent());
        }

        if (lastExtent != null && !tablet.getExtent().isPreviousExtent(lastExtent)) {
          holes++;
        }

        lastExtent = tablet.getExtent();
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
          maxPerServer = serverCounts.max();
          waitTime = maxPerServer * 10;
        } else
          waitTime = waitFor * 10L;
        waitTime = Math.max(100, waitTime);
        waitTime = Math.min(5000, waitTime);
        log.trace("Waiting for {}({}) tablets, startRow = {} lastRow = {}, holes={} sleeping:{}ms",
            waitFor, maxPerServer, startRow, lastRow, holes, waitTime);
        sleepUninterruptibly(waitTime, TimeUnit.MILLISECONDS);
      } else {
        break;
      }

    }
  }

  @Override
  public void offline(String tableName)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    offline(tableName, false);
  }

  @Override
  public void offline(String tableName, boolean wait)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {

    checkArgument(tableName != null, "tableName is null");
    TableId tableId = Tables.getTableId(context, tableName);
    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.canonical().getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_OFFLINE,
          args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

    if (wait)
      waitForTableStateTransition(tableId, TableState.OFFLINE);
  }

  @Override
  public boolean isOnline(String tableName) throws AccumuloException, TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");

    TableId tableId = Tables.getTableId(context, tableName);

    TableState expectedState = Tables.getTableState(context, tableId, true);
    return expectedState == TableState.ONLINE;
  }

  @Override
  public void online(String tableName)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    online(tableName, false);
  }

  @Override
  public void online(String tableName, boolean wait)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");

    TableId tableId = Tables.getTableId(context, tableName);

    /**
     * ACCUMULO-4574 if table is already online return without executing fate operation.
     */

    if (isOnline(tableName)) {
      if (wait)
        waitForTableStateTransition(tableId, TableState.ONLINE);
      return;
    }

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableId.canonical().getBytes(UTF_8)));
    Map<String,String> opts = new HashMap<>();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_ONLINE,
          args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }

    if (wait)
      waitForTableStateTransition(tableId, TableState.ONLINE);
  }

  @Override
  public void clearLocatorCache(String tableName) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    TabletLocator tabLocator =
        TabletLocator.getLocator(context, Tables.getTableId(context, tableName));
    tabLocator.invalidateCache();
  }

  @Override
  public Map<String,String> tableIdMap() {
    return Tables.getNameToIdMap(context).entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().canonical(), (v1, v2) -> {
          throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2));
        }, TreeMap::new));
  }

  @Override
  public Text getMaxRow(String tableName, Authorizations auths, Text startRow,
      boolean startInclusive, Text endRow, boolean endInclusive) throws TableNotFoundException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(auths != null, "auths is null");
    Scanner scanner = context.createScanner(tableName, auths);
    return FindMax.findMax(scanner, startRow, startInclusive, endRow, endInclusive);
  }

  @Override
  public List<DiskUsage> getDiskUsage(Set<String> tableNames)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    List<TDiskUsage> diskUsages = null;
    while (diskUsages == null) {
      Pair<String,Client> pair = null;
      try {
        // this operation may us a lot of memory... its likely that connections to tabletservers
        // hosting metadata tablets will be cached, so do not use cached
        // connections
        pair = ServerClient.getConnection(context, new ClientService.Client.Factory(), false);
        diskUsages = pair.getSecond().getDiskUsage(tableNames, context.rpcCreds());
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
          log.debug("Disk usage request failed {}, retrying ... ", pair.getFirst(), e);
        }
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } catch (TException e) {
        // may be a TApplicationException which indicates error on the server side
        throw new AccumuloException(e);
      } finally {
        // must always return thrift connection
        if (pair != null)
          ServerClient.close(pair.getSecond());
      }
    }

    List<DiskUsage> finalUsages = new ArrayList<>();
    for (TDiskUsage diskUsage : diskUsages) {
      finalUsages.add(new DiskUsage(new TreeSet<>(diskUsage.getTables()), diskUsage.getUsage()));
    }

    return finalUsages;
  }

  /**
   * Search multiple directories for exportMetadata.zip, the control file used for the importable
   * command.
   *
   * @param context
   *          used to obtain filesystem based on configuration
   * @param importDirs
   *          the set of directories to search.
   * @return the Path representing the location of the file.
   * @throws AccumuloException
   *           if zero or more than one copy of the exportMetadata.zip file are found in the
   *           directories provided.
   */
  public static Path findExportFile(ClientContext context, Set<String> importDirs)
      throws AccumuloException {
    LinkedHashSet<Path> exportFiles = new LinkedHashSet<>();
    for (String importDir : importDirs) {
      Path exportFilePath = null;
      try {
        FileSystem fs = new Path(importDir).getFileSystem(context.getHadoopConf());
        exportFilePath = new Path(importDir, Constants.EXPORT_FILE);
        log.debug("Looking for export metadata in {}", exportFilePath);
        if (fs.exists(exportFilePath)) {
          log.debug("Found export metadata in {}", exportFilePath);
          exportFiles.add(exportFilePath);
        }
      } catch (IOException ioe) {
        log.warn("Non-Fatal IOException reading export file: {}", exportFilePath, ioe);
      }
    }

    if (exportFiles.size() > 1) {
      String fileList = Arrays.toString(exportFiles.toArray());
      log.warn("Found multiple export metadata files: " + fileList);
      throw new AccumuloException("Found multiple export metadata files: " + fileList);
    } else if (exportFiles.isEmpty()) {
      log.warn("Unable to locate export metadata");
      throw new AccumuloException("Unable to locate export metadata");
    }

    return exportFiles.iterator().next();
  }

  public static Map<String,String> getExportedProps(FileSystem fs, Path path) throws IOException {
    HashMap<String,String> props = new HashMap<>();

    try (ZipInputStream zis = new ZipInputStream(fs.open(path))) {
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().equals(Constants.EXPORT_TABLE_CONFIG_FILE)) {
          try (BufferedReader in = new BufferedReader(new InputStreamReader(zis, UTF_8))) {
            String line;
            while ((line = in.readLine()) != null) {
              String[] sa = line.split("=", 2);
              props.put(sa[0], sa[1]);
            }
          }

          break;
        }
      }
    }
    return props;
  }

  @Override
  public void importTable(String tableName, Set<String> importDirs)
      throws TableExistsException, AccumuloException, AccumuloSecurityException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(importDirs != null, "importDir is null");
    checkArgument(tableName.length() <= MAX_TABLE_NAME_LEN,
        "Table name is longer than " + MAX_TABLE_NAME_LEN + " characters");

    Set<String> checkedImportDirs = new HashSet<>();
    try {
      for (String s : importDirs) {
        checkedImportDirs.add(checkPath(s, "Table", "").toString());
      }
    } catch (IOException e) {
      throw new AccumuloException(e);
    }

    try {
      Path exportFilePath = findExportFile(context, checkedImportDirs);
      FileSystem fs = exportFilePath.getFileSystem(context.getHadoopConf());
      Map<String,String> props = getExportedProps(fs, exportFilePath);

      for (Entry<String,String> entry : props.entrySet()) {
        if (Property.isClassProperty(entry.getKey())
            && !entry.getValue().contains(Constants.CORE_PACKAGE_NAME)) {
          LoggerFactory.getLogger(this.getClass()).info(
              "Imported table sets '{}' to '{}'.  Ensure this class is on Accumulo classpath.",
              sanitize(entry.getKey()), sanitize(entry.getValue()));
        }
      }
    } catch (IOException ioe) {
      LoggerFactory.getLogger(this.getClass()).warn(
          "Failed to check if imported table references external java classes : {}",
          ioe.getMessage());
    }

    Stream<String> argStream = Stream.concat(Stream.of(tableName), checkedImportDirs.stream());
    List<ByteBuffer> args =
        argStream.map(String::getBytes).map(ByteBuffer::wrap).collect(Collectors.toList());

    Map<String,String> opts = Collections.emptyMap();

    try {
      doTableFateOperation(tableName, AccumuloException.class, FateOperation.TABLE_IMPORT, args,
          opts);
    } catch (TableNotFoundException e) {
      // should not happen
      throw new AssertionError(e);
    }

  }

  /**
   * Prevent potential CRLF injection into logs from read in user data See
   * https://find-sec-bugs.github.io/bugs.htm#CRLF_INJECTION_LOGS
   */
  private String sanitize(String msg) {
    return msg.replaceAll("[\r\n]", "");
  }

  @Override
  public void exportTable(String tableName, String exportDir)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(exportDir != null, "exportDir is null");

    List<ByteBuffer> args = Arrays.asList(ByteBuffer.wrap(tableName.getBytes(UTF_8)),
        ByteBuffer.wrap(exportDir.getBytes(UTF_8)));

    Map<String,String> opts = Collections.emptyMap();

    try {
      doTableFateOperation(tableName, TableNotFoundException.class, FateOperation.TABLE_EXPORT,
          args, opts);
    } catch (TableExistsException e) {
      // should not happen
      throw new AssertionError(e);
    }
  }

  @Override
  public boolean testClassLoad(final String tableName, final String className,
      final String asTypeName)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    checkArgument(tableName != null, "tableName is null");
    checkArgument(className != null, "className is null");
    checkArgument(asTypeName != null, "asTypeName is null");

    try {
      return ServerClient.executeRaw(context,
          client -> client.checkTableClass(TraceUtil.traceInfo(), context.rpcCreds(), tableName,
              className, asTypeName));
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
  public void attachIterator(String tableName, IteratorSetting setting,
      EnumSet<IteratorScope> scopes)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    testClassLoad(tableName, setting.getIteratorClass(), SortedKeyValueIterator.class.getName());
    super.attachIterator(tableName, setting, scopes);
  }

  @Override
  public int addConstraint(String tableName, String constraintClassName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    testClassLoad(tableName, constraintClassName, Constraint.class.getName());
    return super.addConstraint(tableName, constraintClassName);
  }

  private void doTableFateOperation(String tableOrNamespaceName,
      Class<? extends Exception> namespaceNotFoundExceptionClass, FateOperation op,
      List<ByteBuffer> args, Map<String,String> opts) throws AccumuloSecurityException,
      AccumuloException, TableExistsException, TableNotFoundException {
    try {
      doFateOperation(op, args, opts, tableOrNamespaceName);
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
        throw new TableNotFoundException(null, tableOrNamespaceName, "Namespace not found", e);
      } else {
        // should not happen
        throw new AssertionError(e);
      }
    }
  }

  private void clearSamplerOptions(String tableName)
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    String prefix = Property.TABLE_SAMPLER_OPTS.getKey();
    for (Entry<String,String> entry : getProperties(tableName)) {
      String property = entry.getKey();
      if (property.startsWith(prefix)) {
        removeProperty(tableName, property);
      }
    }
  }

  @Override
  public void setSamplerConfiguration(String tableName, SamplerConfiguration samplerConfiguration)
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    clearSamplerOptions(tableName);

    List<Pair<String,String>> props =
        new SamplerConfigurationImpl(samplerConfiguration).toTableProperties();
    for (Pair<String,String> pair : props) {
      setProperty(tableName, pair.getFirst(), pair.getSecond());
    }
  }

  @Override
  public void clearSamplerConfiguration(String tableName)
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    removeProperty(tableName, Property.TABLE_SAMPLER.getKey());
    clearSamplerOptions(tableName);
  }

  @Override
  public SamplerConfiguration getSamplerConfiguration(String tableName)
      throws TableNotFoundException, AccumuloException {
    AccumuloConfiguration conf = new ConfigurationCopy(this.getProperties(tableName));
    SamplerConfigurationImpl sci = SamplerConfigurationImpl.newSamplerConfig(conf);
    if (sci == null) {
      return null;
    }
    return sci.toSamplerConfiguration();
  }

  private static class LocationsImpl implements Locations {

    private Map<Range,List<TabletId>> groupedByRanges;
    private Map<TabletId,List<Range>> groupedByTablets;
    private Map<TabletId,String> tabletLocations;

    public LocationsImpl(Map<String,Map<KeyExtent,List<Range>>> binnedRanges) {
      groupedByTablets = new HashMap<>();
      groupedByRanges = null;
      tabletLocations = new HashMap<>();

      for (Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
        String location = entry.getKey();

        for (Entry<KeyExtent,List<Range>> entry2 : entry.getValue().entrySet()) {
          TabletIdImpl tabletId = new TabletIdImpl(entry2.getKey());
          tabletLocations.put(tabletId, location);
          List<Range> prev =
              groupedByTablets.put(tabletId, Collections.unmodifiableList(entry2.getValue()));
          if (prev != null) {
            throw new RuntimeException(
                "Unexpected : tablet at multiple locations : " + location + " " + tabletId);
          }
        }
      }

      groupedByTablets = Collections.unmodifiableMap(groupedByTablets);
    }

    @Override
    public String getTabletLocation(TabletId tabletId) {
      return tabletLocations.get(tabletId);
    }

    @Override
    public Map<Range,List<TabletId>> groupByRange() {
      if (groupedByRanges == null) {
        Map<Range,List<TabletId>> tmp = new HashMap<>();

        groupedByTablets.forEach((tabletId, rangeList) -> rangeList
            .forEach(range -> tmp.computeIfAbsent(range, k -> new ArrayList<>()).add(tabletId)));

        Map<Range,List<TabletId>> tmp2 = new HashMap<>();
        for (Entry<Range,List<TabletId>> entry : tmp.entrySet()) {
          tmp2.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }

        groupedByRanges = Collections.unmodifiableMap(tmp2);
      }

      return groupedByRanges;
    }

    @Override
    public Map<TabletId,List<Range>> groupByTablet() {
      return groupedByTablets;
    }
  }

  @Override
  public Locations locate(String tableName, Collection<Range> ranges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    requireNonNull(tableName, "tableName must be non null");
    requireNonNull(ranges, "ranges must be non null");

    TableId tableId = Tables.getTableId(context, tableName);
    TabletLocator locator = TabletLocator.getLocator(context, tableId);

    List<Range> rangeList = null;
    if (ranges instanceof List) {
      rangeList = (List<Range>) ranges;
    } else {
      rangeList = new ArrayList<>(ranges);
    }

    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();

    locator.invalidateCache();

    Retry retry = Retry.builder().infiniteRetries().retryAfter(100, MILLISECONDS)
        .incrementBy(100, MILLISECONDS).maxWait(2, SECONDS).backOffFactor(1.5)
        .logInterval(3, TimeUnit.MINUTES).createRetry();

    while (!locator.binRanges(context, rangeList, binnedRanges).isEmpty()) {

      if (!Tables.exists(context, tableId))
        throw new TableNotFoundException(tableId.canonical(), tableName, null);
      if (Tables.getTableState(context, tableId) == TableState.OFFLINE)
        throw new TableOfflineException(Tables.getTableOfflineMsg(context, tableId));

      binnedRanges.clear();

      try {
        retry.waitForNextAttempt();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      locator.invalidateCache();
    }

    return new LocationsImpl(binnedRanges);
  }

  @Override
  public SummaryRetriever summaries(String tableName) {

    return new SummaryRetriever() {

      private Text startRow = null;
      private Text endRow = null;
      private List<TSummarizerConfiguration> summariesToFetch = Collections.emptyList();
      private String summarizerClassRegex;
      private boolean flush = false;

      @Override
      public SummaryRetriever startRow(Text startRow) {
        Objects.requireNonNull(startRow);
        if (endRow != null) {
          Preconditions.checkArgument(startRow.compareTo(endRow) < 0,
              "Start row must be less than end row : %s >= %s", startRow, endRow);
        }
        this.startRow = startRow;
        return this;
      }

      @Override
      public SummaryRetriever startRow(CharSequence startRow) {
        return startRow(new Text(startRow.toString()));
      }

      @Override
      public List<Summary> retrieve()
          throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        TableId tableId = Tables.getTableId(context, tableName);
        if (Tables.getTableState(context, tableId) == TableState.OFFLINE)
          throw new TableOfflineException(Tables.getTableOfflineMsg(context, tableId));

        TRowRange range =
            new TRowRange(TextUtil.getByteBuffer(startRow), TextUtil.getByteBuffer(endRow));
        TSummaryRequest request =
            new TSummaryRequest(tableId.canonical(), range, summariesToFetch, summarizerClassRegex);
        if (flush) {
          _flush(tableId, startRow, endRow, true);
        }

        TSummaries ret =
            ServerClient.execute(context, new TabletClientService.Client.Factory(), client -> {
              TSummaries tsr =
                  client.startGetSummaries(TraceUtil.traceInfo(), context.rpcCreds(), request);
              while (!tsr.finished) {
                tsr = client.contiuneGetSummaries(TraceUtil.traceInfo(), tsr.sessionId);
              }
              return tsr;
            });
        return new SummaryCollection(ret).getSummaries();
      }

      @Override
      public SummaryRetriever endRow(Text endRow) {
        Objects.requireNonNull(endRow);
        if (startRow != null) {
          Preconditions.checkArgument(startRow.compareTo(endRow) < 0,
              "Start row must be less than end row : %s >= %s", startRow, endRow);
        }
        this.endRow = endRow;
        return this;
      }

      @Override
      public SummaryRetriever endRow(CharSequence endRow) {
        return endRow(new Text(endRow.toString()));
      }

      @Override
      public SummaryRetriever withConfiguration(Collection<SummarizerConfiguration> configs) {
        Objects.requireNonNull(configs);
        summariesToFetch = configs.stream().map(SummarizerConfigurationUtil::toThrift)
            .collect(Collectors.toList());
        return this;
      }

      @Override
      public SummaryRetriever withConfiguration(SummarizerConfiguration... config) {
        Objects.requireNonNull(config);
        return withConfiguration(Arrays.asList(config));
      }

      @Override
      public SummaryRetriever withMatchingConfiguration(String regex) {
        Objects.requireNonNull(regex);
        // Do a sanity check here to make sure that regex compiles, instead of having it fail on a
        // tserver.
        Pattern.compile(regex);
        this.summarizerClassRegex = regex;
        return this;
      }

      @Override
      public SummaryRetriever flush(boolean b) {
        this.flush = b;
        return this;
      }
    };
  }

  @Override
  public void addSummarizers(String tableName, SummarizerConfiguration... newConfigs)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    HashSet<SummarizerConfiguration> currentConfigs =
        new HashSet<>(SummarizerConfiguration.fromTableProperties(getProperties(tableName)));
    HashSet<SummarizerConfiguration> newConfigSet = new HashSet<>(Arrays.asList(newConfigs));

    newConfigSet.removeIf(currentConfigs::contains);

    Set<String> newIds =
        newConfigSet.stream().map(SummarizerConfiguration::getPropertyId).collect(toSet());

    for (SummarizerConfiguration csc : currentConfigs) {
      if (newIds.contains(csc.getPropertyId())) {
        throw new IllegalArgumentException("Summarizer property id is in use by " + csc);
      }
    }

    Set<Entry<String,String>> es =
        SummarizerConfiguration.toTableProperties(newConfigSet).entrySet();
    for (Entry<String,String> entry : es) {
      setProperty(tableName, entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void removeSummarizers(String tableName, Predicate<SummarizerConfiguration> predicate)
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    Collection<SummarizerConfiguration> summarizerConfigs =
        SummarizerConfiguration.fromTableProperties(getProperties(tableName));
    for (SummarizerConfiguration sc : summarizerConfigs) {
      if (predicate.test(sc)) {
        Set<String> ks = sc.toTableProperties().keySet();
        for (String key : ks) {
          removeProperty(tableName, key);
        }
      }
    }
  }

  @Override
  public List<SummarizerConfiguration> listSummarizers(String tableName)
      throws AccumuloException, TableNotFoundException {
    return new ArrayList<>(SummarizerConfiguration.fromTableProperties(getProperties(tableName)));
  }

  @Override
  public ImportDestinationArguments importDirectory(String directory) {
    return importDirectory(directory, false);
  }

  @Override
  public ImportDestinationArguments importDirectory(final String directory,
      final boolean ignoreEmptyDir) {
    return new BulkImport(directory, ignoreEmptyDir, context);
  }
}

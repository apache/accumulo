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
package org.apache.accumulo.tserver;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.clientImpl.CompressedIterators;
import org.apache.accumulo.core.clientImpl.DurabilityImpl;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.clientImpl.TabletType;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MapFileInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TCMResult;
import org.apache.accumulo.core.dataImpl.thrift.TCMStatus;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TConditionalMutation;
import org.apache.accumulo.core.dataImpl.thrift.TConditionalSession;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.apache.accumulo.core.dataImpl.thrift.TMutation;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.dataImpl.thrift.TRowRange;
import org.apache.accumulo.core.dataImpl.thrift.TSummaries;
import org.apache.accumulo.core.dataImpl.thrift.TSummaryRequest;
import org.apache.accumulo.core.dataImpl.thrift.UpdateErrors;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.logging.TabletLogger;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.core.summary.Gatherer;
import org.apache.accumulo.core.summary.Gatherer.FileSystemResolver;
import org.apache.accumulo.core.summary.SummaryCollection;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.fate.zookeeper.ServiceLock;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.compaction.CompactionInfo;
import org.apache.accumulo.server.compaction.FileCompactor;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.fs.TooManyFilesException;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.tserver.ConditionCheckerContext.ConditionChecker;
import org.apache.accumulo.tserver.RowLocks.RowLock;
import org.apache.accumulo.tserver.data.ServerConditionalMutation;
import org.apache.accumulo.tserver.scan.LookupTask;
import org.apache.accumulo.tserver.scan.NextBatchTask;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.accumulo.tserver.session.ConditionalSession;
import org.apache.accumulo.tserver.session.MultiScanSession;
import org.apache.accumulo.tserver.session.SingleScanSession;
import org.apache.accumulo.tserver.session.SummarySession;
import org.apache.accumulo.tserver.session.UpdateSession;
import org.apache.accumulo.tserver.tablet.CommitSession;
import org.apache.accumulo.tserver.tablet.KVEntry;
import org.apache.accumulo.tserver.tablet.PreparedMutations;
import org.apache.accumulo.tserver.tablet.ScanBatch;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletClosedException;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.collect.Collections2;

public class ThriftClientHandler extends ClientServiceHandler implements TabletClientService.Iface {

  private static final Logger log = LoggerFactory.getLogger(ThriftClientHandler.class);
  private static final long MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS = 1000;
  private static final long RECENTLY_SPLIT_MILLIES = 60 * 1000;
  private final TabletServer server;
  private final WriteTracker writeTracker = new WriteTracker();
  private final RowLocks rowLocks = new RowLocks();

  public ThriftClientHandler(TabletServer server) {
    super(server.getContext(), new TransactionWatcher(server.getContext()));
    this.server = server;
    log.debug("{} created", ThriftClientHandler.class.getName());
  }

  @Override
  public List<TKeyExtent> bulkImport(TInfo tinfo, TCredentials credentials, final long tid,
      final Map<TKeyExtent,Map<String,MapFileInfo>> files, final boolean setTime)
      throws ThriftSecurityException {

    if (!security.canPerformSystemActions(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    try {
      return transactionWatcher.run(Constants.BULK_ARBITRATOR_TYPE, tid, () -> {
        List<TKeyExtent> failures = new ArrayList<>();

        for (Entry<TKeyExtent,Map<String,MapFileInfo>> entry : files.entrySet()) {
          TKeyExtent tke = entry.getKey();
          Map<String,MapFileInfo> fileMap = entry.getValue();
          Map<TabletFile,MapFileInfo> fileRefMap = new HashMap<>();
          for (Entry<String,MapFileInfo> mapping : fileMap.entrySet()) {
            Path path = new Path(mapping.getKey());
            FileSystem ns = context.getVolumeManager().getFileSystemByPath(path);
            path = ns.makeQualified(path);
            fileRefMap.put(new TabletFile(path), mapping.getValue());
          }

          Tablet importTablet = server.getOnlineTablet(KeyExtent.fromThrift(tke));

          if (importTablet == null) {
            failures.add(tke);
          } else {
            try {
              importTablet.importMapFiles(tid, fileRefMap, setTime);
            } catch (IOException ioe) {
              log.info("files {} not imported to {}: {}", fileMap.keySet(),
                  KeyExtent.fromThrift(tke), ioe.getMessage());
              failures.add(tke);
            }
          }
        }
        return failures;
      });
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void loadFiles(TInfo tinfo, TCredentials credentials, long tid, String dir,
      Map<TKeyExtent,Map<String,MapFileInfo>> tabletImports, boolean setTime)
      throws ThriftSecurityException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    transactionWatcher.runQuietly(Constants.BULK_ARBITRATOR_TYPE, tid, () -> {
      tabletImports.forEach((tke, fileMap) -> {
        Map<TabletFile,MapFileInfo> newFileMap = new HashMap<>();

        for (Entry<String,MapFileInfo> mapping : fileMap.entrySet()) {
          Path path = new Path(dir, mapping.getKey());
          FileSystem ns = context.getVolumeManager().getFileSystemByPath(path);
          path = ns.makeQualified(path);
          newFileMap.put(new TabletFile(path), mapping.getValue());
        }
        var files = newFileMap.keySet().stream().map(TabletFile::getPathStr).collect(toList());
        server.updateBulkImportState(files, BulkImportState.INITIAL);

        Tablet importTablet = server.getOnlineTablet(KeyExtent.fromThrift(tke));

        if (importTablet != null) {
          try {
            server.updateBulkImportState(files, BulkImportState.PROCESSING);
            importTablet.importMapFiles(tid, newFileMap, setTime);
          } catch (IOException ioe) {
            log.debug("files {} not imported to {}: {}", fileMap.keySet(),
                KeyExtent.fromThrift(tke), ioe.getMessage());
          } finally {
            server.removeBulkImportState(files);
          }
        }
      });
    });

  }

  private ScanDispatcher getScanDispatcher(KeyExtent extent) {
    if (extent.isRootTablet() || extent.isMeta()) {
      // dispatcher is only for user tables
      return null;
    }

    return context.getTableConfiguration(extent.tableId()).getScanDispatcher();
  }

  @Override
  public InitialScan startScan(TInfo tinfo, TCredentials credentials, TKeyExtent textent,
      TRange range, List<TColumn> columns, int batchSize, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      boolean isolated, long readaheadThreshold, TSamplerConfiguration tSamplerConfig,
      long batchTimeOut, String contextArg, Map<String,String> executionHints)
      throws NotServingTabletException, ThriftSecurityException,
      org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException,
      TSampleNotPresentException {

    TableId tableId = TableId.of(new String(textent.getTable(), UTF_8));
    NamespaceId namespaceId;
    try {
      namespaceId = Tables.getNamespaceId(server.getContext(), tableId);
    } catch (TableNotFoundException e1) {
      throw new NotServingTabletException(textent);
    }
    if (!security.canScan(credentials, tableId, namespaceId, range, columns, ssiList, ssio,
        authorizations)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    if (!security.authenticatedUserHasAuthorizations(credentials, authorizations)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.BAD_AUTHORIZATIONS);
    }

    final KeyExtent extent = KeyExtent.fromThrift(textent);

    // wait for any writes that are in flight.. this done to ensure
    // consistency across client restarts... assume a client writes
    // to accumulo and dies while waiting for a confirmation from
    // accumulo... the client process restarts and tries to read
    // data from accumulo making the assumption that it will get
    // any writes previously made... however if the server side thread
    // processing the write from the dead client is still in progress,
    // the restarted client may not see the write unless we wait here.
    // this behavior is very important when the client is reading the
    // metadata
    if (waitForWrites) {
      writeTracker.waitForWrites(TabletType.type(extent));
    }

    Tablet tablet = server.getOnlineTablet(extent);
    if (tablet == null) {
      throw new NotServingTabletException(textent);
    }

    HashSet<Column> columnSet = new HashSet<>();
    for (TColumn tcolumn : columns) {
      columnSet.add(new Column(tcolumn));
    }

    ScanParameters scanParams = new ScanParameters(batchSize, new Authorizations(authorizations),
        columnSet, ssiList, ssio, isolated, SamplerConfigurationImpl.fromThrift(tSamplerConfig),
        batchTimeOut, contextArg);

    final SingleScanSession scanSession =
        new SingleScanSession(credentials, extent, scanParams, readaheadThreshold, executionHints);
    scanSession.scanner =
        tablet.createScanner(new Range(range), scanParams, scanSession.interruptFlag);

    long sid = server.sessionManager.createSession(scanSession, true);

    ScanResult scanResult;
    try {
      scanResult = continueScan(tinfo, sid, scanSession);
    } catch (NoSuchScanIDException e) {
      log.error("The impossible happened", e);
      throw new RuntimeException();
    } finally {
      server.sessionManager.unreserveSession(sid);
    }

    return new InitialScan(sid, scanResult);
  }

  @Override
  public ScanResult continueScan(TInfo tinfo, long scanID) throws NoSuchScanIDException,
      NotServingTabletException, org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException,
      TSampleNotPresentException {
    SingleScanSession scanSession =
        (SingleScanSession) server.sessionManager.reserveSession(scanID);
    if (scanSession == null) {
      throw new NoSuchScanIDException();
    }

    try {
      return continueScan(tinfo, scanID, scanSession);
    } finally {
      server.sessionManager.unreserveSession(scanSession);
    }
  }

  private ScanResult continueScan(TInfo tinfo, long scanID, SingleScanSession scanSession)
      throws NoSuchScanIDException, NotServingTabletException,
      org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException,
      TSampleNotPresentException {

    if (scanSession.nextBatchTask == null) {
      scanSession.nextBatchTask = new NextBatchTask(server, scanID, scanSession.interruptFlag);
      server.resourceManager.executeReadAhead(scanSession.extent,
          getScanDispatcher(scanSession.extent), scanSession, scanSession.nextBatchTask);
    }

    ScanBatch bresult;
    try {
      bresult = scanSession.nextBatchTask.get(MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS,
          TimeUnit.MILLISECONDS);
      scanSession.nextBatchTask = null;
    } catch (ExecutionException e) {
      server.sessionManager.removeSession(scanID);
      if (e.getCause() instanceof NotServingTabletException) {
        throw (NotServingTabletException) e.getCause();
      } else if (e.getCause() instanceof TooManyFilesException) {
        throw new org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException(
            scanSession.extent.toThrift());
      } else if (e.getCause() instanceof SampleNotPresentException) {
        throw new TSampleNotPresentException(scanSession.extent.toThrift());
      } else if (e.getCause() instanceof IOException) {
        sleepUninterruptibly(MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS, TimeUnit.MILLISECONDS);
        List<KVEntry> empty = Collections.emptyList();
        bresult = new ScanBatch(empty, true);
        scanSession.nextBatchTask = null;
      } else {
        throw new RuntimeException(e);
      }
    } catch (CancellationException ce) {
      server.sessionManager.removeSession(scanID);
      Tablet tablet = server.getOnlineTablet(scanSession.extent);
      if (tablet == null || tablet.isClosed()) {
        throw new NotServingTabletException(scanSession.extent.toThrift());
      } else {
        throw new NoSuchScanIDException();
      }
    } catch (TimeoutException e) {
      List<TKeyValue> param = Collections.emptyList();
      long timeout = server.getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
      server.sessionManager.removeIfNotAccessed(scanID, timeout);
      return new ScanResult(param, true);
    } catch (Exception t) {
      server.sessionManager.removeSession(scanID);
      log.warn("Failed to get next batch", t);
      throw new RuntimeException(t);
    }

    ScanResult scanResult = new ScanResult(Key.compress(bresult.getResults()), bresult.isMore());

    scanSession.entriesReturned += scanResult.results.size();

    scanSession.batchCount++;

    if (scanResult.more && scanSession.batchCount > scanSession.readaheadThreshold) {
      // start reading next batch while current batch is transmitted
      // to client
      scanSession.nextBatchTask = new NextBatchTask(server, scanID, scanSession.interruptFlag);
      server.resourceManager.executeReadAhead(scanSession.extent,
          getScanDispatcher(scanSession.extent), scanSession, scanSession.nextBatchTask);
    }

    if (!scanResult.more) {
      closeScan(tinfo, scanID);
    }

    return scanResult;
  }

  @Override
  public void closeScan(TInfo tinfo, long scanID) {
    final SingleScanSession ss = (SingleScanSession) server.sessionManager.removeSession(scanID);
    if (ss != null) {
      long t2 = System.currentTimeMillis();

      if (log.isTraceEnabled()) {
        log.trace(String.format("ScanSess tid %s %s %,d entries in %.2f secs, nbTimes = [%s] ",
            TServerUtils.clientAddress.get(), ss.extent.tableId(), ss.entriesReturned,
            (t2 - ss.startTime) / 1000.0, ss.runStats.toString()));
      }

      server.scanMetrics.addScan(t2 - ss.startTime);
      server.scanMetrics.addResult(ss.entriesReturned);
    }
  }

  @Override
  public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials,
      Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      TSamplerConfiguration tSamplerConfig, long batchTimeOut, String contextArg,
      Map<String,String> executionHints)
      throws ThriftSecurityException, TSampleNotPresentException {
    // find all of the tables that need to be scanned
    final HashSet<TableId> tables = new HashSet<>();
    for (TKeyExtent keyExtent : tbatch.keySet()) {
      tables.add(TableId.of(new String(keyExtent.getTable(), UTF_8)));
    }

    if (tables.size() != 1) {
      throw new IllegalArgumentException("Cannot batch scan over multiple tables");
    }

    // check if user has permission to the tables
    for (TableId tableId : tables) {
      NamespaceId namespaceId = getNamespaceId(credentials, tableId);
      if (!security.canScan(credentials, tableId, namespaceId, tbatch, tcolumns, ssiList, ssio,
          authorizations)) {
        throw new ThriftSecurityException(credentials.getPrincipal(),
            SecurityErrorCode.PERMISSION_DENIED);
      }
    }

    try {
      if (!security.authenticatedUserHasAuthorizations(credentials, authorizations)) {
        throw new ThriftSecurityException(credentials.getPrincipal(),
            SecurityErrorCode.BAD_AUTHORIZATIONS);
      }
    } catch (ThriftSecurityException tse) {
      log.error("{} is not authorized", credentials.getPrincipal(), tse);
      throw tse;
    }

    // @formatter:off
    Map<KeyExtent, List<Range>> batch = tbatch.entrySet().stream().collect(Collectors.toMap(
                    entry -> KeyExtent.fromThrift(entry.getKey()),
                    entry -> entry.getValue().stream().map(Range::new).collect(Collectors.toList())
    ));
    // @formatter:on

    // This is used to determine which thread pool to use
    KeyExtent threadPoolExtent = batch.keySet().iterator().next();

    if (waitForWrites) {
      writeTracker.waitForWrites(TabletType.type(batch.keySet()));
    }

    Set<Column> columnSet = tcolumns.isEmpty() ? Collections.emptySet()
        : new HashSet<>(Collections2.transform(tcolumns, Column::new));

    ScanParameters scanParams =
        new ScanParameters(-1, new Authorizations(authorizations), columnSet, ssiList, ssio, false,
            SamplerConfigurationImpl.fromThrift(tSamplerConfig), batchTimeOut, contextArg);

    final MultiScanSession mss =
        new MultiScanSession(credentials, threadPoolExtent, batch, scanParams, executionHints);

    mss.numTablets = batch.size();
    for (List<Range> ranges : batch.values()) {
      mss.numRanges += ranges.size();
    }

    long sid = server.sessionManager.createSession(mss, true);

    MultiScanResult result;
    try {
      result = continueMultiScan(sid, mss);
    } finally {
      server.sessionManager.unreserveSession(sid);
    }

    return new InitialMultiScan(sid, result);
  }

  @Override
  public MultiScanResult continueMultiScan(TInfo tinfo, long scanID)
      throws NoSuchScanIDException, TSampleNotPresentException {

    MultiScanSession session = (MultiScanSession) server.sessionManager.reserveSession(scanID);

    if (session == null) {
      throw new NoSuchScanIDException();
    }

    try {
      return continueMultiScan(scanID, session);
    } finally {
      server.sessionManager.unreserveSession(session);
    }
  }

  private MultiScanResult continueMultiScan(long scanID, MultiScanSession session)
      throws TSampleNotPresentException {

    if (session.lookupTask == null) {
      session.lookupTask = new LookupTask(server, scanID);
      server.resourceManager.executeReadAhead(session.threadPoolExtent,
          getScanDispatcher(session.threadPoolExtent), session, session.lookupTask);
    }

    try {
      MultiScanResult scanResult =
          session.lookupTask.get(MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS, TimeUnit.MILLISECONDS);
      session.lookupTask = null;
      return scanResult;
    } catch (ExecutionException e) {
      server.sessionManager.removeSession(scanID);
      if (e.getCause() instanceof SampleNotPresentException) {
        throw new TSampleNotPresentException();
      } else {
        log.warn("Failed to get multiscan result", e);
        throw new RuntimeException(e);
      }
    } catch (TimeoutException e1) {
      long timeout = server.getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
      server.sessionManager.removeIfNotAccessed(scanID, timeout);
      List<TKeyValue> results = Collections.emptyList();
      Map<TKeyExtent,List<TRange>> failures = Collections.emptyMap();
      List<TKeyExtent> fullScans = Collections.emptyList();
      return new MultiScanResult(results, failures, fullScans, null, null, false, true);
    } catch (Exception t) {
      server.sessionManager.removeSession(scanID);
      log.warn("Failed to get multiscan result", t);
      throw new RuntimeException(t);
    }
  }

  @Override
  public void closeMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException {
    MultiScanSession session = (MultiScanSession) server.sessionManager.removeSession(scanID);
    if (session == null) {
      throw new NoSuchScanIDException();
    }

    long t2 = System.currentTimeMillis();

    if (log.isTraceEnabled()) {
      log.trace(String.format(
          "MultiScanSess %s %,d entries in %.2f secs"
              + " (lookup_time:%.2f secs tablets:%,d ranges:%,d) ",
          TServerUtils.clientAddress.get(), session.numEntries, (t2 - session.startTime) / 1000.0,
          session.totalLookupTime / 1000.0, session.numTablets, session.numRanges));
    }
  }

  @Override
  public long startUpdate(TInfo tinfo, TCredentials credentials, TDurability tdurabilty)
      throws ThriftSecurityException {
    // Make sure user is real
    Durability durability = DurabilityImpl.fromThrift(tdurabilty);
    security.authenticateUser(credentials, credentials);
    server.updateMetrics.addPermissionErrors(0);

    UpdateSession us =
        new UpdateSession(new TservConstraintEnv(server.getContext(), security, credentials),
            credentials, durability);
    return server.sessionManager.createSession(us, false);
  }

  private void setUpdateTablet(UpdateSession us, KeyExtent keyExtent) {
    long t1 = System.currentTimeMillis();
    if (us.currentTablet != null && us.currentTablet.getExtent().equals(keyExtent)) {
      return;
    }
    if (us.currentTablet == null
        && (us.failures.containsKey(keyExtent) || us.authFailures.containsKey(keyExtent))) {
      // if there were previous failures, then do not accept additional writes
      return;
    }

    TableId tableId = null;
    try {
      // if user has no permission to write to this table, add it to
      // the failures list
      boolean sameTable = us.currentTablet != null
          && (us.currentTablet.getExtent().tableId().equals(keyExtent.tableId()));
      tableId = keyExtent.tableId();
      if (sameTable || security.canWrite(us.getCredentials(), tableId,
          Tables.getNamespaceId(server.getContext(), tableId))) {
        long t2 = System.currentTimeMillis();
        us.authTimes.addStat(t2 - t1);
        us.currentTablet = server.getOnlineTablet(keyExtent);
        if (us.currentTablet != null) {
          us.queuedMutations.put(us.currentTablet, new ArrayList<>());
        } else {
          // not serving tablet, so report all mutations as
          // failures
          us.failures.put(keyExtent, 0L);
          server.updateMetrics.addUnknownTabletErrors(0);
        }
      } else {
        log.warn("Denying access to table {} for user {}", keyExtent.tableId(), us.getUser());
        long t2 = System.currentTimeMillis();
        us.authTimes.addStat(t2 - t1);
        us.currentTablet = null;
        us.authFailures.put(keyExtent, SecurityErrorCode.PERMISSION_DENIED);
        server.updateMetrics.addPermissionErrors(0);
        return;
      }
    } catch (TableNotFoundException tnfe) {
      log.error("Table " + tableId + " not found ", tnfe);
      long t2 = System.currentTimeMillis();
      us.authTimes.addStat(t2 - t1);
      us.currentTablet = null;
      us.authFailures.put(keyExtent, SecurityErrorCode.TABLE_DOESNT_EXIST);
      server.updateMetrics.addUnknownTabletErrors(0);
      return;
    } catch (ThriftSecurityException e) {
      log.error("Denying permission to check user " + us.getUser() + " with user " + e.getUser(),
          e);
      long t2 = System.currentTimeMillis();
      us.authTimes.addStat(t2 - t1);
      us.currentTablet = null;
      us.authFailures.put(keyExtent, e.getCode());
      server.updateMetrics.addPermissionErrors(0);
      return;
    }
  }

  @Override
  public void applyUpdates(TInfo tinfo, long updateID, TKeyExtent tkeyExtent,
      List<TMutation> tmutations) {
    UpdateSession us = (UpdateSession) server.sessionManager.reserveSession(updateID);
    if (us == null) {
      return;
    }

    boolean reserved = true;
    try {
      KeyExtent keyExtent = KeyExtent.fromThrift(tkeyExtent);
      setUpdateTablet(us, keyExtent);

      if (us.currentTablet != null) {
        long additionalMutationSize = 0;
        List<Mutation> mutations = us.queuedMutations.get(us.currentTablet);
        for (TMutation tmutation : tmutations) {
          Mutation mutation = new ServerMutation(tmutation);
          mutations.add(mutation);
          additionalMutationSize += mutation.numBytes();
        }
        us.queuedMutationSize += additionalMutationSize;
        long totalQueued = server.updateTotalQueuedMutationSize(additionalMutationSize);
        long total = server.getConfiguration().getAsBytes(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX);
        if (totalQueued > total) {
          try {
            flush(us);
          } catch (HoldTimeoutException hte) {
            // Assumption is that the client has timed out and is gone. If that's not the case,
            // then removing the session should cause the client to fail
            // in such a way that it retries.
            log.debug("HoldTimeoutException during applyUpdates, removing session");
            server.sessionManager.removeSession(updateID, true);
            reserved = false;
          }
        }
      }
    } finally {
      if (reserved) {
        server.sessionManager.unreserveSession(us);
      }
    }
  }

  private void flush(UpdateSession us) {

    int mutationCount = 0;
    Map<CommitSession,List<Mutation>> sendables = new HashMap<>();
    Map<CommitSession,TabletMutations> loggables = new HashMap<>();
    Throwable error = null;

    long pt1 = System.currentTimeMillis();

    boolean containsMetadataTablet = false;
    for (Tablet tablet : us.queuedMutations.keySet()) {
      if (tablet.getExtent().isMeta()) {
        containsMetadataTablet = true;
      }
    }

    if (!containsMetadataTablet && !us.queuedMutations.isEmpty()) {
      server.resourceManager.waitUntilCommitsAreEnabled();
    }

    try (TraceScope prep = Trace.startSpan("prep")) {
      for (Entry<Tablet,? extends List<Mutation>> entry : us.queuedMutations.entrySet()) {

        Tablet tablet = entry.getKey();
        Durability durability =
            DurabilityImpl.resolveDurabilty(us.durability, tablet.getDurability());
        List<Mutation> mutations = entry.getValue();
        if (!mutations.isEmpty()) {
          try {
            server.updateMetrics.addMutationArraySize(mutations.size());

            PreparedMutations prepared = tablet.prepareMutationsForCommit(us.cenv, mutations);

            if (prepared.tabletClosed()) {
              if (us.currentTablet == tablet) {
                us.currentTablet = null;
              }
              us.failures.put(tablet.getExtent(), us.successfulCommits.get(tablet));
            } else {
              if (!prepared.getNonViolators().isEmpty()) {
                List<Mutation> validMutations = prepared.getNonViolators();
                CommitSession session = prepared.getCommitSession();
                if (durability != Durability.NONE) {
                  loggables.put(session, new TabletMutations(session, validMutations, durability));
                }
                sendables.put(session, validMutations);
              }

              if (!prepared.getViolations().isEmpty()) {
                us.violations.add(prepared.getViolations());
                server.updateMetrics.addConstraintViolations(0);
              }
              // Use the size of the original mutation list, regardless of how many mutations
              // did not violate constraints.
              mutationCount += mutations.size();

            }
          } catch (Exception t) {
            error = t;
            log.error("Unexpected error preparing for commit", error);
            break;
          }
        }
      }
    }

    long pt2 = System.currentTimeMillis();
    us.prepareTimes.addStat(pt2 - pt1);
    updateAvgPrepTime(pt2 - pt1, us.queuedMutations.size());

    if (error != null) {
      sendables.forEach((commitSession, value) -> commitSession.abortCommit());
      throw new RuntimeException(error);
    }
    try {
      try (TraceScope wal = Trace.startSpan("wal")) {
        while (true) {
          try {
            long t1 = System.currentTimeMillis();

            server.logger.logManyTablets(loggables);

            long t2 = System.currentTimeMillis();
            us.walogTimes.addStat(t2 - t1);
            updateWalogWriteTime((t2 - t1));
            break;
          } catch (IOException | FSError ex) {
            log.warn("logging mutations failed, retrying");
          } catch (Exception t) {
            log.error("Unknown exception logging mutations, counts"
                + " for mutations in flight not decremented!", t);
            throw new RuntimeException(t);
          }
        }
      }

      try (TraceScope commit = Trace.startSpan("commit")) {
        long t1 = System.currentTimeMillis();
        sendables.forEach((commitSession, mutations) -> {
          commitSession.commit(mutations);
          KeyExtent extent = commitSession.getExtent();

          if (us.currentTablet != null && extent == us.currentTablet.getExtent()) {
            // because constraint violations may filter out some
            // mutations, for proper accounting with the client code,
            // need to increment the count based on the original
            // number of mutations from the client NOT the filtered number
            us.successfulCommits.increment(us.currentTablet,
                us.queuedMutations.get(us.currentTablet).size());
          }
        });
        long t2 = System.currentTimeMillis();

        us.flushTime += (t2 - pt1);
        us.commitTimes.addStat(t2 - t1);

        updateAvgCommitTime(t2 - t1, sendables.size());
      }
    } finally {
      us.queuedMutations.clear();
      if (us.currentTablet != null) {
        us.queuedMutations.put(us.currentTablet, new ArrayList<>());
      }
      server.updateTotalQueuedMutationSize(-us.queuedMutationSize);
      us.queuedMutationSize = 0;
    }
    us.totalUpdates += mutationCount;
  }

  private void updateWalogWriteTime(long time) {
    server.updateMetrics.addWalogWriteTime(time);
  }

  private void updateAvgCommitTime(long time, int size) {
    if (size > 0)
      server.updateMetrics.addCommitTime((long) (time / (double) size));
  }

  private void updateAvgPrepTime(long time, int size) {
    if (size > 0)
      server.updateMetrics.addCommitPrep((long) (time / (double) size));
  }

  @Override
  public UpdateErrors closeUpdate(TInfo tinfo, long updateID) throws NoSuchScanIDException {
    final UpdateSession us = (UpdateSession) server.sessionManager.removeSession(updateID);
    if (us == null) {
      throw new NoSuchScanIDException();
    }

    // clients may or may not see data from an update session while
    // it is in progress, however when the update session is closed
    // want to ensure that reads wait for the write to finish
    long opid = writeTracker.startWrite(us.queuedMutations.keySet());

    try {
      flush(us);
    } catch (HoldTimeoutException e) {
      // Assumption is that the client has timed out and is gone. If that's not the case throw an
      // exception that will cause it to retry.
      log.debug("HoldTimeoutException during closeUpdate, reporting no such session");
      throw new NoSuchScanIDException();
    } finally {
      writeTracker.finishWrite(opid);
    }

    if (log.isTraceEnabled()) {
      log.trace(
          String.format("UpSess %s %,d in %.3fs, at=[%s] ft=%.3fs(pt=%.3fs lt=%.3fs ct=%.3fs)",
              TServerUtils.clientAddress.get(), us.totalUpdates,
              (System.currentTimeMillis() - us.startTime) / 1000.0, us.authTimes.toString(),
              us.flushTime / 1000.0, us.prepareTimes.sum() / 1000.0, us.walogTimes.sum() / 1000.0,
              us.commitTimes.sum() / 1000.0));
    }
    if (!us.failures.isEmpty()) {
      Entry<KeyExtent,Long> first = us.failures.entrySet().iterator().next();
      log.debug(String.format("Failures: %d, first extent %s successful commits: %d",
          us.failures.size(), first.getKey().toString(), first.getValue()));
    }
    List<ConstraintViolationSummary> violations = us.violations.asList();
    if (!violations.isEmpty()) {
      ConstraintViolationSummary first = us.violations.asList().iterator().next();
      log.debug(String.format("Violations: %d, first %s occurs %d", violations.size(),
          first.violationDescription, first.numberOfViolatingMutations));
    }
    if (!us.authFailures.isEmpty()) {
      KeyExtent first = us.authFailures.keySet().iterator().next();
      log.debug(String.format("Authentication Failures: %d, first %s", us.authFailures.size(),
          first.toString()));
    }
    return new UpdateErrors(
        us.failures.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toThrift(), Entry::getValue)),
        violations.stream().map(ConstraintViolationSummary::toThrift).collect(Collectors.toList()),
        us.authFailures.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().toThrift(), Entry::getValue)));
  }

  @Override
  public void update(TInfo tinfo, TCredentials credentials, TKeyExtent tkeyExtent,
      TMutation tmutation, TDurability tdurability)
      throws NotServingTabletException, ConstraintViolationException, ThriftSecurityException {

    final TableId tableId = TableId.of(new String(tkeyExtent.getTable(), UTF_8));
    NamespaceId namespaceId = getNamespaceId(credentials, tableId);
    if (!security.canWrite(credentials, tableId, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }
    final KeyExtent keyExtent = KeyExtent.fromThrift(tkeyExtent);
    final Tablet tablet = server.getOnlineTablet(KeyExtent.copyOf(keyExtent));
    if (tablet == null) {
      throw new NotServingTabletException(tkeyExtent);
    }
    Durability tabletDurability = tablet.getDurability();

    if (!keyExtent.isMeta()) {
      try {
        server.resourceManager.waitUntilCommitsAreEnabled();
      } catch (HoldTimeoutException hte) {
        // Major hack. Assumption is that the client has timed out and is gone. If that's not the
        // case, then throwing the following will let client know there
        // was a failure and it should retry.
        throw new NotServingTabletException(tkeyExtent);
      }
    }

    final long opid = writeTracker.startWrite(TabletType.type(keyExtent));

    try {
      final Mutation mutation = new ServerMutation(tmutation);
      final List<Mutation> mutations = Collections.singletonList(mutation);

      PreparedMutations prepared;
      try (TraceScope prep = Trace.startSpan("prep")) {
        prepared = tablet.prepareMutationsForCommit(
            new TservConstraintEnv(server.getContext(), security, credentials), mutations);
      }

      if (prepared.tabletClosed()) {
        throw new NotServingTabletException(tkeyExtent);
      } else if (!prepared.getViolators().isEmpty()) {
        throw new ConstraintViolationException(prepared.getViolations().asList().stream()
            .map(ConstraintViolationSummary::toThrift).collect(Collectors.toList()));
      } else {
        CommitSession session = prepared.getCommitSession();
        Durability durability = DurabilityImpl
            .resolveDurabilty(DurabilityImpl.fromThrift(tdurability), tabletDurability);

        // Instead of always looping on true, skip completely when durability is NONE.
        while (durability != Durability.NONE) {
          try {
            try (TraceScope wal = Trace.startSpan("wal")) {
              server.logger.log(session, mutation, durability);
            }
            break;
          } catch (IOException ex) {
            log.warn("Error writing mutations to log", ex);
          }
        }

        try (TraceScope commit = Trace.startSpan("commit")) {
          session.commit(mutations);
        }
      }
    } finally {
      writeTracker.finishWrite(opid);
    }
  }

  private NamespaceId getNamespaceId(TCredentials credentials, TableId tableId)
      throws ThriftSecurityException {
    try {
      return Tables.getNamespaceId(server.getContext(), tableId);
    } catch (TableNotFoundException e1) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
  }

  private void checkConditions(Map<KeyExtent,List<ServerConditionalMutation>> updates,
      ArrayList<TCMResult> results, ConditionalSession cs, List<String> symbols)
      throws IOException {
    Iterator<Entry<KeyExtent,List<ServerConditionalMutation>>> iter = updates.entrySet().iterator();

    final CompressedIterators compressedIters = new CompressedIterators(symbols);
    ConditionCheckerContext checkerContext = new ConditionCheckerContext(server.getContext(),
        compressedIters, context.getTableConfiguration(cs.tableId));

    while (iter.hasNext()) {
      final Entry<KeyExtent,List<ServerConditionalMutation>> entry = iter.next();
      final Tablet tablet = server.getOnlineTablet(entry.getKey());

      if (tablet == null || tablet.isClosed()) {
        for (ServerConditionalMutation scm : entry.getValue()) {
          results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
        }
        iter.remove();
      } else {
        final List<ServerConditionalMutation> okMutations =
            new ArrayList<>(entry.getValue().size());
        final List<TCMResult> resultsSubList = results.subList(results.size(), results.size());

        ConditionChecker checker =
            checkerContext.newChecker(entry.getValue(), okMutations, resultsSubList);
        try {
          tablet.checkConditions(checker, cs.auths, cs.interruptFlag);

          if (okMutations.isEmpty()) {
            iter.remove();
          } else {
            entry.setValue(okMutations);
          }
        } catch (TabletClosedException | IterationInterruptedException | TooManyFilesException e) {
          // clear anything added while checking conditions.
          resultsSubList.clear();

          for (ServerConditionalMutation scm : entry.getValue()) {
            results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
          }
          iter.remove();
        }
      }
    }
  }

  private void writeConditionalMutations(Map<KeyExtent,List<ServerConditionalMutation>> updates,
      ArrayList<TCMResult> results, ConditionalSession sess) {
    Set<Entry<KeyExtent,List<ServerConditionalMutation>>> es = updates.entrySet();

    Map<CommitSession,List<Mutation>> sendables = new HashMap<>();
    Map<CommitSession,TabletMutations> loggables = new HashMap<>();

    boolean sessionCanceled = sess.interruptFlag.get();

    try (TraceScope prepSpan = Trace.startSpan("prep")) {
      long t1 = System.currentTimeMillis();
      for (Entry<KeyExtent,List<ServerConditionalMutation>> entry : es) {
        final Tablet tablet = server.getOnlineTablet(entry.getKey());
        if (tablet == null || tablet.isClosed() || sessionCanceled) {
          addMutationsAsTCMResults(results, entry.getValue(), TCMStatus.IGNORED);
        } else {
          final Durability durability =
              DurabilityImpl.resolveDurabilty(sess.durability, tablet.getDurability());

          @SuppressWarnings("unchecked")
          List<Mutation> mutations = (List<Mutation>) (List<? extends Mutation>) entry.getValue();
          if (!mutations.isEmpty()) {

            PreparedMutations prepared = tablet.prepareMutationsForCommit(
                new TservConstraintEnv(server.getContext(), security, sess.credentials), mutations);

            if (prepared.tabletClosed()) {
              addMutationsAsTCMResults(results, mutations, TCMStatus.IGNORED);
            } else {
              if (!prepared.getNonViolators().isEmpty()) {
                // Only log and commit mutations that did not violate constraints.
                List<Mutation> validMutations = prepared.getNonViolators();
                addMutationsAsTCMResults(results, validMutations, TCMStatus.ACCEPTED);
                CommitSession session = prepared.getCommitSession();
                if (durability != Durability.NONE) {
                  loggables.put(session, new TabletMutations(session, validMutations, durability));
                }
                sendables.put(session, validMutations);
              }

              if (!prepared.getViolators().isEmpty()) {
                addMutationsAsTCMResults(results, prepared.getViolators(), TCMStatus.VIOLATED);
              }
            }
          }
        }
      }

      long t2 = System.currentTimeMillis();
      updateAvgPrepTime(t2 - t1, es.size());
    }

    try (TraceScope walSpan = Trace.startSpan("wal")) {
      while (!loggables.isEmpty()) {
        try {
          long t1 = System.currentTimeMillis();
          server.logger.logManyTablets(loggables);
          long t2 = System.currentTimeMillis();
          updateWalogWriteTime(t2 - t1);
          break;
        } catch (IOException | FSError ex) {
          log.warn("logging mutations failed, retrying");
        } catch (Exception t) {
          log.error("Unknown exception logging mutations, counts for"
              + " mutations in flight not decremented!", t);
          throw new RuntimeException(t);
        }
      }
    }

    try (TraceScope commitSpan = Trace.startSpan("commit")) {
      long t1 = System.currentTimeMillis();
      sendables.forEach(CommitSession::commit);
      long t2 = System.currentTimeMillis();
      updateAvgCommitTime(t2 - t1, sendables.size());
    }
  }

  /**
   * Transform and add each mutation as a {@link TCMResult} with the mutation's ID and the specified
   * status to the {@link TCMResult} list.
   */
  private void addMutationsAsTCMResults(final List<TCMResult> list,
      final Collection<? extends Mutation> mutations, final TCMStatus status) {
    mutations.stream()
        .map(mutation -> new TCMResult(((ServerConditionalMutation) mutation).getID(), status))
        .forEach(list::add);
  }

  private Map<KeyExtent,List<ServerConditionalMutation>> conditionalUpdate(ConditionalSession cs,
      Map<KeyExtent,List<ServerConditionalMutation>> updates, ArrayList<TCMResult> results,
      List<String> symbols) throws IOException {
    // sort each list of mutations, this is done to avoid deadlock and doing seeks in order is
    // more efficient and detect duplicate rows.
    ConditionalMutationSet.sortConditionalMutations(updates);

    Map<KeyExtent,List<ServerConditionalMutation>> deferred = new HashMap<>();

    // can not process two mutations for the same row, because one will not see what the other
    // writes
    ConditionalMutationSet.deferDuplicatesRows(updates, deferred);

    // get as many locks as possible w/o blocking... defer any rows that are locked
    List<RowLock> locks = rowLocks.acquireRowlocks(updates, deferred);
    try {
      try (TraceScope checkSpan = Trace.startSpan("Check conditions")) {
        checkConditions(updates, results, cs, symbols);
      }

      try (TraceScope updateSpan = Trace.startSpan("apply conditional mutations")) {
        writeConditionalMutations(updates, results, cs);
      }
    } finally {
      rowLocks.releaseRowLocks(locks);
    }
    return deferred;
  }

  @Override
  public TConditionalSession startConditionalUpdate(TInfo tinfo, TCredentials credentials,
      List<ByteBuffer> authorizations, String tableIdStr, TDurability tdurabilty,
      String classLoaderContext) throws ThriftSecurityException, TException {

    TableId tableId = TableId.of(tableIdStr);
    Authorizations userauths = null;
    NamespaceId namespaceId = getNamespaceId(credentials, tableId);
    if (!security.canConditionallyUpdate(credentials, tableId, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    userauths = security.getUserAuthorizations(credentials);
    for (ByteBuffer auth : authorizations) {
      if (!userauths.contains(ByteBufferUtil.toBytes(auth))) {
        throw new ThriftSecurityException(credentials.getPrincipal(),
            SecurityErrorCode.BAD_AUTHORIZATIONS);
      }
    }

    ConditionalSession cs = new ConditionalSession(credentials, new Authorizations(authorizations),
        tableId, DurabilityImpl.fromThrift(tdurabilty));

    long sid = server.sessionManager.createSession(cs, false);
    return new TConditionalSession(sid, server.getLockID(), server.sessionManager.getMaxIdleTime());
  }

  @Override
  public List<TCMResult> conditionalUpdate(TInfo tinfo, long sessID,
      Map<TKeyExtent,List<TConditionalMutation>> mutations, List<String> symbols)
      throws NoSuchScanIDException, TException {

    ConditionalSession cs = (ConditionalSession) server.sessionManager.reserveSession(sessID);

    if (cs == null || cs.interruptFlag.get()) {
      throw new NoSuchScanIDException();
    }

    if (!cs.tableId.equals(MetadataTable.ID) && !cs.tableId.equals(RootTable.ID)) {
      try {
        server.resourceManager.waitUntilCommitsAreEnabled();
      } catch (HoldTimeoutException hte) {
        // Assumption is that the client has timed out and is gone. If that's not the case throw
        // an exception that will cause it to retry.
        log.debug("HoldTimeoutException during conditionalUpdate, reporting no such session");
        throw new NoSuchScanIDException();
      }
    }

    TableId tid = cs.tableId;
    long opid = writeTracker.startWrite(TabletType.type(new KeyExtent(tid, null, null)));

    try {
      // @formatter:off
      Map<KeyExtent, List<ServerConditionalMutation>> updates = mutations.entrySet().stream().collect(Collectors.toMap(
                      entry -> KeyExtent.fromThrift(entry.getKey()),
                      entry -> entry.getValue().stream().map(ServerConditionalMutation::new).collect(Collectors.toList())
      ));
      // @formatter:on
      for (KeyExtent ke : updates.keySet()) {
        if (!ke.tableId().equals(tid)) {
          throw new IllegalArgumentException("Unexpected table id " + tid + " != " + ke.tableId());
        }
      }

      ArrayList<TCMResult> results = new ArrayList<>();

      Map<KeyExtent,List<ServerConditionalMutation>> deferred =
          conditionalUpdate(cs, updates, results, symbols);

      while (!deferred.isEmpty()) {
        deferred = conditionalUpdate(cs, deferred, results, symbols);
      }

      return results;
    } catch (IOException ioe) {
      throw new TException(ioe);
    } finally {
      writeTracker.finishWrite(opid);
      server.sessionManager.unreserveSession(sessID);
    }
  }

  @Override
  public void invalidateConditionalUpdate(TInfo tinfo, long sessID) {
    // this method should wait for any running conditional update to complete
    // after this method returns a conditional update should not be able to start

    ConditionalSession cs = (ConditionalSession) server.sessionManager.getSession(sessID);
    if (cs != null) {
      cs.interruptFlag.set(true);
    }

    cs = (ConditionalSession) server.sessionManager.reserveSession(sessID, true);
    if (cs != null) {
      server.sessionManager.removeSession(sessID, true);
    }
  }

  @Override
  public void closeConditionalUpdate(TInfo tinfo, long sessID) {
    server.sessionManager.removeSession(sessID, false);
  }

  @Override
  public void splitTablet(TInfo tinfo, TCredentials credentials, TKeyExtent tkeyExtent,
      ByteBuffer splitPoint) throws NotServingTabletException, ThriftSecurityException {

    TableId tableId = TableId.of(new String(ByteBufferUtil.toBytes(tkeyExtent.table)));
    NamespaceId namespaceId = getNamespaceId(credentials, tableId);

    if (!security.canSplitTablet(credentials, tableId, namespaceId)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    KeyExtent keyExtent = KeyExtent.fromThrift(tkeyExtent);

    Tablet tablet = server.getOnlineTablet(keyExtent);
    if (tablet == null) {
      throw new NotServingTabletException(tkeyExtent);
    }

    if (keyExtent.endRow() == null
        || !keyExtent.endRow().equals(ByteBufferUtil.toText(splitPoint))) {
      try {
        if (server.splitTablet(tablet, ByteBufferUtil.toBytes(splitPoint)) == null) {
          throw new NotServingTabletException(tkeyExtent);
        }
      } catch (IOException e) {
        log.warn("Failed to split " + keyExtent, e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public TabletServerStatus getTabletServerStatus(TInfo tinfo, TCredentials credentials) {
    return server.getStats(server.sessionManager.getActiveScansPerTable());
  }

  @Override
  public List<TabletStats> getTabletStats(TInfo tinfo, TCredentials credentials, String tableId) {
    List<TabletStats> result = new ArrayList<>();
    TableId text = TableId.of(tableId);
    KeyExtent start = new KeyExtent(text, new Text(), null);
    for (Entry<KeyExtent,Tablet> entry : server.getOnlineTablets().tailMap(start).entrySet()) {
      KeyExtent ke = entry.getKey();
      if (ke.tableId().compareTo(text) == 0) {
        Tablet tablet = entry.getValue();
        TabletStats stats = tablet.getTabletStats();
        stats.extent = ke.toThrift();
        stats.ingestRate = tablet.ingestRate();
        stats.queryRate = tablet.queryRate();
        stats.splitCreationTime = tablet.getSplitCreationTime();
        stats.numEntries = tablet.getNumEntries();
        result.add(stats);
      }
    }
    return result;
  }

  private void checkPermission(TCredentials credentials, String lock, final String request)
      throws ThriftSecurityException {
    try {
      log.trace("Got {} message from user: {}", request, credentials.getPrincipal());
      if (!security.canPerformSystemActions(credentials)) {
        log.warn("Got {} message from user: {}", request, credentials.getPrincipal());
        throw new ThriftSecurityException(credentials.getPrincipal(),
            SecurityErrorCode.PERMISSION_DENIED);
      }
    } catch (ThriftSecurityException e) {
      log.warn("Got {} message from unauthenticatable user: {}", request, e.getUser());
      if (context.getCredentials().getToken().getClass().getName()
          .equals(credentials.getTokenClassName())) {
        log.error("Got message from a service with a mismatched configuration."
            + " Please ensure a compatible configuration.", e);
      }
      throw e;
    }

    if (server.getLock() == null || !server.getLock().wasLockAcquired()) {
      log.debug("Got {} message before my lock was acquired, ignoring...", request);
      throw new RuntimeException("Lock not acquired");
    }

    if (server.getLock() != null && server.getLock().wasLockAcquired()
        && !server.getLock().isLocked()) {
      Halt.halt(1, () -> {
        log.info("Tablet server no longer holds lock during checkPermission() : {}, exiting",
            request);
        server.gcLogger.logGCInfo(server.getConfiguration());
      });
    }

    if (lock != null) {
      ZooUtil.LockID lid =
          new ZooUtil.LockID(context.getZooKeeperRoot() + Constants.ZMANAGER_LOCK, lock);

      try {
        if (!ServiceLock.isLockHeld(server.managerLockCache, lid)) {
          // maybe the cache is out of date and a new manager holds the
          // lock?
          server.managerLockCache.clear();
          if (!ServiceLock.isLockHeld(server.managerLockCache, lid)) {
            log.warn("Got {} message from a manager that does not hold the current lock {}",
                request, lock);
            throw new RuntimeException("bad manager lock");
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("bad manager lock", e);
      }
    }
  }

  @Override
  public void loadTablet(TInfo tinfo, TCredentials credentials, String lock,
      final TKeyExtent textent) {

    try {
      checkPermission(credentials, lock, "loadTablet");
    } catch (ThriftSecurityException e) {
      log.error("Caller doesn't have permission to load a tablet", e);
      throw new RuntimeException(e);
    }

    final KeyExtent extent = KeyExtent.fromThrift(textent);

    synchronized (server.unopenedTablets) {
      synchronized (server.openingTablets) {
        synchronized (server.onlineTablets) {

          // checking if this exact tablet is in any of the sets
          // below is not a strong enough check
          // when splits and fix splits occurring

          Set<KeyExtent> unopenedOverlapping =
              KeyExtent.findOverlapping(extent, server.unopenedTablets);
          Set<KeyExtent> openingOverlapping =
              KeyExtent.findOverlapping(extent, server.openingTablets);
          Set<KeyExtent> onlineOverlapping =
              KeyExtent.findOverlapping(extent, server.getOnlineTablets());

          Set<KeyExtent> all = new HashSet<>();
          all.addAll(unopenedOverlapping);
          all.addAll(openingOverlapping);
          all.addAll(onlineOverlapping);

          if (!all.isEmpty()) {

            // ignore any tablets that have recently split, for error logging
            for (KeyExtent e2 : onlineOverlapping) {
              Tablet tablet = server.getOnlineTablet(e2);
              if (System.currentTimeMillis() - tablet.getSplitCreationTime()
                  < RECENTLY_SPLIT_MILLIES) {
                all.remove(e2);
              }
            }

            // ignore self, for error logging
            all.remove(extent);

            if (!all.isEmpty()) {
              log.error("Tablet {} overlaps previously assigned {} {} {}", extent,
                  unopenedOverlapping, openingOverlapping, onlineOverlapping + " " + all);
            }
            return;
          }

          server.unopenedTablets.add(extent);
        }
      }
    }

    TabletLogger.loading(extent, server.getTabletSession());

    final AssignmentHandler ah = new AssignmentHandler(server, extent);
    // final Runnable ah = new LoggingRunnable(log, );
    // Root tablet assignment must take place immediately

    if (extent.isRootTablet()) {
      Threads.createThread("Root Tablet Assignment", () -> {
        ah.run();
        if (server.getOnlineTablets().containsKey(extent)) {
          log.info("Root tablet loaded: {}", extent);
        } else {
          log.info("Root tablet failed to load");
        }
      }).start();
    } else {
      if (extent.isMeta()) {
        server.resourceManager.addMetaDataAssignment(extent, log, ah);
      } else {
        server.resourceManager.addAssignment(extent, log, ah);
      }
    }
  }

  @Override
  public void unloadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent textent,
      TUnloadTabletGoal goal, long requestTime) {
    try {
      checkPermission(credentials, lock, "unloadTablet");
    } catch (ThriftSecurityException e) {
      log.error("Caller doesn't have permission to unload a tablet", e);
      throw new RuntimeException(e);
    }

    KeyExtent extent = KeyExtent.fromThrift(textent);

    server.resourceManager.addMigration(extent,
        new UnloadTabletHandler(server, extent, goal, requestTime));
  }

  @Override
  public void flush(TInfo tinfo, TCredentials credentials, String lock, String tableId,
      ByteBuffer startRow, ByteBuffer endRow) {
    try {
      checkPermission(credentials, lock, "flush");
    } catch (ThriftSecurityException e) {
      log.error("Caller doesn't have permission to flush a table", e);
      throw new RuntimeException(e);
    }

    ArrayList<Tablet> tabletsToFlush = new ArrayList<>();

    KeyExtent ke = new KeyExtent(TableId.of(tableId), ByteBufferUtil.toText(endRow),
        ByteBufferUtil.toText(startRow));

    for (Tablet tablet : server.getOnlineTablets().values()) {
      if (ke.overlaps(tablet.getExtent())) {
        tabletsToFlush.add(tablet);
      }
    }

    Long flushID = null;

    for (Tablet tablet : tabletsToFlush) {
      if (flushID == null) {
        // read the flush id once from zookeeper instead of reading
        // it for each tablet
        try {
          flushID = tablet.getFlushID();
        } catch (NoNodeException e) {
          // table was probably deleted
          log.info("Asked to flush table that has no flush id {} {}", ke, e.getMessage());
          return;
        }
      }
      tablet.flush(flushID);
    }
  }

  @Override
  public void flushTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent textent) {
    try {
      checkPermission(credentials, lock, "flushTablet");
    } catch (ThriftSecurityException e) {
      log.error("Caller doesn't have permission to flush a tablet", e);
      throw new RuntimeException(e);
    }

    Tablet tablet = server.getOnlineTablet(KeyExtent.fromThrift(textent));
    if (tablet != null) {
      log.info("Flushing {}", tablet.getExtent());
      try {
        tablet.flush(tablet.getFlushID());
      } catch (NoNodeException nne) {
        log.info("Asked to flush tablet that has no flush id {} {}", KeyExtent.fromThrift(textent),
            nne.getMessage());
      }
    }
  }

  @Override
  public void halt(TInfo tinfo, TCredentials credentials, String lock)
      throws ThriftSecurityException {

    checkPermission(credentials, lock, "halt");

    Halt.halt(0, () -> {
      log.info("Manager requested tablet server halt");
      server.gcLogger.logGCInfo(server.getConfiguration());
      server.requestStop();
      try {
        server.getLock().unlock();
      } catch (Exception e) {
        log.error("Caught exception unlocking TabletServer lock", e);
      }
    });
  }

  @Override
  public void fastHalt(TInfo info, TCredentials credentials, String lock) {
    try {
      halt(info, credentials, lock);
    } catch (Exception e) {
      log.warn("Error halting", e);
    }
  }

  @Override
  public TabletStats getHistoricalStats(TInfo tinfo, TCredentials credentials) {
    return server.statsKeeper.getTabletStats();
  }

  @Override
  public List<ActiveScan> getActiveScans(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    try {
      checkPermission(credentials, null, "getScans");
    } catch (ThriftSecurityException e) {
      log.error("Caller doesn't have permission to get active scans", e);
      throw e;
    }

    return server.sessionManager.getActiveScans();
  }

  @Override
  public void chop(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent textent) {
    try {
      checkPermission(credentials, lock, "chop");
    } catch (ThriftSecurityException e) {
      log.error("Caller doesn't have permission to chop extent", e);
      throw new RuntimeException(e);
    }

    KeyExtent ke = KeyExtent.fromThrift(textent);

    Tablet tablet = server.getOnlineTablet(ke);
    if (tablet != null) {
      tablet.chopFiles();
    }
  }

  @Override
  public void compact(TInfo tinfo, TCredentials credentials, String lock, String tableId,
      ByteBuffer startRow, ByteBuffer endRow) {
    try {
      checkPermission(credentials, lock, "compact");
    } catch (ThriftSecurityException e) {
      log.error("Caller doesn't have permission to compact a table", e);
      throw new RuntimeException(e);
    }

    KeyExtent ke = new KeyExtent(TableId.of(tableId), ByteBufferUtil.toText(endRow),
        ByteBufferUtil.toText(startRow));

    Pair<Long,CompactionConfig> compactionInfo = null;

    for (Tablet tablet : server.getOnlineTablets().values()) {
      if (ke.overlaps(tablet.getExtent())) {
        // all for the same table id, so only need to read
        // compaction id once
        if (compactionInfo == null) {
          try {
            compactionInfo = tablet.getCompactionID();
          } catch (NoNodeException e) {
            log.info("Asked to compact table with no compaction id {} {}", ke, e.getMessage());
            return;
          }
        }
        tablet.compactAll(compactionInfo.getFirst(), compactionInfo.getSecond());
      }
    }
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    try {
      checkPermission(credentials, null, "getActiveCompactions");
    } catch (ThriftSecurityException e) {
      log.error("Caller doesn't have permission to get active compactions", e);
      throw e;
    }

    List<CompactionInfo> compactions = FileCompactor.getRunningCompactions();
    List<ActiveCompaction> ret = new ArrayList<>(compactions.size());

    for (CompactionInfo compactionInfo : compactions) {
      ret.add(compactionInfo.toThrift());
    }

    return ret;
  }

  @Override
  public List<TCompactionQueueSummary> getCompactionQueueInfo(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {

    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    return server.getCompactionManager().getCompactionQueueSummaries();
  }

  @Override
  public TExternalCompactionJob reserveCompactionJob(TInfo tinfo, TCredentials credentials,
      String queueName, long priority, String compactor, String externalCompactionId)
      throws ThriftSecurityException, TException {

    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    ExternalCompactionId eci = ExternalCompactionId.of(externalCompactionId);

    var extCompaction = server.getCompactionManager().reserveExternalCompaction(queueName, priority,
        compactor, eci);

    if (extCompaction != null) {
      return extCompaction.toThrift();
    }

    return new TExternalCompactionJob();
  }

  @Override
  public void compactionJobFinished(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent extent, long fileSize, long entries)
      throws ThriftSecurityException, TException {

    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    server.getCompactionManager().commitExternalCompaction(
        ExternalCompactionId.of(externalCompactionId), KeyExtent.fromThrift(extent),
        server.getOnlineTablets(), fileSize, entries);
  }

  @Override
  public void compactionJobFailed(TInfo tinfo, TCredentials credentials,
      String externalCompactionId, TKeyExtent extent) throws TException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    server.getCompactionManager().externalCompactionFailed(
        ExternalCompactionId.of(externalCompactionId), KeyExtent.fromThrift(extent),
        server.getOnlineTablets());
  }

  @Override
  public List<String> getActiveLogs(TInfo tinfo, TCredentials credentials) {
    String log = server.logger.getLogFile();
    // Might be null if there no active logger
    if (log == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(log);
  }

  @Override
  public void removeLogs(TInfo tinfo, TCredentials credentials, List<String> filenames) {
    log.warn("Garbage collector is attempting to remove logs through the tablet server");
    log.warn("This is probably because your file"
        + " Garbage Collector is an older version than your tablet servers.\n"
        + "Restart your file Garbage Collector.");
  }

  private TSummaries getSummaries(Future<SummaryCollection> future) throws TimeoutException {
    try {
      SummaryCollection sc =
          future.get(MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS, TimeUnit.MILLISECONDS);
      return sc.toThrift();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private TSummaries handleTimeout(long sessionId) {
    long timeout = server.getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
    server.sessionManager.removeIfNotAccessed(sessionId, timeout);
    return new TSummaries(false, sessionId, -1, -1, null);
  }

  private TSummaries startSummaryOperation(TCredentials credentials,
      Future<SummaryCollection> future) {
    try {
      return getSummaries(future);
    } catch (TimeoutException e) {
      long sid =
          server.sessionManager.createSession(new SummarySession(credentials, future), false);
      while (sid == 0) {
        server.sessionManager.removeSession(sid);
        sid = server.sessionManager.createSession(new SummarySession(credentials, future), false);
      }
      return handleTimeout(sid);
    }
  }

  @Override
  public TSummaries startGetSummaries(TInfo tinfo, TCredentials credentials,
      TSummaryRequest request)
      throws ThriftSecurityException, ThriftTableOperationException, TException {
    NamespaceId namespaceId;
    TableId tableId = TableId.of(request.getTableId());
    try {
      namespaceId = Tables.getNamespaceId(server.getContext(), tableId);
    } catch (TableNotFoundException e1) {
      throw new ThriftTableOperationException(tableId.canonical(), null, null,
          TableOperationExceptionType.NOTFOUND, null);
    }

    if (!security.canGetSummaries(credentials, tableId, namespaceId)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    ExecutorService es = server.resourceManager.getSummaryPartitionExecutor();
    var tableConf = context.getTableConfiguration(tableId);
    Future<SummaryCollection> future =
        new Gatherer(server.getContext(), request, tableConf, tableConf.getCryptoService())
            .gather(es);

    return startSummaryOperation(credentials, future);
  }

  @Override
  public TSummaries startGetSummariesForPartition(TInfo tinfo, TCredentials credentials,
      TSummaryRequest request, int modulus, int remainder)
      throws ThriftSecurityException, TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    ExecutorService spe = server.resourceManager.getSummaryRemoteExecutor();
    TableConfiguration tableConfig =
        context.getTableConfiguration(TableId.of(request.getTableId()));
    Future<SummaryCollection> future =
        new Gatherer(server.getContext(), request, tableConfig, tableConfig.getCryptoService())
            .processPartition(spe, modulus, remainder);

    return startSummaryOperation(credentials, future);
  }

  @Override
  public TSummaries startGetSummariesFromFiles(TInfo tinfo, TCredentials credentials,
      TSummaryRequest request, Map<String,List<TRowRange>> files)
      throws ThriftSecurityException, TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    ExecutorService srp = server.resourceManager.getSummaryRetrievalExecutor();
    TableConfiguration tableCfg = context.getTableConfiguration(TableId.of(request.getTableId()));
    BlockCache summaryCache = server.resourceManager.getSummaryCache();
    BlockCache indexCache = server.resourceManager.getIndexCache();
    Cache<String,Long> fileLenCache = server.resourceManager.getFileLenCache();
    VolumeManager fs = context.getVolumeManager();
    FileSystemResolver volMgr = fs::getFileSystemByPath;
    Future<SummaryCollection> future =
        new Gatherer(server.getContext(), request, tableCfg, tableCfg.getCryptoService())
            .processFiles(volMgr, files, summaryCache, indexCache, fileLenCache, srp);

    return startSummaryOperation(credentials, future);
  }

  @Override
  public TSummaries contiuneGetSummaries(TInfo tinfo, long sessionId)
      throws NoSuchScanIDException, TException {
    SummarySession session = (SummarySession) server.sessionManager.getSession(sessionId);
    if (session == null) {
      throw new NoSuchScanIDException();
    }

    Future<SummaryCollection> future = session.getFuture();
    try {
      TSummaries tsums = getSummaries(future);
      server.sessionManager.removeSession(sessionId);
      return tsums;
    } catch (TimeoutException e) {
      return handleTimeout(sessionId);
    }
  }
}

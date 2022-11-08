/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver;

import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.TabletType;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TColumn;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.scan.ScanDispatcher;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.ScanServerBusyException;
import org.apache.accumulo.core.tabletserver.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.TabletScanClientService;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.TooManyFilesException;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.tserver.scan.LookupTask;
import org.apache.accumulo.tserver.scan.NextBatchTask;
import org.apache.accumulo.tserver.scan.ScanParameters;
import org.apache.accumulo.tserver.session.MultiScanSession;
import org.apache.accumulo.tserver.session.ScanSession;
import org.apache.accumulo.tserver.session.ScanSession.TabletResolver;
import org.apache.accumulo.tserver.session.SingleScanSession;
import org.apache.accumulo.tserver.tablet.KVEntry;
import org.apache.accumulo.tserver.tablet.ScanBatch;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;

public class ThriftScanClientHandler implements TabletScanClientService.Iface {

  private static final Logger log = LoggerFactory.getLogger(ThriftScanClientHandler.class);

  private final TabletHostingServer server;
  protected final ServerContext context;
  protected final SecurityOperation security;
  private final WriteTracker writeTracker;
  private final long MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS;

  public ThriftScanClientHandler(TabletHostingServer server, WriteTracker writeTracker) {
    this.server = server;
    this.context = server.getContext();
    this.writeTracker = writeTracker;
    this.security = context.getSecurityOperation();
    MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS = server.getContext().getConfiguration()
        .getTimeInMillis(Property.TSERV_SCAN_RESULTS_MAX_TIMEOUT);
  }

  private NamespaceId getNamespaceId(TCredentials credentials, TableId tableId)
      throws ThriftSecurityException {
    try {
      return server.getContext().getNamespaceId(tableId);
    } catch (TableNotFoundException e1) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.TABLE_DOESNT_EXIST);
    }
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
      long batchTimeOut, String contextArg, Map<String,String> executionHints, long busyTimeout)
      throws NotServingTabletException, ThriftSecurityException,
      org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException,
      TSampleNotPresentException, ScanServerBusyException {
    final KeyExtent extent = KeyExtent.fromThrift(textent);
    TabletResolver resolver = new TabletResolver() {
      @Override
      public Tablet getTablet(KeyExtent extent) {
        return server.getOnlineTablet(extent);
      }

      @Override
      public void close() {}
    };
    return this.startScan(tinfo, credentials, extent, range, columns, batchSize, ssiList, ssio,
        authorizations, waitForWrites, isolated, readaheadThreshold, tSamplerConfig, batchTimeOut,
        contextArg, executionHints, resolver, busyTimeout);
  }

  public InitialScan startScan(TInfo tinfo, TCredentials credentials, KeyExtent extent,
      TRange range, List<TColumn> columns, int batchSize, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      boolean isolated, long readaheadThreshold, TSamplerConfiguration tSamplerConfig,
      long batchTimeOut, String contextArg, Map<String,String> executionHints,
      ScanSession.TabletResolver tabletResolver, long busyTimeout) throws NotServingTabletException,
      ThriftSecurityException, org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException,
      TSampleNotPresentException, ScanServerBusyException {

    server.getScanMetrics().incrementStartScan(1.0D);

    TableId tableId = extent.tableId();
    NamespaceId namespaceId;
    try {
      namespaceId = server.getContext().getNamespaceId(tableId);
    } catch (TableNotFoundException e1) {
      throw new NotServingTabletException(extent.toThrift());
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

    TabletBase tablet = tabletResolver.getTablet(extent);
    if (tablet == null) {
      throw new NotServingTabletException(extent.toThrift());
    }

    HashSet<Column> columnSet = new HashSet<>();
    for (TColumn tcolumn : columns) {
      columnSet.add(new Column(tcolumn));
    }

    ScanParameters scanParams = new ScanParameters(batchSize, new Authorizations(authorizations),
        columnSet, ssiList, ssio, isolated, SamplerConfigurationImpl.fromThrift(tSamplerConfig),
        batchTimeOut, contextArg);

    final SingleScanSession scanSession = new SingleScanSession(credentials, extent, scanParams,
        readaheadThreshold, executionHints, tabletResolver);
    scanSession.scanner =
        tablet.createScanner(new Range(range), scanParams, scanSession.interruptFlag);

    long sid = server.getSessionManager().createSession(scanSession, true);

    ScanResult scanResult;
    try {
      scanResult = continueScan(tinfo, sid, scanSession, busyTimeout);
    } catch (NoSuchScanIDException e) {
      log.error("The impossible happened", e);
      throw new RuntimeException();
    } finally {
      server.getSessionManager().unreserveSession(sid);
    }

    return new InitialScan(sid, scanResult);
  }

  @Override
  public ScanResult continueScan(TInfo tinfo, long scanID, long busyTimeout)
      throws NoSuchScanIDException, NotServingTabletException,
      org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException,
      TSampleNotPresentException, ScanServerBusyException {
    SingleScanSession scanSession =
        (SingleScanSession) server.getSessionManager().reserveSession(scanID);
    if (scanSession == null) {
      throw new NoSuchScanIDException();
    }

    try {
      return continueScan(tinfo, scanID, scanSession, busyTimeout);
    } finally {
      server.getSessionManager().unreserveSession(scanSession);
    }
  }

  protected ScanResult continueScan(TInfo tinfo, long scanID, SingleScanSession scanSession,
      long busyTimeout) throws NoSuchScanIDException, NotServingTabletException,
      org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException,
      TSampleNotPresentException, ScanServerBusyException {

    server.getScanMetrics().incrementContinueScan(1.0D);

    if (scanSession.nextBatchTask == null) {
      scanSession.nextBatchTask = new NextBatchTask(server, scanID, scanSession.interruptFlag);
      server.getResourceManager().executeReadAhead(scanSession.extent,
          getScanDispatcher(scanSession.extent), scanSession, scanSession.nextBatchTask);
    }

    ScanBatch bresult;
    try {
      bresult = scanSession.nextBatchTask.get(busyTimeout, MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS,
          TimeUnit.MILLISECONDS);
      scanSession.nextBatchTask = null;
    } catch (ExecutionException e) {
      server.getSessionManager().removeSession(scanID);
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
      server.getSessionManager().removeSession(scanID);
      TabletBase tablet = scanSession.getTabletResolver().getTablet(scanSession.extent);
      if (busyTimeout > 0) {
        server.getScanMetrics().incrementScanBusyTimeout(1.0D);
        throw new ScanServerBusyException();
      } else if (tablet == null || tablet.isClosed()) {
        throw new NotServingTabletException(scanSession.extent.toThrift());
      } else {
        throw new NoSuchScanIDException();
      }
    } catch (TimeoutException e) {
      List<TKeyValue> param = Collections.emptyList();
      long timeout = server.getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
      server.getSessionManager().removeIfNotAccessed(scanID, timeout);
      return new ScanResult(param, true);
    } catch (Exception t) {
      server.getSessionManager().removeSession(scanID);
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
      server.getResourceManager().executeReadAhead(scanSession.extent,
          getScanDispatcher(scanSession.extent), scanSession, scanSession.nextBatchTask);
    }

    if (!scanResult.more) {
      closeScan(tinfo, scanID);
    }

    return scanResult;
  }

  @Override
  public void closeScan(TInfo tinfo, long scanID) {

    server.getScanMetrics().incrementCloseScan(1.0D);

    final SingleScanSession ss =
        (SingleScanSession) server.getSessionManager().removeSession(scanID);
    if (ss != null) {
      long t2 = System.currentTimeMillis();

      if (log.isTraceEnabled()) {
        log.trace(String.format("ScanSess tid %s %s %,d entries in %.2f secs, nbTimes = [%s] ",
            TServerUtils.clientAddress.get(), ss.extent.tableId(), ss.entriesReturned,
            (t2 - ss.startTime) / 1000.0, ss.runStats.toString()));
      }

      server.getScanMetrics().addScan(t2 - ss.startTime);
      server.getScanMetrics().addResult(ss.entriesReturned);
    }
  }

  @Override
  public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials,
      Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns, List<IterInfo> ssiList,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      TSamplerConfiguration tSamplerConfig, long batchTimeOut, String contextArg,
      Map<String,String> executionHints, long busyTimeout)
      throws ThriftSecurityException, TSampleNotPresentException, ScanServerBusyException {

    final Map<KeyExtent,List<TRange>> batch = new HashMap<>();
    tbatch.forEach((k, v) -> {
      batch.put(KeyExtent.fromThrift(k), v);
    });
    TabletResolver resolver = new TabletResolver() {
      @Override
      public Tablet getTablet(KeyExtent extent) {
        return server.getOnlineTablet(extent);
      }

      @Override
      public void close() {}
    };
    return this.startMultiScan(tinfo, credentials, tcolumns, ssiList, batch, ssio, authorizations,
        waitForWrites, tSamplerConfig, batchTimeOut, contextArg, executionHints, resolver,
        busyTimeout);
  }

  public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials,
      List<TColumn> tcolumns, List<IterInfo> ssiList, Map<KeyExtent,List<TRange>> tbatch,
      Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
      TSamplerConfiguration tSamplerConfig, long batchTimeOut, String contextArg,
      Map<String,String> executionHints, ScanSession.TabletResolver tabletResolver,
      long busyTimeout)
      throws ThriftSecurityException, TSampleNotPresentException, ScanServerBusyException {

    server.getScanMetrics().incrementStartScan(1.0D);

    // find all of the tables that need to be scanned
    final HashSet<TableId> tables = new HashSet<>();
    for (KeyExtent keyExtent : tbatch.keySet()) {
      tables.add(keyExtent.tableId());
    }

    if (tables.size() != 1) {
      throw new IllegalArgumentException("Cannot batch scan over multiple tables");
    }

    // check if user has permission to the tables
    for (TableId tableId : tables) {
      NamespaceId namespaceId = getNamespaceId(credentials, tableId);
      if (!security.canScan(credentials, tableId, namespaceId)) {
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
                    entry -> entry.getKey(),
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

    final MultiScanSession mss = new MultiScanSession(credentials, threadPoolExtent, batch,
        scanParams, executionHints, tabletResolver);

    mss.numTablets = batch.size();
    for (List<Range> ranges : batch.values()) {
      mss.numRanges += ranges.size();
    }

    long sid = server.getSessionManager().createSession(mss, true);

    MultiScanResult result;
    try {
      result = continueMultiScan(sid, mss, busyTimeout);
    } finally {
      server.getSessionManager().unreserveSession(sid);
    }

    return new InitialMultiScan(sid, result);
  }

  @Override
  public MultiScanResult continueMultiScan(TInfo tinfo, long scanID, long busyTimeout)
      throws NoSuchScanIDException, TSampleNotPresentException, ScanServerBusyException {

    MultiScanSession session = (MultiScanSession) server.getSessionManager().reserveSession(scanID);

    if (session == null) {
      throw new NoSuchScanIDException();
    }

    try {
      return continueMultiScan(scanID, session, busyTimeout);
    } finally {
      server.getSessionManager().unreserveSession(session);
    }
  }

  private MultiScanResult continueMultiScan(long scanID, MultiScanSession session, long busyTimeout)
      throws TSampleNotPresentException, ScanServerBusyException {

    server.getScanMetrics().incrementContinueScan(1.0D);

    if (session.lookupTask == null) {
      session.lookupTask = new LookupTask(server, scanID);
      server.getResourceManager().executeReadAhead(session.threadPoolExtent,
          getScanDispatcher(session.threadPoolExtent), session, session.lookupTask);
    }

    try {

      MultiScanResult scanResult = session.lookupTask.get(busyTimeout,
          MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS, TimeUnit.MILLISECONDS);
      session.lookupTask = null;
      return scanResult;
    } catch (ExecutionException e) {
      server.getSessionManager().removeSession(scanID);
      if (e.getCause() instanceof SampleNotPresentException) {
        throw new TSampleNotPresentException();
      } else {
        log.warn("Failed to get multiscan result", e);
        throw new RuntimeException(e);
      }
    } catch (CancellationException ce) {
      server.getSessionManager().removeSession(scanID);
      if (busyTimeout > 0) {
        server.getScanMetrics().incrementScanBusyTimeout(1.0D);
        throw new ScanServerBusyException();
      } else {
        log.warn("Failed to get multiscan result", ce);
        throw new RuntimeException(ce);
      }
    } catch (TimeoutException e1) {
      long timeout = server.getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
      server.getSessionManager().removeIfNotAccessed(scanID, timeout);
      List<TKeyValue> results = Collections.emptyList();
      Map<TKeyExtent,List<TRange>> failures = Collections.emptyMap();
      List<TKeyExtent> fullScans = Collections.emptyList();
      return new MultiScanResult(results, failures, fullScans, null, null, false, true);
    } catch (Exception t) {
      server.getSessionManager().removeSession(scanID);
      log.warn("Failed to get multiscan result", t);
      throw new RuntimeException(t);
    }
  }

  @Override
  public void closeMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException {

    server.getScanMetrics().incrementCloseScan(1.0D);

    MultiScanSession session = (MultiScanSession) server.getSessionManager().removeSession(scanID);
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
  public List<ActiveScan> getActiveScans(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    try {
      TabletClientHandler.checkPermission(security, context, server, credentials, null, "getScans");
    } catch (ThriftSecurityException e) {
      log.error("Caller doesn't have permission to get active scans", e);
      throw e;
    }

    return server.getSessionManager().getActiveScans();
  }

}

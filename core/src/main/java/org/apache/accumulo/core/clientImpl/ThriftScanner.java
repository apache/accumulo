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
package org.apache.accumulo.core.clientImpl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.InvalidTabletHostingRequestException;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.ClientTabletCache.CachedTablet;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.spi.scan.ScanServerAttempt;
import org.apache.accumulo.core.spi.scan.ScanServerSelections;
import org.apache.accumulo.core.spi.scan.ScanServerSelector;
import org.apache.accumulo.core.tabletscan.thrift.ScanServerBusyException;
import org.apache.accumulo.core.tabletscan.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletscan.thrift.TabletScanClientService;
import org.apache.accumulo.core.tabletscan.thrift.TooManyFilesException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Retry;
import org.apache.accumulo.core.util.Timer;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;

public class ThriftScanner {
  private static final Logger log = LoggerFactory.getLogger(ThriftScanner.class);

  // This set is initially empty when the client starts. The first time this
  // client contacts a server it will wait for any writes that are in progress.
  // This is to account for the case where a client may have sent writes
  // to accumulo and dies while waiting for a confirmation from
  // accumulo. The client process restarts and tries to read
  // data from accumulo making the assumption that it will get
  // any writes previously made, however if the server side thread
  // processing the write from the dead client is still in progress,
  // the restarted client may not see the write unless we wait here.
  // this behavior is very important when the client is reading the
  // metadata
  public static final Map<TabletType,Set<String>> serversWaitedForWrites =
      new EnumMap<>(TabletType.class);

  static {
    for (TabletType ttype : TabletType.values()) {
      serversWaitedForWrites.put(ttype, Collections.synchronizedSet(new HashSet<>()));
    }
  }

  public static boolean getBatchFromServer(ClientContext context, Range range, KeyExtent extent,
      String server, SortedMap<Key,Value> results, SortedSet<Column> fetchedColumns,
      List<IterInfo> serverSideIteratorList,
      Map<String,Map<String,String>> serverSideIteratorOptions, int size,
      Authorizations authorizations, long batchTimeOut, String classLoaderContext)
      throws AccumuloException, AccumuloSecurityException {
    if (server == null) {
      throw new AccumuloException(new IOException());
    }

    final HostAndPort parsedServer = HostAndPort.fromString(server);
    try {
      TInfo tinfo = TraceUtil.traceInfo();
      TabletScanClientService.Client client =
          ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN, parsedServer, context);
      try {
        // not reading whole rows (or stopping on row boundaries) so there is no need to enable
        // isolation below
        ScanState scanState = new ScanState(context, extent.tableId(), authorizations, range,
            fetchedColumns, size, serverSideIteratorList, serverSideIteratorOptions, false,
            Constants.SCANNER_DEFAULT_READAHEAD_THRESHOLD, null, batchTimeOut, classLoaderContext,
            null, false);

        TabletType ttype = TabletType.type(extent);
        boolean waitForWrites = !serversWaitedForWrites.get(ttype).contains(server);
        InitialScan isr = client.startScan(tinfo, scanState.context.rpcCreds(), extent.toThrift(),
            scanState.range.toThrift(),
            scanState.columns.stream().map(Column::toThrift).collect(Collectors.toList()),
            scanState.size, scanState.serverSideIteratorList, scanState.serverSideIteratorOptions,
            scanState.authorizations.getAuthorizationsBB(), waitForWrites, scanState.isolated,
            scanState.readaheadThreshold, null, scanState.batchTimeOut, classLoaderContext,
            scanState.executionHints, 0L);
        if (waitForWrites) {
          serversWaitedForWrites.get(ttype).add(server);
        }

        Key.decompress(isr.result.results);

        for (TKeyValue kv : isr.result.results) {
          results.put(new Key(kv.key), new Value(kv.value));
        }

        client.closeScan(tinfo, isr.scanID);

        return isr.result.more;
      } finally {
        ThriftUtil.returnClient(client, context);
      }
    } catch (TApplicationException tae) {
      throw new AccumuloServerException(server, tae);
    } catch (TooManyFilesException e) {
      log.debug("Tablet ({}) has too many files {} : {}", extent, server, e.getMessage());
    } catch (ThriftSecurityException e) {
      log.warn("Security Violation in scan request to {}: {}", server, e.getMessage());
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      log.debug("Error getting transport to {}: {}", server, e.getMessage());
    }

    throw new AccumuloException("getBatchFromServer: failed");
  }

  enum ServerType {
    TSERVER, SSERVER
  }

  static class ScanAddress {
    final String serverAddress;
    final ServerType serverType;
    final CachedTablet tabletInfo;

    public ScanAddress(String serverAddress, ServerType serverType, CachedTablet tabletInfo) {
      this.serverAddress = Objects.requireNonNull(serverAddress);
      this.serverType = Objects.requireNonNull(serverType);
      this.tabletInfo = Objects.requireNonNull(tabletInfo);
    }

    public KeyExtent getExtent() {
      return tabletInfo.getExtent();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ScanAddress that = (ScanAddress) o;
      return serverAddress.equals(that.serverAddress) && serverType == that.serverType
          && getExtent().equals(that.getExtent());
    }

    @Override
    public int hashCode() {
      return Objects.hash(serverAddress, serverType, tabletInfo);
    }
  }

  public static class ScanState {

    boolean isolated;
    TableId tableId;
    Text startRow;
    boolean skipStartRow;
    long readaheadThreshold;
    long batchTimeOut;
    boolean runOnScanServer;

    Range range;

    int size;

    ClientContext context;
    Authorizations authorizations;
    List<Column> columns;

    ScanAddress prevLoc;
    Long scanID;

    String classLoaderContext;

    boolean finished = false;

    List<IterInfo> serverSideIteratorList;

    Map<String,Map<String,String>> serverSideIteratorOptions;

    SamplerConfiguration samplerConfig;
    Map<String,String> executionHints;

    ScanServerAttemptsImpl scanAttempts;

    Duration busyTimeout;

    int tabletsScanned;

    KeyExtent prevExtent = null;

    public ScanState(ClientContext context, TableId tableId, Authorizations authorizations,
        Range range, SortedSet<Column> fetchedColumns, int size,
        List<IterInfo> serverSideIteratorList,
        Map<String,Map<String,String>> serverSideIteratorOptions, boolean isolated,
        long readaheadThreshold, SamplerConfiguration samplerConfig, long batchTimeOut,
        String classLoaderContext, Map<String,String> executionHints, boolean useScanServer) {
      this.context = context;
      this.authorizations = authorizations;
      this.classLoaderContext = classLoaderContext;

      columns = new ArrayList<>(fetchedColumns.size());
      for (Column column : fetchedColumns) {
        columns.add(column);
      }

      this.tableId = tableId;
      this.range = range;

      Key startKey = range.getStartKey();
      if (startKey == null) {
        startKey = new Key();
      }
      this.startRow = startKey.getRow();

      this.skipStartRow = false;

      this.size = size;

      this.serverSideIteratorList = serverSideIteratorList;
      this.serverSideIteratorOptions = serverSideIteratorOptions;

      this.isolated = isolated;
      this.readaheadThreshold = readaheadThreshold;

      this.samplerConfig = samplerConfig;

      this.batchTimeOut = batchTimeOut;

      if (executionHints == null || executionHints.isEmpty()) {
        this.executionHints = null; // avoid thrift serialization for empty map
      } else {
        this.executionHints = executionHints;
      }

      this.runOnScanServer = useScanServer;

      if (useScanServer) {
        scanAttempts = new ScanServerAttemptsImpl();
      }

      this.tabletsScanned = 0;
    }

    long startTimeNanos = 0;
    long getNextScanAddressTimeNanos = 0;

    public void incrementTabletsScanned(KeyExtent extent) {
      if (!extent.equals(prevExtent)) {
        tabletsScanned++;
        prevExtent = extent;
      }
    }
  }

  static <T> Optional<T> waitUntil(Supplier<Optional<T>> condition, Duration maxWaitTime,
      String description, Duration timeoutLeft, ClientContext context, TableId tableId,
      Logger log) {

    Retry retry = Retry.builder().infiniteRetries().retryAfter(Duration.ofMillis(100))
        .incrementBy(Duration.ofMillis(100)).maxWait(Duration.ofSeconds(1)).backOffFactor(1.5)
        .logInterval(Duration.ofMinutes(3)).createRetry();

    long startTime = System.nanoTime();
    Optional<T> optional = condition.get();
    while (optional.isEmpty()) {
      log.trace("For tableId {} scan server selector is waiting for '{}'", tableId, description);

      var elapsedTime = Duration.ofNanos(System.nanoTime() - startTime);

      if (elapsedTime.compareTo(timeoutLeft) > 0) {
        throw new TimedOutException("While waiting for '" + description
            + "' in order to select a scan server, the scan timed out. ");
      }

      if (elapsedTime.compareTo(maxWaitTime) > 0) {
        return Optional.empty();
      }

      context.requireNotDeleted(tableId);

      try {
        retry.waitForNextAttempt(log, String.format(
            "For tableId %s scan server selector is waiting for '%s'", tableId, description));
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      optional = condition.get();
    }

    return optional;
  }

  public static class ScanTimedOutException extends TimedOutException {

    private static final long serialVersionUID = 1L;

    public ScanTimedOutException(String msg) {
      super(msg);
    }
  }

  static long pause(long millis, long maxSleep, boolean runOnScanServer)
      throws InterruptedException {
    if (!runOnScanServer) {
      // the client side scan server plugin controls sleep time... this sleep is for regular scans
      // where the scan server plugin does not have control
      Thread.sleep(millis);
    }
    // wait 2 * last time, with +-10% random jitter
    return (long) (Math.min(millis * 2, maxSleep) * (.9 + RANDOM.get().nextDouble() / 5));
  }

  private static Optional<ScanAddress> getScanServerAddress(ClientContext context,
      ScanState scanState, CachedTablet loc, long timeOut, long startTime) {
    Preconditions.checkArgument(scanState.runOnScanServer);

    ScanAddress addr = null;

    if (scanState.scanID != null && scanState.prevLoc != null
        && scanState.prevLoc.serverType == ServerType.SSERVER
        && scanState.prevLoc.getExtent().equals(loc.getExtent())) {
      // this is the case of continuing a scan on a scan server for the same tablet, so lets not
      // call the scan server selector and just go back to the previous scan server
      addr = scanState.prevLoc;
      log.trace(
          "For tablet {} continuing scan on scan server {} without consulting scan server selector, using busyTimeout {}",
          loc.getExtent(), addr.serverAddress, scanState.busyTimeout);
    } else {
      var tabletId = new TabletIdImpl(loc.getExtent());
      // obtain a snapshot once and only expose this snapshot to the plugin for consistency
      var attempts = scanState.scanAttempts.snapshot();

      Duration timeoutLeft = Duration.ofSeconds(timeOut)
          .minus(Duration.ofMillis(System.currentTimeMillis() - startTime));

      var params = new ScanServerSelector.SelectorParameters() {

        @Override
        public List<TabletId> getTablets() {
          return List.of(tabletId);
        }

        @Override
        public Collection<? extends ScanServerAttempt> getAttempts(TabletId tabletId) {
          return attempts.getOrDefault(tabletId, Set.of());
        }

        @Override
        public Map<String,String> getHints() {
          if (scanState.executionHints == null) {
            return Map.of();
          }
          return scanState.executionHints;
        }

        @Override
        public <T> Optional<T> waitUntil(Supplier<Optional<T>> condition, Duration maxWaitTime,
            String description) {
          return ThriftScanner.waitUntil(condition, maxWaitTime, description, timeoutLeft, context,
              loc.getExtent().tableId(), log);
        }
      };

      ScanServerSelections actions = context.getScanServerSelector().selectServers(params);

      Duration delay = null;

      String scanServer = actions.getScanServer(tabletId);
      if (scanServer != null) {
        addr = new ScanAddress(scanServer, ServerType.SSERVER, loc);
        delay = actions.getDelay();
        scanState.busyTimeout = actions.getBusyTimeout();
        log.trace("For tablet {} scan server selector chose scan_server:{} delay:{} busyTimeout:{}",
            loc.getExtent(), scanServer, delay, scanState.busyTimeout);
      } else {
        Optional<String> tserverLoc = loc.getTserverLocation();

        if (tserverLoc.isPresent()) {
          addr = new ScanAddress(loc.getTserverLocation().orElseThrow(), ServerType.TSERVER, loc);
          delay = actions.getDelay();
          scanState.busyTimeout = Duration.ZERO;
          log.trace("For tablet {} scan server selector chose tablet_server: {}", loc.getExtent(),
              addr);
        } else {
          log.trace(
              "For tablet {} scan server selector chose tablet_server, but the tablet is not currently hosted",
              loc.getExtent());
          return Optional.empty();
        }
      }

      if (!delay.isZero()) {
        try {
          Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }

    return Optional.of(addr);
  }

  /**
   * @see ClientTabletCache#findTablet(ClientContext, Text, boolean, ClientTabletCache.LocationNeed,
   *      int, Range)
   */
  private static int computeMinimumHostAhead(ScanState scanState,
      ClientTabletCache.LocationNeed hostingNeed) {
    int minimumHostAhead = 0;

    if (hostingNeed == ClientTabletCache.LocationNeed.REQUIRED) {
      long currTime = System.nanoTime();

      double timeRatio = 0;

      long totalTime = currTime - scanState.startTimeNanos;
      if (totalTime > 0) {
        // The following computes (total time spent in this method)/(total time scanning and time
        // spent in this method)
        timeRatio = (double) scanState.getNextScanAddressTimeNanos / (double) (totalTime);
      }

      if (timeRatio < 0 || timeRatio > 1) {
        log.warn("Computed ratio of time spent in getNextScanAddress has unexpected value : {} ",
            timeRatio);
        timeRatio = Math.max(0.0, Math.min(1.0, timeRatio));
      }

      //
      // Do not want to host all tablets in the scan range in case not all data in the range is
      // read. Need to determine how many tablets to host ahead of time. The following information
      // is used to determine how many tablets to host ahead of time.
      //
      // 1. The number of tablets this scan has already read. The more tablets that this scan has
      // read, the more likely that it will read many more tablets.
      //
      // 2. The timeRatio computed above. As timeRatio approaches 1.0 it means we are spending
      // most of our time waiting on the next tablet to have an address. When are spending most of
      // our time waiting for a tablet to have an address we want to increase the number of tablets
      // we request to host ahead of time so we can hopefully spend less time waiting on that.
      //

      if (timeRatio > .9) {
        minimumHostAhead = 16;
      } else if (timeRatio > .75) {
        minimumHostAhead = 8;
      } else if (timeRatio > .5) {
        minimumHostAhead = 4;
      } else if (timeRatio > .25) {
        minimumHostAhead = 2;
      } else {
        minimumHostAhead = 1;
      }

      minimumHostAhead = Math.min(scanState.tabletsScanned, minimumHostAhead);
    }
    return minimumHostAhead;
  }

  static ScanAddress getNextScanAddress(ClientContext context, ScanState scanState, long timeOut,
      long startTime, long maxSleepTime)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloServerException,
      InterruptedException, ScanTimedOutException, InvalidTabletHostingRequestException {

    String lastError = null;
    String error = null;
    long sleepMillis = 100;

    ScanAddress addr = null;

    var hostingNeed = scanState.runOnScanServer ? ClientTabletCache.LocationNeed.NOT_REQUIRED
        : ClientTabletCache.LocationNeed.REQUIRED;

    int minimumHostAhead = computeMinimumHostAhead(scanState, hostingNeed);

    while (addr == null) {
      long currentTime = System.currentTimeMillis();
      if ((currentTime - startTime) / 1000.0 > timeOut) {
        throw new ScanTimedOutException("Failed to locate next server to scan before timeout");
      }

      CachedTablet loc = null;

      Span child1 = TraceUtil.startSpan(ThriftScanner.class, "scan::locateTablet");
      try (Scope locateSpan = child1.makeCurrent()) {

        loc = ClientTabletCache.getInstance(context, scanState.tableId).findTablet(context,
            scanState.startRow, scanState.skipStartRow, hostingNeed, minimumHostAhead,
            scanState.range);

        if (loc == null) {
          context.requireNotDeleted(scanState.tableId);
          context.requireNotOffline(scanState.tableId, null);

          error = "Failed to locate tablet for table : " + scanState.tableId + " row : "
              + scanState.startRow;
          if (!error.equals(lastError)) {
            log.debug("{}", error);
          } else if (log.isTraceEnabled()) {
            log.trace("{}", error);
          }
          lastError = error;
          sleepMillis = pause(sleepMillis, maxSleepTime, scanState.runOnScanServer);
        } else {
          scanState.incrementTabletsScanned(loc.getExtent());

          // when a tablet splits we do want to continue scanning the low child
          // of the split if we are already passed it
          Range dataRange = loc.getExtent().toDataRange();

          if (scanState.range.getStartKey() != null
              && dataRange.afterEndKey(scanState.range.getStartKey())) {
            // go to the next tablet
            scanState.startRow = loc.getExtent().endRow();
            scanState.skipStartRow = true;
            // force another lookup
            loc = null;
          } else if (scanState.range.getEndKey() != null
              && dataRange.beforeStartKey(scanState.range.getEndKey())) {
            // should not happen
            throw new IllegalStateException("Unexpected tablet, extent : " + loc.getExtent()
                + "  range : " + scanState.range + " startRow : " + scanState.startRow);
          }
        }
      } catch (AccumuloServerException e) {
        TraceUtil.setException(child1, e, true);
        log.debug("Scan failed, server side exception : {}", e.getMessage());
        throw e;
      } catch (AccumuloException e) {
        error = "exception from tablet loc " + e.getMessage();
        if (!error.equals(lastError)) {
          log.debug("{}", error);
        } else if (log.isTraceEnabled()) {
          log.trace("{}", error);
        }

        TraceUtil.setException(child1, e, false);

        lastError = error;
        sleepMillis = pause(sleepMillis, maxSleepTime, scanState.runOnScanServer);
      } finally {
        child1.end();
      }

      if (loc != null) {
        if (scanState.runOnScanServer) {
          addr = getScanServerAddress(context, scanState, loc, timeOut, startTime).orElse(null);
          if (addr == null && loc.getTserverLocation().isEmpty()) {
            // wanted to fall back to tserver but tablet was not hosted so make another loop
            hostingNeed = ClientTabletCache.LocationNeed.REQUIRED;
          }
        } else {
          addr = new ScanAddress(loc.getTserverLocation().orElseThrow(), ServerType.TSERVER, loc);
        }
      }
    }

    return addr;
  }

  public static List<KeyValue> scan(ClientContext context, ScanState scanState, long timeOut)
      throws ScanTimedOutException, AccumuloException, AccumuloSecurityException,
      TableNotFoundException {

    long startTime = System.currentTimeMillis();
    String lastError = null;
    String error = null;
    int tooManyFilesCount = 0;
    long sleepMillis = 100;
    final long maxSleepTime =
        context.getConfiguration().getTimeInMillis(Property.GENERAL_MAX_SCANNER_RETRY_PERIOD);

    List<KeyValue> results = null;

    Span parent = TraceUtil.startSpan(ThriftScanner.class, "scan");
    try (Scope scope = parent.makeCurrent()) {
      while (results == null && !scanState.finished) {
        if (Thread.currentThread().isInterrupted()) {
          throw new AccumuloException("Thread interrupted");
        }

        if ((System.currentTimeMillis() - startTime) / 1000.0 > timeOut) {
          throw new ScanTimedOutException(
              "Failed to retrieve next batch of key values before timeout");
        }

        ScanAddress addr;
        long beginTime = System.nanoTime();
        try {
          addr = getNextScanAddress(context, scanState, timeOut, startTime, maxSleepTime);
        } finally {
          // track the initial time that we started tracking the time for getting the next scan
          // address
          if (scanState.startTimeNanos == 0) {
            scanState.startTimeNanos = beginTime;
          }

          // track the total amount of time spent getting the next scan address
          scanState.getNextScanAddressTimeNanos += System.nanoTime() - beginTime;
        }

        Span child2 = TraceUtil.startSpan(ThriftScanner.class, "scan::location",
            Map.of("tserver", addr.serverAddress));
        try (Scope scanLocation = child2.makeCurrent()) {
          results = scan(addr, scanState, context);
        } catch (AccumuloSecurityException e) {
          context.clearTableListCache();
          context.requireNotDeleted(scanState.tableId);
          e.setTableInfo(context.getPrintableTableInfoFromId(scanState.tableId));
          TraceUtil.setException(child2, e, true);
          throw e;
        } catch (TApplicationException tae) {
          TraceUtil.setException(child2, tae, true);
          throw new AccumuloServerException(addr.serverAddress, tae);
        } catch (TSampleNotPresentException tsnpe) {
          String message = "Table " + context.getPrintableTableInfoFromId(scanState.tableId)
              + " does not have sampling configured or built";
          TraceUtil.setException(child2, tsnpe, true);
          throw new SampleNotPresentException(message, tsnpe);
        } catch (NotServingTabletException e) {
          error = "Scan failed, not serving tablet " + addr.serverAddress;
          if (!error.equals(lastError)) {
            log.debug("{}", error);
          } else if (log.isTraceEnabled()) {
            log.trace("{}", error);
          }
          lastError = error;

          ClientTabletCache.getInstance(context, scanState.tableId)
              .invalidateCache(addr.getExtent());

          // no need to try the current scan id somewhere else
          scanState.scanID = null;

          if (scanState.isolated) {
            TraceUtil.setException(child2, e, true);
            throw new IsolationException();
          }

          TraceUtil.setException(child2, e, false);
          sleepMillis = pause(sleepMillis, maxSleepTime, scanState.runOnScanServer);
        } catch (ScanServerBusyException e) {
          error = "Scan failed, scan server was busy " + addr.serverAddress;
          if (!error.equals(lastError)) {
            log.debug("{}", error);
          } else if (log.isTraceEnabled()) {
            log.trace("{}", error);
          }
          lastError = error;

          if (scanState.isolated) {
            TraceUtil.setException(child2, e, true);
            throw new IsolationException();
          }

          TraceUtil.setException(child2, e, false);
          scanState.scanID = null;
        } catch (NoSuchScanIDException e) {
          error = "Scan failed, no such scan id " + scanState.scanID + " " + addr.serverAddress;
          if (!error.equals(lastError)) {
            log.debug("{}", error);
          } else if (log.isTraceEnabled()) {
            log.trace("{}", error);
          }
          lastError = error;

          if (scanState.isolated) {
            TraceUtil.setException(child2, e, true);
            throw new IsolationException();
          }

          TraceUtil.setException(child2, e, false);
          scanState.scanID = null;
        } catch (TooManyFilesException e) {
          error = "Tablet has too many files " + addr.serverAddress + " retrying...";
          if (error.equals(lastError)) {
            tooManyFilesCount++;
            if (tooManyFilesCount == 300) {
              log.warn("{}", error);
            } else if (log.isTraceEnabled()) {
              log.trace("{}", error);
            }
          } else {
            log.debug("{}", error);
            tooManyFilesCount = 0;
          }
          lastError = error;

          // not sure what state the scan session on the server side is
          // in after this occurs, so lets be cautious and start a new
          // scan session
          scanState.scanID = null;

          if (scanState.isolated) {
            TraceUtil.setException(child2, e, true);
            throw new IsolationException();
          }

          TraceUtil.setException(child2, e, false);
          sleepMillis = pause(sleepMillis, maxSleepTime, scanState.runOnScanServer);
        } catch (TException e) {
          if (addr.serverType == ServerType.TSERVER) {
            // only tsever locations are in cache, invalidating a scan server would not find
            // anything the cache
            ClientTabletCache.getInstance(context, scanState.tableId).invalidateCache(context,
                addr.serverAddress);
          }
          error = "Scan failed, thrift error " + e.getClass().getName() + "  " + e.getMessage()
              + " " + addr.serverAddress;
          if (!error.equals(lastError)) {
            log.debug("{}", error);
          } else if (log.isTraceEnabled()) {
            log.trace("{}", error);
          }
          lastError = error;

          // do not want to continue using the same scan id, if a timeout occurred could cause a
          // batch to be skipped
          // because a thread on the server side may still be processing the timed out continue scan
          scanState.scanID = null;

          if (scanState.isolated) {
            TraceUtil.setException(child2, e, true);
            throw new IsolationException();
          }

          TraceUtil.setException(child2, e, false);
          sleepMillis = pause(sleepMillis, maxSleepTime, scanState.runOnScanServer);
        } finally {
          child2.end();
        }
      }

      if (results != null && results.isEmpty() && scanState.finished) {
        results = null;
      }

      return results;
    } catch (InterruptedException ex) {
      TraceUtil.setException(parent, ex, true);
      throw new AccumuloException(ex);
    } catch (InvalidTabletHostingRequestException e) {
      TraceUtil.setException(parent, e, true);
      throw new AccumuloException(e);
    } finally {
      parent.end();
    }
  }

  private static List<KeyValue> scan(ScanAddress addr, ScanState scanState, ClientContext context)
      throws AccumuloSecurityException, NotServingTabletException, TException,
      NoSuchScanIDException, TooManyFilesException, TSampleNotPresentException {
    if (scanState.finished) {
      return null;
    }

    if (addr.serverType == ServerType.SSERVER) {
      try {
        return scanRpc(addr, scanState, context, scanState.busyTimeout.toMillis());
      } catch (ScanServerBusyException ssbe) {
        var reporter = scanState.scanAttempts.createReporter(addr.serverAddress,
            new TabletIdImpl(addr.getExtent()));
        reporter.report(ScanServerAttempt.Result.BUSY);
        throw ssbe;
      } catch (Exception e) {
        var reporter = scanState.scanAttempts.createReporter(addr.serverAddress,
            new TabletIdImpl(addr.getExtent()));
        reporter.report(ScanServerAttempt.Result.ERROR);
        throw e;
      }
    } else {
      return scanRpc(addr, scanState, context, 0L);
    }
  }

  private static List<KeyValue> scanRpc(ScanAddress addr, ScanState scanState,
      ClientContext context, long busyTimeout) throws AccumuloSecurityException,
      NotServingTabletException, TException, NoSuchScanIDException, TooManyFilesException,
      TSampleNotPresentException, ScanServerBusyException {

    Timer timer = null;

    final TInfo tinfo = TraceUtil.traceInfo();

    final HostAndPort parsedLocation = HostAndPort.fromString(addr.serverAddress);
    TabletScanClientService.Client client =
        ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN, parsedLocation, context);

    String old = Thread.currentThread().getName();
    try {
      ScanResult sr;

      if (scanState.prevLoc != null && !scanState.prevLoc.equals(addr)) {
        scanState.scanID = null;
      }

      scanState.prevLoc = addr;

      if (scanState.scanID == null) {
        Thread.currentThread().setName("Starting scan tserver=" + addr.serverAddress + " tableId="
            + addr.getExtent().tableId());

        if (log.isTraceEnabled()) {
          String msg = "Starting scan server=" + addr.serverAddress + " tablet=" + addr.getExtent()
              + " range=" + scanState.range + " ssil=" + scanState.serverSideIteratorList + " ssio="
              + scanState.serverSideIteratorOptions + " context=" + scanState.classLoaderContext;
          log.trace("tid={} {}", Thread.currentThread().getId(), msg);
          timer = Timer.startNew();
        }

        TabletType ttype = TabletType.type(addr.getExtent());
        boolean waitForWrites = !serversWaitedForWrites.get(ttype).contains(addr.serverAddress);

        InitialScan is = client.startScan(tinfo, scanState.context.rpcCreds(),
            addr.getExtent().toThrift(), scanState.range.toThrift(),
            scanState.columns.stream().map(Column::toThrift).collect(Collectors.toList()),
            scanState.size, scanState.serverSideIteratorList, scanState.serverSideIteratorOptions,
            scanState.authorizations.getAuthorizationsBB(), waitForWrites, scanState.isolated,
            scanState.readaheadThreshold,
            SamplerConfigurationImpl.toThrift(scanState.samplerConfig), scanState.batchTimeOut,
            scanState.classLoaderContext, scanState.executionHints, busyTimeout);
        if (waitForWrites) {
          serversWaitedForWrites.get(ttype).add(addr.serverAddress);
        }

        sr = is.result;

        if (sr.more) {
          scanState.scanID = is.scanID;
        } else {
          client.closeScan(tinfo, is.scanID);
        }

      } else {
        // log.debug("Calling continue scan : "+scanState.range+" loc = "+loc);
        String msg =
            "Continuing scan tserver=" + addr.serverAddress + " scanid=" + scanState.scanID;
        Thread.currentThread().setName(msg);

        if (log.isTraceEnabled()) {
          log.trace("tid={} {}", Thread.currentThread().getId(), msg);
          timer = Timer.startNew();
        }

        sr = client.continueScan(tinfo, scanState.scanID, busyTimeout);
        if (!sr.more) {
          client.closeScan(tinfo, scanState.scanID);
          scanState.scanID = null;
        }
      }

      if (sr.more) {
        if (timer != null) {
          log.trace("tid={} Finished scan in {} #results={} scanid={}",
              Thread.currentThread().getId(),
              String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0), sr.results.size(),
              scanState.scanID);
        }
      } else {
        if (addr.getExtent().endRow() == null) {
          scanState.finished = true;

          if (timer != null) {
            log.trace("tid={} Completely finished scan in {} #results={}",
                Thread.currentThread().getId(),
                String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0),
                sr.results.size());
          }

        } else if (scanState.range.getEndKey() == null || !scanState.range
            .afterEndKey(new Key(addr.getExtent().endRow()).followingKey(PartialKey.ROW))) {
          scanState.startRow = addr.getExtent().endRow();
          scanState.skipStartRow = true;

          if (timer != null) {
            log.trace("tid={} Finished scanning tablet in {} #results={}",
                Thread.currentThread().getId(),
                String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0),
                sr.results.size());
          }
        } else {
          scanState.finished = true;
          if (timer != null) {
            log.trace("tid={} Completely finished in {} #results={}",
                Thread.currentThread().getId(),
                String.format("%.3f secs", timer.elapsed(MILLISECONDS) / 1000.0),
                sr.results.size());
          }
        }
      }

      Key.decompress(sr.results);

      if (!sr.results.isEmpty() && !scanState.finished) {
        scanState.range = new Range(new Key(sr.results.get(sr.results.size() - 1).key), false,
            scanState.range.getEndKey(), scanState.range.isEndKeyInclusive());
      }

      List<KeyValue> results = new ArrayList<>(sr.results.size());
      for (TKeyValue tkv : sr.results) {
        results.add(new KeyValue(new Key(tkv.key), tkv.value));
      }

      return results;

    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } finally {
      ThriftUtil.returnClient(client, context);
      Thread.currentThread().setName(old);
    }
  }

  static void close(ScanState scanState) {
    if (!scanState.finished && scanState.scanID != null && scanState.prevLoc != null) {
      TInfo tinfo = TraceUtil.traceInfo();

      log.debug("Closing active scan {} {}", scanState.prevLoc, scanState.scanID);
      HostAndPort parsedLocation = HostAndPort.fromString(scanState.prevLoc.serverAddress);
      TabletScanClientService.Client client = null;
      try {
        client =
            ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN, parsedLocation, scanState.context);
        client.closeScan(tinfo, scanState.scanID);
      } catch (TException e) {
        // ignore this is a best effort
        log.debug("Failed to close active scan " + scanState.prevLoc + " " + scanState.scanID, e);
      } finally {
        if (client != null) {
          ThriftUtil.returnClient(client, scanState.context);
        }
      }
    }
  }
}

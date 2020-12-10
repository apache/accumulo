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

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.clientImpl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialScan;
import org.apache.accumulo.core.dataImpl.thrift.IterInfo;
import org.apache.accumulo.core.dataImpl.thrift.ScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.hadoop.io.Text;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftScanner {
  private static final Logger log = LoggerFactory.getLogger(ThriftScanner.class);

  public static final Map<TabletType,Set<String>> serversWaitedForWrites =
      new EnumMap<>(TabletType.class);
  private static Random secureRandom = new SecureRandom();

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
    if (server == null)
      throw new AccumuloException(new IOException());

    final HostAndPort parsedServer = HostAndPort.fromString(server);
    try {
      TInfo tinfo = TraceUtil.traceInfo();
      TabletClientService.Client client = ThriftUtil.getTServerClient(parsedServer, context);
      try {
        // not reading whole rows (or stopping on row boundaries) so there is no need to enable
        // isolation below
        ScanState scanState = new ScanState(context, extent.tableId(), authorizations, range,
            fetchedColumns, size, serverSideIteratorList, serverSideIteratorOptions, false,
            Constants.SCANNER_DEFAULT_READAHEAD_THRESHOLD, null, batchTimeOut, classLoaderContext,
            null);

        TabletType ttype = TabletType.type(extent);
        boolean waitForWrites = !serversWaitedForWrites.get(ttype).contains(server);
        InitialScan isr = client.startScan(tinfo, scanState.context.rpcCreds(), extent.toThrift(),
            scanState.range.toThrift(),
            scanState.columns.stream().map(Column::toThrift).collect(Collectors.toList()),
            scanState.size, scanState.serverSideIteratorList, scanState.serverSideIteratorOptions,
            scanState.authorizations.getAuthorizationsBB(), waitForWrites, scanState.isolated,
            scanState.readaheadThreshold, null, scanState.batchTimeOut, classLoaderContext,
            scanState.executionHints);
        if (waitForWrites)
          serversWaitedForWrites.get(ttype).add(server);

        Key.decompress(isr.result.results);

        for (TKeyValue kv : isr.result.results)
          results.put(new Key(kv.key), new Value(kv.value));

        client.closeScan(tinfo, isr.scanID);

        return isr.result.more;
      } finally {
        ThriftUtil.returnClient(client);
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

  public static class ScanState {

    boolean isolated;
    TableId tableId;
    Text startRow;
    boolean skipStartRow;
    long readaheadThreshold;
    long batchTimeOut;

    Range range;

    int size;

    ClientContext context;
    Authorizations authorizations;
    List<Column> columns;

    TabletLocation prevLoc;
    Long scanID;

    String classLoaderContext;

    boolean finished = false;

    List<IterInfo> serverSideIteratorList;

    Map<String,Map<String,String>> serverSideIteratorOptions;

    SamplerConfiguration samplerConfig;
    Map<String,String> executionHints;

    public ScanState(ClientContext context, TableId tableId, Authorizations authorizations,
        Range range, SortedSet<Column> fetchedColumns, int size,
        List<IterInfo> serverSideIteratorList,
        Map<String,Map<String,String>> serverSideIteratorOptions, boolean isolated,
        long readaheadThreshold, SamplerConfiguration samplerConfig, long batchTimeOut,
        String classLoaderContext, Map<String,String> executionHints) {
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

      if (executionHints == null || executionHints.isEmpty())
        this.executionHints = null; // avoid thrift serialization for empty map
      else
        this.executionHints = executionHints;
    }
  }

  public static class ScanTimedOutException extends IOException {

    private static final long serialVersionUID = 1L;

  }

  static long pause(long millis, long maxSleep) throws InterruptedException {
    Thread.sleep(millis);
    // wait 2 * last time, with +-10% random jitter
    return (long) (Math.min(millis * 2, maxSleep) * (.9 + secureRandom.nextDouble() / 5));
  }

  public static List<KeyValue> scan(ClientContext context, ScanState scanState, long timeOut)
      throws ScanTimedOutException, AccumuloException, AccumuloSecurityException,
      TableNotFoundException {
    TabletLocation loc = null;
    long startTime = System.currentTimeMillis();
    String lastError = null;
    String error = null;
    int tooManyFilesCount = 0;
    long sleepMillis = 100;
    final long maxSleepTime =
        context.getConfiguration().getTimeInMillis(Property.GENERAL_MAX_SCANNER_RETRY_PERIOD);

    List<KeyValue> results = null;

    try (TraceScope span = Trace.startSpan("scan")) {
      while (results == null && !scanState.finished) {
        if (Thread.currentThread().isInterrupted()) {
          throw new AccumuloException("Thread interrupted");
        }

        if ((System.currentTimeMillis() - startTime) / 1000.0 > timeOut)
          throw new ScanTimedOutException();

        while (loc == null) {
          long currentTime = System.currentTimeMillis();
          if ((currentTime - startTime) / 1000.0 > timeOut)
            throw new ScanTimedOutException();

          try (TraceScope locateSpan = Trace.startSpan("scan:locateTablet")) {
            loc = TabletLocator.getLocator(context, scanState.tableId).locateTablet(context,
                scanState.startRow, scanState.skipStartRow, false);

            if (loc == null) {
              if (!Tables.exists(context, scanState.tableId))
                throw new TableDeletedException(scanState.tableId.canonical());
              else if (Tables.getTableState(context, scanState.tableId) == TableState.OFFLINE)
                throw new TableOfflineException(
                    Tables.getTableOfflineMsg(context, scanState.tableId));

              error = "Failed to locate tablet for table : " + scanState.tableId + " row : "
                  + scanState.startRow;
              if (!error.equals(lastError))
                log.debug("{}", error);
              else if (log.isTraceEnabled())
                log.trace("{}", error);
              lastError = error;
              sleepMillis = pause(sleepMillis, maxSleepTime);
            } else {
              // when a tablet splits we do want to continue scanning the low child
              // of the split if we are already passed it
              Range dataRange = loc.tablet_extent.toDataRange();

              if (scanState.range.getStartKey() != null
                  && dataRange.afterEndKey(scanState.range.getStartKey())) {
                // go to the next tablet
                scanState.startRow = loc.tablet_extent.endRow();
                scanState.skipStartRow = true;
                loc = null;
              } else if (scanState.range.getEndKey() != null
                  && dataRange.beforeStartKey(scanState.range.getEndKey())) {
                // should not happen
                throw new RuntimeException("Unexpected tablet, extent : " + loc.tablet_extent
                    + "  range : " + scanState.range + " startRow : " + scanState.startRow);
              }
            }
          } catch (AccumuloServerException e) {
            log.debug("Scan failed, server side exception : {}", e.getMessage());
            throw e;
          } catch (AccumuloException e) {
            error = "exception from tablet loc " + e.getMessage();
            if (!error.equals(lastError))
              log.debug("{}", error);
            else if (log.isTraceEnabled())
              log.trace("{}", error);

            lastError = error;
            sleepMillis = pause(sleepMillis, maxSleepTime);
          }
        }

        try (TraceScope scanLocation = Trace.startSpan("scan:location")) {
          if (scanLocation.getSpan() != null) {
            scanLocation.getSpan().addKVAnnotation("tserver", loc.tablet_location);
          }
          results = scan(loc, scanState, context);
        } catch (AccumuloSecurityException e) {
          Tables.clearCache(context);
          if (!Tables.exists(context, scanState.tableId))
            throw new TableDeletedException(scanState.tableId.canonical());
          e.setTableInfo(Tables.getPrintableTableInfoFromId(context, scanState.tableId));
          throw e;
        } catch (TApplicationException tae) {
          throw new AccumuloServerException(loc.tablet_location, tae);
        } catch (TSampleNotPresentException tsnpe) {
          String message = "Table " + Tables.getPrintableTableInfoFromId(context, scanState.tableId)
              + " does not have sampling configured or built";
          throw new SampleNotPresentException(message, tsnpe);
        } catch (NotServingTabletException e) {
          error = "Scan failed, not serving tablet " + loc;
          if (!error.equals(lastError))
            log.debug("{}", error);
          else if (log.isTraceEnabled())
            log.trace("{}", error);
          lastError = error;

          TabletLocator.getLocator(context, scanState.tableId).invalidateCache(loc.tablet_extent);
          loc = null;

          // no need to try the current scan id somewhere else
          scanState.scanID = null;

          if (scanState.isolated)
            throw new IsolationException();

          sleepMillis = pause(sleepMillis, maxSleepTime);
        } catch (NoSuchScanIDException e) {
          error = "Scan failed, no such scan id " + scanState.scanID + " " + loc;
          if (!error.equals(lastError))
            log.debug("{}", error);
          else if (log.isTraceEnabled())
            log.trace("{}", error);
          lastError = error;

          if (scanState.isolated)
            throw new IsolationException();

          scanState.scanID = null;
        } catch (TooManyFilesException e) {
          error = "Tablet has too many files " + loc + " retrying...";
          if (error.equals(lastError)) {
            tooManyFilesCount++;
            if (tooManyFilesCount == 300)
              log.warn("{}", error);
            else if (log.isTraceEnabled())
              log.trace("{}", error);
          } else {
            log.debug("{}", error);
            tooManyFilesCount = 0;
          }
          lastError = error;

          // not sure what state the scan session on the server side is
          // in after this occurs, so lets be cautious and start a new
          // scan session
          scanState.scanID = null;

          if (scanState.isolated)
            throw new IsolationException();

          sleepMillis = pause(sleepMillis, maxSleepTime);
        } catch (TException e) {
          TabletLocator.getLocator(context, scanState.tableId).invalidateCache(context,
              loc.tablet_location);
          error = "Scan failed, thrift error " + e.getClass().getName() + "  " + e.getMessage()
              + " " + loc;
          if (!error.equals(lastError))
            log.debug("{}", error);
          else if (log.isTraceEnabled())
            log.trace("{}", error);
          lastError = error;
          loc = null;

          // do not want to continue using the same scan id, if a timeout occurred could cause a
          // batch to be skipped
          // because a thread on the server side may still be processing the timed out continue scan
          scanState.scanID = null;

          if (scanState.isolated)
            throw new IsolationException();

          sleepMillis = pause(sleepMillis, maxSleepTime);
        }
      }

      if (results != null && results.isEmpty() && scanState.finished) {
        results = null;
      }

      return results;
    } catch (InterruptedException ex) {
      throw new AccumuloException(ex);
    }
  }

  private static List<KeyValue> scan(TabletLocation loc, ScanState scanState, ClientContext context)
      throws AccumuloSecurityException, NotServingTabletException, TException,
      NoSuchScanIDException, TooManyFilesException, TSampleNotPresentException {
    if (scanState.finished)
      return null;

    OpTimer timer = null;

    final TInfo tinfo = TraceUtil.traceInfo();
    final HostAndPort parsedLocation = HostAndPort.fromString(loc.tablet_location);
    TabletClientService.Client client = ThriftUtil.getTServerClient(parsedLocation, context);

    String old = Thread.currentThread().getName();
    try {
      ScanResult sr;

      if (scanState.prevLoc != null && !scanState.prevLoc.equals(loc))
        scanState.scanID = null;

      scanState.prevLoc = loc;

      if (scanState.scanID == null) {
        Thread.currentThread().setName("Starting scan tserver=" + loc.tablet_location + " tableId="
            + loc.tablet_extent.tableId());

        if (log.isTraceEnabled()) {
          String msg = "Starting scan tserver=" + loc.tablet_location + " tablet="
              + loc.tablet_extent + " range=" + scanState.range + " ssil="
              + scanState.serverSideIteratorList + " ssio=" + scanState.serverSideIteratorOptions
              + " context=" + scanState.classLoaderContext;
          log.trace("tid={} {}", Thread.currentThread().getId(), msg);
          timer = new OpTimer().start();
        }

        TabletType ttype = TabletType.type(loc.tablet_extent);
        boolean waitForWrites = !serversWaitedForWrites.get(ttype).contains(loc.tablet_location);

        InitialScan is = client.startScan(tinfo, scanState.context.rpcCreds(),
            loc.tablet_extent.toThrift(), scanState.range.toThrift(),
            scanState.columns.stream().map(Column::toThrift).collect(Collectors.toList()),
            scanState.size, scanState.serverSideIteratorList, scanState.serverSideIteratorOptions,
            scanState.authorizations.getAuthorizationsBB(), waitForWrites, scanState.isolated,
            scanState.readaheadThreshold,
            SamplerConfigurationImpl.toThrift(scanState.samplerConfig), scanState.batchTimeOut,
            scanState.classLoaderContext, scanState.executionHints);
        if (waitForWrites)
          serversWaitedForWrites.get(ttype).add(loc.tablet_location);

        sr = is.result;

        if (sr.more)
          scanState.scanID = is.scanID;
        else
          client.closeScan(tinfo, is.scanID);

      } else {
        // log.debug("Calling continue scan : "+scanState.range+" loc = "+loc);
        String msg =
            "Continuing scan tserver=" + loc.tablet_location + " scanid=" + scanState.scanID;
        Thread.currentThread().setName(msg);

        if (log.isTraceEnabled()) {
          log.trace("tid={} {}", Thread.currentThread().getId(), msg);
          timer = new OpTimer().start();
        }

        sr = client.continueScan(tinfo, scanState.scanID);
        if (!sr.more) {
          client.closeScan(tinfo, scanState.scanID);
          scanState.scanID = null;
        }
      }

      if (sr.more) {
        if (timer != null) {
          timer.stop();
          log.trace("tid={} Finished scan in {} #results={} scanid={}",
              Thread.currentThread().getId(),
              String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)), sr.results.size(),
              scanState.scanID);
        }
      } else {
        // log.debug("No more : tab end row = "+loc.tablet_extent.getEndRow()+" range =
        // "+scanState.range);
        if (loc.tablet_extent.endRow() == null) {
          scanState.finished = true;

          if (timer != null) {
            timer.stop();
            log.trace("tid={} Completely finished scan in {} #results={}",
                Thread.currentThread().getId(),
                String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)), sr.results.size());
          }

        } else if (scanState.range.getEndKey() == null || !scanState.range
            .afterEndKey(new Key(loc.tablet_extent.endRow()).followingKey(PartialKey.ROW))) {
          scanState.startRow = loc.tablet_extent.endRow();
          scanState.skipStartRow = true;

          if (timer != null) {
            timer.stop();
            log.trace("tid={} Finished scanning tablet in {} #results={}",
                Thread.currentThread().getId(),
                String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)), sr.results.size());
          }
        } else {
          scanState.finished = true;
          if (timer != null) {
            timer.stop();
            log.trace("tid={} Completely finished in {} #results={}",
                Thread.currentThread().getId(),
                String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)), sr.results.size());
          }
        }
      }

      Key.decompress(sr.results);

      if (!sr.results.isEmpty() && !scanState.finished)
        scanState.range = new Range(new Key(sr.results.get(sr.results.size() - 1).key), false,
            scanState.range.getEndKey(), scanState.range.isEndKeyInclusive());

      List<KeyValue> results = new ArrayList<>(sr.results.size());
      for (TKeyValue tkv : sr.results)
        results.add(new KeyValue(new Key(tkv.key), tkv.value));

      return results;

    } catch (ThriftSecurityException e) {
      throw new AccumuloSecurityException(e.user, e.code, e);
    } finally {
      ThriftUtil.returnClient(client);
      Thread.currentThread().setName(old);
    }
  }

  static void close(ScanState scanState) {
    if (!scanState.finished && scanState.scanID != null && scanState.prevLoc != null) {
      TInfo tinfo = TraceUtil.traceInfo();

      log.debug("Closing active scan {} {}", scanState.prevLoc, scanState.scanID);
      HostAndPort parsedLocation = HostAndPort.fromString(scanState.prevLoc.tablet_location);
      TabletClientService.Client client = null;
      try {
        client = ThriftUtil.getTServerClient(parsedLocation, scanState.context);
        client.closeScan(tinfo, scanState.scanID);
      } catch (TException e) {
        // ignore this is a best effort
        log.debug("Failed to close active scan " + scanState.prevLoc + " " + scanState.scanID, e);
      } finally {
        if (client != null)
          ThriftUtil.returnClient(client);
      }
    }
  }
}

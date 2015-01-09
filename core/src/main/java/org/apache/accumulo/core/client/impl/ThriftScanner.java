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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.impl.TabletLocator.TabletLocation;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.InitialScan;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.ScanResult;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;

public class ThriftScanner {
  private static final Logger log = Logger.getLogger(ThriftScanner.class);

  public static final Map<TabletType,Set<String>> serversWaitedForWrites = new EnumMap<TabletType,Set<String>>(TabletType.class);

  static {
    for (TabletType ttype : TabletType.values()) {
      serversWaitedForWrites.put(ttype, Collections.synchronizedSet(new HashSet<String>()));
    }
  }

  public static boolean getBatchFromServer(Instance instance, Credentials credentials, Range range, KeyExtent extent, String server,
      SortedMap<Key,Value> results, SortedSet<Column> fetchedColumns, List<IterInfo> serverSideIteratorList,
      Map<String,Map<String,String>> serverSideIteratorOptions, int size, Authorizations authorizations, boolean retry, AccumuloConfiguration conf)
      throws AccumuloException, AccumuloSecurityException, NotServingTabletException {
    if (server == null)
      throw new AccumuloException(new IOException());

    try {
      TInfo tinfo = Tracer.traceInfo();
      TabletClientService.Client client = ThriftUtil.getTServerClient(server, conf);
      try {
        // not reading whole rows (or stopping on row boundries) so there is no need to enable isolation below
        ScanState scanState = new ScanState(instance, credentials, extent.getTableId(), authorizations, range, fetchedColumns, size, serverSideIteratorList,
            serverSideIteratorOptions, false);

        TabletType ttype = TabletType.type(extent);
        boolean waitForWrites = !serversWaitedForWrites.get(ttype).contains(server);
        InitialScan isr = client.startScan(tinfo, scanState.credentials.toThrift(instance), extent.toThrift(), scanState.range.toThrift(),
            Translator.translate(scanState.columns, Translators.CT), scanState.size, scanState.serverSideIteratorList, scanState.serverSideIteratorOptions,
            scanState.authorizations.getAuthorizationsBB(), waitForWrites, scanState.isolated, scanState.readaheadThreshold);
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
      log.debug("Tablet (" + extent + ") has too many files " + server + " : " + e);
    } catch (ThriftSecurityException e) {
      log.warn("Security Violation in scan request to " + server + ": " + e);
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TException e) {
      log.debug("Error getting transport to " + server + " : " + e);
    }

    throw new AccumuloException("getBatchFromServer: failed");
  }

  public static class ScanState {

    boolean isolated;
    Text tableId;
    Text startRow;
    boolean skipStartRow;
    long readaheadThreshold;

    Range range;

    int size;

    Instance instance;
    Credentials credentials;
    Authorizations authorizations;
    List<Column> columns;

    TabletLocation prevLoc;
    Long scanID;

    boolean finished = false;

    List<IterInfo> serverSideIteratorList;

    Map<String,Map<String,String>> serverSideIteratorOptions;

    public ScanState(Instance instance, Credentials credentials, Text tableId, Authorizations authorizations, Range range, SortedSet<Column> fetchedColumns,
        int size, List<IterInfo> serverSideIteratorList, Map<String,Map<String,String>> serverSideIteratorOptions, boolean isolated) {
      this(instance, credentials, tableId, authorizations, range, fetchedColumns, size, serverSideIteratorList, serverSideIteratorOptions, isolated,
          Constants.SCANNER_DEFAULT_READAHEAD_THRESHOLD);
    }

    public ScanState(Instance instance, Credentials credentials, Text tableId, Authorizations authorizations, Range range, SortedSet<Column> fetchedColumns,
        int size, List<IterInfo> serverSideIteratorList, Map<String,Map<String,String>> serverSideIteratorOptions, boolean isolated, long readaheadThreshold) {
      this.instance = instance;
      this.credentials = credentials;
      this.authorizations = authorizations;

      columns = new ArrayList<Column>(fetchedColumns.size());
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

    }
  }

  public static class ScanTimedOutException extends IOException {

    private static final long serialVersionUID = 1L;

  }

  public static List<KeyValue> scan(Instance instance, Credentials credentials, ScanState scanState, int timeOut, AccumuloConfiguration conf)
      throws ScanTimedOutException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    TabletLocation loc = null;

    long startTime = System.currentTimeMillis();
    String lastError = null;
    String error = null;
    int tooManyFilesCount = 0;

    List<KeyValue> results = null;

    Span span = Trace.start("scan");
    try {
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

          Span locateSpan = Trace.start("scan:locateTablet");
          try {
            loc = TabletLocator.getLocator(instance, scanState.tableId).locateTablet(credentials, scanState.startRow, scanState.skipStartRow, false);

            if (loc == null) {
              if (!Tables.exists(instance, scanState.tableId.toString()))
                throw new TableDeletedException(scanState.tableId.toString());
              else if (Tables.getTableState(instance, scanState.tableId.toString()) == TableState.OFFLINE)
                throw new TableOfflineException(instance, scanState.tableId.toString());

              error = "Failed to locate tablet for table : " + scanState.tableId + " row : " + scanState.startRow;
              if (!error.equals(lastError))
                log.debug(error);
              else if (log.isTraceEnabled())
                log.trace(error);
              lastError = error;
              Thread.sleep(100);
            } else {
              // when a tablet splits we do want to continue scanning the low child
              // of the split if we are already passed it
              Range dataRange = loc.tablet_extent.toDataRange();

              if (scanState.range.getStartKey() != null && dataRange.afterEndKey(scanState.range.getStartKey())) {
                // go to the next tablet
                scanState.startRow = loc.tablet_extent.getEndRow();
                scanState.skipStartRow = true;
                loc = null;
              } else if (scanState.range.getEndKey() != null && dataRange.beforeStartKey(scanState.range.getEndKey())) {
                // should not happen
                throw new RuntimeException("Unexpected tablet, extent : " + loc.tablet_extent + "  range : " + scanState.range + " startRow : "
                    + scanState.startRow);
              }
            }
          } catch (AccumuloServerException e) {
            log.debug("Scan failed, server side exception : " + e.getMessage());
            throw e;
          } catch (AccumuloException e) {
            error = "exception from tablet loc " + e.getMessage();
            if (!error.equals(lastError))
              log.debug(error);
            else if (log.isTraceEnabled())
              log.trace(error);

            lastError = error;
            Thread.sleep(100);
          } finally {
            locateSpan.stop();
          }
        }

        Span scanLocation = Trace.start("scan:location");
        scanLocation.data("tserver", loc.tablet_location);
        try {
          results = scan(loc, scanState, conf);
        } catch (AccumuloSecurityException e) {
          Tables.clearCache(instance);
          if (!Tables.exists(instance, scanState.tableId.toString()))
            throw new TableDeletedException(scanState.tableId.toString());
          e.setTableInfo(Tables.getPrintableTableInfoFromId(instance, scanState.tableId.toString()));
          throw e;
        } catch (TApplicationException tae) {
          throw new AccumuloServerException(loc.tablet_location, tae);
        } catch (NotServingTabletException e) {
          error = "Scan failed, not serving tablet " + loc;
          if (!error.equals(lastError))
            log.debug(error);
          else if (log.isTraceEnabled())
            log.trace(error);
          lastError = error;

          TabletLocator.getLocator(instance, scanState.tableId).invalidateCache(loc.tablet_extent);
          loc = null;

          // no need to try the current scan id somewhere else
          scanState.scanID = null;

          if (scanState.isolated)
            throw new IsolationException();

          Thread.sleep(100);
        } catch (NoSuchScanIDException e) {
          error = "Scan failed, no such scan id " + scanState.scanID + " " + loc;
          if (!error.equals(lastError))
            log.debug(error);
          else if (log.isTraceEnabled())
            log.trace(error);
          lastError = error;

          if (scanState.isolated)
            throw new IsolationException();

          scanState.scanID = null;
        } catch (TooManyFilesException e) {
          error = "Tablet has too many files " + loc + " retrying...";
          if (!error.equals(lastError)) {
            log.debug(error);
            tooManyFilesCount = 0;
          } else {
            tooManyFilesCount++;
            if (tooManyFilesCount == 300)
              log.warn(error);
            else if (log.isTraceEnabled())
              log.trace(error);
          }
          lastError = error;

          // not sure what state the scan session on the server side is
          // in after this occurs, so lets be cautious and start a new
          // scan session
          scanState.scanID = null;

          if (scanState.isolated)
            throw new IsolationException();

          Thread.sleep(100);
        } catch (TException e) {
          TabletLocator.getLocator(instance, scanState.tableId).invalidateCache(loc.tablet_location);
          error = "Scan failed, thrift error " + e.getClass().getName() + "  " + e.getMessage() + " " + loc;
          if (!error.equals(lastError))
            log.debug(error);
          else if (log.isTraceEnabled())
            log.trace(error);
          lastError = error;
          loc = null;

          // do not want to continue using the same scan id, if a timeout occurred could cause a batch to be skipped
          // because a thread on the server side may still be processing the timed out continue scan
          scanState.scanID = null;

          if (scanState.isolated)
            throw new IsolationException();

          Thread.sleep(100);
        } finally {
          scanLocation.stop();
        }
      }

      if (results != null && results.size() == 0 && scanState.finished) {
        results = null;
      }

      return results;
    } catch (InterruptedException ex) {
      throw new AccumuloException(ex);
    } finally {
      span.stop();
    }
  }

  private static List<KeyValue> scan(TabletLocation loc, ScanState scanState, AccumuloConfiguration conf) throws AccumuloSecurityException,
      NotServingTabletException, TException, NoSuchScanIDException, TooManyFilesException {
    if (scanState.finished)
      return null;

    OpTimer opTimer = new OpTimer(log, Level.TRACE);

    TInfo tinfo = Tracer.traceInfo();
    TabletClientService.Client client = ThriftUtil.getTServerClient(loc.tablet_location, conf);

    String old = Thread.currentThread().getName();
    try {
      ScanResult sr;

      if (scanState.prevLoc != null && !scanState.prevLoc.equals(loc))
        scanState.scanID = null;

      scanState.prevLoc = loc;

      if (scanState.scanID == null) {
        String msg = "Starting scan tserver=" + loc.tablet_location + " tablet=" + loc.tablet_extent + " range=" + scanState.range + " ssil="
            + scanState.serverSideIteratorList + " ssio=" + scanState.serverSideIteratorOptions;
        Thread.currentThread().setName(msg);
        opTimer.start(msg);

        TabletType ttype = TabletType.type(loc.tablet_extent);
        boolean waitForWrites = !serversWaitedForWrites.get(ttype).contains(loc.tablet_location);
        InitialScan is = client.startScan(tinfo, scanState.credentials.toThrift(scanState.instance), loc.tablet_extent.toThrift(), scanState.range.toThrift(),
            Translator.translate(scanState.columns, Translators.CT), scanState.size, scanState.serverSideIteratorList, scanState.serverSideIteratorOptions,
            scanState.authorizations.getAuthorizationsBB(), waitForWrites, scanState.isolated, scanState.readaheadThreshold);
        if (waitForWrites)
          serversWaitedForWrites.get(ttype).add(loc.tablet_location);

        sr = is.result;

        if (sr.more)
          scanState.scanID = is.scanID;
        else
          client.closeScan(tinfo, is.scanID);

      } else {
        // log.debug("Calling continue scan : "+scanState.range+"  loc = "+loc);
        String msg = "Continuing scan tserver=" + loc.tablet_location + " scanid=" + scanState.scanID;
        Thread.currentThread().setName(msg);
        opTimer.start(msg);

        sr = client.continueScan(tinfo, scanState.scanID);
        if (!sr.more) {
          client.closeScan(tinfo, scanState.scanID);
          scanState.scanID = null;
        }
      }

      if (!sr.more) {
        // log.debug("No more : tab end row = "+loc.tablet_extent.getEndRow()+" range = "+scanState.range);
        if (loc.tablet_extent.getEndRow() == null) {
          scanState.finished = true;
          opTimer.stop("Completely finished scan in %DURATION% #results=" + sr.results.size());
        } else if (scanState.range.getEndKey() == null || !scanState.range.afterEndKey(new Key(loc.tablet_extent.getEndRow()).followingKey(PartialKey.ROW))) {
          scanState.startRow = loc.tablet_extent.getEndRow();
          scanState.skipStartRow = true;
          opTimer.stop("Finished scanning tablet in %DURATION% #results=" + sr.results.size());
        } else {
          scanState.finished = true;
          opTimer.stop("Completely finished scan in %DURATION% #results=" + sr.results.size());
        }
      } else {
        opTimer.stop("Finished scan in %DURATION% #results=" + sr.results.size() + " scanid=" + scanState.scanID);
      }

      Key.decompress(sr.results);

      if (sr.results.size() > 0 && !scanState.finished)
        scanState.range = new Range(new Key(sr.results.get(sr.results.size() - 1).key), false, scanState.range.getEndKey(), scanState.range.isEndKeyInclusive());

      List<KeyValue> results = new ArrayList<KeyValue>(sr.results.size());
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
}

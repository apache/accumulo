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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.InitialMultiScan;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.Credentials;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.trace.instrument.TraceRunnable;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TabletServerBatchReaderIterator implements Iterator<Entry<Key,Value>> {

  private static final Logger log = Logger.getLogger(TabletServerBatchReaderIterator.class);

  private final Instance instance;
  private final Credentials credentials;
  private final String table;
  private Authorizations authorizations = Authorizations.EMPTY;
  private final int numThreads;
  private final ExecutorService queryThreadPool;
  private final ScannerOptions options;

  private ArrayBlockingQueue<List<Entry<Key,Value>>> resultsQueue;
  private Iterator<Entry<Key,Value>> batchIterator;
  private List<Entry<Key,Value>> batch;
  private static final List<Entry<Key,Value>> LAST_BATCH = new ArrayList<Map.Entry<Key,Value>>();
  private final Object nextLock = new Object();

  private long failSleepTime = 100;

  private volatile Throwable fatalException = null;

  private Map<String,TimeoutTracker> timeoutTrackers;
  private Set<String> timedoutServers;
  private long timeout;

  private TabletLocator locator;

  public interface ResultReceiver {
    void receive(List<Entry<Key,Value>> entries);
  }

  private static class MyEntry implements Entry<Key,Value> {

    private Key key;
    private Value value;

    MyEntry(Key key, Value value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public Key getKey() {
      return key;
    }

    @Override
    public Value getValue() {
      return value;
    }

    @Override
    public Value setValue(Value value) {
      throw new UnsupportedOperationException();
    }

  }

  public TabletServerBatchReaderIterator(Instance instance, Credentials credentials, String table, Authorizations authorizations, ArrayList<Range> ranges,
      int numThreads, ExecutorService queryThreadPool, ScannerOptions scannerOptions, long timeout) {

    this.instance = instance;
    this.credentials = credentials;
    this.table = table;
    this.authorizations = authorizations;
    this.numThreads = numThreads;
    this.queryThreadPool = queryThreadPool;
    this.options = new ScannerOptions(scannerOptions);
    resultsQueue = new ArrayBlockingQueue<List<Entry<Key,Value>>>(numThreads);

    this.locator = new TimeoutTabletLocator(TabletLocator.getLocator(instance, new Text(table)), timeout);

    timeoutTrackers = Collections.synchronizedMap(new HashMap<String,TabletServerBatchReaderIterator.TimeoutTracker>());
    timedoutServers = Collections.synchronizedSet(new HashSet<String>());
    this.timeout = timeout;

    if (options.fetchedColumns.size() > 0) {
      ArrayList<Range> ranges2 = new ArrayList<Range>(ranges.size());
      for (Range range : ranges) {
        ranges2.add(range.bound(options.fetchedColumns.first(), options.fetchedColumns.last()));
      }

      ranges = ranges2;
    }

    ResultReceiver rr = new ResultReceiver() {

      @Override
      public void receive(List<Entry<Key,Value>> entries) {
        try {
          resultsQueue.put(entries);
        } catch (InterruptedException e) {
          if (TabletServerBatchReaderIterator.this.queryThreadPool.isShutdown())
            log.debug("Failed to add Batch Scan result", e);
          else
            log.warn("Failed to add Batch Scan result", e);
          fatalException = e;
          throw new RuntimeException(e);

        }
      }

    };

    try {
      lookup(ranges, rr);
    } catch (RuntimeException re) {
      throw re;
    } catch (Exception e) {
      throw new RuntimeException("Failed to create iterator", e);
    }
  }

  @Override
  public boolean hasNext() {
    synchronized (nextLock) {
      if (batch == LAST_BATCH)
        return false;

      if (batch != null && batchIterator.hasNext())
        return true;

      // don't have one cached, try to cache one and return success
      try {
        batch = null;
        while (batch == null && fatalException == null && !queryThreadPool.isShutdown())
          batch = resultsQueue.poll(1, TimeUnit.SECONDS);

        if (fatalException != null)
          if (fatalException instanceof RuntimeException)
            throw (RuntimeException) fatalException;
          else
            throw new RuntimeException(fatalException);

        if (queryThreadPool.isShutdown()) {
          String shortMsg = "The BatchScanner was unexpectedly closed while this Iterator was still in use.";
          log.error(shortMsg + " Ensure that a reference to the BatchScanner is retained so that it can be closed when this Iterator is exhausted."
              + " Not retaining a reference to the BatchScanner guarantees that you are leaking threads in your client JVM.");
          throw new RuntimeException(shortMsg + " Ensure proper handling of the BatchScanner.");
        }

        batchIterator = batch.iterator();
        return batch != LAST_BATCH;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Entry<Key,Value> next() {
    // if there's one waiting, or hasNext() can get one, return it
    synchronized (nextLock) {
      if (hasNext())
        return batchIterator.next();
      else
        throw new NoSuchElementException();
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  private synchronized void lookup(List<Range> ranges, ResultReceiver receiver) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    List<Column> columns = new ArrayList<Column>(options.fetchedColumns);
    ranges = Range.mergeOverlapping(ranges);

    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();

    binRanges(locator, ranges, binnedRanges);

    doLookups(binnedRanges, receiver, columns);
  }

  private void binRanges(TabletLocator tabletLocator, List<Range> ranges, Map<String,Map<KeyExtent,List<Range>>> binnedRanges) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {

    int lastFailureSize = Integer.MAX_VALUE;

    while (true) {

      binnedRanges.clear();
      List<Range> failures = tabletLocator.binRanges(credentials, ranges, binnedRanges);

      if (failures.size() > 0) {
        // tried to only do table state checks when failures.size() == ranges.size(), however this did
        // not work because nothing ever invalidated entries in the tabletLocator cache... so even though
        // the table was deleted the tablet locator entries for the deleted table were not cleared... so
        // need to always do the check when failures occur
        if (failures.size() >= lastFailureSize)
          if (!Tables.exists(instance, table))
            throw new TableDeletedException(table);
          else if (Tables.getTableState(instance, table) == TableState.OFFLINE)
            throw new TableOfflineException(instance, table);

        lastFailureSize = failures.size();

        if (log.isTraceEnabled())
          log.trace("Failed to bin " + failures.size() + " ranges, tablet locations were null, retrying in 100ms");
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } else {
        break;
      }

    }

    // truncate the ranges to within the tablets... this makes it easier to know what work
    // needs to be redone when failures occurs and tablets have merged or split
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges2 = new HashMap<String,Map<KeyExtent,List<Range>>>();
    for (Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
      Map<KeyExtent,List<Range>> tabletMap = new HashMap<KeyExtent,List<Range>>();
      binnedRanges2.put(entry.getKey(), tabletMap);
      for (Entry<KeyExtent,List<Range>> tabletRanges : entry.getValue().entrySet()) {
        Range tabletRange = tabletRanges.getKey().toDataRange();
        List<Range> clippedRanges = new ArrayList<Range>();
        tabletMap.put(tabletRanges.getKey(), clippedRanges);
        for (Range range : tabletRanges.getValue())
          clippedRanges.add(tabletRange.clip(range));
      }
    }

    binnedRanges.clear();
    binnedRanges.putAll(binnedRanges2);
  }

  private void processFailures(Map<KeyExtent,List<Range>> failures, ResultReceiver receiver, List<Column> columns) throws AccumuloException,
      AccumuloSecurityException, TableNotFoundException {
    if (log.isTraceEnabled())
      log.trace("Failed to execute multiscans against " + failures.size() + " tablets, retrying...");

    try {
      Thread.sleep(failSleepTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

      // We were interrupted (close called on batchscanner) just exit
      log.debug("Exiting failure processing on interrupt");
      return;
    }

    failSleepTime = Math.min(5000, failSleepTime * 2);

    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<String,Map<KeyExtent,List<Range>>>();
    List<Range> allRanges = new ArrayList<Range>();

    for (List<Range> ranges : failures.values())
      allRanges.addAll(ranges);

    // since the first call to binRanges clipped the ranges to within a tablet, we should not get only
    // bin to the set of failed tablets
    binRanges(locator, allRanges, binnedRanges);

    doLookups(binnedRanges, receiver, columns);
  }

  private String getTableInfo() {
    return Tables.getPrintableTableInfoFromId(instance, table);
  }

  private class QueryTask implements Runnable {

    private String tsLocation;
    private Map<KeyExtent,List<Range>> tabletsRanges;
    private ResultReceiver receiver;
    private Semaphore semaphore = null;
    private final Map<KeyExtent,List<Range>> failures;
    private List<Column> columns;
    private int semaphoreSize;

    QueryTask(String tsLocation, Map<KeyExtent,List<Range>> tabletsRanges, Map<KeyExtent,List<Range>> failures, ResultReceiver receiver, List<Column> columns) {
      this.tsLocation = tsLocation;
      this.tabletsRanges = tabletsRanges;
      this.receiver = receiver;
      this.columns = columns;
      this.failures = failures;
    }

    void setSemaphore(Semaphore semaphore, int semaphoreSize) {
      this.semaphore = semaphore;
      this.semaphoreSize = semaphoreSize;
    }

    @Override
    public void run() {
      String threadName = Thread.currentThread().getName();
      Thread.currentThread().setName(threadName + " looking up " + tabletsRanges.size() + " ranges at " + tsLocation);
      Map<KeyExtent,List<Range>> unscanned = new HashMap<KeyExtent,List<Range>>();
      Map<KeyExtent,List<Range>> tsFailures = new HashMap<KeyExtent,List<Range>>();
      try {
        TimeoutTracker timeoutTracker = timeoutTrackers.get(tsLocation);
        if (timeoutTracker == null) {
          timeoutTracker = new TimeoutTracker(tsLocation, timedoutServers, timeout);
          timeoutTrackers.put(tsLocation, timeoutTracker);
        }
        doLookup(instance, credentials, tsLocation, tabletsRanges, tsFailures, unscanned, receiver, columns, options, authorizations,
            ServerConfigurationUtil.getConfiguration(instance), timeoutTracker);
        if (tsFailures.size() > 0) {
          locator.invalidateCache(tsFailures.keySet());
          synchronized (failures) {
            failures.putAll(tsFailures);
          }
        }

      } catch (IOException e) {
        synchronized (failures) {
          failures.putAll(tsFailures);
          failures.putAll(unscanned);
        }

        locator.invalidateCache(tsLocation);
        log.debug(e.getMessage(), e);
      } catch (AccumuloSecurityException e) {
        e.setTableInfo(getTableInfo());
        log.debug(e.getMessage(), e);

        Tables.clearCache(instance);
        if (!Tables.exists(instance, table))
          fatalException = new TableDeletedException(table);
        else
          fatalException = e;
      } catch (Throwable t) {
        if (queryThreadPool.isShutdown())
          log.debug(t.getMessage(), t);
        else
          log.warn(t.getMessage(), t);
        fatalException = t;
      } finally {
        semaphore.release();
        Thread.currentThread().setName(threadName);
        if (semaphore.tryAcquire(semaphoreSize)) {
          // finished processing all queries
          if (fatalException == null && failures.size() > 0) {
            // there were some failures
            try {
              processFailures(failures, receiver, columns);
            } catch (TableNotFoundException e) {
              log.debug(e.getMessage(), e);
              fatalException = e;
            } catch (AccumuloException e) {
              log.debug(e.getMessage(), e);
              fatalException = e;
            } catch (AccumuloSecurityException e) {
              e.setTableInfo(getTableInfo());
              log.debug(e.getMessage(), e);
              fatalException = e;
            } catch (Throwable t) {
              log.debug(t.getMessage(), t);
              fatalException = t;
            }

            if (fatalException != null) {
              // we are finished with this batch query
              if (!resultsQueue.offer(LAST_BATCH)) {
                log.debug("Could not add to result queue after seeing fatalException in processFailures", fatalException);
              }
            }
          } else {
            // we are finished with this batch query
            if (fatalException != null) {
              if (!resultsQueue.offer(LAST_BATCH)) {
                log.debug("Could not add to result queue after seeing fatalException", fatalException);
              }
            } else {
              try {
                resultsQueue.put(LAST_BATCH);
              } catch (InterruptedException e) {
                fatalException = e;
                if (!resultsQueue.offer(LAST_BATCH)) {
                  log.debug("Could not add to result queue after seeing fatalException", fatalException);
                }
              }
            }
          }
        }
      }
    }

  }

  private void doLookups(Map<String,Map<KeyExtent,List<Range>>> binnedRanges, final ResultReceiver receiver, List<Column> columns) {

    if (timedoutServers.containsAll(binnedRanges.keySet())) {
      // all servers have timed out
      throw new TimedOutException(timedoutServers);
    }

    // when there are lots of threads and a few tablet servers
    // it is good to break request to tablet servers up, the
    // following code determines if this is the case
    int maxTabletsPerRequest = Integer.MAX_VALUE;
    if (numThreads / binnedRanges.size() > 1) {
      int totalNumberOfTablets = 0;
      for (Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
        totalNumberOfTablets += entry.getValue().size();
      }

      maxTabletsPerRequest = totalNumberOfTablets / numThreads;
      if (maxTabletsPerRequest == 0) {
        maxTabletsPerRequest = 1;
      }

    }

    Map<KeyExtent,List<Range>> failures = new HashMap<KeyExtent,List<Range>>();

    if (timedoutServers.size() > 0) {
      // go ahead and fail any timed out servers
      for (Iterator<Entry<String,Map<KeyExtent,List<Range>>>> iterator = binnedRanges.entrySet().iterator(); iterator.hasNext();) {
        Entry<String,Map<KeyExtent,List<Range>>> entry = iterator.next();
        if (timedoutServers.contains(entry.getKey())) {
          failures.putAll(entry.getValue());
          iterator.remove();
        }
      }
    }

    // randomize tabletserver order... this will help when there are multiple
    // batch readers and writers running against accumulo
    List<String> locations = new ArrayList<String>(binnedRanges.keySet());
    Collections.shuffle(locations);

    List<QueryTask> queryTasks = new ArrayList<QueryTask>();

    for (final String tsLocation : locations) {

      final Map<KeyExtent,List<Range>> tabletsRanges = binnedRanges.get(tsLocation);
      if (maxTabletsPerRequest == Integer.MAX_VALUE || tabletsRanges.size() == 1) {
        QueryTask queryTask = new QueryTask(tsLocation, tabletsRanges, failures, receiver, columns);
        queryTasks.add(queryTask);
      } else {
        HashMap<KeyExtent,List<Range>> tabletSubset = new HashMap<KeyExtent,List<Range>>();
        for (Entry<KeyExtent,List<Range>> entry : tabletsRanges.entrySet()) {
          tabletSubset.put(entry.getKey(), entry.getValue());
          if (tabletSubset.size() >= maxTabletsPerRequest) {
            QueryTask queryTask = new QueryTask(tsLocation, tabletSubset, failures, receiver, columns);
            queryTasks.add(queryTask);
            tabletSubset = new HashMap<KeyExtent,List<Range>>();
          }
        }

        if (tabletSubset.size() > 0) {
          QueryTask queryTask = new QueryTask(tsLocation, tabletSubset, failures, receiver, columns);
          queryTasks.add(queryTask);
        }
      }
    }

    final Semaphore semaphore = new Semaphore(queryTasks.size());
    semaphore.acquireUninterruptibly(queryTasks.size());

    for (QueryTask queryTask : queryTasks) {
      queryTask.setSemaphore(semaphore, queryTasks.size());
      queryThreadPool.execute(new TraceRunnable(queryTask));
    }
  }

  static void trackScanning(Map<KeyExtent,List<Range>> failures, Map<KeyExtent,List<Range>> unscanned, MultiScanResult scanResult) {

    // translate returned failures, remove them from unscanned, and add them to failures
    Map<KeyExtent,List<Range>> retFailures = Translator.translate(scanResult.failures, Translators.TKET, new Translator.ListTranslator<TRange,Range>(
        Translators.TRT));
    unscanned.keySet().removeAll(retFailures.keySet());
    failures.putAll(retFailures);

    // translate full scans and remove them from unscanned
    HashSet<KeyExtent> fullScans = new HashSet<KeyExtent>(Translator.translate(scanResult.fullScans, Translators.TKET));
    unscanned.keySet().removeAll(fullScans);

    // remove partial scan from unscanned
    if (scanResult.partScan != null) {
      KeyExtent ke = new KeyExtent(scanResult.partScan);
      Key nextKey = new Key(scanResult.partNextKey);

      ListIterator<Range> iterator = unscanned.get(ke).listIterator();
      while (iterator.hasNext()) {
        Range range = iterator.next();

        if (range.afterEndKey(nextKey) || (nextKey.equals(range.getEndKey()) && scanResult.partNextKeyInclusive != range.isEndKeyInclusive())) {
          iterator.remove();
        } else if (range.contains(nextKey)) {
          iterator.remove();
          Range partRange = new Range(nextKey, scanResult.partNextKeyInclusive, range.getEndKey(), range.isEndKeyInclusive());
          iterator.add(partRange);
        }
      }
    }
  }

  private static class TimeoutTracker {

    String server;
    Set<String> badServers;
    long timeOut;
    long activityTime;
    Long firstErrorTime = null;

    TimeoutTracker(String server, Set<String> badServers, long timeOut) {
      this(timeOut);
      this.server = server;
      this.badServers = badServers;
    }

    TimeoutTracker(long timeOut) {
      this.timeOut = timeOut;
    }

    void startingScan() {
      activityTime = System.currentTimeMillis();
    }

    void check() throws IOException {
      if (System.currentTimeMillis() - activityTime > timeOut) {
        badServers.add(server);
        throw new IOException("Time exceeded " + (System.currentTimeMillis() - activityTime) + " " + server);
      }
    }

    void madeProgress() {
      activityTime = System.currentTimeMillis();
      firstErrorTime = null;
    }

    void errorOccured(Exception e) {
      if (firstErrorTime == null) {
        firstErrorTime = activityTime;
      } else if (System.currentTimeMillis() - firstErrorTime > timeOut) {
        badServers.add(server);
      }
    }

    /**
     */
    public long getTimeOut() {
      return timeOut;
    }
  }

  public static void doLookup(Instance instance, Credentials credentials, String server, Map<KeyExtent,List<Range>> requested,
      Map<KeyExtent,List<Range>> failures, Map<KeyExtent,List<Range>> unscanned, ResultReceiver receiver, List<Column> columns, ScannerOptions options,
      Authorizations authorizations, AccumuloConfiguration conf) throws IOException, AccumuloSecurityException, AccumuloServerException {
    doLookup(instance, credentials, server, requested, failures, unscanned, receiver, columns, options, authorizations, conf,
        new TimeoutTracker(Long.MAX_VALUE));
  }

  static void doLookup(Instance instance, Credentials credentials, String server, Map<KeyExtent,List<Range>> requested, Map<KeyExtent,List<Range>> failures,
      Map<KeyExtent,List<Range>> unscanned, ResultReceiver receiver, List<Column> columns, ScannerOptions options, Authorizations authorizations,
      AccumuloConfiguration conf, TimeoutTracker timeoutTracker) throws IOException, AccumuloSecurityException, AccumuloServerException {

    if (requested.size() == 0) {
      return;
    }

    // copy requested to unscanned map. we will remove ranges as they are scanned in trackScanning()
    for (Entry<KeyExtent,List<Range>> entry : requested.entrySet()) {
      ArrayList<Range> ranges = new ArrayList<Range>();
      for (Range range : entry.getValue()) {
        ranges.add(new Range(range));
      }
      unscanned.put(new KeyExtent(entry.getKey()), ranges);
    }

    timeoutTracker.startingScan();
    TTransport transport = null;
    try {
      TabletClientService.Client client;
      if (timeoutTracker.getTimeOut() < conf.getTimeInMillis(Property.GENERAL_RPC_TIMEOUT))
        client = ThriftUtil.getTServerClient(server, conf, timeoutTracker.getTimeOut());
      else
        client = ThriftUtil.getTServerClient(server, conf);

      try {

        OpTimer opTimer = new OpTimer(log, Level.TRACE).start("Starting multi scan, tserver=" + server + "  #tablets=" + requested.size() + "  #ranges="
            + sumSizes(requested.values()) + " ssil=" + options.serverSideIteratorList + " ssio=" + options.serverSideIteratorOptions);

        TabletType ttype = TabletType.type(requested.keySet());
        boolean waitForWrites = !ThriftScanner.serversWaitedForWrites.get(ttype).contains(server);

        Map<TKeyExtent,List<TRange>> thriftTabletRanges = Translator.translate(requested, Translators.KET, new Translator.ListTranslator<Range,TRange>(
            Translators.RT));
        InitialMultiScan imsr = client.startMultiScan(Tracer.traceInfo(), credentials.toThrift(instance), thriftTabletRanges,
            Translator.translate(columns, Translators.CT), options.serverSideIteratorList, options.serverSideIteratorOptions,
            ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations()), waitForWrites);
        if (waitForWrites)
          ThriftScanner.serversWaitedForWrites.get(ttype).add(server);

        MultiScanResult scanResult = imsr.result;

        opTimer.stop("Got 1st multi scan results, #results=" + scanResult.results.size() + (scanResult.more ? "  scanID=" + imsr.scanID : "")
            + " in %DURATION%");

        ArrayList<Entry<Key,Value>> entries = new ArrayList<Map.Entry<Key,Value>>(scanResult.results.size());
        for (TKeyValue kv : scanResult.results) {
          entries.add(new MyEntry(new Key(kv.key), new Value(kv.value)));
        }

        if (entries.size() > 0)
          receiver.receive(entries);

        if (entries.size() > 0 || scanResult.fullScans.size() > 0)
          timeoutTracker.madeProgress();

        trackScanning(failures, unscanned, scanResult);

        while (scanResult.more) {

          timeoutTracker.check();

          opTimer.start("Continuing multi scan, scanid=" + imsr.scanID);
          scanResult = client.continueMultiScan(Tracer.traceInfo(), imsr.scanID);
          opTimer.stop("Got more multi scan results, #results=" + scanResult.results.size() + (scanResult.more ? "  scanID=" + imsr.scanID : "")
              + " in %DURATION%");

          entries = new ArrayList<Map.Entry<Key,Value>>(scanResult.results.size());
          for (TKeyValue kv : scanResult.results) {
            entries.add(new MyEntry(new Key(kv.key), new Value(kv.value)));
          }

          if (entries.size() > 0)
            receiver.receive(entries);

          if (entries.size() > 0 || scanResult.fullScans.size() > 0)
            timeoutTracker.madeProgress();

          trackScanning(failures, unscanned, scanResult);
        }

        client.closeMultiScan(Tracer.traceInfo(), imsr.scanID);

      } finally {
        ThriftUtil.returnClient(client);
      }
    } catch (TTransportException e) {
      log.debug("Server : " + server + " msg : " + e.getMessage());
      timeoutTracker.errorOccured(e);
      throw new IOException(e);
    } catch (ThriftSecurityException e) {
      log.debug("Server : " + server + " msg : " + e.getMessage(), e);
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TApplicationException e) {
      log.debug("Server : " + server + " msg : " + e.getMessage(), e);
      throw new AccumuloServerException(server, e);
    } catch (NoSuchScanIDException e) {
      log.debug("Server : " + server + " msg : " + e.getMessage(), e);
      throw new IOException(e);
    } catch (TException e) {
      log.debug("Server : " + server + " msg : " + e.getMessage(), e);
      timeoutTracker.errorOccured(e);
      throw new IOException(e);
    } finally {
      ThriftTransportPool.getInstance().returnTransport(transport);
    }
  }

  static int sumSizes(Collection<List<Range>> values) {
    int sum = 0;

    for (List<Range> list : values) {
      sum += list.size();
    }

    return sum;
  }
}

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
import java.util.AbstractMap.SimpleImmutableEntry;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.InitialMultiScan;
import org.apache.accumulo.core.dataImpl.thrift.MultiScanResult;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyValue;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.OpTimer;
import org.apache.htrace.wrappers.TraceRunnable;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabletServerBatchReaderIterator implements Iterator<Entry<Key,Value>> {

  private static final Logger log = LoggerFactory.getLogger(TabletServerBatchReaderIterator.class);

  private final ClientContext context;
  private final TableId tableId;
  private Authorizations authorizations = Authorizations.EMPTY;
  private final int numThreads;
  private final ExecutorService queryThreadPool;
  private final ScannerOptions options;

  private ArrayBlockingQueue<List<Entry<Key,Value>>> resultsQueue;
  private Iterator<Entry<Key,Value>> batchIterator;
  private List<Entry<Key,Value>> batch;
  private static final List<Entry<Key,Value>> LAST_BATCH = new ArrayList<>();
  private final Object nextLock = new Object();

  private long failSleepTime = 100;

  private volatile Throwable fatalException = null;

  private Map<String,TimeoutTracker> timeoutTrackers;
  private Set<String> timedoutServers;
  private final long timeout;

  private TabletLocator locator;

  public interface ResultReceiver {
    void receive(List<Entry<Key,Value>> entries);
  }

  public TabletServerBatchReaderIterator(ClientContext context, TableId tableId,
      Authorizations authorizations, ArrayList<Range> ranges, int numThreads,
      ExecutorService queryThreadPool, ScannerOptions scannerOptions, long timeout) {

    this.context = context;
    this.tableId = tableId;
    this.authorizations = authorizations;
    this.numThreads = numThreads;
    this.queryThreadPool = queryThreadPool;
    this.options = new ScannerOptions(scannerOptions);
    resultsQueue = new ArrayBlockingQueue<>(numThreads);

    this.locator = new TimeoutTabletLocator(timeout, context, tableId);

    timeoutTrackers = Collections.synchronizedMap(new HashMap<>());
    timedoutServers = Collections.synchronizedSet(new HashSet<>());
    this.timeout = timeout;

    if (!options.fetchedColumns.isEmpty()) {
      ArrayList<Range> ranges2 = new ArrayList<>(ranges.size());
      for (Range range : ranges) {
        ranges2.add(range.bound(options.fetchedColumns.first(), options.fetchedColumns.last()));
      }

      ranges = ranges2;
    }

    ResultReceiver rr = entries -> {
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
          String shortMsg =
              "The BatchScanner was unexpectedly closed while" + " this Iterator was still in use.";
          log.error("{} Ensure that a reference to the BatchScanner is retained"
              + " so that it can be closed when this Iterator is exhausted. Not"
              + " retaining a reference to the BatchScanner guarantees that you are"
              + " leaking threads in your client JVM.", shortMsg);
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

  private synchronized void lookup(List<Range> ranges, ResultReceiver receiver)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    List<Column> columns = new ArrayList<>(options.fetchedColumns);
    ranges = Range.mergeOverlapping(ranges);

    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();

    binRanges(locator, ranges, binnedRanges);

    doLookups(binnedRanges, receiver, columns);
  }

  private void binRanges(TabletLocator tabletLocator, List<Range> ranges,
      Map<String,Map<KeyExtent,List<Range>>> binnedRanges)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    int lastFailureSize = Integer.MAX_VALUE;

    while (true) {

      binnedRanges.clear();
      List<Range> failures = tabletLocator.binRanges(context, ranges, binnedRanges);

      if (failures.isEmpty()) {
        break;
      } else {
        // tried to only do table state checks when failures.size() == ranges.size(), however this
        // did
        // not work because nothing ever invalidated entries in the tabletLocator cache... so even
        // though
        // the table was deleted the tablet locator entries for the deleted table were not
        // cleared... so
        // need to always do the check when failures occur
        if (failures.size() >= lastFailureSize)
          if (!Tables.exists(context, tableId))
            throw new TableDeletedException(tableId.canonical());
          else if (Tables.getTableState(context, tableId) == TableState.OFFLINE)
            throw new TableOfflineException(Tables.getTableOfflineMsg(context, tableId));

        lastFailureSize = failures.size();

        if (log.isTraceEnabled())
          log.trace("Failed to bin {} ranges, tablet locations were null, retrying in 100ms",
              failures.size());

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

    }

    // truncate the ranges to within the tablets... this makes it easier to know what work
    // needs to be redone when failures occurs and tablets have merged or split
    Map<String,Map<KeyExtent,List<Range>>> binnedRanges2 = new HashMap<>();
    for (Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
      Map<KeyExtent,List<Range>> tabletMap = new HashMap<>();
      binnedRanges2.put(entry.getKey(), tabletMap);
      for (Entry<KeyExtent,List<Range>> tabletRanges : entry.getValue().entrySet()) {
        Range tabletRange = tabletRanges.getKey().toDataRange();
        List<Range> clippedRanges = new ArrayList<>();
        tabletMap.put(tabletRanges.getKey(), clippedRanges);
        for (Range range : tabletRanges.getValue())
          clippedRanges.add(tabletRange.clip(range));
      }
    }

    binnedRanges.clear();
    binnedRanges.putAll(binnedRanges2);
  }

  private void processFailures(Map<KeyExtent,List<Range>> failures, ResultReceiver receiver,
      List<Column> columns)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    if (log.isTraceEnabled())
      log.trace("Failed to execute multiscans against {} tablets, retrying...", failures.size());

    try {
      Thread.sleep(failSleepTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();

      // We were interrupted (close called on batchscanner) just exit
      log.debug("Exiting failure processing on interrupt");
      return;
    }

    failSleepTime = Math.min(5000, failSleepTime * 2);

    Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();
    List<Range> allRanges = new ArrayList<>();

    for (List<Range> ranges : failures.values())
      allRanges.addAll(ranges);

    // since the first call to binRanges clipped the ranges to within a tablet, we should not get
    // only
    // bin to the set of failed tablets
    binRanges(locator, allRanges, binnedRanges);

    doLookups(binnedRanges, receiver, columns);
  }

  private String getTableInfo() {
    return Tables.getPrintableTableInfoFromId(context, tableId);
  }

  private class QueryTask implements Runnable {

    private String tsLocation;
    private Map<KeyExtent,List<Range>> tabletsRanges;
    private ResultReceiver receiver;
    private Semaphore semaphore = null;
    private final Map<KeyExtent,List<Range>> failures;
    private List<Column> columns;
    private int semaphoreSize;

    QueryTask(String tsLocation, Map<KeyExtent,List<Range>> tabletsRanges,
        Map<KeyExtent,List<Range>> failures, ResultReceiver receiver, List<Column> columns) {
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
      Thread.currentThread()
          .setName(threadName + " looking up " + tabletsRanges.size() + " ranges at " + tsLocation);
      Map<KeyExtent,List<Range>> unscanned = new HashMap<>();
      Map<KeyExtent,List<Range>> tsFailures = new HashMap<>();
      try {
        TimeoutTracker timeoutTracker = timeoutTrackers.get(tsLocation);
        if (timeoutTracker == null) {
          timeoutTracker = new TimeoutTracker(tsLocation, timedoutServers, timeout);
          timeoutTrackers.put(tsLocation, timeoutTracker);
        }
        doLookup(context, tsLocation, tabletsRanges, tsFailures, unscanned, receiver, columns,
            options, authorizations, timeoutTracker);
        if (!tsFailures.isEmpty()) {
          locator.invalidateCache(tsFailures.keySet());
          synchronized (failures) {
            failures.putAll(tsFailures);
          }
        }

      } catch (IOException e) {
        if (!TabletServerBatchReaderIterator.this.queryThreadPool.isShutdown()) {
          synchronized (failures) {
            failures.putAll(tsFailures);
            failures.putAll(unscanned);
          }

          locator.invalidateCache(context, tsLocation);
        }
        log.debug("IOException thrown", e);
      } catch (AccumuloSecurityException e) {
        e.setTableInfo(getTableInfo());
        log.debug("AccumuloSecurityException thrown", e);

        Tables.clearCache(context);
        if (Tables.exists(context, tableId))
          fatalException = e;
        else
          fatalException = new TableDeletedException(tableId.canonical());
      } catch (SampleNotPresentException e) {
        fatalException = e;
      } catch (Exception t) {
        if (queryThreadPool.isShutdown())
          log.debug("Caught exception, but queryThreadPool is shutdown", t);
        else
          log.warn("Caught exception, but queryThreadPool is not shutdown", t);
        fatalException = t;
      } catch (Throwable t) {
        fatalException = t;
        throw t; // let uncaught exception handler deal with the Error
      } finally {
        semaphore.release();
        Thread.currentThread().setName(threadName);
        if (semaphore.tryAcquire(semaphoreSize)) {
          // finished processing all queries
          if (fatalException == null && !failures.isEmpty()) {
            // there were some failures
            try {
              processFailures(failures, receiver, columns);
            } catch (TableNotFoundException | AccumuloException e) {
              log.debug("{}", e.getMessage(), e);
              fatalException = e;
            } catch (AccumuloSecurityException e) {
              e.setTableInfo(getTableInfo());
              log.debug("{}", e.getMessage(), e);
              fatalException = e;
            } catch (Exception t) {
              log.debug("{}", t.getMessage(), t);
              fatalException = t;
            }

            if (fatalException != null) {
              // we are finished with this batch query
              if (!resultsQueue.offer(LAST_BATCH)) {
                log.debug(
                    "Could not add to result queue after seeing fatalException in processFailures",
                    fatalException);
              }
            }
          } else {
            // we are finished with this batch query
            if (fatalException != null) {
              if (!resultsQueue.offer(LAST_BATCH)) {
                log.debug("Could not add to result queue after seeing fatalException",
                    fatalException);
              }
            } else {
              try {
                resultsQueue.put(LAST_BATCH);
              } catch (InterruptedException e) {
                fatalException = e;
                if (!resultsQueue.offer(LAST_BATCH)) {
                  log.debug("Could not add to result queue after seeing fatalException",
                      fatalException);
                }
              }
            }
          }
        }
      }
    }

  }

  private void doLookups(Map<String,Map<KeyExtent,List<Range>>> binnedRanges,
      final ResultReceiver receiver, List<Column> columns) {

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

    Map<KeyExtent,List<Range>> failures = new HashMap<>();

    if (!timedoutServers.isEmpty()) {
      // go ahead and fail any timed out servers
      for (Iterator<Entry<String,Map<KeyExtent,List<Range>>>> iterator =
          binnedRanges.entrySet().iterator(); iterator.hasNext();) {
        Entry<String,Map<KeyExtent,List<Range>>> entry = iterator.next();
        if (timedoutServers.contains(entry.getKey())) {
          failures.putAll(entry.getValue());
          iterator.remove();
        }
      }
    }

    // randomize tabletserver order... this will help when there are multiple
    // batch readers and writers running against accumulo
    List<String> locations = new ArrayList<>(binnedRanges.keySet());
    Collections.shuffle(locations);

    List<QueryTask> queryTasks = new ArrayList<>();

    for (final String tsLocation : locations) {

      final Map<KeyExtent,List<Range>> tabletsRanges = binnedRanges.get(tsLocation);
      if (maxTabletsPerRequest == Integer.MAX_VALUE || tabletsRanges.size() == 1) {
        QueryTask queryTask = new QueryTask(tsLocation, tabletsRanges, failures, receiver, columns);
        queryTasks.add(queryTask);
      } else {
        HashMap<KeyExtent,List<Range>> tabletSubset = new HashMap<>();
        for (Entry<KeyExtent,List<Range>> entry : tabletsRanges.entrySet()) {
          tabletSubset.put(entry.getKey(), entry.getValue());
          if (tabletSubset.size() >= maxTabletsPerRequest) {
            QueryTask queryTask =
                new QueryTask(tsLocation, tabletSubset, failures, receiver, columns);
            queryTasks.add(queryTask);
            tabletSubset = new HashMap<>();
          }
        }

        if (!tabletSubset.isEmpty()) {
          QueryTask queryTask =
              new QueryTask(tsLocation, tabletSubset, failures, receiver, columns);
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

  static void trackScanning(Map<KeyExtent,List<Range>> failures,
      Map<KeyExtent,List<Range>> unscanned, MultiScanResult scanResult) {

    // translate returned failures, remove them from unscanned, and add them to failures
    // @formatter:off
    Map<KeyExtent, List<Range>> retFailures = scanResult.failures.entrySet().stream().collect(Collectors.toMap(
                    (entry) -> KeyExtent.fromThrift(entry.getKey()),
                    (entry) -> entry.getValue().stream().map(Range::new).collect(Collectors.toList())
    ));
    // @formatter:on
    unscanned.keySet().removeAll(retFailures.keySet());
    failures.putAll(retFailures);

    // translate full scans and remove them from unscanned
    Set<KeyExtent> fullScans =
        scanResult.fullScans.stream().map(KeyExtent::fromThrift).collect(Collectors.toSet());
    unscanned.keySet().removeAll(fullScans);

    // remove partial scan from unscanned
    if (scanResult.partScan != null) {
      KeyExtent ke = KeyExtent.fromThrift(scanResult.partScan);
      Key nextKey = new Key(scanResult.partNextKey);

      ListIterator<Range> iterator = unscanned.get(ke).listIterator();
      while (iterator.hasNext()) {
        Range range = iterator.next();

        if (range.afterEndKey(nextKey) || (nextKey.equals(range.getEndKey())
            && scanResult.partNextKeyInclusive != range.isEndKeyInclusive())) {
          iterator.remove();
        } else if (range.contains(nextKey)) {
          iterator.remove();
          Range partRange = new Range(nextKey, scanResult.partNextKeyInclusive, range.getEndKey(),
              range.isEndKeyInclusive());
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
        throw new IOException(
            "Time exceeded " + (System.currentTimeMillis() - activityTime) + " " + server);
      }
    }

    void madeProgress() {
      activityTime = System.currentTimeMillis();
      firstErrorTime = null;
    }

    void errorOccured() {
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

  public static void doLookup(ClientContext context, String server,
      Map<KeyExtent,List<Range>> requested, Map<KeyExtent,List<Range>> failures,
      Map<KeyExtent,List<Range>> unscanned, ResultReceiver receiver, List<Column> columns,
      ScannerOptions options, Authorizations authorizations)
      throws IOException, AccumuloSecurityException, AccumuloServerException {
    doLookup(context, server, requested, failures, unscanned, receiver, columns, options,
        authorizations, new TimeoutTracker(Long.MAX_VALUE));
  }

  static void doLookup(ClientContext context, String server, Map<KeyExtent,List<Range>> requested,
      Map<KeyExtent,List<Range>> failures, Map<KeyExtent,List<Range>> unscanned,
      ResultReceiver receiver, List<Column> columns, ScannerOptions options,
      Authorizations authorizations, TimeoutTracker timeoutTracker)
      throws IOException, AccumuloSecurityException, AccumuloServerException {

    if (requested.isEmpty()) {
      return;
    }

    // copy requested to unscanned map. we will remove ranges as they are scanned in trackScanning()
    for (Entry<KeyExtent,List<Range>> entry : requested.entrySet()) {
      ArrayList<Range> ranges = new ArrayList<>();
      for (Range range : entry.getValue()) {
        ranges.add(new Range(range));
      }
      unscanned.put(KeyExtent.copyOf(entry.getKey()), ranges);
    }

    timeoutTracker.startingScan();
    TTransport transport = null;
    try {
      final HostAndPort parsedServer = HostAndPort.fromString(server);
      final TabletClientService.Client client;
      if (timeoutTracker.getTimeOut() < context.getClientTimeoutInMillis())
        client = ThriftUtil.getTServerClient(parsedServer, context, timeoutTracker.getTimeOut());
      else
        client = ThriftUtil.getTServerClient(parsedServer, context);

      try {

        OpTimer timer = null;

        if (log.isTraceEnabled()) {
          log.trace(
              "tid={} Starting multi scan, tserver={}  #tablets={}  #ranges={} ssil={} ssio={}",
              Thread.currentThread().getId(), server, requested.size(),
              sumSizes(requested.values()), options.serverSideIteratorList,
              options.serverSideIteratorOptions);

          timer = new OpTimer().start();
        }

        TabletType ttype = TabletType.type(requested.keySet());
        boolean waitForWrites = !ThriftScanner.serversWaitedForWrites.get(ttype).contains(server);

        // @formatter:off
        Map<TKeyExtent, List<TRange>> thriftTabletRanges = requested.entrySet().stream().collect(Collectors.toMap(
                        (entry) -> entry.getKey().toThrift(),
                        (entry) -> entry.getValue().stream().map(Range::toThrift).collect(Collectors.toList())
        ));
        // @formatter:on

        Map<String,String> execHints =
            options.executionHints.isEmpty() ? null : options.executionHints;

        InitialMultiScan imsr = client.startMultiScan(TraceUtil.traceInfo(), context.rpcCreds(),
            thriftTabletRanges, columns.stream().map(Column::toThrift).collect(Collectors.toList()),
            options.serverSideIteratorList, options.serverSideIteratorOptions,
            ByteBufferUtil.toByteBuffers(authorizations.getAuthorizations()), waitForWrites,
            SamplerConfigurationImpl.toThrift(options.getSamplerConfiguration()),
            options.batchTimeOut, options.classLoaderContext, execHints);
        if (waitForWrites)
          ThriftScanner.serversWaitedForWrites.get(ttype).add(server.toString());

        MultiScanResult scanResult = imsr.result;

        if (timer != null) {
          timer.stop();
          log.trace("tid={} Got 1st multi scan results, #results={} {} in {}",
              Thread.currentThread().getId(), scanResult.results.size(),
              (scanResult.more ? "scanID=" + imsr.scanID : ""),
              String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
        }

        ArrayList<Entry<Key,Value>> entries = new ArrayList<>(scanResult.results.size());
        for (TKeyValue kv : scanResult.results) {
          entries.add(new SimpleImmutableEntry<>(new Key(kv.key), new Value(kv.value)));
        }

        if (!entries.isEmpty())
          receiver.receive(entries);

        if (!entries.isEmpty() || !scanResult.fullScans.isEmpty())
          timeoutTracker.madeProgress();

        trackScanning(failures, unscanned, scanResult);

        AtomicLong nextOpid = new AtomicLong();

        while (scanResult.more) {

          timeoutTracker.check();

          if (timer != null) {
            log.trace("tid={} oid={} Continuing multi scan, scanid={}",
                Thread.currentThread().getId(), nextOpid.get(), imsr.scanID);
            timer.reset().start();
          }

          scanResult = client.continueMultiScan(TraceUtil.traceInfo(), imsr.scanID);

          if (timer != null) {
            timer.stop();
            log.trace("tid={} oid={} Got more multi scan results, #results={} {} in {}",
                Thread.currentThread().getId(), nextOpid.getAndIncrement(),
                scanResult.results.size(), (scanResult.more ? " scanID=" + imsr.scanID : ""),
                String.format("%.3f secs", timer.scale(TimeUnit.SECONDS)));
          }

          entries = new ArrayList<>(scanResult.results.size());
          for (TKeyValue kv : scanResult.results) {
            entries.add(new SimpleImmutableEntry<>(new Key(kv.key), new Value(kv.value)));
          }

          if (!entries.isEmpty())
            receiver.receive(entries);

          if (!entries.isEmpty() || !scanResult.fullScans.isEmpty())
            timeoutTracker.madeProgress();

          trackScanning(failures, unscanned, scanResult);
        }

        client.closeMultiScan(TraceUtil.traceInfo(), imsr.scanID);

      } finally {
        ThriftUtil.returnClient(client);
      }
    } catch (TTransportException e) {
      log.debug("Server : {} msg : {}", server, e.getMessage());
      timeoutTracker.errorOccured();
      throw new IOException(e);
    } catch (ThriftSecurityException e) {
      log.debug("Server : {} msg : {}", server, e.getMessage(), e);
      throw new AccumuloSecurityException(e.user, e.code, e);
    } catch (TApplicationException e) {
      log.debug("Server : {} msg : {}", server, e.getMessage(), e);
      throw new AccumuloServerException(server, e);
    } catch (NoSuchScanIDException e) {
      log.debug("Server : {} msg : {}", server, e.getMessage(), e);
      throw new IOException(e);
    } catch (TSampleNotPresentException e) {
      log.debug("Server : " + server + " msg : " + e.getMessage(), e);
      String tableInfo = "?";
      if (e.getExtent() != null) {
        TableId tableId = KeyExtent.fromThrift(e.getExtent()).tableId();
        tableInfo = Tables.getPrintableTableInfoFromId(context, tableId);
      }
      String message = "Table " + tableInfo + " does not have sampling configured or built";
      throw new SampleNotPresentException(message, e);
    } catch (TException e) {
      log.debug("Server : {} msg : {}", server, e.getMessage(), e);
      timeoutTracker.errorOccured();
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

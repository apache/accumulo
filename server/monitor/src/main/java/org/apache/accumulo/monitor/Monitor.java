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
package org.apache.accumulo.monitor;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import jakarta.inject.Singleton;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.gc.thrift.GCMonitorService;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.lock.ServiceLockSupport.HAServiceLockWatcher;
import org.apache.accumulo.core.manager.thrift.ManagerClientService;
import org.apache.accumulo.core.manager.thrift.ManagerMonitorInfo;
import org.apache.accumulo.core.manager.thrift.TableInfo;
import org.apache.accumulo.core.manager.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.tabletscan.thrift.ActiveScan;
import org.apache.accumulo.core.tabletscan.thrift.TabletScanClientService;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService.Client;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.monitor.rest.compactions.external.ExternalCompactionInfo;
import org.apache.accumulo.monitor.rest.compactions.external.RunningCompactions;
import org.apache.accumulo.monitor.rest.compactions.external.RunningCompactorDetails;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.HighlyAvailableService;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.TableInfoUtil;
import org.apache.zookeeper.KeeperException;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.logging.LoggingFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.mvc.MvcFeature;
import org.glassfish.jersey.server.mvc.freemarker.FreemarkerMvcFeature;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;

/**
 * Serve manager statistics with an embedded web server.
 */
public class Monitor extends AbstractServer implements HighlyAvailableService {

  private static final Logger log = LoggerFactory.getLogger(Monitor.class);
  private static final int REFRESH_TIME = 5;

  private final long START_TIME;

  public static void main(String[] args) throws Exception {
    try (Monitor monitor = new Monitor(new ConfigOpts(), args)) {
      monitor.runServer();
    }
  }

  Monitor(ConfigOpts opts, String[] args) {
    super("monitor", opts, args);
    START_TIME = System.currentTimeMillis();
  }

  private final AtomicLong lastRecalc = new AtomicLong(0L);
  private double totalIngestRate = 0.0;
  private double totalQueryRate = 0.0;
  private double totalScanRate = 0.0;
  private long totalEntries = 0L;
  private int totalTabletCount = 0;
  private long totalHoldTime = 0;
  private long totalLookups = 0;
  private int totalTables = 0;
  private final AtomicBoolean monitorInitialized = new AtomicBoolean(false);

  private EventCounter lookupRateTracker = new EventCounter();
  private EventCounter indexCacheHitTracker = new EventCounter();
  private EventCounter indexCacheRequestTracker = new EventCounter();
  private EventCounter dataCacheHitTracker = new EventCounter();
  private EventCounter dataCacheRequestTracker = new EventCounter();

  private final AtomicBoolean fetching = new AtomicBoolean(false);
  private ManagerMonitorInfo mmi;
  private GCStatus gcStatus;
  private Optional<HostAndPort> coordinatorHost = Optional.empty();
  private long coordinatorCheckNanos = 0L;
  private CompactionCoordinatorService.Client coordinatorClient;
  private final String coordinatorMissingMsg =
      "Error getting the compaction coordinator. Check that it is running. It is not "
          + "started automatically with other cluster processes so must be started by running "
          + "'accumulo compaction-coordinator'.";

  private EmbeddedWebServer server;
  private int livePort = 0;

  private ServiceLock monitorLock;

  private static class EventCounter {

    Map<String,Pair<Long,Long>> prevSamples = new HashMap<>();
    Map<String,Pair<Long,Long>> samples = new HashMap<>();
    Set<String> serversUpdated = new HashSet<>();

    synchronized void startingUpdates() {
      serversUpdated.clear();
    }

    synchronized void updateTabletServer(String name, long sampleTime, long numEvents) {
      Pair<Long,Long> newSample = new Pair<>(sampleTime, numEvents);
      Pair<Long,Long> lastSample = samples.get(name);

      if (lastSample == null || !lastSample.equals(newSample)) {
        samples.put(name, newSample);
        if (lastSample != null) {
          prevSamples.put(name, lastSample);
        }
      }
      serversUpdated.add(name);
    }

    synchronized void finishedUpdating() {
      // remove any tablet servers not updated
      samples.keySet().retainAll(serversUpdated);
      prevSamples.keySet().retainAll(serversUpdated);
    }

    synchronized double calculateRate() {
      double totalRate = 0;

      for (Entry<String,Pair<Long,Long>> entry : prevSamples.entrySet()) {
        Pair<Long,Long> prevSample = entry.getValue();
        Pair<Long,Long> sample = samples.get(entry.getKey());

        totalRate += (sample.getSecond() - prevSample.getSecond())
            / ((sample.getFirst() - prevSample.getFirst()) / (double) 1000);
      }

      return totalRate;
    }

  }

  public void fetchData() {
    ServerContext context = getContext();
    double totalIngestRate = 0.;
    double totalQueryRate = 0.;
    double totalScanRate = 0.;
    long totalEntries = 0;
    int totalTabletCount = 0;
    long totalHoldTime = 0;
    long totalLookups = 0;
    boolean retry = true;

    // only recalc every so often
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastRecalc.get() < REFRESH_TIME * 1000) {
      return;
    }

    // try to begin fetching; return if unsuccessful (because another thread is already fetching)
    if (!fetching.compareAndSet(false, true)) {
      return;
    }
    // DO NOT ADD CODE HERE that could throw an exception before we enter the try block
    // Otherwise, we'll never release the lock by unsetting 'fetching' in the the finally block
    try {
      while (retry) {
        ManagerClientService.Client client = null;
        try {
          client = ThriftClientTypes.MANAGER.getConnection(context);
          if (client != null) {
            mmi = client.getManagerStats(TraceUtil.traceInfo(), context.rpcCreds());
            retry = false;
          } else {
            mmi = null;
            log.error("Unable to get info from Manager");
          }
          gcStatus = fetchGcStatus();
        } catch (Exception e) {
          mmi = null;
          log.info("Error fetching stats: ", e);
        } finally {
          if (client != null) {
            ThriftUtil.close(client, context);
          }
        }
        if (mmi == null) {
          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      }
      if (mmi != null) {

        lookupRateTracker.startingUpdates();
        indexCacheHitTracker.startingUpdates();
        indexCacheRequestTracker.startingUpdates();
        dataCacheHitTracker.startingUpdates();
        dataCacheRequestTracker.startingUpdates();

        for (TabletServerStatus server : mmi.tServerInfo) {
          TableInfo summary = TableInfoUtil.summarizeTableStats(server);
          totalIngestRate += summary.ingestRate;
          totalQueryRate += summary.queryRate;
          totalScanRate += summary.scanRate;
          totalEntries += summary.recs;
          totalHoldTime += server.holdTime;
          totalLookups += server.lookups;
          lookupRateTracker.updateTabletServer(server.name, server.lastContact, server.lookups);
          indexCacheHitTracker.updateTabletServer(server.name, server.lastContact,
              server.indexCacheHits);
          indexCacheRequestTracker.updateTabletServer(server.name, server.lastContact,
              server.indexCacheRequest);
          dataCacheHitTracker.updateTabletServer(server.name, server.lastContact,
              server.dataCacheHits);
          dataCacheRequestTracker.updateTabletServer(server.name, server.lastContact,
              server.dataCacheRequest);
        }

        lookupRateTracker.finishedUpdating();
        indexCacheHitTracker.finishedUpdating();
        indexCacheRequestTracker.finishedUpdating();
        dataCacheHitTracker.finishedUpdating();
        dataCacheRequestTracker.finishedUpdating();

        int totalTables = 0;
        for (TableInfo tInfo : mmi.tableMap.values()) {
          totalTabletCount += tInfo.tablets;
          totalTables++;
        }
        this.totalIngestRate = totalIngestRate;
        this.totalTables = totalTables;
        this.totalQueryRate = totalQueryRate;
        this.totalScanRate = totalScanRate;
        this.totalEntries = totalEntries;
        this.totalTabletCount = totalTabletCount;
        this.totalHoldTime = totalHoldTime;
        this.totalLookups = totalLookups;

      }

      // check for compaction coordinator host and only notify its discovery
      Optional<HostAndPort> previousHost;
      if (System.nanoTime() - coordinatorCheckNanos > fetchTimeNanos) {
        previousHost = coordinatorHost;
        coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(context);
        coordinatorCheckNanos = System.nanoTime();
        if (previousHost.isEmpty() && coordinatorHost.isPresent()) {
          log.info("External Compaction Coordinator found at {}", coordinatorHost.orElseThrow());
        }
      }

    } finally {
      if (coordinatorClient != null) {
        ThriftUtil.returnClient(coordinatorClient, context);
        coordinatorClient = null;
      }
      lastRecalc.set(currentTime);
      // stop fetching; log an error if this thread wasn't already fetching
      if (!fetching.compareAndSet(true, false)) {
        throw new AssertionError("Not supposed to happen; somebody broke this code");
      }
    }
  }

  private GCStatus fetchGcStatus() {
    ServerContext context = getContext();
    GCStatus result = null;
    HostAndPort address = null;
    try {
      // Read the gc location from its lock
      ZooReaderWriter zk = context.getZooSession().asReaderWriter();
      var path = ServiceLock.path(context.getZooKeeperRoot() + Constants.ZGC_LOCK);
      List<String> locks = ServiceLock.validateAndSort(path, zk.getChildren(path.toString()));
      if (locks != null && !locks.isEmpty()) {
        address = ServiceLockData.parse(zk.getData(path + "/" + locks.get(0)))
            .map(sld -> sld.getAddress(ThriftService.GC)).orElse(null);
        if (address == null) {
          log.warn("Unable to contact the garbage collector (no address)");
          return null;
        }
        GCMonitorService.Client client =
            ThriftUtil.getClient(ThriftClientTypes.GC, address, context);
        try {
          result = client.getStatus(TraceUtil.traceInfo(), context.rpcCreds());
        } finally {
          ThriftUtil.returnClient(client, context);
        }
      }
    } catch (Exception ex) {
      log.warn("Unable to contact the garbage collector at " + address, ex);
    }
    return result;
  }

  @Override
  public void run() {
    ServerContext context = getContext();
    int[] ports = getConfiguration().getPort(Property.MONITOR_PORT);
    for (int port : ports) {
      try {
        log.debug("Trying monitor on port {}", port);
        server = new EmbeddedWebServer(this, port);
        server.addServlet(getDefaultServlet(), "/resources/*");
        server.addServlet(getRestServlet(), "/rest/*");
        server.addServlet(getViewServlet(), "/*");
        server.start();
        livePort = port;
        break;
      } catch (Exception ex) {
        log.error("Unable to start embedded web server", ex);
      }
    }
    if (!server.isRunning()) {
      throw new RuntimeException(
          "Unable to start embedded web server on ports: " + Arrays.toString(ports));
    } else {
      log.debug("Monitor started on port {}", livePort);
    }

    String advertiseHost = getHostname();
    if (advertiseHost.equals("0.0.0.0")) {
      try {
        advertiseHost = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        log.error("Unable to get hostname", e);
      }
    }
    HostAndPort monitorHostAndPort = HostAndPort.fromParts(advertiseHost, livePort);
    log.debug("Using {} to advertise monitor location in ZooKeeper", monitorHostAndPort);

    try {
      getMonitorLock(monitorHostAndPort);
    } catch (Exception e) {
      log.error("Failed to get Monitor ZooKeeper lock");
      throw new RuntimeException(e);
    }

    MetricsInfo metricsInfo = getContext().getMetricsInfo();
    metricsInfo.addMetricsProducers(this);
    metricsInfo.init(MetricsInfo.serviceTags(getContext().getInstanceName(), getApplicationName(),
        monitorHostAndPort, ""));

    try {
      URL url = new URL(server.isSecure() ? "https" : "http", advertiseHost, server.getPort(), "/");
      final String path = context.getZooKeeperRoot() + Constants.ZMONITOR_HTTP_ADDR;
      final ZooReaderWriter zoo = context.getZooSession().asReaderWriter();
      // Delete before we try to re-create in case the previous session hasn't yet expired
      zoo.delete(path);
      zoo.putEphemeralData(path, url.toString().getBytes(UTF_8));
      log.info("Set monitor address in zookeeper to {}", url);
    } catch (Exception ex) {
      log.error("Unable to advertise monitor HTTP address in zookeeper", ex);
    }

    // need to regularly fetch data so plot data is updated
    Threads.createThread("Data fetcher", () -> {
      while (true) {
        try {
          fetchData();
        } catch (Exception e) {
          log.warn("{}", e.getMessage(), e);
        }
        sleepUninterruptibly(333, TimeUnit.MILLISECONDS);
      }
    }).start();

    monitorInitialized.set(true);
  }

  private ServletHolder getDefaultServlet() {
    return new ServletHolder(new DefaultServlet() {
      private static final long serialVersionUID = 1L;

      @Override
      public Resource getResource(String pathInContext) {
        return Resource.newClassPathResource("/org/apache/accumulo/monitor" + pathInContext);
      }
    });
  }

  public static class MonitorFactory extends AbstractBinder implements Factory<Monitor> {

    private final Monitor monitor;

    public MonitorFactory(Monitor monitor) {
      this.monitor = monitor;
    }

    @Override
    public Monitor provide() {
      return monitor;
    }

    @Override
    public void dispose(Monitor instance) {}

    @Override
    protected void configure() {
      bindFactory(this).to(Monitor.class).in(Singleton.class);
    }
  }

  private ServletHolder getViewServlet() {
    final ResourceConfig rc = new ResourceConfig().packages("org.apache.accumulo.monitor.view")
        .register(new MonitorFactory(this))
        .register(new LoggingFeature(java.util.logging.Logger.getLogger(this.getClass().getName())))
        .register(FreemarkerMvcFeature.class)
        .property(MvcFeature.TEMPLATE_BASE_PATH, "/org/apache/accumulo/monitor/templates");
    return new ServletHolder(new ServletContainer(rc));
  }

  private ServletHolder getRestServlet() {
    final ResourceConfig rc = new ResourceConfig().packages("org.apache.accumulo.monitor.rest")
        .register(new MonitorFactory(this))
        .register(new LoggingFeature(java.util.logging.Logger.getLogger(this.getClass().getName())))
        .register(JacksonFeature.class);
    return new ServletHolder(new ServletContainer(rc));
  }

  public static class ScanStats {
    public final long scanCount;
    public final Long oldestScan;
    public final long fetched;

    ScanStats(List<ActiveScan> active) {
      this.scanCount = active.size();
      long oldest = -1;
      for (ActiveScan scan : active) {
        oldest = Math.max(oldest, scan.age);
      }
      this.oldestScan = oldest < 0 ? null : oldest;
      // use clock time for date friendly display
      this.fetched = System.currentTimeMillis();
    }
  }

  public static class CompactionStats {
    public final long count;
    public final Long oldest;
    public final long fetched;

    CompactionStats(List<ActiveCompaction> active) {
      this.count = active.size();
      long oldest = -1;
      for (ActiveCompaction a : active) {
        oldest = Math.max(oldest, a.age);
      }
      this.oldest = oldest < 0 ? null : oldest;
      // use clock time for date friendly display
      this.fetched = System.currentTimeMillis();
    }
  }

  private final Map<HostAndPort,ScanStats> tserverScans = new HashMap<>();
  private final Map<HostAndPort,ScanStats> sserverScans = new HashMap<>();
  private final Map<HostAndPort,CompactionStats> allCompactions = new HashMap<>();
  private final ExternalCompactionInfo ecInfo = new ExternalCompactionInfo();

  private long scansFetchedNanos = System.nanoTime();
  private long compactsFetchedNanos = System.nanoTime();
  private long ecInfoFetchedNanos = System.nanoTime();
  private final long fetchTimeNanos = TimeUnit.MINUTES.toNanos(1);
  private final long ageOffEntriesMillis = TimeUnit.MINUTES.toMillis(15);
  // When there are a large amount of external compactions running the list of external compactions
  // could consume a lot of memory. The purpose of this memoizing supplier is to try to avoid
  // creating the list of running external compactions in memory per web request. If multiple
  // request come in around the same time they should use the same list. It is still possible to
  // have multiple list in memory if one request obtains a copy and then another request comes in
  // after the timeout and the supplier recomputes the list. The longer the timeout on the supplier
  // is the less likely we are to have multiple list of external compactions in memory, however
  // increasing the timeout will make the monitor less responsive.
  private final Supplier<ExternalCompactionsSnapshot> extCompactionSnapshot =
      Suppliers.memoizeWithExpiration(() -> computeExternalCompactionsSnapshot(), fetchTimeNanos,
          TimeUnit.NANOSECONDS);

  /**
   * Fetch the active scans but only if fetchTimeNanos has elapsed.
   */
  public synchronized Map<HostAndPort,ScanStats> getScans() {
    if (System.nanoTime() - scansFetchedNanos > fetchTimeNanos) {
      log.info("User initiated fetch of Active TabletServer Scans");
      fetchScans();
    }
    return Map.copyOf(tserverScans);
  }

  public synchronized Map<HostAndPort,ScanStats> getScanServerScans() {
    if (System.nanoTime() - scansFetchedNanos > fetchTimeNanos) {
      log.info("User initiated fetch of Active ScanServer Scans");
      fetchScans();
    }
    return Map.copyOf(sserverScans);
  }

  /**
   * Fetch the active compactions but only if fetchTimeNanos has elapsed.
   */
  public synchronized Map<HostAndPort,CompactionStats> getCompactions() {
    if (System.nanoTime() - compactsFetchedNanos > fetchTimeNanos) {
      log.info("User initiated fetch of Active Compactions");
      fetchCompactions();
    }
    return Map.copyOf(allCompactions);
  }

  public synchronized ExternalCompactionInfo getCompactorsInfo() {
    if (coordinatorHost.isEmpty()) {
      throw new IllegalStateException("Tried fetching from compaction coordinator that's missing");
    }
    if (System.nanoTime() - ecInfoFetchedNanos > fetchTimeNanos) {
      log.info("User initiated fetch of External Compaction info");
      Map<String,Set<HostAndPort>> compactors =
          ExternalCompactionUtil.getCompactorAddrs(getContext());
      log.debug("Found compactors: " + compactors);
      ecInfo.setFetchedTimeMillis(System.currentTimeMillis());
      ecInfo.setCompactors(compactors);
      ecInfo.setCoordinatorHost(coordinatorHost);

      ecInfoFetchedNanos = System.nanoTime();
    }
    return ecInfo;
  }

  private static class ExternalCompactionsSnapshot {
    public final RunningCompactions runningCompactions;
    public final Map<String,TExternalCompaction> ecRunningMap;

    private ExternalCompactionsSnapshot(Optional<Map<String,TExternalCompaction>> ecRunningMapOpt) {
      this.ecRunningMap =
          ecRunningMapOpt.map(Collections::unmodifiableMap).orElse(Collections.emptyMap());
      this.runningCompactions = new RunningCompactions(this.ecRunningMap);
    }
  }

  private ExternalCompactionsSnapshot computeExternalCompactionsSnapshot() {
    if (coordinatorHost.isEmpty()) {
      throw new IllegalStateException(coordinatorMissingMsg);
    }
    var ccHost = coordinatorHost.orElseThrow();
    log.info("User initiated fetch of running External Compactions from " + ccHost);
    var client = getCoordinator(ccHost);
    TExternalCompactionList running;
    try {
      running = client.getRunningCompactions(TraceUtil.traceInfo(), getContext().rpcCreds());
    } catch (Exception e) {
      throw new IllegalStateException("Unable to get running compactions from " + ccHost, e);
    }

    return new ExternalCompactionsSnapshot(Optional.ofNullable(running.getCompactions()));
  }

  public RunningCompactions getRunnningCompactions() {
    return extCompactionSnapshot.get().runningCompactions;
  }

  public RunningCompactorDetails getRunningCompactorDetails(ExternalCompactionId ecid) {
    TExternalCompaction extCompaction =
        extCompactionSnapshot.get().ecRunningMap.get(ecid.canonical());
    if (extCompaction == null) {
      return null;
    }
    return new RunningCompactorDetails(extCompaction);
  }

  private CompactionCoordinatorService.Client getCoordinator(HostAndPort address) {
    if (coordinatorClient == null) {
      try {
        coordinatorClient =
            ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, address, getContext());
      } catch (Exception e) {
        log.error("Unable to get Compaction coordinator at {}", address);
        throw new IllegalStateException(coordinatorMissingMsg, e);
      }
    }
    return coordinatorClient;
  }

  private void fetchScans() {
    ServerContext context = getContext();
    for (String server : context.instanceOperations().getTabletServers()) {
      final HostAndPort parsedServer = HostAndPort.fromString(server);
      TabletScanClientService.Client tserver = null;
      try {
        tserver = ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN, parsedServer, context);
        List<ActiveScan> scans = tserver.getActiveScans(null, context.rpcCreds());
        tserverScans.put(parsedServer, new ScanStats(scans));
        scansFetchedNanos = System.nanoTime();
      } catch (Exception ex) {
        log.error("Failed to get active scans from {}", server, ex);
      } finally {
        ThriftUtil.returnClient(tserver, context);
      }
    }
    // Age off old scan information
    Iterator<Entry<HostAndPort,ScanStats>> tserverIter = tserverScans.entrySet().iterator();
    // clock time used for fetched for date friendly display
    long now = System.currentTimeMillis();
    while (tserverIter.hasNext()) {
      Entry<HostAndPort,ScanStats> entry = tserverIter.next();
      if (now - entry.getValue().fetched > ageOffEntriesMillis) {
        tserverIter.remove();
      }
    }
    // Scan Servers
    for (String server : context.instanceOperations().getScanServers()) {
      final HostAndPort parsedServer = HostAndPort.fromString(server);
      TabletScanClientService.Client sserver = null;
      try {
        sserver = ThriftUtil.getClient(ThriftClientTypes.TABLET_SCAN, parsedServer, context);
        List<ActiveScan> scans = sserver.getActiveScans(null, context.rpcCreds());
        sserverScans.put(parsedServer, new ScanStats(scans));
        scansFetchedNanos = System.nanoTime();
      } catch (Exception ex) {
        log.error("Failed to get active scans from {}", server, ex);
      } finally {
        ThriftUtil.returnClient(sserver, context);
      }
    }
    // Age off old scan information
    Iterator<Entry<HostAndPort,ScanStats>> sserverIter = sserverScans.entrySet().iterator();
    // clock time used for fetched for date friendly display
    now = System.currentTimeMillis();
    while (sserverIter.hasNext()) {
      Entry<HostAndPort,ScanStats> entry = sserverIter.next();
      if (now - entry.getValue().fetched > ageOffEntriesMillis) {
        sserverIter.remove();
      }
    }
  }

  private void fetchCompactions() {
    ServerContext context = getContext();
    for (String server : context.instanceOperations().getTabletServers()) {
      final HostAndPort parsedServer = HostAndPort.fromString(server);
      Client tserver = null;
      try {
        tserver = ThriftUtil.getClient(ThriftClientTypes.TABLET_SERVER, parsedServer, context);
        var compacts = tserver.getActiveCompactions(null, context.rpcCreds());
        allCompactions.put(parsedServer, new CompactionStats(compacts));
        compactsFetchedNanos = System.nanoTime();
      } catch (Exception ex) {
        log.debug("Failed to get active compactions from {}", server, ex);
      } finally {
        ThriftUtil.returnClient(tserver, context);
      }
    }
    // Age off old compaction information
    var entryIter = allCompactions.entrySet().iterator();
    // clock time used for fetched for date friendly display
    long now = System.currentTimeMillis();
    while (entryIter.hasNext()) {
      var entry = entryIter.next();
      if (now - entry.getValue().fetched > ageOffEntriesMillis) {
        entryIter.remove();
      }
    }
  }

  /**
   * Get the monitor lock in ZooKeeper
   */
  private void getMonitorLock(HostAndPort monitorLocation)
      throws KeeperException, InterruptedException {
    ServerContext context = getContext();
    final String zRoot = context.getZooKeeperRoot();
    final String monitorPath = zRoot + Constants.ZMONITOR;
    final var monitorLockPath = ServiceLock.path(zRoot + Constants.ZMONITOR_LOCK);

    // Ensure that everything is kosher with ZK as this has changed.
    ZooReaderWriter zoo = context.getZooSession().asReaderWriter();
    if (zoo.exists(monitorPath)) {
      byte[] data = zoo.getData(monitorPath);
      // If the node isn't empty, it's from a previous install (has hostname:port for HTTP server)
      if (data.length != 0) {
        // Recursively delete from that parent node
        zoo.recursiveDelete(monitorPath, NodeMissingPolicy.SKIP);

        // And then make the nodes that we expect for the incoming ephemeral nodes
        zoo.putPersistentData(monitorPath, new byte[0], NodeExistsPolicy.FAIL);
        zoo.putPersistentData(monitorLockPath.toString(), new byte[0], NodeExistsPolicy.FAIL);
      } else if (!zoo.exists(monitorLockPath.toString())) {
        // monitor node in ZK exists and is empty as we expect
        // but the monitor/lock node does not
        zoo.putPersistentData(monitorLockPath.toString(), new byte[0], NodeExistsPolicy.FAIL);
      }
    } else {
      // 1.5.0 and earlier
      zoo.putPersistentData(zRoot + Constants.ZMONITOR, new byte[0], NodeExistsPolicy.FAIL);
      if (!zoo.exists(monitorLockPath.toString())) {
        // Somehow the monitor node exists but not monitor/lock
        zoo.putPersistentData(monitorLockPath.toString(), new byte[0], NodeExistsPolicy.FAIL);
      }
    }

    // Get a ZooLock for the monitor
    UUID zooLockUUID = UUID.randomUUID();
    monitorLock = new ServiceLock(context.getZooSession(), monitorLockPath, zooLockUUID);

    while (true) {
      HAServiceLockWatcher monitorLockWatcher = new HAServiceLockWatcher("monitor");
      monitorLock.lock(monitorLockWatcher, new ServiceLockData(zooLockUUID,
          monitorLocation.getHost() + ":" + monitorLocation.getPort(), ThriftService.NONE));

      monitorLockWatcher.waitForChange();

      if (monitorLockWatcher.isLockAcquired()) {
        break;
      }

      if (!monitorLockWatcher.isFailedToAcquireLock()) {
        throw new IllegalStateException("monitor lock in unknown state");
      }

      monitorLock.tryToCancelAsyncLockOrUnlock();

      sleepUninterruptibly(
          context.getConfiguration().getTimeInMillis(Property.MONITOR_LOCK_CHECK_INTERVAL),
          TimeUnit.MILLISECONDS);
    }

    log.info("Got Monitor lock.");
  }

  public ManagerMonitorInfo getMmi() {
    return mmi;
  }

  public int getTotalTables() {
    return totalTables;
  }

  public int getTotalTabletCount() {
    return totalTabletCount;
  }

  public long getTotalEntries() {
    return totalEntries;
  }

  public double getTotalIngestRate() {
    return totalIngestRate;
  }

  public double getTotalQueryRate() {
    return totalQueryRate;
  }

  public double getTotalScanRate() {
    return totalScanRate;
  }

  public long getTotalHoldTime() {
    return totalHoldTime;
  }

  public GCStatus getGcStatus() {
    return gcStatus;
  }

  public long getTotalLookups() {
    return totalLookups;
  }

  public long getStartTime() {
    return START_TIME;
  }

  public double getLookupRate() {
    return lookupRateTracker.calculateRate();
  }

  @Override
  public boolean isActiveService() {
    return monitorInitialized.get();
  }

  public Optional<HostAndPort> getCoordinatorHost() {
    return coordinatorHost;
  }

  public int getLivePort() {
    return livePort;
  }

  @Override
  public ServiceLock getLock() {
    return monitorLock;
  }
}

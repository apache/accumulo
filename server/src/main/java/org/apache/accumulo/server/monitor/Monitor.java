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
package org.apache.accumulo.server.monitor;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.MasterClient;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.FileUtil;
import org.apache.accumulo.core.gc.thrift.GCMonitorService;
import org.apache.accumulo.core.gc.thrift.GCStatus;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.MasterMonitorInfo;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.security.SecurityUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.monitor.servlets.DefaultServlet;
import org.apache.accumulo.server.monitor.servlets.GcStatusServlet;
import org.apache.accumulo.server.monitor.servlets.JSONServlet;
import org.apache.accumulo.server.monitor.servlets.LogServlet;
import org.apache.accumulo.server.monitor.servlets.MasterServlet;
import org.apache.accumulo.server.monitor.servlets.OperationServlet;
import org.apache.accumulo.server.monitor.servlets.ProblemServlet;
import org.apache.accumulo.server.monitor.servlets.ShellServlet;
import org.apache.accumulo.server.monitor.servlets.TServersServlet;
import org.apache.accumulo.server.monitor.servlets.TablesServlet;
import org.apache.accumulo.server.monitor.servlets.VisServlet;
import org.apache.accumulo.server.monitor.servlets.XMLServlet;
import org.apache.accumulo.server.monitor.servlets.trace.ListType;
import org.apache.accumulo.server.monitor.servlets.trace.ShowTrace;
import org.apache.accumulo.server.monitor.servlets.trace.Summary;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.problems.ProblemType;
import org.apache.accumulo.server.security.SecurityConstants;
import org.apache.accumulo.server.util.EmbeddedWebServer;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.trace.instrument.Tracer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

/**
 * Serve master statistics with an embedded web server.
 */
public class Monitor {
  private static final Logger log = Logger.getLogger(Monitor.class);

  public static final int REFRESH_TIME = 5;
  private static long lastRecalc = 0L;
  private static double totalIngestRate = 0.0;
  private static double totalIngestByteRate = 0.0;
  private static double totalQueryRate = 0.0;
  private static double totalScanRate = 0.0;
  private static double totalQueryByteRate = 0.0;
  private static long totalEntries = 0L;
  private static int totalTabletCount = 0;
  private static int onlineTabletCount = 0;
  private static long totalHoldTime = 0;
  private static long totalLookups = 0;
  private static int totalTables = 0;

  private static class MaxList<T> extends LinkedList<Pair<Long,T>> {
    private static final long serialVersionUID = 1L;

    private long maxDelta;

    public MaxList(long maxDelta) {
      this.maxDelta = maxDelta;
    }

    @Override
    public boolean add(Pair<Long,T> obj) {
      boolean result = super.add(obj);

      if (obj.getFirst() - get(0).getFirst() > maxDelta)
        remove(0);

      return result;
    }

  }

  private static final int MAX_TIME_PERIOD = 60 * 60 * 1000;
  private static final List<Pair<Long,Double>> loadOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Double>> ingestRateOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Double>> ingestByteRateOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Integer>> recoveriesOverTime = Collections.synchronizedList(new MaxList<Integer>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Integer>> minorCompactionsOverTime = Collections.synchronizedList(new MaxList<Integer>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Integer>> majorCompactionsOverTime = Collections.synchronizedList(new MaxList<Integer>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Double>> lookupsOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Integer>> queryRateOverTime = Collections.synchronizedList(new MaxList<Integer>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Integer>> scanRateOverTime = Collections.synchronizedList(new MaxList<Integer>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Double>> queryByteRateOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Double>> indexCacheHitRateOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static final List<Pair<Long,Double>> dataCacheHitRateOverTime = Collections.synchronizedList(new MaxList<Double>(MAX_TIME_PERIOD));
  private static EventCounter lookupRateTracker = new EventCounter();
  private static EventCounter indexCacheHitTracker = new EventCounter();
  private static EventCounter indexCacheRequestTracker = new EventCounter();
  private static EventCounter dataCacheHitTracker = new EventCounter();
  private static EventCounter dataCacheRequestTracker = new EventCounter();

  private static volatile boolean fetching = false;
  private static MasterMonitorInfo mmi;
  private static Map<String,Map<ProblemType,Integer>> problemSummary = Collections.emptyMap();
  private static Exception problemException;
  private static GCStatus gcStatus;

  private static Instance instance;

  private static ServerConfiguration config;

  private static EmbeddedWebServer server;

  private ZooLock monitorLock;

  public static Map<String,Double> summarizeTableStats(MasterMonitorInfo mmi) {
    Map<String,Double> compactingByTable = new HashMap<String,Double>();
    if (mmi != null && mmi.tServerInfo != null) {
      for (TabletServerStatus status : mmi.tServerInfo) {
        if (status != null && status.tableMap != null) {
          for (String table : status.tableMap.keySet()) {
            Double holdTime = compactingByTable.get(table);
            compactingByTable.put(table, Math.max(holdTime == null ? 0. : holdTime.doubleValue(), status.holdTime));
          }
        }
      }
    }
    return compactingByTable;
  }

  public static void add(TableInfo total, TableInfo more) {
    if (total.minors == null)
      total.minors = new Compacting();
    if (total.majors == null)
      total.majors = new Compacting();
    if (total.scans == null)
      total.scans = new Compacting();
    if (more.minors != null) {
      total.minors.running += more.minors.running;
      total.minors.queued += more.minors.queued;
    }
    if (more.majors != null) {
      total.majors.running += more.majors.running;
      total.majors.queued += more.majors.queued;
    }
    if (more.scans != null) {
      total.scans.running += more.scans.running;
      total.scans.queued += more.scans.queued;
    }
    total.onlineTablets += more.onlineTablets;
    total.recs += more.recs;
    total.recsInMemory += more.recsInMemory;
    total.tablets += more.tablets;
    total.ingestRate += more.ingestRate;
    total.ingestByteRate += more.ingestByteRate;
    total.queryRate += more.queryRate;
    total.queryByteRate += more.queryByteRate;
    total.scanRate += more.scanRate;
  }

  public static TableInfo summarizeTableStats(TabletServerStatus status) {
    TableInfo summary = new TableInfo();
    summary.majors = new Compacting();
    summary.minors = new Compacting();
    summary.scans = new Compacting();
    for (TableInfo rates : status.tableMap.values()) {
      add(summary, rates);
    }
    return summary;
  }

  private static class EventCounter {

    Map<String,Pair<Long,Long>> prevSamples = new HashMap<String,Pair<Long,Long>>();
    Map<String,Pair<Long,Long>> samples = new HashMap<String,Pair<Long,Long>>();
    Set<String> serversUpdated = new HashSet<String>();

    void startingUpdates() {
      serversUpdated.clear();
    }

    void updateTabletServer(String name, long sampleTime, long numEvents) {
      Pair<Long,Long> newSample = new Pair<Long,Long>(sampleTime, numEvents);
      Pair<Long,Long> lastSample = samples.get(name);

      if (lastSample == null || !lastSample.equals(newSample)) {
        samples.put(name, newSample);
        if (lastSample != null) {
          prevSamples.put(name, lastSample);
        }
      }
      serversUpdated.add(name);
    }

    void finishedUpdating() {
      // remove any tablet servers not updated
      samples.keySet().retainAll(serversUpdated);
      prevSamples.keySet().retainAll(serversUpdated);
    }

    double calculateRate() {
      double totalRate = 0;

      for (Entry<String,Pair<Long,Long>> entry : prevSamples.entrySet()) {
        Pair<Long,Long> prevSample = entry.getValue();
        Pair<Long,Long> sample = samples.get(entry.getKey());

        totalRate += (sample.getSecond() - prevSample.getSecond()) / ((sample.getFirst() - prevSample.getFirst()) / (double) 1000);
      }

      return totalRate;
    }

    long calculateCount() {
      long count = 0;

      for (Entry<String,Pair<Long,Long>> entry : prevSamples.entrySet()) {
        Pair<Long,Long> prevSample = entry.getValue();
        Pair<Long,Long> sample = samples.get(entry.getKey());

        count += sample.getSecond() - prevSample.getSecond();
      }

      return count;
    }
  }

  public static void fetchData() {
    double totalIngestRate = 0.;
    double totalIngestByteRate = 0.;
    double totalQueryRate = 0.;
    double totalQueryByteRate = 0.;
    double totalScanRate = 0.;
    long totalEntries = 0;
    int totalTabletCount = 0;
    int onlineTabletCount = 0;
    long totalHoldTime = 0;
    long totalLookups = 0;
    boolean retry = true;

    // only recalc every so often
    long currentTime = System.currentTimeMillis();
    if (currentTime - lastRecalc < REFRESH_TIME * 1000)
      return;

    synchronized (Monitor.class) {
      if (fetching)
        return;
      fetching = true;
    }

    try {
      while (retry) {
        MasterClientService.Iface client = null;
        try {
          client = MasterClient.getConnection(HdfsZooInstance.getInstance());
          if (client != null) {
            mmi = client.getMasterStats(Tracer.traceInfo(), SecurityConstants.getSystemCredentials());
            retry = false;
          } else {
            mmi = null;
          }
          Monitor.gcStatus = fetchGcStatus();
        } catch (Exception e) {
          mmi = null;
          log.info("Error fetching stats: " + e);
        } finally {
          if (client != null) {
            MasterClient.close(client);
          }
        }
        if (mmi == null)
          UtilWaitThread.sleep(1000);
      }
      if (mmi != null) {
        int majorCompactions = 0;
        int minorCompactions = 0;

        lookupRateTracker.startingUpdates();
        indexCacheHitTracker.startingUpdates();
        indexCacheRequestTracker.startingUpdates();
        dataCacheHitTracker.startingUpdates();
        dataCacheRequestTracker.startingUpdates();

        for (TabletServerStatus server : mmi.tServerInfo) {
          TableInfo summary = Monitor.summarizeTableStats(server);
          totalIngestRate += summary.ingestRate;
          totalIngestByteRate += summary.ingestByteRate;
          totalQueryRate += summary.queryRate;
          totalScanRate += summary.scanRate;
          totalQueryByteRate += summary.queryByteRate;
          totalEntries += summary.recs;
          totalHoldTime += server.holdTime;
          totalLookups += server.lookups;
          majorCompactions += summary.majors.running;
          minorCompactions += summary.minors.running;
          lookupRateTracker.updateTabletServer(server.name, server.lastContact, server.lookups);
          indexCacheHitTracker.updateTabletServer(server.name, server.lastContact, server.indexCacheHits);
          indexCacheRequestTracker.updateTabletServer(server.name, server.lastContact, server.indexCacheRequest);
          dataCacheHitTracker.updateTabletServer(server.name, server.lastContact, server.dataCacheHits);
          dataCacheRequestTracker.updateTabletServer(server.name, server.lastContact, server.dataCacheRequest);
        }

        lookupRateTracker.finishedUpdating();
        indexCacheHitTracker.finishedUpdating();
        indexCacheRequestTracker.finishedUpdating();
        dataCacheHitTracker.finishedUpdating();
        dataCacheRequestTracker.finishedUpdating();

        int totalTables = 0;
        for (TableInfo tInfo : mmi.tableMap.values()) {
          totalTabletCount += tInfo.tablets;
          onlineTabletCount += tInfo.onlineTablets;
          totalTables++;
        }
        Monitor.totalIngestRate = totalIngestRate;
        Monitor.totalTables = totalTables;
        totalIngestByteRate = totalIngestByteRate / 1000000.0;
        Monitor.totalIngestByteRate = totalIngestByteRate;
        Monitor.totalQueryRate = totalQueryRate;
        Monitor.totalScanRate = totalScanRate;
        totalQueryByteRate = totalQueryByteRate / 1000000.0;
        Monitor.totalQueryByteRate = totalQueryByteRate;
        Monitor.totalEntries = totalEntries;
        Monitor.totalTabletCount = totalTabletCount;
        Monitor.onlineTabletCount = onlineTabletCount;
        Monitor.totalHoldTime = totalHoldTime;
        Monitor.totalLookups = totalLookups;

        ingestRateOverTime.add(new Pair<Long,Double>(currentTime, totalIngestRate));
        ingestByteRateOverTime.add(new Pair<Long,Double>(currentTime, totalIngestByteRate));

        double totalLoad = 0.;
        for (TabletServerStatus status : mmi.tServerInfo) {
          if (status != null)
            totalLoad += status.osLoad;
        }
        loadOverTime.add(new Pair<Long,Double>(currentTime, totalLoad));

        minorCompactionsOverTime.add(new Pair<Long,Integer>(currentTime, minorCompactions));
        majorCompactionsOverTime.add(new Pair<Long,Integer>(currentTime, majorCompactions));

        lookupsOverTime.add(new Pair<Long,Double>(currentTime, lookupRateTracker.calculateRate()));

        queryRateOverTime.add(new Pair<Long,Integer>(currentTime, (int) totalQueryRate));
        queryByteRateOverTime.add(new Pair<Long,Double>(currentTime, totalQueryByteRate));

        scanRateOverTime.add(new Pair<Long,Integer>(currentTime, (int) totalScanRate));

        calcCacheHitRate(indexCacheHitRateOverTime, currentTime, indexCacheHitTracker, indexCacheRequestTracker);
        calcCacheHitRate(dataCacheHitRateOverTime, currentTime, dataCacheHitTracker, dataCacheRequestTracker);
      }
      try {
        Monitor.problemSummary = ProblemReports.getInstance().summarize();
        Monitor.problemException = null;
      } catch (Exception e) {
        log.info("Failed to obtain problem reports ", e);
        Monitor.problemSummary = Collections.emptyMap();
        Monitor.problemException = e;
      }

    } finally {
      synchronized (Monitor.class) {
        fetching = false;
        lastRecalc = currentTime;
      }
    }
  }

  private static void calcCacheHitRate(List<Pair<Long,Double>> hitRate, long currentTime, EventCounter cacheHits, EventCounter cacheReq) {
    long req = cacheReq.calculateCount();
    if (req > 0)
      hitRate.add(new Pair<Long,Double>(currentTime, cacheHits.calculateCount() / (double) cacheReq.calculateCount()));
    else
      hitRate.add(new Pair<Long,Double>(currentTime, null));
  }

  private static GCStatus fetchGcStatus() {
    GCStatus result = null;
    InetSocketAddress address = null;
    try {
      // Read the gc location from its lock
      ZooReaderWriter zk = ZooReaderWriter.getInstance();

      String path = ZooUtil.getRoot(instance) + Constants.ZGC_LOCK;
      List<String> locks = zk.getChildren(path, null);
      if (locks != null && locks.size() > 0) {
        Collections.sort(locks);
        address = new ServerServices(new String(zk.getData(path + "/" + locks.get(0), null), Constants.UTF8)).getAddress(Service.GC_CLIENT);
        GCMonitorService.Client client = ThriftUtil.getClient(new GCMonitorService.Client.Factory(), address, config.getConfiguration());
        try {
          result = client.getStatus(Tracer.traceInfo(), SecurityConstants.getSystemCredentials());
        } finally {
          ThriftUtil.returnClient(client);
        }
      }
    } catch (Exception ex) {
      log.warn("Unable to contact the garbage collector at " + address, ex);
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    SecurityUtil.serverLogin();

    FileSystem fs = FileUtil.getFileSystem(CachedConfiguration.getInstance(), ServerConfiguration.getSiteConfiguration());
    String hostname = Accumulo.getLocalAddress(args);
    instance = HdfsZooInstance.getInstance();
    config = new ServerConfiguration(instance);
    Accumulo.init(fs, config, "monitor");
    Monitor monitor = new Monitor();
    Accumulo.enableTracing(hostname, "monitor");
    monitor.run(hostname);
  }

  private static long START_TIME;

  public void run(String hostname) {
    try {
      getMonitorLock();
    } catch (Exception e) {
      log.error("Failed to get Monitor ZooKeeper lock");
      throw new RuntimeException(e);
    }

    Monitor.START_TIME = System.currentTimeMillis();
    int port = config.getConfiguration().getPort(Property.MONITOR_PORT);
    try {
      log.debug("Creating monitor on port " + port);
      server = new EmbeddedWebServer(hostname, port);
    } catch (Throwable ex) {
      log.error("Unable to start embedded web server", ex);
      throw new RuntimeException(ex);
    }

    server.addServlet(DefaultServlet.class, "/");
    server.addServlet(OperationServlet.class, "/op");
    server.addServlet(MasterServlet.class, "/master");
    server.addServlet(TablesServlet.class, "/tables");
    server.addServlet(TServersServlet.class, "/tservers");
    server.addServlet(ProblemServlet.class, "/problems");
    server.addServlet(GcStatusServlet.class, "/gc");
    server.addServlet(LogServlet.class, "/log");
    server.addServlet(XMLServlet.class, "/xml");
    server.addServlet(JSONServlet.class, "/json");
    server.addServlet(VisServlet.class, "/vis");
    server.addServlet(Summary.class, "/trace/summary");
    server.addServlet(ListType.class, "/trace/listType");
    server.addServlet(ShowTrace.class, "/trace/show");
    if (server.isUsingSsl())
      server.addServlet(ShellServlet.class, "/shell");
    server.start();

    InetSocketAddress monitorAddress = null;
    try {
      hostname = InetAddress.getLocalHost().getHostName();

      log.debug("Using " + hostname + " to advertise monitor location in ZooKeeper");

      monitorAddress = new InetSocketAddress(hostname, server.getPort());
      String monitorAddressAndPort = org.apache.accumulo.core.util.AddressUtil.toString(monitorAddress);
      ZooReaderWriter.getInstance().putPersistentData(ZooUtil.getRoot(instance) + Constants.ZMONITOR_HTTP_ADDR, monitorAddressAndPort.getBytes(Constants.UTF8),
          NodeExistsPolicy.OVERWRITE);
      log.info("Set monitor address in zookeeper to " + monitorAddress);
    } catch (Exception ex) {
      log.error("Unable to set monitor HTTP address in zookeeper", ex);
    }

    if (null != monitorAddress) {
      LogService.startLogListener(Monitor.getSystemConfiguration(), instance.getInstanceID(),
          org.apache.accumulo.core.util.AddressUtil.getHostAddress(monitorAddress));
    } else {
      log.warn("Not starting log4j listener as we could not determine address to use");
    }

    new Daemon(new LoggingRunnable(log, new ZooKeeperStatus()), "ZooKeeperStatus").start();

    // need to regularly fetch data so plot data is updated
    new Daemon(new LoggingRunnable(log, new Runnable() {

      @Override
      public void run() {
        while (true) {
          try {
            Monitor.fetchData();
          } catch (Exception e) {
            log.warn(e.getMessage(), e);
          }

          UtilWaitThread.sleep(333);
        }

      }
    }), "Data fetcher").start();
  }

  /**
   * Get the monitor lock in ZooKeeper
   * 
   * @throws KeeperException
   * @throws InterruptedException
   */
  private void getMonitorLock() throws KeeperException, InterruptedException {
    final String zRoot = ZooUtil.getRoot(instance);
    final String monitorPath = zRoot + Constants.ZMONITOR;
    final String monitorLockPath = zRoot + Constants.ZMONITOR_LOCK;

    // Ensure that everything is kosher with ZK as this has changed.
    ZooReaderWriter zoo = ZooReaderWriter.getInstance();
    if (zoo.exists(monitorPath)) {
      byte[] data = zoo.getData(monitorPath, null);
      // If the node isn't empty, it's from a previous install (has hostname:port for HTTP server)
      if (0 != data.length) {
        // Recursively delete from that parent node
        zoo.recursiveDelete(monitorPath, NodeMissingPolicy.SKIP);

        // And then make the nodes that we expect for the incoming ephemeral nodes
        zoo.putPersistentData(monitorPath, new byte[0], NodeExistsPolicy.FAIL);
        zoo.putPersistentData(monitorLockPath, new byte[0], NodeExistsPolicy.FAIL);
      } else if (!zoo.exists(monitorLockPath)) {
        // monitor node in ZK exists and is empty as we expect
        // but the monitor/lock node does not
        zoo.putPersistentData(monitorLockPath, new byte[0], NodeExistsPolicy.FAIL);
      }
    } else {
      // 1.5.0 and earlier
      zoo.putPersistentData(zRoot + Constants.ZMONITOR, new byte[0], NodeExistsPolicy.FAIL);
      if (!zoo.exists(monitorLockPath)) {
        // Somehow the monitor node exists but not monitor/lock
        zoo.putPersistentData(monitorLockPath, new byte[0], NodeExistsPolicy.FAIL);
      }
    }

    // Get a ZooLock for the monitor
    while (true) {
      MoniterLockWatcher monitorLockWatcher = new MoniterLockWatcher();
      monitorLock = new ZooLock(monitorLockPath);
      monitorLock.lockAsync(monitorLockWatcher, new byte[0]);

      monitorLockWatcher.waitForChange();

      if (monitorLockWatcher.acquiredLock) {
        break;
      }

      if (!monitorLockWatcher.failedToAcquireLock) {
        throw new IllegalStateException("monitor lock in unknown state");
      }

      monitorLock.tryToCancelAsyncLockOrUnlock();

      UtilWaitThread.sleep(instance.getConfiguration().getTimeInMillis(Property.MONITOR_LOCK_CHECK_INTERVAL));
    }

    log.info("Got Monitor lock.");
  }

  /**
   * Async Watcher for monitor lock
   */
  private static class MoniterLockWatcher implements ZooLock.AsyncLockWatcher {

    boolean acquiredLock = false;
    boolean failedToAcquireLock = false;

    @Override
    public void lostLock(LockLossReason reason) {
      Halt.halt("Monitor lock in zookeeper lost (reason = " + reason + "), exiting!", -1);
    }

    @Override
    public void unableToMonitorLockNode(final Throwable e) {
      Halt.halt(-1, new Runnable() {
        @Override
        public void run() {
          log.fatal("No longer able to monitor Monitor lock node", e);
        }
      });

    }

    @Override
    public synchronized void acquiredLock() {
      if (acquiredLock || failedToAcquireLock) {
        Halt.halt("Zoolock in unexpected state AL " + acquiredLock + " " + failedToAcquireLock, -1);
      }

      acquiredLock = true;
      notifyAll();
    }

    @Override
    public synchronized void failedToAcquireLock(Exception e) {
      log.warn("Failed to get monitor lock " + e);

      if (acquiredLock) {
        Halt.halt("Zoolock in unexpected state FAL " + acquiredLock + " " + failedToAcquireLock, -1);
      }

      failedToAcquireLock = true;
      notifyAll();
    }

    public synchronized void waitForChange() {
      while (!acquiredLock && !failedToAcquireLock) {
        try {
          wait();
        } catch (InterruptedException e) {}
      }
    }
  }

  public static MasterMonitorInfo getMmi() {
    return mmi;
  }

  public static int getTotalTables() {
    return totalTables;
  }

  public static int getTotalTabletCount() {
    return totalTabletCount;
  }

  public static int getOnlineTabletCount() {
    return onlineTabletCount;
  }

  public static long getTotalEntries() {
    return totalEntries;
  }

  public static double getTotalIngestRate() {
    return totalIngestRate;
  }

  public static double getTotalIngestByteRate() {
    return totalIngestByteRate;
  }

  public static double getTotalQueryRate() {
    return totalQueryRate;
  }

  public static double getTotalScanRate() {
    return totalScanRate;
  }

  public static double getTotalQueryByteRate() {
    return totalQueryByteRate;
  }

  public static long getTotalHoldTime() {
    return totalHoldTime;
  }

  public static Exception getProblemException() {
    return problemException;
  }

  public static Map<String,Map<ProblemType,Integer>> getProblemSummary() {
    return problemSummary;
  }

  public static GCStatus getGcStatus() {
    return gcStatus;
  }

  public static long getTotalLookups() {
    return totalLookups;
  }

  public static long getStartTime() {
    return START_TIME;
  }

  public static List<Pair<Long,Double>> getLoadOverTime() {
    synchronized (loadOverTime) {
      return new ArrayList<Pair<Long,Double>>(loadOverTime);
    }
  }

  public static List<Pair<Long,Double>> getIngestRateOverTime() {
    synchronized (ingestRateOverTime) {
      return new ArrayList<Pair<Long,Double>>(ingestRateOverTime);
    }
  }

  public static List<Pair<Long,Double>> getIngestByteRateOverTime() {
    synchronized (ingestByteRateOverTime) {
      return new ArrayList<Pair<Long,Double>>(ingestByteRateOverTime);
    }
  }

  public static List<Pair<Long,Integer>> getRecoveriesOverTime() {
    synchronized (recoveriesOverTime) {
      return new ArrayList<Pair<Long,Integer>>(recoveriesOverTime);
    }
  }

  public static List<Pair<Long,Integer>> getMinorCompactionsOverTime() {
    synchronized (minorCompactionsOverTime) {
      return new ArrayList<Pair<Long,Integer>>(minorCompactionsOverTime);
    }
  }

  public static List<Pair<Long,Integer>> getMajorCompactionsOverTime() {
    synchronized (majorCompactionsOverTime) {
      return new ArrayList<Pair<Long,Integer>>(majorCompactionsOverTime);
    }
  }

  public static List<Pair<Long,Double>> getLookupsOverTime() {
    synchronized (lookupsOverTime) {
      return new ArrayList<Pair<Long,Double>>(lookupsOverTime);
    }
  }

  public static double getLookupRate() {
    return lookupRateTracker.calculateRate();
  }

  public static List<Pair<Long,Integer>> getQueryRateOverTime() {
    synchronized (queryRateOverTime) {
      return new ArrayList<Pair<Long,Integer>>(queryRateOverTime);
    }
  }

  public static List<Pair<Long,Integer>> getScanRateOverTime() {
    synchronized (scanRateOverTime) {
      return new ArrayList<Pair<Long,Integer>>(scanRateOverTime);
    }
  }

  public static List<Pair<Long,Double>> getQueryByteRateOverTime() {
    synchronized (queryByteRateOverTime) {
      return new ArrayList<Pair<Long,Double>>(queryByteRateOverTime);
    }
  }

  public static List<Pair<Long,Double>> getIndexCacheHitRateOverTime() {
    synchronized (indexCacheHitRateOverTime) {
      return new ArrayList<Pair<Long,Double>>(indexCacheHitRateOverTime);
    }
  }

  public static List<Pair<Long,Double>> getDataCacheHitRateOverTime() {
    synchronized (dataCacheHitRateOverTime) {
      return new ArrayList<Pair<Long,Double>>(dataCacheHitRateOverTime);
    }
  }

  public static AccumuloConfiguration getSystemConfiguration() {
    return config.getConfiguration();
  }

  public static Instance getInstance() {
    return instance;
  }

  public static boolean isUsingSsl() {
    return server.isUsingSsl();
  }
}

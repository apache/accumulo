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
package org.apache.accumulo.tserver;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.accumulo.server.problems.ProblemType.TABLET_LOAD;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.CompressedIterators;
import org.apache.accumulo.core.client.impl.CompressedIterators.IterConfig;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletType;
import org.apache.accumulo.core.client.impl.Translator;
import org.apache.accumulo.core.client.impl.Translator.TKeyExtentTranslator;
import org.apache.accumulo.core.client.impl.Translator.TRangeTranslator;
import org.apache.accumulo.core.client.impl.Translators;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.constraints.Constraint.Environment;
import org.apache.accumulo.core.constraints.Violations;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.InitialMultiScan;
import org.apache.accumulo.core.data.thrift.InitialScan;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.data.thrift.ScanResult;
import org.apache.accumulo.core.data.thrift.TCMResult;
import org.apache.accumulo.core.data.thrift.TCMStatus;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TCondition;
import org.apache.accumulo.core.data.thrift.TConditionalMutation;
import org.apache.accumulo.core.data.thrift.TConditionalSession;
import org.apache.accumulo.core.data.thrift.TKey;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.data.thrift.UpdateErrors;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.AuthorizationContainer;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.ScanState;
import org.apache.accumulo.core.tabletserver.thrift.ScanType;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Processor;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.LoggingRunnable;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.core.util.ThriftUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfiguration;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.fs.VolumeUtil;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.master.recovery.RecoveryPath;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.DistributedStoreException;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.server.master.state.TabletStateStore;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.accumulo.server.metrics.AbstractMetricsImpl;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.FileSystemMonitor;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.MasterMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.RpcWrapper;
import org.apache.accumulo.server.util.TServerUtils;
import org.apache.accumulo.server.util.TServerUtils.ServerAddress;
import org.apache.accumulo.server.util.time.RelativeTime;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.start.classloader.vfs.ContextManager;
import org.apache.accumulo.trace.instrument.Span;
import org.apache.accumulo.trace.instrument.Trace;
import org.apache.accumulo.trace.thrift.TInfo;
import org.apache.accumulo.tserver.Compactor.CompactionInfo;
import org.apache.accumulo.tserver.RowLocks.RowLock;
import org.apache.accumulo.tserver.Tablet.CommitSession;
import org.apache.accumulo.tserver.Tablet.KVEntry;
import org.apache.accumulo.tserver.Tablet.LookupResult;
import org.apache.accumulo.tserver.Tablet.MinorCompactionReason;
import org.apache.accumulo.tserver.Tablet.ScanBatch;
import org.apache.accumulo.tserver.Tablet.Scanner;
import org.apache.accumulo.tserver.Tablet.SplitInfo;
import org.apache.accumulo.tserver.Tablet.TConstraintViolationException;
import org.apache.accumulo.tserver.Tablet.TabletClosedException;
import org.apache.accumulo.tserver.TabletServerResourceManager.TabletResourceManager;
import org.apache.accumulo.tserver.TabletStatsKeeper.Operation;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.data.ServerConditionalMutation;
import org.apache.accumulo.tserver.log.DfsLogger;
import org.apache.accumulo.tserver.log.LogSorter;
import org.apache.accumulo.tserver.log.MutationReceiver;
import org.apache.accumulo.tserver.log.TabletServerLogger;
import org.apache.accumulo.tserver.mastermessage.MasterMessage;
import org.apache.accumulo.tserver.mastermessage.SplitReportMessage;
import org.apache.accumulo.tserver.mastermessage.TabletStatusMessage;
import org.apache.accumulo.tserver.metrics.TabletServerMBean;
import org.apache.accumulo.tserver.metrics.TabletServerMinCMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerUpdateMetrics;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.server.TServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

import com.google.common.net.HostAndPort;

enum ScanRunState {
  QUEUED, RUNNING, FINISHED
}

public class TabletServer extends AbstractMetricsImpl implements org.apache.accumulo.tserver.metrics.TabletServerMBean {
  private static final Logger log = Logger.getLogger(TabletServer.class);

  private static HashMap<String,Long> prevGcTime = new HashMap<String,Long>();
  private static long lastMemorySize = 0;
  private static long lastMemoryCheckTime = 0;
  private static long gcTimeIncreasedCount;

  private static final long MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS = 1000;
  private static final long RECENTLY_SPLIT_MILLIES = 60 * 1000;
  private static final long TIME_BETWEEN_GC_CHECKS = 5000;
  private static final long TIME_BETWEEN_LOCATOR_CACHE_CLEARS = 60 * 60 * 1000;

  private TabletServerLogger logger;

  protected TabletServerMinCMetrics mincMetrics = new TabletServerMinCMetrics();

  private ServerConfiguration serverConfig;
  private LogSorter logSorter = null;

  public TabletServer(ServerConfiguration conf, VolumeManager fs) {
    super();
    this.serverConfig = conf;
    this.instance = conf.getInstance();
    this.fs = fs;
    this.logSorter = new LogSorter(instance, fs, getSystemConfiguration());
    SimpleTimer.getInstance().schedule(new Runnable() {
      @Override
      public void run() {
        synchronized (onlineTablets) {
          long now = System.currentTimeMillis();
          for (Tablet tablet : onlineTablets.values())
            try {
              tablet.updateRates(now);
            } catch (Exception ex) {
              log.error(ex, ex);
            }
        }
      }
    }, TIME_BETWEEN_GC_CHECKS, TIME_BETWEEN_GC_CHECKS);
    SimpleTimer.getInstance().schedule(new Runnable() {
      @Override
      public void run() {
        TabletLocator.clearLocators();
      }
    }, jitter(TIME_BETWEEN_LOCATOR_CACHE_CLEARS), jitter(TIME_BETWEEN_LOCATOR_CACHE_CLEARS));
  }

  private static long jitter(long ms) {
    Random r = new Random();
    // add a random 10% wait
    return (long)((1. + (r.nextDouble() / 10)) * ms);
  }

  private synchronized static void logGCInfo(AccumuloConfiguration conf) {
    long now = System.currentTimeMillis();

    List<GarbageCollectorMXBean> gcmBeans = ManagementFactory.getGarbageCollectorMXBeans();
    Runtime rt = Runtime.getRuntime();

    StringBuilder sb = new StringBuilder("gc");

    boolean sawChange = false;

    long maxIncreaseInCollectionTime = 0;

    for (GarbageCollectorMXBean gcBean : gcmBeans) {
      Long prevTime = prevGcTime.get(gcBean.getName());
      long pt = 0;
      if (prevTime != null) {
        pt = prevTime;
      }

      long time = gcBean.getCollectionTime();

      if (time - pt != 0) {
        sawChange = true;
      }

      long increaseInCollectionTime = time - pt;
      sb.append(String.format(" %s=%,.2f(+%,.2f) secs", gcBean.getName(), time / 1000.0, increaseInCollectionTime / 1000.0));
      maxIncreaseInCollectionTime = Math.max(increaseInCollectionTime, maxIncreaseInCollectionTime);
      prevGcTime.put(gcBean.getName(), time);
    }

    long mem = rt.freeMemory();
    if (maxIncreaseInCollectionTime == 0) {
      gcTimeIncreasedCount = 0;
    } else {
      gcTimeIncreasedCount++;
      if (gcTimeIncreasedCount > 3 && mem < rt.maxMemory() * 0.05) {
        log.warn("Running low on memory");
        gcTimeIncreasedCount = 0;
      }
    }

    if (mem > lastMemorySize) {
      sawChange = true;
    }

    String sign = "+";
    if (mem - lastMemorySize <= 0) {
      sign = "";
    }

    sb.append(String.format(" freemem=%,d(%s%,d) totalmem=%,d", mem, sign, (mem - lastMemorySize), rt.totalMemory()));

    if (sawChange) {
      log.debug(sb.toString());
    }

    final long keepAliveTimeout = conf.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT);
    if (lastMemoryCheckTime > 0 && lastMemoryCheckTime < now) {
      long diff = now - lastMemoryCheckTime;
      if (diff > keepAliveTimeout) {
        log.warn(String.format("GC pause checker not called in a timely fashion. Expected every %.1f seconds but was %.1f seconds since last check",
            TIME_BETWEEN_GC_CHECKS / 1000., diff / 1000.));
      }
      lastMemoryCheckTime = now;
      return;
    }

    if (maxIncreaseInCollectionTime > keepAliveTimeout) {
      Halt.halt("Garbage collection may be interfering with lock keep-alive.  Halting.", -1);
    }

    lastMemorySize = mem;
    lastMemoryCheckTime = now;
  }

  private TabletStatsKeeper statsKeeper;

  private static class Session {
    long lastAccessTime;
    long startTime;
    String user;
    String client = TServerUtils.clientAddress.get();
    public boolean reserved;

    public void cleanup() {}
  }

  private static class SessionManager {

    SecureRandom random;
    Map<Long,Session> sessions;
    long maxIdle;

    SessionManager(AccumuloConfiguration conf) {
      random = new SecureRandom();
      sessions = new HashMap<Long,Session>();

      maxIdle = conf.getTimeInMillis(Property.TSERV_SESSION_MAXIDLE);

      Runnable r = new Runnable() {
        @Override
        public void run() {
          sweep(maxIdle);
        }
      };

      SimpleTimer.getInstance().schedule(r, 0, Math.max(maxIdle / 2, 1000));
    }

    synchronized long createSession(Session session, boolean reserve) {
      long sid = random.nextLong();

      while (sessions.containsKey(sid)) {
        sid = random.nextLong();
      }

      sessions.put(sid, session);

      session.reserved = reserve;

      session.startTime = session.lastAccessTime = System.currentTimeMillis();

      return sid;
    }

    long getMaxIdleTime() {
      return maxIdle;
    }

    /**
     * while a session is reserved, it cannot be canceled or removed
     */
    synchronized Session reserveSession(long sessionId) {
      Session session = sessions.get(sessionId);
      if (session != null) {
        if (session.reserved)
          throw new IllegalStateException();
        session.reserved = true;
      }

      return session;

    }

    synchronized Session reserveSession(long sessionId, boolean wait) {
      Session session = sessions.get(sessionId);
      if (session != null) {
        while (wait && session.reserved) {
          try {
            wait(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException();
          }
        }

        if (session.reserved)
          throw new IllegalStateException();
        session.reserved = true;
      }

      return session;

    }

    synchronized void unreserveSession(Session session) {
      if (!session.reserved)
        throw new IllegalStateException();
      notifyAll();
      session.reserved = false;
      session.lastAccessTime = System.currentTimeMillis();
    }

    synchronized void unreserveSession(long sessionId) {
      Session session = getSession(sessionId);
      if (session != null)
        unreserveSession(session);
    }

    synchronized Session getSession(long sessionId) {
      Session session = sessions.get(sessionId);
      if (session != null)
        session.lastAccessTime = System.currentTimeMillis();
      return session;
    }

    Session removeSession(long sessionId) {
      return removeSession(sessionId, false);
    }

    Session removeSession(long sessionId, boolean unreserve) {
      Session session = null;
      synchronized (this) {
        session = sessions.remove(sessionId);
        if (unreserve && session != null)
          unreserveSession(session);
      }

      // do clean up out side of lock..
      if (session != null)
        session.cleanup();

      return session;
    }

    private void sweep(long maxIdle) {
      ArrayList<Session> sessionsToCleanup = new ArrayList<Session>();
      synchronized (this) {
        Iterator<Session> iter = sessions.values().iterator();
        while (iter.hasNext()) {
          Session session = iter.next();
          long idleTime = System.currentTimeMillis() - session.lastAccessTime;
          if (idleTime > maxIdle && !session.reserved) {
            log.info("Closing idle session from user=" + session.user + ", client=" + session.client + ", idle=" + idleTime + "ms");
            iter.remove();
            sessionsToCleanup.add(session);
          }
        }
      }

      // do clean up outside of lock
      for (Session session : sessionsToCleanup) {
        session.cleanup();
      }
    }

    synchronized void removeIfNotAccessed(final long sessionId, final long delay) {
      Session session = sessions.get(sessionId);
      if (session != null) {
        final long removeTime = session.lastAccessTime;
        TimerTask r = new TimerTask() {
          @Override
          public void run() {
            Session sessionToCleanup = null;
            synchronized (SessionManager.this) {
              Session session2 = sessions.get(sessionId);
              if (session2 != null && session2.lastAccessTime == removeTime && !session2.reserved) {
                log.info("Closing not accessed session from user=" + session2.user + ", client=" + session2.client + ", duration=" + delay + "ms");
                sessions.remove(sessionId);
                sessionToCleanup = session2;
              }
            }

            // call clean up outside of lock
            if (sessionToCleanup != null)
              sessionToCleanup.cleanup();
          }
        };

        SimpleTimer.getInstance().schedule(r, delay);
      }
    }

    public synchronized Map<String,MapCounter<ScanRunState>> getActiveScansPerTable() {
      Map<String,MapCounter<ScanRunState>> counts = new HashMap<String,MapCounter<ScanRunState>>();
      for (Entry<Long,Session> entry : sessions.entrySet()) {

        Session session = entry.getValue();
        @SuppressWarnings("rawtypes")
        ScanTask nbt = null;
        String tableID = null;

        if (session instanceof ScanSession) {
          ScanSession ss = (ScanSession) session;
          nbt = ss.nextBatchTask;
          tableID = ss.extent.getTableId().toString();
        } else if (session instanceof MultiScanSession) {
          MultiScanSession mss = (MultiScanSession) session;
          nbt = mss.lookupTask;
          tableID = mss.threadPoolExtent.getTableId().toString();
        }

        if (nbt == null)
          continue;

        ScanRunState srs = nbt.getScanRunState();

        if (srs == ScanRunState.FINISHED)
          continue;

        MapCounter<ScanRunState> stateCounts = counts.get(tableID);
        if (stateCounts == null) {
          stateCounts = new MapCounter<ScanRunState>();
          counts.put(tableID, stateCounts);
        }

        stateCounts.increment(srs, 1);
      }

      return counts;
    }

    public synchronized List<ActiveScan> getActiveScans() {

      ArrayList<ActiveScan> activeScans = new ArrayList<ActiveScan>();

      long ct = System.currentTimeMillis();

      for (Entry<Long,Session> entry : sessions.entrySet()) {
        Session session = entry.getValue();
        if (session instanceof ScanSession) {
          ScanSession ss = (ScanSession) session;

          ScanState state = ScanState.RUNNING;

          ScanTask<ScanBatch> nbt = ss.nextBatchTask;
          if (nbt == null) {
            state = ScanState.IDLE;
          } else {
            switch (nbt.getScanRunState()) {
              case QUEUED:
                state = ScanState.QUEUED;
                break;
              case FINISHED:
                state = ScanState.IDLE;
                break;
              case RUNNING:
              default:
                /* do nothing */
                break;
            }
          }

          ActiveScan activeScan = new ActiveScan(ss.client, ss.user, ss.extent.getTableId().toString(), ct - ss.startTime, ct - ss.lastAccessTime,
              ScanType.SINGLE, state, ss.extent.toThrift(), Translator.translate(ss.columnSet, Translators.CT), ss.ssiList, ss.ssio,
              ss.auths.getAuthorizationsBB());

          // scanId added by ACCUMULO-2641 is an optional thrift argument and not available in ActiveScan constructor
          activeScan.setScanId(entry.getKey());
          activeScans.add(activeScan);

        } else if (session instanceof MultiScanSession) {
          MultiScanSession mss = (MultiScanSession) session;

          ScanState state = ScanState.RUNNING;

          ScanTask<MultiScanResult> nbt = mss.lookupTask;
          if (nbt == null) {
            state = ScanState.IDLE;
          } else {
            switch (nbt.getScanRunState()) {
              case QUEUED:
                state = ScanState.QUEUED;
                break;
              case FINISHED:
                state = ScanState.IDLE;
                break;
              case RUNNING:
              default:
                /* do nothing */
                break;
            }
          }

          activeScans.add(new ActiveScan(mss.client, mss.user, mss.threadPoolExtent.getTableId().toString(), ct - mss.startTime, ct - mss.lastAccessTime,
              ScanType.BATCH, state, mss.threadPoolExtent.toThrift(), Translator.translate(mss.columnSet, Translators.CT), mss.ssiList, mss.ssio, mss.auths
                  .getAuthorizationsBB()));
        }
      }

      return activeScans;
    }
  }

  static class TservConstraintEnv implements Environment {

    private TCredentials credentials;
    private SecurityOperation security;
    private Authorizations auths;
    private KeyExtent ke;

    TservConstraintEnv(SecurityOperation secOp, TCredentials credentials) {
      this.security = secOp;
      this.credentials = credentials;
    }

    void setExtent(KeyExtent ke) {
      this.ke = ke;
    }

    @Override
    public KeyExtent getExtent() {
      return ke;
    }

    @Override
    public String getUser() {
      return credentials.getPrincipal();
    }

    @Override
    @Deprecated
    public Authorizations getAuthorizations() {
      if (auths == null)
        try {
          this.auths = security.getUserAuthorizations(credentials);
        } catch (ThriftSecurityException e) {
          throw new RuntimeException(e);
        }
      return auths;
    }

    @Override
    public AuthorizationContainer getAuthorizationsContainer() {
      return new AuthorizationContainer() {
        @Override
        public boolean contains(ByteSequence auth) {
          try {
            return security.userHasAuthorizations(credentials,
                Collections.<ByteBuffer> singletonList(ByteBuffer.wrap(auth.getBackingArray(), auth.offset(), auth.length())));
          } catch (ThriftSecurityException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
  }

  private abstract class ScanTask<T> implements RunnableFuture<T> {

    protected AtomicBoolean interruptFlag;
    protected ArrayBlockingQueue<Object> resultQueue;
    protected AtomicInteger state;
    protected AtomicReference<ScanRunState> runState;

    private static final int INITIAL = 1;
    private static final int ADDED = 2;
    private static final int CANCELED = 3;

    ScanTask() {
      interruptFlag = new AtomicBoolean(false);
      runState = new AtomicReference<ScanRunState>(ScanRunState.QUEUED);
      state = new AtomicInteger(INITIAL);
      resultQueue = new ArrayBlockingQueue<Object>(1);
    }

    protected void addResult(Object o) {
      if (state.compareAndSet(INITIAL, ADDED))
        resultQueue.add(o);
      else if (state.get() == ADDED)
        throw new IllegalStateException("Tried to add more than one result");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (!mayInterruptIfRunning)
        throw new IllegalArgumentException("Cancel will always attempt to interupt running next batch task");

      if (state.get() == CANCELED)
        return true;

      if (state.compareAndSet(INITIAL, CANCELED)) {
        interruptFlag.set(true);
        resultQueue = null;
        return true;
      }

      return false;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {

      ArrayBlockingQueue<Object> localRQ = resultQueue;

      if (state.get() == CANCELED)
        throw new CancellationException();

      if (localRQ == null && state.get() == ADDED)
        throw new IllegalStateException("Tried to get result twice");

      Object r = localRQ.poll(timeout, unit);

      // could have been canceled while waiting
      if (state.get() == CANCELED) {
        if (r != null)
          throw new IllegalStateException("Nothing should have been added when in canceled state");

        throw new CancellationException();
      }

      if (r == null)
        throw new TimeoutException();

      // make this method stop working now that something is being
      // returned
      resultQueue = null;

      if (r instanceof Throwable)
        throw new ExecutionException((Throwable) r);

      return (T) r;
    }

    @Override
    public boolean isCancelled() {
      return state.get() == CANCELED;
    }

    @Override
    public boolean isDone() {
      return runState.get().equals(ScanRunState.FINISHED);
    }

    public ScanRunState getScanRunState() {
      return runState.get();
    }

  }

  private static class ConditionalSession extends Session {
    public TCredentials credentials;
    public Authorizations auths;
    public String tableId;
    public AtomicBoolean interruptFlag;

    @Override
    public void cleanup() {
      interruptFlag.set(true);
    }
  }

  private static class UpdateSession extends Session {
    public Tablet currentTablet;
    public MapCounter<Tablet> successfulCommits = new MapCounter<Tablet>();
    Map<KeyExtent,Long> failures = new HashMap<KeyExtent,Long>();
    HashMap<KeyExtent,SecurityErrorCode> authFailures = new HashMap<KeyExtent,SecurityErrorCode>();
    public Violations violations;
    public TCredentials credentials;
    public long totalUpdates = 0;
    public long flushTime = 0;
    Stat prepareTimes = new Stat();
    Stat walogTimes = new Stat();
    Stat commitTimes = new Stat();
    Stat authTimes = new Stat();
    public Map<Tablet,List<Mutation>> queuedMutations = new HashMap<Tablet,List<Mutation>>();
    public long queuedMutationSize = 0;
    TservConstraintEnv cenv = null;
  }

  private static class ScanSession extends Session {
    public KeyExtent extent;
    public HashSet<Column> columnSet;
    public List<IterInfo> ssiList;
    public Map<String,Map<String,String>> ssio;
    public Authorizations auths;
    public long entriesReturned = 0;
    public Stat nbTimes = new Stat();
    public long batchCount = 0;
    public volatile ScanTask<ScanBatch> nextBatchTask;
    public AtomicBoolean interruptFlag;
    public Scanner scanner;
    public long readaheadThreshold = Constants.SCANNER_DEFAULT_READAHEAD_THRESHOLD;

    @Override
    public void cleanup() {
      try {
        if (nextBatchTask != null)
          nextBatchTask.cancel(true);
      } finally {
        if (scanner != null)
          scanner.close();
      }
    }

  }

  private static class MultiScanSession extends Session {
    HashSet<Column> columnSet;
    Map<KeyExtent,List<Range>> queries;
    public List<IterInfo> ssiList;
    public Map<String,Map<String,String>> ssio;
    public Authorizations auths;

    // stats
    int numRanges;
    int numTablets;
    int numEntries;
    long totalLookupTime;

    public volatile ScanTask<MultiScanResult> lookupTask;
    public KeyExtent threadPoolExtent;

    @Override
    public void cleanup() {
      if (lookupTask != null)
        lookupTask.cancel(true);
    }
  }

  /**
   * This little class keeps track of writes in progress and allows readers to wait for writes that started before the read. It assumes that the operation ids
   * are monotonically increasing.
   *
   */
  static class WriteTracker {
    private static AtomicLong operationCounter = new AtomicLong(1);
    private Map<TabletType,TreeSet<Long>> inProgressWrites = new EnumMap<TabletType,TreeSet<Long>>(TabletType.class);

    WriteTracker() {
      for (TabletType ttype : TabletType.values()) {
        inProgressWrites.put(ttype, new TreeSet<Long>());
      }
    }

    synchronized long startWrite(TabletType ttype) {
      long operationId = operationCounter.getAndIncrement();
      inProgressWrites.get(ttype).add(operationId);
      return operationId;
    }

    synchronized void finishWrite(long operationId) {
      if (operationId == -1)
        return;

      boolean removed = false;

      for (TabletType ttype : TabletType.values()) {
        removed = inProgressWrites.get(ttype).remove(operationId);
        if (removed)
          break;
      }

      if (!removed) {
        throw new IllegalArgumentException("Attempted to finish write not in progress,  operationId " + operationId);
      }

      this.notifyAll();
    }

    synchronized void waitForWrites(TabletType ttype) {
      long operationId = operationCounter.getAndIncrement();
      while (inProgressWrites.get(ttype).floor(operationId) != null) {
        try {
          this.wait();
        } catch (InterruptedException e) {
          log.error(e, e);
        }
      }
    }

    public long startWrite(Set<Tablet> keySet) {
      if (keySet.size() == 0)
        return -1;

      ArrayList<KeyExtent> extents = new ArrayList<KeyExtent>(keySet.size());

      for (Tablet tablet : keySet)
        extents.add(tablet.getExtent());

      return startWrite(TabletType.type(extents));
    }
  }

  public AccumuloConfiguration getSystemConfiguration() {
    return serverConfig.getConfiguration();
  }

  TransactionWatcher watcher = new TransactionWatcher();

  private class ThriftClientHandler extends ClientServiceHandler implements TabletClientService.Iface {

    SessionManager sessionManager;

    AccumuloConfiguration acuConf = getSystemConfiguration();

    TabletServerUpdateMetrics updateMetrics = new TabletServerUpdateMetrics();

    TabletServerScanMetrics scanMetrics = new TabletServerScanMetrics();

    WriteTracker writeTracker = new WriteTracker();

    private RowLocks rowLocks = new RowLocks();

    ThriftClientHandler() {
      super(instance, watcher, fs);
      log.debug(ThriftClientHandler.class.getName() + " created");
      sessionManager = new SessionManager(getSystemConfiguration());
      // Register the metrics MBean
      try {
        updateMetrics.register();
        scanMetrics.register();
      } catch (Exception e) {
        log.error("Exception registering MBean with MBean Server", e);
      }
    }

    @Override
    public List<TKeyExtent> bulkImport(TInfo tinfo, TCredentials credentials, long tid, Map<TKeyExtent,Map<String,MapFileInfo>> files, boolean setTime)
        throws ThriftSecurityException {

      if (!security.canPerformSystemActions(credentials))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

      List<TKeyExtent> failures = new ArrayList<TKeyExtent>();

      for (Entry<TKeyExtent,Map<String,MapFileInfo>> entry : files.entrySet()) {
        TKeyExtent tke = entry.getKey();
        Map<String,MapFileInfo> fileMap = entry.getValue();
        Map<FileRef,MapFileInfo> fileRefMap = new HashMap<FileRef,MapFileInfo>();
        for (Entry<String,MapFileInfo> mapping : fileMap.entrySet()) {
          Path path = new Path(mapping.getKey());
          FileSystem ns = fs.getVolumeByPath(path).getFileSystem();
          path = ns.makeQualified(path);
          fileRefMap.put(new FileRef(path.toString(), path), mapping.getValue());
        }

        Tablet importTablet = onlineTablets.get(new KeyExtent(tke));

        if (importTablet == null) {
          failures.add(tke);
        } else {
          try {
            importTablet.importMapFiles(tid, fileRefMap, setTime);
          } catch (IOException ioe) {
            log.info("files " + fileMap.keySet() + " not imported to " + new KeyExtent(tke) + ": " + ioe.getMessage());
            failures.add(tke);
          }
        }
      }
      return failures;
    }

    private class NextBatchTask extends ScanTask<ScanBatch> {

      private long scanID;

      NextBatchTask(long scanID, AtomicBoolean interruptFlag) {
        this.scanID = scanID;
        this.interruptFlag = interruptFlag;

        if (interruptFlag.get())
          cancel(true);
      }

      @Override
      public void run() {

        final ScanSession scanSession = (ScanSession) sessionManager.getSession(scanID);
        String oldThreadName = Thread.currentThread().getName();

        try {
          if (isCancelled() || scanSession == null)
            return;

          runState.set(ScanRunState.RUNNING);

          Thread.currentThread().setName(
              "User: " + scanSession.user + " Start: " + scanSession.startTime + " Client: " + scanSession.client + " Tablet: " + scanSession.extent);

          Tablet tablet = onlineTablets.get(scanSession.extent);

          if (tablet == null) {
            addResult(new org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException(scanSession.extent.toThrift()));
            return;
          }

          long t1 = System.currentTimeMillis();
          ScanBatch batch = scanSession.scanner.read();
          long t2 = System.currentTimeMillis();
          scanSession.nbTimes.addStat(t2 - t1);

          // there should only be one thing on the queue at a time, so
          // it should be ok to call add()
          // instead of put()... if add() fails because queue is at
          // capacity it means there is code
          // problem somewhere
          addResult(batch);
        } catch (TabletClosedException e) {
          addResult(new org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException(scanSession.extent.toThrift()));
        } catch (IterationInterruptedException iie) {
          if (!isCancelled()) {
            log.warn("Iteration interrupted, when scan not cancelled", iie);
            addResult(iie);
          }
        } catch (TooManyFilesException tmfe) {
          addResult(tmfe);
        } catch (Throwable e) {
          log.warn("exception while scanning tablet " + (scanSession == null ? "(unknown)" : scanSession.extent), e);
          addResult(e);
        } finally {
          runState.set(ScanRunState.FINISHED);
          Thread.currentThread().setName(oldThreadName);
        }

      }
    }

    private class LookupTask extends ScanTask<MultiScanResult> {

      private long scanID;

      LookupTask(long scanID) {
        this.scanID = scanID;
      }

      @Override
      public void run() {
        MultiScanSession session = (MultiScanSession) sessionManager.getSession(scanID);
        String oldThreadName = Thread.currentThread().getName();

        try {
          if (isCancelled() || session == null)
            return;

          TableConfiguration acuTableConf = ServerConfiguration.getTableConfiguration(instance, session.threadPoolExtent.getTableId().toString());
          long maxResultsSize = acuTableConf.getMemoryInBytes(Property.TABLE_SCAN_MAXMEM);

          runState.set(ScanRunState.RUNNING);
          Thread.currentThread().setName("Client: " + session.client + " User: " + session.user + " Start: " + session.startTime + " Table: ");

          long bytesAdded = 0;
          long maxScanTime = 4000;

          long startTime = System.currentTimeMillis();

          ArrayList<KVEntry> results = new ArrayList<KVEntry>();
          Map<KeyExtent,List<Range>> failures = new HashMap<KeyExtent,List<Range>>();
          ArrayList<KeyExtent> fullScans = new ArrayList<KeyExtent>();
          KeyExtent partScan = null;
          Key partNextKey = null;
          boolean partNextKeyInclusive = false;

          Iterator<Entry<KeyExtent,List<Range>>> iter = session.queries.entrySet().iterator();

          // check the time so that the read ahead thread is not monopolized
          while (iter.hasNext() && bytesAdded < maxResultsSize && (System.currentTimeMillis() - startTime) < maxScanTime) {
            Entry<KeyExtent,List<Range>> entry = iter.next();

            iter.remove();

            // check that tablet server is serving requested tablet
            Tablet tablet = onlineTablets.get(entry.getKey());
            if (tablet == null) {
              failures.put(entry.getKey(), entry.getValue());
              continue;
            }
            Thread.currentThread().setName(
                "Client: " + session.client + " User: " + session.user + " Start: " + session.startTime + " Tablet: " + entry.getKey().toString());

            LookupResult lookupResult;
            try {

              // do the following check to avoid a race condition
              // between setting false below and the task being
              // canceled
              if (isCancelled())
                interruptFlag.set(true);

              lookupResult = tablet.lookup(entry.getValue(), session.columnSet, session.auths, results, maxResultsSize - bytesAdded, session.ssiList,
                  session.ssio, interruptFlag);

              // if the tablet was closed it it possible that the
              // interrupt flag was set.... do not want it set for
              // the next
              // lookup
              interruptFlag.set(false);

            } catch (IOException e) {
              log.warn("lookup failed for tablet " + entry.getKey(), e);
              throw new RuntimeException(e);
            }

            bytesAdded += lookupResult.bytesAdded;

            if (lookupResult.unfinishedRanges.size() > 0) {
              if (lookupResult.closed) {
                failures.put(entry.getKey(), lookupResult.unfinishedRanges);
              } else {
                session.queries.put(entry.getKey(), lookupResult.unfinishedRanges);
                partScan = entry.getKey();
                partNextKey = lookupResult.unfinishedRanges.get(0).getStartKey();
                partNextKeyInclusive = lookupResult.unfinishedRanges.get(0).isStartKeyInclusive();
              }
            } else {
              fullScans.add(entry.getKey());
            }
          }

          long finishTime = System.currentTimeMillis();
          session.totalLookupTime += (finishTime - startTime);
          session.numEntries += results.size();

          // convert everything to thrift before adding result
          List<TKeyValue> retResults = new ArrayList<TKeyValue>();
          for (KVEntry entry : results)
            retResults.add(new TKeyValue(entry.getKey().toThrift(), ByteBuffer.wrap(entry.getValue().get())));
          Map<TKeyExtent,List<TRange>> retFailures = Translator.translate(failures, Translators.KET,
              new Translator.ListTranslator<Range,TRange>(Translators.RT));
          List<TKeyExtent> retFullScans = Translator.translate(fullScans, Translators.KET);
          TKeyExtent retPartScan = null;
          TKey retPartNextKey = null;
          if (partScan != null) {
            retPartScan = partScan.toThrift();
            retPartNextKey = partNextKey.toThrift();
          }
          // add results to queue
          addResult(new MultiScanResult(retResults, retFailures, retFullScans, retPartScan, retPartNextKey, partNextKeyInclusive, session.queries.size() != 0));
        } catch (IterationInterruptedException iie) {
          if (!isCancelled()) {
            log.warn("Iteration interrupted, when scan not cancelled", iie);
            addResult(iie);
          }
        } catch (Throwable e) {
          log.warn("exception while doing multi-scan ", e);
          addResult(e);
        } finally {
          Thread.currentThread().setName(oldThreadName);
          runState.set(ScanRunState.FINISHED);
        }
      }
    }

    @Override
    public InitialScan startScan(TInfo tinfo, TCredentials credentials, TKeyExtent textent, TRange range, List<TColumn> columns, int batchSize,
        List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites, boolean isolated,
        long readaheadThreshold) throws NotServingTabletException, ThriftSecurityException, org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException {

      String tableId = new String(textent.getTable(), UTF_8);
      if (!security.canScan(credentials, tableId, Tables.getNamespaceId(instance, tableId), range, columns, ssiList, ssio, authorizations))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

      if (!security.userHasAuthorizations(credentials, authorizations))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_AUTHORIZATIONS);

      KeyExtent extent = new KeyExtent(textent);

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
      if (waitForWrites)
        writeTracker.waitForWrites(TabletType.type(extent));

      Tablet tablet = onlineTablets.get(extent);
      if (tablet == null)
        throw new NotServingTabletException(textent);

      ScanSession scanSession = new ScanSession();
      scanSession.user = credentials.getPrincipal();
      scanSession.extent = new KeyExtent(extent);
      scanSession.columnSet = new HashSet<Column>();
      scanSession.ssiList = ssiList;
      scanSession.ssio = ssio;
      scanSession.auths = new Authorizations(authorizations);
      scanSession.interruptFlag = new AtomicBoolean();
      scanSession.readaheadThreshold = readaheadThreshold;

      for (TColumn tcolumn : columns) {
        scanSession.columnSet.add(new Column(tcolumn));
      }

      scanSession.scanner = tablet.createScanner(new Range(range), batchSize, scanSession.columnSet, scanSession.auths, ssiList, ssio, isolated,
          scanSession.interruptFlag);

      long sid = sessionManager.createSession(scanSession, true);

      ScanResult scanResult;
      try {
        scanResult = continueScan(tinfo, sid, scanSession);
      } catch (NoSuchScanIDException e) {
        log.error("The impossible happened", e);
        throw new RuntimeException();
      } finally {
        sessionManager.unreserveSession(sid);
      }

      return new InitialScan(sid, scanResult);
    }

    @Override
    public ScanResult continueScan(TInfo tinfo, long scanID) throws NoSuchScanIDException, NotServingTabletException,
        org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException {
      ScanSession scanSession = (ScanSession) sessionManager.reserveSession(scanID);
      if (scanSession == null) {
        throw new NoSuchScanIDException();
      }

      try {
        return continueScan(tinfo, scanID, scanSession);
      } finally {
        sessionManager.unreserveSession(scanSession);
      }
    }

    private ScanResult continueScan(TInfo tinfo, long scanID, ScanSession scanSession) throws NoSuchScanIDException, NotServingTabletException,
        org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException {

      if (scanSession.nextBatchTask == null) {
        scanSession.nextBatchTask = new NextBatchTask(scanID, scanSession.interruptFlag);
        resourceManager.executeReadAhead(scanSession.extent, scanSession.nextBatchTask);
      }

      ScanBatch bresult;
      try {
        bresult = scanSession.nextBatchTask.get(MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS, TimeUnit.MILLISECONDS);
        scanSession.nextBatchTask = null;
      } catch (ExecutionException e) {
        sessionManager.removeSession(scanID);
        if (e.getCause() instanceof NotServingTabletException)
          throw (NotServingTabletException) e.getCause();
        else if (e.getCause() instanceof TooManyFilesException)
          throw new org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException(scanSession.extent.toThrift());
        else
          throw new RuntimeException(e);
      } catch (CancellationException ce) {
        sessionManager.removeSession(scanID);
        Tablet tablet = onlineTablets.get(scanSession.extent);
        if (tablet == null || tablet.isClosed())
          throw new NotServingTabletException(scanSession.extent.toThrift());
        else
          throw new NoSuchScanIDException();
      } catch (TimeoutException e) {
        List<TKeyValue> param = Collections.emptyList();
        long timeout = acuConf.getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
        sessionManager.removeIfNotAccessed(scanID, timeout);
        return new ScanResult(param, true);
      } catch (Throwable t) {
        sessionManager.removeSession(scanID);
        log.warn("Failed to get next batch", t);
        throw new RuntimeException(t);
      }

      ScanResult scanResult = new ScanResult(Key.compress(bresult.results), bresult.more);

      scanSession.entriesReturned += scanResult.results.size();

      scanSession.batchCount++;

      if (scanResult.more && scanSession.batchCount > scanSession.readaheadThreshold) {
        // start reading next batch while current batch is transmitted
        // to client
        scanSession.nextBatchTask = new NextBatchTask(scanID, scanSession.interruptFlag);
        resourceManager.executeReadAhead(scanSession.extent, scanSession.nextBatchTask);
      }

      if (!scanResult.more)
        closeScan(tinfo, scanID);

      return scanResult;
    }

    @Override
    public void closeScan(TInfo tinfo, long scanID) {
      ScanSession ss = (ScanSession) sessionManager.removeSession(scanID);
      if (ss != null) {
        long t2 = System.currentTimeMillis();

        log.debug(String.format("ScanSess tid %s %s %,d entries in %.2f secs, nbTimes = [%s] ", TServerUtils.clientAddress.get(), ss.extent.getTableId()
            .toString(), ss.entriesReturned, (t2 - ss.startTime) / 1000.0, ss.nbTimes.toString()));
        if (scanMetrics.isEnabled()) {
          scanMetrics.add(TabletServerScanMetrics.scan, t2 - ss.startTime);
          scanMetrics.add(TabletServerScanMetrics.resultSize, ss.entriesReturned);
        }
      }
    }

    @Override
    public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials, Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns,
        List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites) throws ThriftSecurityException {
      // find all of the tables that need to be scanned
      HashSet<String> tables = new HashSet<String>();
      for (TKeyExtent keyExtent : tbatch.keySet()) {
        tables.add(new String(keyExtent.getTable(), UTF_8));
      }

      if (tables.size() != 1)
        throw new IllegalArgumentException("Cannot batch scan over multiple tables");

      // check if user has permission to the tables
      for (String tableId : tables)
        if (!security.canScan(credentials, tableId, Tables.getNamespaceId(instance, tableId), tbatch, tcolumns, ssiList, ssio, authorizations))
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

      try {
        if (!security.userHasAuthorizations(credentials, authorizations))
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_AUTHORIZATIONS);
      } catch (ThriftSecurityException tse) {
        log.error(tse, tse);
        throw tse;
      }
      Map<KeyExtent,List<Range>> batch = Translator.translate(tbatch, new TKeyExtentTranslator(), new Translator.ListTranslator<TRange,Range>(
          new TRangeTranslator()));

      // This is used to determine which thread pool to use
      KeyExtent threadPoolExtent = batch.keySet().iterator().next();

      if (waitForWrites)
        writeTracker.waitForWrites(TabletType.type(batch.keySet()));

      MultiScanSession mss = new MultiScanSession();
      mss.user = credentials.getPrincipal();
      mss.queries = batch;
      mss.columnSet = new HashSet<Column>(tcolumns.size());
      mss.ssiList = ssiList;
      mss.ssio = ssio;
      mss.auths = new Authorizations(authorizations);

      mss.numTablets = batch.size();
      for (List<Range> ranges : batch.values()) {
        mss.numRanges += ranges.size();
      }

      for (TColumn tcolumn : tcolumns)
        mss.columnSet.add(new Column(tcolumn));

      mss.threadPoolExtent = threadPoolExtent;

      long sid = sessionManager.createSession(mss, true);

      MultiScanResult result;
      try {
        result = continueMultiScan(tinfo, sid, mss);
      } catch (NoSuchScanIDException e) {
        log.error("the impossible happened", e);
        throw new RuntimeException("the impossible happened", e);
      } finally {
        sessionManager.unreserveSession(sid);
      }

      return new InitialMultiScan(sid, result);
    }

    @Override
    public MultiScanResult continueMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException {

      MultiScanSession session = (MultiScanSession) sessionManager.reserveSession(scanID);

      if (session == null) {
        throw new NoSuchScanIDException();
      }

      try {
        return continueMultiScan(tinfo, scanID, session);
      } finally {
        sessionManager.unreserveSession(session);
      }
    }

    private MultiScanResult continueMultiScan(TInfo tinfo, long scanID, MultiScanSession session) throws NoSuchScanIDException {

      if (session.lookupTask == null) {
        session.lookupTask = new LookupTask(scanID);
        resourceManager.executeReadAhead(session.threadPoolExtent, session.lookupTask);
      }

      try {
        MultiScanResult scanResult = session.lookupTask.get(MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS, TimeUnit.MILLISECONDS);
        session.lookupTask = null;
        return scanResult;
      } catch (TimeoutException e1) {
        long timeout = acuConf.getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
        sessionManager.removeIfNotAccessed(scanID, timeout);
        List<TKeyValue> results = Collections.emptyList();
        Map<TKeyExtent,List<TRange>> failures = Collections.emptyMap();
        List<TKeyExtent> fullScans = Collections.emptyList();
        return new MultiScanResult(results, failures, fullScans, null, null, false, true);
      } catch (Throwable t) {
        sessionManager.removeSession(scanID);
        log.warn("Failed to get multiscan result", t);
        throw new RuntimeException(t);
      }
    }

    @Override
    public void closeMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException {
      MultiScanSession session = (MultiScanSession) sessionManager.removeSession(scanID);
      if (session == null) {
        throw new NoSuchScanIDException();
      }

      long t2 = System.currentTimeMillis();
      log.debug(String.format("MultiScanSess %s %,d entries in %.2f secs (lookup_time:%.2f secs tablets:%,d ranges:%,d) ", TServerUtils.clientAddress.get(),
          session.numEntries, (t2 - session.startTime) / 1000.0, session.totalLookupTime / 1000.0, session.numTablets, session.numRanges));
    }

    @Override
    public long startUpdate(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException {
      // Make sure user is real

      security.authenticateUser(credentials, credentials);
      if (updateMetrics.isEnabled())
        updateMetrics.add(TabletServerUpdateMetrics.permissionErrors, 0);

      UpdateSession us = new UpdateSession();
      us.violations = new Violations();
      us.credentials = credentials;
      us.cenv = new TservConstraintEnv(security, us.credentials);

      long sid = sessionManager.createSession(us, false);

      return sid;
    }

    private void setUpdateTablet(UpdateSession us, KeyExtent keyExtent) {
      long t1 = System.currentTimeMillis();
      if (us.currentTablet != null && us.currentTablet.getExtent().equals(keyExtent))
        return;
      if (us.currentTablet == null && (us.failures.containsKey(keyExtent) || us.authFailures.containsKey(keyExtent))) {
        // if there were previous failures, then do not accept additional writes
        return;
      }

      try {
        // if user has no permission to write to this table, add it to
        // the failures list
        boolean sameTable = us.currentTablet != null && (us.currentTablet.getExtent().getTableId().equals(keyExtent.getTableId()));
        String tableId = keyExtent.getTableId().toString();
        if (sameTable || security.canWrite(us.credentials, tableId, Tables.getNamespaceId(instance, tableId))) {
          long t2 = System.currentTimeMillis();
          us.authTimes.addStat(t2 - t1);
          us.currentTablet = onlineTablets.get(keyExtent);
          if (us.currentTablet != null) {
            us.queuedMutations.put(us.currentTablet, new ArrayList<Mutation>());
          } else {
            // not serving tablet, so report all mutations as
            // failures
            us.failures.put(keyExtent, 0l);
            if (updateMetrics.isEnabled())
              updateMetrics.add(TabletServerUpdateMetrics.unknownTabletErrors, 0);
          }
        } else {
          log.warn("Denying access to table " + keyExtent.getTableId() + " for user " + us.credentials.getPrincipal());
          long t2 = System.currentTimeMillis();
          us.authTimes.addStat(t2 - t1);
          us.currentTablet = null;
          us.authFailures.put(keyExtent, SecurityErrorCode.PERMISSION_DENIED);
          if (updateMetrics.isEnabled())
            updateMetrics.add(TabletServerUpdateMetrics.permissionErrors, 0);
          return;
        }
      } catch (ThriftSecurityException e) {
        log.error("Denying permission to check user " + us.credentials.getPrincipal() + " with user " + e.getUser(), e);
        long t2 = System.currentTimeMillis();
        us.authTimes.addStat(t2 - t1);
        us.currentTablet = null;
        us.authFailures.put(keyExtent, e.getCode());
        if (updateMetrics.isEnabled())
          updateMetrics.add(TabletServerUpdateMetrics.permissionErrors, 0);
        return;
      }
    }

    @Override
    public void applyUpdates(TInfo tinfo, long updateID, TKeyExtent tkeyExtent, List<TMutation> tmutations) {
      UpdateSession us = (UpdateSession) sessionManager.reserveSession(updateID);
      if (us == null) {
        throw new RuntimeException("No Such SessionID");
      }

      try {
        KeyExtent keyExtent = new KeyExtent(tkeyExtent);
        setUpdateTablet(us, keyExtent);

        if (us.currentTablet != null) {
          List<Mutation> mutations = us.queuedMutations.get(us.currentTablet);
          for (TMutation tmutation : tmutations) {
            Mutation mutation = new ServerMutation(tmutation);
            mutations.add(mutation);
            us.queuedMutationSize += mutation.numBytes();
          }
          if (us.queuedMutationSize > getSystemConfiguration().getMemoryInBytes(Property.TSERV_MUTATION_QUEUE_MAX))
            flush(us);
        }
      } finally {
        sessionManager.unreserveSession(us);
      }
    }

    private void flush(UpdateSession us) {

      int mutationCount = 0;
      Map<CommitSession,List<Mutation>> sendables = new HashMap<CommitSession,List<Mutation>>();
      Throwable error = null;

      long pt1 = System.currentTimeMillis();

      boolean containsMetadataTablet = false;
      for (Tablet tablet : us.queuedMutations.keySet())
        if (tablet.getExtent().isMeta())
          containsMetadataTablet = true;

      if (!containsMetadataTablet && us.queuedMutations.size() > 0)
        TabletServer.this.resourceManager.waitUntilCommitsAreEnabled();

      Span prep = Trace.start("prep");
      try {
        for (Entry<Tablet,? extends List<Mutation>> entry : us.queuedMutations.entrySet()) {

          Tablet tablet = entry.getKey();
          List<Mutation> mutations = entry.getValue();
          if (mutations.size() > 0) {
            try {
              if (updateMetrics.isEnabled())
                updateMetrics.add(TabletServerUpdateMetrics.mutationArraySize, mutations.size());

              CommitSession commitSession = tablet.prepareMutationsForCommit(us.cenv, mutations);
              if (commitSession == null) {
                if (us.currentTablet == tablet) {
                  us.currentTablet = null;
                }
                us.failures.put(tablet.getExtent(), us.successfulCommits.get(tablet));
              } else {
                sendables.put(commitSession, mutations);
                mutationCount += mutations.size();
              }

            } catch (TConstraintViolationException e) {
              us.violations.add(e.getViolations());
              if (updateMetrics.isEnabled())
                updateMetrics.add(TabletServerUpdateMetrics.constraintViolations, 0);

              if (e.getNonViolators().size() > 0) {
                // only log and commit mutations if there were some
                // that did not
                // violate constraints... this is what
                // prepareMutationsForCommit()
                // expects
                sendables.put(e.getCommitSession(), e.getNonViolators());
              }

              mutationCount += mutations.size();

            } catch (HoldTimeoutException t) {
              error = t;
              log.debug("Giving up on mutations due to a long memory hold time");
              break;
            } catch (Throwable t) {
              error = t;
              log.error("Unexpected error preparing for commit", error);
              break;
            }
          }
        }
      } finally {
        prep.stop();
      }

      long pt2 = System.currentTimeMillis();
      us.prepareTimes.addStat(pt2 - pt1);
      updateAvgPrepTime(pt2 - pt1, us.queuedMutations.size());

      if (error != null) {
        for (Entry<CommitSession,List<Mutation>> e : sendables.entrySet()) {
          e.getKey().abortCommit(e.getValue());
        }
        throw new RuntimeException(error);
      }
      try {
        Span wal = Trace.start("wal");
        try {
          while (true) {
            try {
              long t1 = System.currentTimeMillis();

              logger.logManyTablets(sendables);

              long t2 = System.currentTimeMillis();
              us.walogTimes.addStat(t2 - t1);
              updateWalogWriteTime((t2 - t1));
              break;
            } catch (IOException ex) {
              log.warn("logging mutations failed, retrying");
            } catch (FSError ex) { // happens when DFS is localFS
              log.warn("logging mutations failed, retrying");
            } catch (Throwable t) {
              log.error("Unknown exception logging mutations, counts for mutations in flight not decremented!", t);
              throw new RuntimeException(t);
            }
          }
        } finally {
          wal.stop();
        }

        Span commit = Trace.start("commit");
        try {
          long t1 = System.currentTimeMillis();
          for (Entry<CommitSession,? extends List<Mutation>> entry : sendables.entrySet()) {
            CommitSession commitSession = entry.getKey();
            List<Mutation> mutations = entry.getValue();

            commitSession.commit(mutations);

            Tablet tablet = commitSession.getTablet();

            if (tablet == us.currentTablet) {
              // because constraint violations may filter out some
              // mutations, for proper
              // accounting with the client code, need to increment
              // the count based
              // on the original number of mutations from the client
              // NOT the filtered number
              us.successfulCommits.increment(tablet, us.queuedMutations.get(tablet).size());
            }
          }
          long t2 = System.currentTimeMillis();

          us.flushTime += (t2 - pt1);
          us.commitTimes.addStat(t2 - t1);

          updateAvgCommitTime(t2 - t1, sendables.size());
        } finally {
          commit.stop();
        }
      } finally {
        us.queuedMutations.clear();
        if (us.currentTablet != null) {
          us.queuedMutations.put(us.currentTablet, new ArrayList<Mutation>());
        }
        us.queuedMutationSize = 0;
      }
      us.totalUpdates += mutationCount;
    }

    private void updateWalogWriteTime(long time) {
      if (updateMetrics.isEnabled())
        updateMetrics.add(TabletServerUpdateMetrics.waLogWriteTime, time);
    }

    private void updateAvgCommitTime(long time, int size) {
      if (updateMetrics.isEnabled())
        updateMetrics.add(TabletServerUpdateMetrics.commitTime, (long) ((time) / (double) size));
    }

    private void updateAvgPrepTime(long time, int size) {
      if (updateMetrics.isEnabled())
        updateMetrics.add(TabletServerUpdateMetrics.commitPrep, (long) ((time) / (double) size));
    }

    @Override
    public UpdateErrors closeUpdate(TInfo tinfo, long updateID) throws NoSuchScanIDException {
      UpdateSession us = (UpdateSession) sessionManager.removeSession(updateID);
      if (us == null) {
        throw new NoSuchScanIDException();
      }

      // clients may or may not see data from an update session while
      // it is in progress, however when the update session is closed
      // want to ensure that reads wait for the write to finish
      long opid = writeTracker.startWrite(us.queuedMutations.keySet());

      try {
        flush(us);
      } finally {
        writeTracker.finishWrite(opid);
      }

      log.debug(String.format("UpSess %s %,d in %.3fs, at=[%s] ft=%.3fs(pt=%.3fs lt=%.3fs ct=%.3fs)", TServerUtils.clientAddress.get(), us.totalUpdates,
          (System.currentTimeMillis() - us.startTime) / 1000.0, us.authTimes.toString(), us.flushTime / 1000.0, us.prepareTimes.getSum() / 1000.0,
          us.walogTimes.getSum() / 1000.0, us.commitTimes.getSum() / 1000.0));
      if (us.failures.size() > 0) {
        Entry<KeyExtent,Long> first = us.failures.entrySet().iterator().next();
        log.debug(String.format("Failures: %d, first extent %s successful commits: %d", us.failures.size(), first.getKey().toString(), first.getValue()));
      }
      List<ConstraintViolationSummary> violations = us.violations.asList();
      if (violations.size() > 0) {
        ConstraintViolationSummary first = us.violations.asList().iterator().next();
        log.debug(String.format("Violations: %d, first %s occurs %d", violations.size(), first.violationDescription, first.numberOfViolatingMutations));
      }
      if (us.authFailures.size() > 0) {
        KeyExtent first = us.authFailures.keySet().iterator().next();
        log.debug(String.format("Authentication Failures: %d, first %s", us.authFailures.size(), first.toString()));
      }

      return new UpdateErrors(Translator.translate(us.failures, Translators.KET), Translator.translate(violations, Translators.CVST), Translator.translate(
          us.authFailures, Translators.KET));
    }

    @Override
    public void update(TInfo tinfo, TCredentials credentials, TKeyExtent tkeyExtent, TMutation tmutation) throws NotServingTabletException,
        ConstraintViolationException, ThriftSecurityException {

      String tableId = new String(tkeyExtent.getTable(), UTF_8);
      if (!security.canWrite(credentials, tableId, Tables.getNamespaceId(instance, tableId)))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
      KeyExtent keyExtent = new KeyExtent(tkeyExtent);
      Tablet tablet = onlineTablets.get(new KeyExtent(keyExtent));
      if (tablet == null) {
        throw new NotServingTabletException(tkeyExtent);
      }

      if (!keyExtent.isMeta())
        TabletServer.this.resourceManager.waitUntilCommitsAreEnabled();

      long opid = writeTracker.startWrite(TabletType.type(keyExtent));

      try {
        Mutation mutation = new ServerMutation(tmutation);
        List<Mutation> mutations = Collections.singletonList(mutation);

        Span prep = Trace.start("prep");
        CommitSession cs;
        try {
          cs = tablet.prepareMutationsForCommit(new TservConstraintEnv(security, credentials), mutations);
        } finally {
          prep.stop();
        }
        if (cs == null) {
          throw new NotServingTabletException(tkeyExtent);
        }

        while (true) {
          try {
            Span wal = Trace.start("wal");
            try {
              logger.log(cs, cs.getWALogSeq(), mutation);
            } finally {
              wal.stop();
            }
            break;
          } catch (IOException ex) {
            log.warn(ex, ex);
          }
        }

        Span commit = Trace.start("commit");
        try {
          cs.commit(mutations);
        } finally {
          commit.stop();
        }
      } catch (TConstraintViolationException e) {
        throw new ConstraintViolationException(Translator.translate(e.getViolations().asList(), Translators.CVST));
      } finally {
        writeTracker.finishWrite(opid);
      }
    }

    private void checkConditions(Map<KeyExtent,List<ServerConditionalMutation>> updates, ArrayList<TCMResult> results, ConditionalSession cs,
        List<String> symbols) throws IOException {
      Iterator<Entry<KeyExtent,List<ServerConditionalMutation>>> iter = updates.entrySet().iterator();

      CompressedIterators compressedIters = new CompressedIterators(symbols);

      while (iter.hasNext()) {
        Entry<KeyExtent,List<ServerConditionalMutation>> entry = iter.next();
        Tablet tablet = onlineTablets.get(entry.getKey());

        if (tablet == null || tablet.isClosed()) {
          for (ServerConditionalMutation scm : entry.getValue())
            results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
          iter.remove();
        } else {
          List<ServerConditionalMutation> okMutations = new ArrayList<ServerConditionalMutation>(entry.getValue().size());

          for (ServerConditionalMutation scm : entry.getValue()) {
            if (checkCondition(results, cs, compressedIters, tablet, scm))
              okMutations.add(scm);
          }

          entry.setValue(okMutations);
        }

      }
    }

    boolean checkCondition(ArrayList<TCMResult> results, ConditionalSession cs, CompressedIterators compressedIters, Tablet tablet,
        ServerConditionalMutation scm) throws IOException {
      boolean add = true;

      Set<Column> emptyCols = Collections.emptySet();

      for (TCondition tc : scm.getConditions()) {

        Range range;
        if (tc.hasTimestamp)
          range = Range.exact(new Text(scm.getRow()), new Text(tc.getCf()), new Text(tc.getCq()), new Text(tc.getCv()), tc.getTs());
        else
          range = Range.exact(new Text(scm.getRow()), new Text(tc.getCf()), new Text(tc.getCq()), new Text(tc.getCv()));

        IterConfig ic = compressedIters.decompress(tc.iterators);

        Scanner scanner = tablet.createScanner(range, 1, emptyCols, cs.auths, ic.ssiList, ic.ssio, false, cs.interruptFlag);

        try {
          ScanBatch batch = scanner.read();

          Value val = null;

          for (KVEntry entry2 : batch.results) {
            val = entry2.getValue();
            break;
          }

          if ((val == null ^ tc.getVal() == null) || (val != null && !Arrays.equals(tc.getVal(), val.get()))) {
            results.add(new TCMResult(scm.getID(), TCMStatus.REJECTED));
            add = false;
            break;
          }

        } catch (TabletClosedException e) {
          results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
          add = false;
          break;
        } catch (IterationInterruptedException iie) {
          results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
          add = false;
          break;
        } catch (TooManyFilesException tmfe) {
          results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
          add = false;
          break;
        }
      }
      return add;
    }

    private void writeConditionalMutations(Map<KeyExtent,List<ServerConditionalMutation>> updates, ArrayList<TCMResult> results, ConditionalSession sess) {
      Set<Entry<KeyExtent,List<ServerConditionalMutation>>> es = updates.entrySet();

      Map<CommitSession,List<Mutation>> sendables = new HashMap<CommitSession,List<Mutation>>();

      boolean sessionCanceled = sess.interruptFlag.get();

      Span prepSpan = Trace.start("prep");
      try {
        long t1 = System.currentTimeMillis();
        for (Entry<KeyExtent,List<ServerConditionalMutation>> entry : es) {
          Tablet tablet = onlineTablets.get(entry.getKey());
          if (tablet == null || tablet.isClosed() || sessionCanceled) {
            for (ServerConditionalMutation scm : entry.getValue())
              results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
          } else {
            try {

              @SuppressWarnings("unchecked")
              List<Mutation> mutations = (List<Mutation>) (List<? extends Mutation>) entry.getValue();
              if (mutations.size() > 0) {

                CommitSession cs = tablet.prepareMutationsForCommit(new TservConstraintEnv(security, sess.credentials), mutations);

                if (cs == null) {
                  for (ServerConditionalMutation scm : entry.getValue())
                    results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
                } else {
                  for (ServerConditionalMutation scm : entry.getValue())
                    results.add(new TCMResult(scm.getID(), TCMStatus.ACCEPTED));
                  sendables.put(cs, mutations);
                }
              }
            } catch (TConstraintViolationException e) {
              if (e.getNonViolators().size() > 0) {
                sendables.put(e.getCommitSession(), e.getNonViolators());
                for (Mutation m : e.getNonViolators())
                  results.add(new TCMResult(((ServerConditionalMutation) m).getID(), TCMStatus.ACCEPTED));
              }

              for (Mutation m : e.getViolators())
                results.add(new TCMResult(((ServerConditionalMutation) m).getID(), TCMStatus.VIOLATED));
            }
          }
        }

        long t2 = System.currentTimeMillis();
        updateAvgPrepTime(t2 - t1, es.size());
      } finally {
        prepSpan.stop();
      }

      Span walSpan = Trace.start("wal");
      try {
        while (true && sendables.size() > 0) {
          try {
            long t1 = System.currentTimeMillis();
            logger.logManyTablets(sendables);
            long t2 = System.currentTimeMillis();
            updateWalogWriteTime(t2 - t1);
            break;
          } catch (IOException ex) {
            log.warn("logging mutations failed, retrying");
          } catch (FSError ex) { // happens when DFS is localFS
            log.warn("logging mutations failed, retrying");
          } catch (Throwable t) {
            log.error("Unknown exception logging mutations, counts for mutations in flight not decremented!", t);
            throw new RuntimeException(t);
          }
        }
      } finally {
        walSpan.stop();
      }

      Span commitSpan = Trace.start("commit");
      try {
        long t1 = System.currentTimeMillis();
        for (Entry<CommitSession,? extends List<Mutation>> entry : sendables.entrySet()) {
          CommitSession commitSession = entry.getKey();
          List<Mutation> mutations = entry.getValue();

          commitSession.commit(mutations);
        }
        long t2 = System.currentTimeMillis();
        updateAvgCommitTime(t2 - t1, sendables.size());
      } finally {
        commitSpan.stop();
      }

    }

    private Map<KeyExtent,List<ServerConditionalMutation>> conditionalUpdate(ConditionalSession cs, Map<KeyExtent,List<ServerConditionalMutation>> updates,
        ArrayList<TCMResult> results, List<String> symbols) throws IOException {
      // sort each list of mutations, this is done to avoid deadlock and doing seeks in order is more efficient and detect duplicate rows.
      ConditionalMutationSet.sortConditionalMutations(updates);

      Map<KeyExtent,List<ServerConditionalMutation>> deferred = new HashMap<KeyExtent,List<ServerConditionalMutation>>();

      // can not process two mutations for the same row, because one will not see what the other writes
      ConditionalMutationSet.deferDuplicatesRows(updates, deferred);

      // get as many locks as possible w/o blocking... defer any rows that are locked
      List<RowLock> locks = rowLocks.acquireRowlocks(updates, deferred);
      try {
        Span checkSpan = Trace.start("Check conditions");
        try {
          checkConditions(updates, results, cs, symbols);
        } finally {
          checkSpan.stop();
        }

        Span updateSpan = Trace.start("apply conditional mutations");
        try {
          writeConditionalMutations(updates, results, cs);
        } finally {
          updateSpan.stop();
        }
      } finally {
        rowLocks.releaseRowLocks(locks);
      }
      return deferred;
    }

    @Override
    public TConditionalSession startConditionalUpdate(TInfo tinfo, TCredentials credentials, List<ByteBuffer> authorizations, String tableId)
        throws ThriftSecurityException, TException {

      Authorizations userauths = null;
      if (!security.canConditionallyUpdate(credentials, tableId, Tables.getNamespaceId(instance, tableId), authorizations))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

      userauths = security.getUserAuthorizations(credentials);
      for (ByteBuffer auth : authorizations)
        if (!userauths.contains(ByteBufferUtil.toBytes(auth)))
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_AUTHORIZATIONS);

      ConditionalSession cs = new ConditionalSession();
      cs.auths = new Authorizations(authorizations);
      cs.credentials = credentials;
      cs.tableId = tableId;
      cs.interruptFlag = new AtomicBoolean();

      long sid = sessionManager.createSession(cs, false);
      return new TConditionalSession(sid, lockID, sessionManager.getMaxIdleTime());
    }

    @Override
    public List<TCMResult> conditionalUpdate(TInfo tinfo, long sessID, Map<TKeyExtent,List<TConditionalMutation>> mutations, List<String> symbols)
        throws NoSuchScanIDException, TException {

      ConditionalSession cs = (ConditionalSession) sessionManager.reserveSession(sessID);

      if (cs == null || cs.interruptFlag.get())
        throw new NoSuchScanIDException();

      if (!cs.tableId.equals(MetadataTable.ID) && !cs.tableId.equals(RootTable.ID))
        TabletServer.this.resourceManager.waitUntilCommitsAreEnabled();

      Text tid = new Text(cs.tableId);
      long opid = writeTracker.startWrite(TabletType.type(new KeyExtent(tid, null, null)));

      try {
        Map<KeyExtent,List<ServerConditionalMutation>> updates = Translator.translate(mutations, Translators.TKET,
            new Translator.ListTranslator<TConditionalMutation,ServerConditionalMutation>(ServerConditionalMutation.TCMT));

        for (KeyExtent ke : updates.keySet())
          if (!ke.getTableId().equals(tid))
            throw new IllegalArgumentException("Unexpected table id " + tid + " != " + ke.getTableId());

        ArrayList<TCMResult> results = new ArrayList<TCMResult>();

        Map<KeyExtent,List<ServerConditionalMutation>> deferred = conditionalUpdate(cs, updates, results, symbols);

        while (deferred.size() > 0) {
          deferred = conditionalUpdate(cs, deferred, results, symbols);
        }

        return results;
      } catch (IOException ioe) {
        throw new TException(ioe);
      } finally {
        writeTracker.finishWrite(opid);
        sessionManager.unreserveSession(sessID);
      }
    }

    @Override
    public void invalidateConditionalUpdate(TInfo tinfo, long sessID) throws TException {
      // this method should wait for any running conditional update to complete
      // after this method returns a conditional update should not be able to start

      ConditionalSession cs = (ConditionalSession) sessionManager.getSession(sessID);
      if (cs != null)
        cs.interruptFlag.set(true);

      cs = (ConditionalSession) sessionManager.reserveSession(sessID, true);
      if (cs != null)
        sessionManager.removeSession(sessID, true);
    }

    @Override
    public void closeConditionalUpdate(TInfo tinfo, long sessID) throws TException {
      sessionManager.removeSession(sessID, false);
    }

    @Override
    public void splitTablet(TInfo tinfo, TCredentials credentials, TKeyExtent tkeyExtent, ByteBuffer splitPoint) throws NotServingTabletException,
        ThriftSecurityException {

      String tableId = new String(ByteBufferUtil.toBytes(tkeyExtent.table));
      String namespaceId;
      try {
        namespaceId = Tables.getNamespaceId(instance, tableId);
      } catch (IllegalArgumentException ex) {
        // table does not exist, try to educate the client
        throw new NotServingTabletException(tkeyExtent);
      }

      if (!security.canSplitTablet(credentials, tableId, namespaceId))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

      KeyExtent keyExtent = new KeyExtent(tkeyExtent);

      Tablet tablet = onlineTablets.get(keyExtent);
      if (tablet == null) {
        throw new NotServingTabletException(tkeyExtent);
      }

      if (keyExtent.getEndRow() == null || !keyExtent.getEndRow().equals(ByteBufferUtil.toText(splitPoint))) {
        try {
          if (TabletServer.this.splitTablet(tablet, ByteBufferUtil.toBytes(splitPoint)) == null) {
            throw new NotServingTabletException(tkeyExtent);
          }
        } catch (IOException e) {
          log.warn("Failed to split " + keyExtent, e);
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public TabletServerStatus getTabletServerStatus(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      return getStats(sessionManager.getActiveScansPerTable());
    }

    @Override
    public List<TabletStats> getTabletStats(TInfo tinfo, TCredentials credentials, String tableId) throws ThriftSecurityException, TException {
      TreeMap<KeyExtent,Tablet> onlineTabletsCopy;
      synchronized (onlineTablets) {
        onlineTabletsCopy = new TreeMap<KeyExtent,Tablet>(onlineTablets);
      }
      List<TabletStats> result = new ArrayList<TabletStats>();
      Text text = new Text(tableId);
      KeyExtent start = new KeyExtent(text, new Text(), null);
      for (Entry<KeyExtent,Tablet> entry : onlineTabletsCopy.tailMap(start).entrySet()) {
        KeyExtent ke = entry.getKey();
        if (ke.getTableId().compareTo(text) == 0) {
          Tablet tablet = entry.getValue();
          TabletStats stats = tablet.timer.getTabletStats();
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

    private ZooCache masterLockCache = new ZooCache();

    private void checkPermission(TCredentials credentials, String lock, final String request) throws ThriftSecurityException {
      boolean fatal = false;
      try {
        log.debug("Got " + request + " message from user: " + credentials.getPrincipal());
        if (!security.canPerformSystemActions(credentials)) {
          log.warn("Got " + request + " message from user: " + credentials.getPrincipal());
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }
      } catch (ThriftSecurityException e) {
        log.warn("Got " + request + " message from unauthenticatable user: " + e.getUser());
        if (SystemCredentials.get().getToken().getClass().getName().equals(credentials.getTokenClassName())) {
          log.fatal("Got message from a service with a mismatched configuration. Please ensure a compatible configuration.", e);
          fatal = true;
        }
        throw e;
      } finally {
        if (fatal) {
          Halt.halt(1, new Runnable() {
            @Override
            public void run() {
              logGCInfo(getSystemConfiguration());
            }
          });
        }
      }

      if (tabletServerLock == null || !tabletServerLock.wasLockAcquired()) {
        log.warn("Got " + request + " message from master before lock acquired, ignoring...");
        throw new RuntimeException("Lock not acquired");
      }

      if (tabletServerLock != null && tabletServerLock.wasLockAcquired() && !tabletServerLock.isLocked()) {
        Halt.halt(1, new Runnable() {
          @Override
          public void run() {
            log.info("Tablet server no longer holds lock during checkPermission() : " + request + ", exiting");
            logGCInfo(getSystemConfiguration());
          }
        });
      }

      if (lock != null) {
        ZooUtil.LockID lid = new ZooUtil.LockID(ZooUtil.getRoot(instance) + Constants.ZMASTER_LOCK, lock);

        try {
          if (!ZooLock.isLockHeld(masterLockCache, lid)) {
            // maybe the cache is out of date and a new master holds the
            // lock?
            masterLockCache.clear();
            if (!ZooLock.isLockHeld(masterLockCache, lid)) {
              log.warn("Got " + request + " message from a master that does not hold the current lock " + lock);
              throw new RuntimeException("bad master lock");
            }
          }
        } catch (Exception e) {
          throw new RuntimeException("bad master lock", e);
        }
      }
    }

    @Override
    public void loadTablet(TInfo tinfo, TCredentials credentials, String lock, final TKeyExtent textent) {

      try {
        checkPermission(credentials, lock, "loadTablet");
      } catch (ThriftSecurityException e) {
        log.error(e, e);
        throw new RuntimeException(e);
      }

      final KeyExtent extent = new KeyExtent(textent);

      synchronized (unopenedTablets) {
        synchronized (openingTablets) {
          synchronized (onlineTablets) {

            // checking if this exact tablet is in any of the sets
            // below is not a strong enough check
            // when splits and fix splits occurring

            Set<KeyExtent> unopenedOverlapping = KeyExtent.findOverlapping(extent, unopenedTablets);
            Set<KeyExtent> openingOverlapping = KeyExtent.findOverlapping(extent, openingTablets);
            Set<KeyExtent> onlineOverlapping = KeyExtent.findOverlapping(extent, onlineTablets);

            Set<KeyExtent> all = new HashSet<KeyExtent>();
            all.addAll(unopenedOverlapping);
            all.addAll(openingOverlapping);
            all.addAll(onlineOverlapping);

            if (!all.isEmpty()) {

              // ignore any tablets that have recently split, for error logging
              for (KeyExtent e2 : onlineOverlapping) {
                Tablet tablet = onlineTablets.get(e2);
                if (System.currentTimeMillis() - tablet.getSplitCreationTime() < RECENTLY_SPLIT_MILLIES) {
                  all.remove(e2);
                }
              }

              // ignore self, for error logging
              all.remove(extent);

              if (all.size() > 0) {
                log.error("Tablet " + extent + " overlaps previously assigned " + unopenedOverlapping + " " + openingOverlapping + " " + onlineOverlapping
                    + " " + all);
              }
              return;
            }

            unopenedTablets.add(extent);
          }
        }
      }

      // add the assignment job to the appropriate queue
      log.info("Loading tablet " + extent);

      final AssignmentHandler ah = new AssignmentHandler(extent);
      // final Runnable ah = new LoggingRunnable(log, );
      // Root tablet assignment must take place immediately
      if (extent.isRootTablet()) {
        new Daemon("Root Tablet Assignment") {
          @Override
          public void run() {
            ah.run();
            if (onlineTablets.containsKey(extent)) {
              log.info("Root tablet loaded: " + extent);
            } else {
              log.info("Root tablet failed to load");
            }

          }
        }.start();
      } else {
        if (extent.isMeta()) {
          resourceManager.addMetaDataAssignment(extent, log, ah);
        } else {
          resourceManager.addAssignment(extent, log, ah);
        }
      }
    }

    @Override
    public void unloadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent textent, boolean save) {
      try {
        checkPermission(credentials, lock, "unloadTablet");
      } catch (ThriftSecurityException e) {
        log.error(e, e);
        throw new RuntimeException(e);
      }

      KeyExtent extent = new KeyExtent(textent);

      resourceManager.addMigration(extent, new LoggingRunnable(log, new UnloadTabletHandler(extent, save)));
    }

    @Override
    public void flush(TInfo tinfo, TCredentials credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow) {
      try {
        checkPermission(credentials, lock, "flush");
      } catch (ThriftSecurityException e) {
        log.error(e, e);
        throw new RuntimeException(e);
      }

      ArrayList<Tablet> tabletsToFlush = new ArrayList<Tablet>();

      KeyExtent ke = new KeyExtent(new Text(tableId), ByteBufferUtil.toText(endRow), ByteBufferUtil.toText(startRow));

      synchronized (onlineTablets) {
        for (Tablet tablet : onlineTablets.values())
          if (ke.overlaps(tablet.getExtent()))
            tabletsToFlush.add(tablet);
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
            log.info("Asked to flush table that has no flush id " + ke + " " + e.getMessage());
            return;
          }
        }
        tablet.flush(flushID);
      }
    }

    @Override
    public void flushTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent textent) throws TException {
      try {
        checkPermission(credentials, lock, "flushTablet");
      } catch (ThriftSecurityException e) {
        log.error(e, e);
        throw new RuntimeException(e);
      }

      Tablet tablet = onlineTablets.get(new KeyExtent(textent));
      if (tablet != null) {
        log.info("Flushing " + tablet.getExtent());
        try {
          tablet.flush(tablet.getFlushID());
        } catch (NoNodeException nne) {
          log.info("Asked to flush tablet that has no flush id " + new KeyExtent(textent) + " " + nne.getMessage());
        }
      }
    }

    @Override
    public void halt(TInfo tinfo, TCredentials credentials, String lock) throws ThriftSecurityException {

      checkPermission(credentials, lock, "halt");

      Halt.halt(0, new Runnable() {
        @Override
        public void run() {
          log.info("Master requested tablet server halt");
          logGCInfo(getSystemConfiguration());
          serverStopRequested = true;
          try {
            tabletServerLock.unlock();
          } catch (Exception e) {
            log.error(e, e);
          }
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
    public TabletStats getHistoricalStats(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      return statsKeeper.getTabletStats();
    }

    @Override
    public List<ActiveScan> getActiveScans(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      try {
        checkPermission(credentials, null, "getScans");
      } catch (ThriftSecurityException e) {
        log.error(e, e);
        throw e;
      }

      return sessionManager.getActiveScans();
    }

    @Override
    public void chop(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent textent) throws TException {
      try {
        checkPermission(credentials, lock, "chop");
      } catch (ThriftSecurityException e) {
        log.error(e, e);
        throw new RuntimeException(e);
      }

      KeyExtent ke = new KeyExtent(textent);

      Tablet tablet = onlineTablets.get(ke);
      if (tablet != null) {
        tablet.chopFiles();
      }
    }

    @Override
    public void compact(TInfo tinfo, TCredentials credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow) throws TException {
      try {
        checkPermission(credentials, lock, "compact");
      } catch (ThriftSecurityException e) {
        log.error(e, e);
        throw new RuntimeException(e);
      }

      KeyExtent ke = new KeyExtent(new Text(tableId), ByteBufferUtil.toText(endRow), ByteBufferUtil.toText(startRow));

      ArrayList<Tablet> tabletsToCompact = new ArrayList<Tablet>();
      synchronized (onlineTablets) {
        for (Tablet tablet : onlineTablets.values())
          if (ke.overlaps(tablet.getExtent()))
            tabletsToCompact.add(tablet);
      }

      Long compactionId = null;

      for (Tablet tablet : tabletsToCompact) {
        // all for the same table id, so only need to read
        // compaction id once
        if (compactionId == null)
          try {
            compactionId = tablet.getCompactionID().getFirst();
          } catch (NoNodeException e) {
            log.info("Asked to compact table with no compaction id " + ke + " " + e.getMessage());
            return;
          }
        tablet.compactAll(compactionId);
      }

    }

    @Override
    public void removeLogs(TInfo tinfo, TCredentials credentials, List<String> filenames) throws TException {
      String myname = getClientAddressString();
      myname = myname.replace(':', '+');
      Set<String> loggers = new HashSet<String>();
      logger.getLogFiles(loggers);
      Set<String> loggerUUIDs = new HashSet<String>();
      for (String logger : loggers)
        loggerUUIDs.add(new Path(logger).getName());

      nextFile: for (String filename : filenames) {
        String uuid = new Path(filename).getName();
        // skip any log we're currently using
        if (loggerUUIDs.contains(uuid))
          continue nextFile;

        List<Tablet> onlineTabletsCopy = new ArrayList<Tablet>();
        synchronized (onlineTablets) {
          onlineTabletsCopy.addAll(onlineTablets.values());
        }
        for (Tablet tablet : onlineTabletsCopy) {
          for (String current : tablet.getCurrentLogFiles()) {
            if (current.contains(uuid)) {
              log.info("Attempted to delete " + filename + " from tablet " + tablet.getExtent());
              continue nextFile;
            }
          }
        }

        try {
          Path source = new Path(filename);
          if (acuConf.getBoolean(Property.TSERV_ARCHIVE_WALOGS)) {
            Path walogArchive = fs.matchingFileSystem(source, ServerConstants.getWalogArchives());
            fs.mkdirs(walogArchive);
            Path dest = new Path(walogArchive, source.getName());
            log.info("Archiving walog " + source + " to " + dest);
            if (!fs.rename(source, dest))
              log.error("rename is unsuccessful");
          } else {
            log.info("Deleting walog " + filename);
            Path sourcePath = new Path(filename);
            if (!(!acuConf.getBoolean(Property.GC_TRASH_IGNORE) && fs.moveToTrash(sourcePath)) && !fs.deleteRecursively(sourcePath))
              log.warn("Failed to delete walog " + source);
            for (String recovery : ServerConstants.getRecoveryDirs()) {
              Path recoveryPath = new Path(recovery, source.getName());
              try {
                if (fs.moveToTrash(recoveryPath) || fs.deleteRecursively(recoveryPath))
                  log.info("Deleted any recovery log " + filename);
              } catch (FileNotFoundException ex) {
                // ignore
              }
            }
          }
        } catch (IOException e) {
          log.warn("Error attempting to delete write-ahead log " + filename + ": " + e);
        }
      }
    }

    @Override
    public List<ActiveCompaction> getActiveCompactions(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      try {
        checkPermission(credentials, null, "getActiveCompactions");
      } catch (ThriftSecurityException e) {
        log.error(e, e);
        throw e;
      }

      List<CompactionInfo> compactions = Compactor.getRunningCompactions();
      List<ActiveCompaction> ret = new ArrayList<ActiveCompaction>(compactions.size());

      for (CompactionInfo compactionInfo : compactions) {
        ret.add(compactionInfo.toThrift());
      }

      return ret;
    }
  }

  private class SplitRunner implements Runnable {
    private Tablet tablet;

    public SplitRunner(Tablet tablet) {
      this.tablet = tablet;
    }

    @Override
    public void run() {
      if (majorCompactorDisabled) {
        // this will make split task that were queued when shutdown was
        // initiated exit
        return;
      }

      splitTablet(tablet);
    }
  }

  boolean isMajorCompactionDisabled() {
    return majorCompactorDisabled;
  }

  void executeSplit(Tablet tablet) {
    resourceManager.executeSplit(tablet.getExtent(), new LoggingRunnable(log, new SplitRunner(tablet)));
  }

  private class MajorCompactor implements Runnable {

    public MajorCompactor(AccumuloConfiguration config) {
      CompactionWatcher.startWatching(config);
    }

    @Override
    public void run() {
      while (!majorCompactorDisabled) {
        try {
          UtilWaitThread.sleep(getSystemConfiguration().getTimeInMillis(Property.TSERV_MAJC_DELAY));

          TreeMap<KeyExtent,Tablet> copyOnlineTablets = new TreeMap<KeyExtent,Tablet>();

          synchronized (onlineTablets) {
            copyOnlineTablets.putAll(onlineTablets); // avoid
            // concurrent
            // modification
          }

          int numMajorCompactionsInProgress = 0;

          Iterator<Entry<KeyExtent,Tablet>> iter = copyOnlineTablets.entrySet().iterator();

          // bail early now if we're shutting down
          while (iter.hasNext() && !majorCompactorDisabled) {

            Entry<KeyExtent,Tablet> entry = iter.next();

            Tablet tablet = entry.getValue();

            // if we need to split AND compact, we need a good way
            // to decide what to do
            if (tablet.needsSplit()) {
              executeSplit(tablet);
              continue;
            }

            int maxLogEntriesPerTablet = getTableConfiguration(tablet.getExtent()).getCount(Property.TABLE_MINC_LOGS_MAX);

            if (tablet.getLogCount() >= maxLogEntriesPerTablet) {
              log.debug("Initiating minor compaction for " + tablet.getExtent() + " because it has " + tablet.getLogCount() + " write ahead logs");
              tablet.initiateMinorCompaction(MinorCompactionReason.SYSTEM);
            }

            synchronized (tablet) {
              if (tablet.initiateMajorCompaction(MajorCompactionReason.NORMAL) || tablet.majorCompactionQueued() || tablet.majorCompactionRunning()) {
                numMajorCompactionsInProgress++;
                continue;
              }
            }
          }

          int idleCompactionsToStart = Math.max(1, getSystemConfiguration().getCount(Property.TSERV_MAJC_MAXCONCURRENT) / 2);

          if (numMajorCompactionsInProgress < idleCompactionsToStart) {
            // system is not major compacting, can schedule some
            // idle compactions
            iter = copyOnlineTablets.entrySet().iterator();

            while (iter.hasNext() && !majorCompactorDisabled && numMajorCompactionsInProgress < idleCompactionsToStart) {
              Entry<KeyExtent,Tablet> entry = iter.next();
              Tablet tablet = entry.getValue();

              if (tablet.initiateMajorCompaction(MajorCompactionReason.IDLE)) {
                numMajorCompactionsInProgress++;
              }
            }
          }
        } catch (Throwable t) {
          log.error("Unexpected exception in " + Thread.currentThread().getName(), t);
          UtilWaitThread.sleep(1000);
        }
      }
    }
  }

  private void splitTablet(Tablet tablet) {
    try {

      TreeMap<KeyExtent,SplitInfo> tabletInfo = splitTablet(tablet, null);
      if (tabletInfo == null) {
        // either split or compact not both
        // were not able to split... so see if a major compaction is
        // needed
        tablet.initiateMajorCompaction(MajorCompactionReason.NORMAL);
      }
    } catch (IOException e) {
      statsKeeper.updateTime(Operation.SPLIT, 0, 0, true);
      log.error("split failed: " + e.getMessage() + " for tablet " + tablet.getExtent(), e);
    } catch (Exception e) {
      statsKeeper.updateTime(Operation.SPLIT, 0, 0, true);
      log.error("Unknown error on split: " + e, e);
    }
  }

  private TreeMap<KeyExtent,SplitInfo> splitTablet(Tablet tablet, byte[] splitPoint) throws IOException {
    long t1 = System.currentTimeMillis();

    TreeMap<KeyExtent,SplitInfo> tabletInfo = tablet.split(splitPoint);
    if (tabletInfo == null) {
      return null;
    }

    log.info("Starting split: " + tablet.getExtent());
    statsKeeper.incrementStatusSplit();
    long start = System.currentTimeMillis();

    Tablet[] newTablets = new Tablet[2];

    Entry<KeyExtent,SplitInfo> first = tabletInfo.firstEntry();
    newTablets[0] = new Tablet(first.getKey(), TabletServer.this, resourceManager.createTabletResourceManager(), first.getValue());

    Entry<KeyExtent,SplitInfo> last = tabletInfo.lastEntry();
    newTablets[1] = new Tablet(last.getKey(), TabletServer.this, resourceManager.createTabletResourceManager(), last.getValue());

    // roll tablet stats over into tablet server's statsKeeper object as
    // historical data
    statsKeeper.saveMinorTimes(tablet.timer);
    statsKeeper.saveMajorTimes(tablet.timer);

    // lose the reference to the old tablet and open two new ones
    synchronized (onlineTablets) {
      onlineTablets.remove(tablet.getExtent());
      onlineTablets.put(newTablets[0].getExtent(), newTablets[0]);
      onlineTablets.put(newTablets[1].getExtent(), newTablets[1]);
    }
    // tell the master
    enqueueMasterMessage(new SplitReportMessage(tablet.getExtent(), newTablets[0].getExtent(), new Text("/" + newTablets[0].getLocation().getName()),
        newTablets[1].getExtent(), new Text("/" + newTablets[1].getLocation().getName())));

    statsKeeper.updateTime(Operation.SPLIT, start, 0, false);
    long t2 = System.currentTimeMillis();
    log.info("Tablet split: " + tablet.getExtent() + " size0 " + newTablets[0].estimateTabletSize() + " size1 " + newTablets[1].estimateTabletSize() + " time "
        + (t2 - t1) + "ms");

    return tabletInfo;
  }

  public long lastPingTime = System.currentTimeMillis();
  public Socket currentMaster;

  // a queue to hold messages that are to be sent back to the master
  private BlockingDeque<MasterMessage> masterMessages = new LinkedBlockingDeque<MasterMessage>();

  // add a message for the main thread to send back to the master
  void enqueueMasterMessage(MasterMessage m) {
    masterMessages.addLast(m);
  }

  private class UnloadTabletHandler implements Runnable {
    private KeyExtent extent;
    private boolean saveState;

    public UnloadTabletHandler(KeyExtent extent, boolean saveState) {
      this.extent = extent;
      this.saveState = saveState;
    }

    @Override
    public void run() {

      Tablet t = null;

      synchronized (unopenedTablets) {
        if (unopenedTablets.contains(extent)) {
          unopenedTablets.remove(extent);
          // enqueueMasterMessage(new TabletUnloadedMessage(extent));
          return;
        }
      }
      synchronized (openingTablets) {
        while (openingTablets.contains(extent)) {
          try {
            openingTablets.wait();
          } catch (InterruptedException e) {}
        }
      }
      synchronized (onlineTablets) {
        if (onlineTablets.containsKey(extent)) {
          t = onlineTablets.get(extent);
        }
      }

      if (t == null) {
        // Tablet has probably been recently unloaded: repeated master
        // unload request is crossing the successful unloaded message
        if (!recentlyUnloadedCache.containsKey(extent)) {
          log.info("told to unload tablet that was not being served " + extent);
          enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.UNLOAD_FAILURE_NOT_SERVING, extent));
        }
        return;
      }

      try {
        t.close(saveState);
      } catch (Throwable e) {

        if ((t.isClosing() || t.isClosed()) && e instanceof IllegalStateException) {
          log.debug("Failed to unload tablet " + extent + "... it was alread closing or closed : " + e.getMessage());
        } else {
          log.error("Failed to close tablet " + extent + "... Aborting migration", e);
          enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.UNLOAD_ERROR, extent));
        }
        return;
      }

      // stop serving tablet - client will get not serving tablet
      // exceptions
      recentlyUnloadedCache.put(extent, System.currentTimeMillis());
      onlineTablets.remove(extent);

      try {
        TServerInstance instance = new TServerInstance(clientAddress, getLock().getSessionId());
        TabletLocationState tls = null;
        try {
          tls = new TabletLocationState(extent, null, instance, null, null, false);
        } catch (BadLocationStateException e) {
          log.error("Unexpected error ", e);
        }
        log.debug("Unassigning " + tls);
        TabletStateStore.unassign(tls);
      } catch (DistributedStoreException ex) {
        log.warn("Unable to update storage", ex);
      } catch (KeeperException e) {
        log.warn("Unable determine our zookeeper session information", e);
      } catch (InterruptedException e) {
        log.warn("Interrupted while getting our zookeeper session information", e);
      }

      // tell the master how it went
      enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.UNLOADED, extent));

      // roll tablet stats over into tablet server's statsKeeper object as
      // historical data
      statsKeeper.saveMinorTimes(t.timer);
      statsKeeper.saveMajorTimes(t.timer);

      log.info("unloaded " + extent);

    }
  }

  protected class AssignmentHandler implements Runnable {
    private KeyExtent extent;
    private int retryAttempt = 0;

    public AssignmentHandler(KeyExtent extent) {
      this.extent = extent;
    }

    public AssignmentHandler(KeyExtent extent, int retryAttempt) {
      this(extent);
      this.retryAttempt = retryAttempt;
    }

    @Override
    public void run() {
      log.info(clientAddress + ": got assignment from master: " + extent);

      synchronized (unopenedTablets) {
        synchronized (openingTablets) {
          synchronized (onlineTablets) {
            // nothing should be moving between sets, do a sanity
            // check
            Set<KeyExtent> unopenedOverlapping = KeyExtent.findOverlapping(extent, unopenedTablets);
            Set<KeyExtent> openingOverlapping = KeyExtent.findOverlapping(extent, openingTablets);
            Set<KeyExtent> onlineOverlapping = KeyExtent.findOverlapping(extent, onlineTablets);

            if (openingOverlapping.contains(extent) || onlineOverlapping.contains(extent))
              return;

            if (!unopenedOverlapping.contains(extent)) {
              log.info("assignment " + extent + " no longer in the unopened set");
              return;
            }

            if (unopenedOverlapping.size() != 1 || openingOverlapping.size() > 0 || onlineOverlapping.size() > 0) {
              throw new IllegalStateException("overlaps assigned " + extent + " " + !unopenedTablets.contains(extent) + " " + unopenedOverlapping + " "
                  + openingOverlapping + " " + onlineOverlapping);
            }
          }

          unopenedTablets.remove(extent);
          openingTablets.add(extent);
        }
      }

      log.debug("Loading extent: " + extent);

      // check Metadata table before accepting assignment
      Text locationToOpen = null;
      SortedMap<Key,Value> tabletsKeyValues = new TreeMap<Key,Value>();
      try {
        Pair<Text,KeyExtent> pair = verifyTabletInformation(extent, TabletServer.this.getTabletSession(), tabletsKeyValues, getClientAddressString(), getLock());
        if (pair != null) {
          locationToOpen = pair.getFirst();
          if (pair.getSecond() != null) {
            synchronized (openingTablets) {
              openingTablets.remove(extent);
              openingTablets.notifyAll();
              // it expected that the new extent will overlap the old one... if it does not, it should not be added to unopenedTablets
              if (!KeyExtent.findOverlapping(extent, new TreeSet<KeyExtent>(Arrays.asList(pair.getSecond()))).contains(pair.getSecond())) {
                throw new IllegalStateException("Fixed split does not overlap " + extent + " " + pair.getSecond());
              }
              unopenedTablets.add(pair.getSecond());
            }
            // split was rolled back... try again
            new AssignmentHandler(pair.getSecond()).run();
            return;
          }
        }
      } catch (Exception e) {
        synchronized (openingTablets) {
          openingTablets.remove(extent);
          openingTablets.notifyAll();
        }
        log.warn("Failed to verify tablet " + extent, e);
        enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.LOAD_FAILURE, extent));
        throw new RuntimeException(e);
      }

      if (locationToOpen == null) {
        log.debug("Reporting tablet " + extent + " assignment failure: unable to verify Tablet Information");
        synchronized (openingTablets) {
          openingTablets.remove(extent);
          openingTablets.notifyAll();
        }
        enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.LOAD_FAILURE, extent));
        return;
      }

      Tablet tablet = null;
      boolean successful = false;

      try {
        TabletResourceManager trm = resourceManager.createTabletResourceManager();

        // this opens the tablet file and fills in the endKey in the
        // extent
        locationToOpen = VolumeUtil.switchRootTabletVolume(extent, locationToOpen);
        tablet = new Tablet(TabletServer.this, locationToOpen, extent, trm, tabletsKeyValues);
        /*
         * If a minor compaction starts after a tablet opens, this indicates a log recovery occurred. This recovered data must be minor compacted.
         *
         * There are three reasons to wait for this minor compaction to finish before placing the tablet in online tablets.
         *
         * 1) The log recovery code does not handle data written to the tablet on multiple tablet servers. 2) The log recovery code does not block if memory is
         * full. Therefore recovering lots of tablets that use a lot of memory could run out of memory. 3) The minor compaction finish event did not make it to
         * the logs (the file will be in metadata, preventing replay of compacted data)... but do not want a majc to wipe the file out from metadata and then
         * have another process failure... this could cause duplicate data to replay
         */
        if (tablet.getNumEntriesInMemory() > 0 && !tablet.minorCompactNow(MinorCompactionReason.RECOVERY)) {
          throw new RuntimeException("Minor compaction after recovery fails for " + extent);
        }

        Assignment assignment = new Assignment(extent, getTabletSession());
        TabletStateStore.setLocation(assignment);

        synchronized (openingTablets) {
          synchronized (onlineTablets) {
            openingTablets.remove(extent);
            onlineTablets.put(extent, tablet);
            openingTablets.notifyAll();
            recentlyUnloadedCache.remove(tablet.getExtent());
          }
        }
        tablet = null; // release this reference
        successful = true;
      } catch (Throwable e) {
        log.warn("exception trying to assign tablet " + extent + " " + locationToOpen, e);
        if (e.getMessage() != null)
          log.warn(e.getMessage());
        String table = extent.getTableId().toString();
        ProblemReports.getInstance().report(new ProblemReport(table, TABLET_LOAD, extent.getUUID().toString(), getClientAddressString(), e));
      }

      if (!successful) {
        synchronized (unopenedTablets) {
          synchronized (openingTablets) {
            openingTablets.remove(extent);
            unopenedTablets.add(extent);
            openingTablets.notifyAll();
          }
        }
        log.warn("failed to open tablet " + extent + " reporting failure to master");
        enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.LOAD_FAILURE, extent));
        long reschedule = Math.min((1l << Math.min(32, retryAttempt)) * 1000, 10 * 60 * 1000l);
        log.warn(String.format("rescheduling tablet load in %.2f seconds", reschedule / 1000.));
        SimpleTimer.getInstance().schedule(new TimerTask() {
          @Override
          public void run() {
            log.info("adding tablet " + extent + " back to the assignment pool (retry " + retryAttempt + ")");
            AssignmentHandler handler = new AssignmentHandler(extent, retryAttempt + 1);
            if (extent.isMeta()) {
              if (extent.isRootTablet()) {
                new Daemon(new LoggingRunnable(log, handler), "Root tablet assignment retry").start();
              } else {
                resourceManager.addMetaDataAssignment(extent, log, handler);
              }
            } else {
              resourceManager.addAssignment(extent, log, handler);
            }
          }
        }, reschedule);
      } else {
        enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.LOADED, extent));
      }
    }
  }

  private VolumeManager fs;
  private Instance instance;

  private final SortedMap<KeyExtent,Tablet> onlineTablets = Collections.synchronizedSortedMap(new TreeMap<KeyExtent,Tablet>());
  private final SortedSet<KeyExtent> unopenedTablets = Collections.synchronizedSortedSet(new TreeSet<KeyExtent>());
  private final SortedSet<KeyExtent> openingTablets = Collections.synchronizedSortedSet(new TreeSet<KeyExtent>());
  @SuppressWarnings("unchecked")
  private final Map<KeyExtent,Long> recentlyUnloadedCache = Collections.synchronizedMap(new LRUMap(1000));

  private Thread majorCompactorThread;

  // used for stopping the server and MasterListener thread
  private volatile boolean serverStopRequested = false;

  private HostAndPort clientAddress;

  private TabletServerResourceManager resourceManager;
  private SecurityOperation security;
  private volatile boolean majorCompactorDisabled = false;

  private volatile boolean shutdownComplete = false;

  private ZooLock tabletServerLock;

  private TServer server;

  private DistributedWorkQueue bulkFailedCopyQ;

  private String lockID;

  private static final String METRICS_PREFIX = "tserver";

  private static ObjectName OBJECT_NAME = null;

  static AtomicLong seekCount = new AtomicLong(0);

  public TabletStatsKeeper getStatsKeeper() {
    return statsKeeper;
  }

  public void addLoggersToMetadata(List<DfsLogger> logs, KeyExtent extent, int id) {
    if (!this.onlineTablets.containsKey(extent)) {
      log.info("Not adding " + logs.size() + " logs for extent " + extent + " as alias " + id + " tablet is offline");
      // minor compaction due to recovery... don't make updates... if it finishes, there will be no WALs,
      // if it doesn't, we'll need to do the same recovery with the old files.
      return;
    }

    log.info("Adding " + logs.size() + " logs for extent " + extent + " as alias " + id);
    long now = RelativeTime.currentTimeMillis();
    List<String> logSet = new ArrayList<String>();
    for (DfsLogger log : logs)
      logSet.add(log.getFileName());
    LogEntry entry = new LogEntry();
    entry.extent = extent;
    entry.tabletId = id;
    entry.timestamp = now;
    entry.server = logs.get(0).getLogger();
    entry.filename = logs.get(0).getFileName();
    entry.logSet = logSet;
    MetadataTableUtil.addLogEntry(SystemCredentials.get(), entry, getLock());
  }

  private HostAndPort startServer(AccumuloConfiguration conf, String address, Property portHint, TProcessor processor, String threadName)
      throws UnknownHostException {
    Property maxMessageSizeProperty = (conf.get(Property.TSERV_MAX_MESSAGE_SIZE) != null ? Property.TSERV_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(conf, address, portHint, processor, this.getClass().getSimpleName(), threadName, Property.TSERV_PORTSEARCH,
        Property.TSERV_MINTHREADS, Property.TSERV_THREADCHECK, maxMessageSizeProperty);
    this.server = sp.server;
    return sp.address;
  }

  private String getMasterAddress() {
    try {
      List<String> locations = instance.getMasterLocations();
      if (locations.size() == 0)
        return null;
      return locations.get(0);
    } catch (Exception e) {
      log.warn("Failed to obtain master host " + e);
    }

    return null;
  }

  // Connect to the master for posting asynchronous results
  private MasterClientService.Client masterConnection(String address) {
    try {
      if (address == null) {
        return null;
      }
      MasterClientService.Client client = ThriftUtil.getClient(new MasterClientService.Client.Factory(), address, Property.GENERAL_RPC_TIMEOUT,
          getSystemConfiguration());
      // log.info("Listener API to master has been opened");
      return client;
    } catch (Exception e) {
      log.warn("Issue with masterConnection (" + address + ") " + e, e);
    }
    return null;
  }

  private void returnMasterConnection(MasterClientService.Client client) {
    ThriftUtil.returnClient(client);
  }

  private HostAndPort startTabletClientService() throws UnknownHostException {
    // start listening for client connection last
    Iface tch = RpcWrapper.service(new ThriftClientHandler());
    Processor<Iface> processor = new Processor<Iface>(tch);
    HostAndPort address = startServer(getSystemConfiguration(), clientAddress.getHostText(), Property.TSERV_CLIENTPORT, processor, "Thrift Client Server");
    log.info("address = " + address);
    return address;
  }

  ZooLock getLock() {
    return tabletServerLock;
  }

  private void announceExistence() {
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    try {
      String zPath = ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/" + getClientAddressString();

      zoo.putPersistentData(zPath, new byte[] {}, NodeExistsPolicy.SKIP);

      tabletServerLock = new ZooLock(zPath);

      LockWatcher lw = new LockWatcher() {

        @Override
        public void lostLock(final LockLossReason reason) {
          Halt.halt(0, new Runnable() {
            @Override
            public void run() {
              if (!serverStopRequested)
                log.fatal("Lost tablet server lock (reason = " + reason + "), exiting.");
              logGCInfo(getSystemConfiguration());
            }
          });
        }

        @Override
        public void unableToMonitorLockNode(final Throwable e) {
          Halt.halt(0, new Runnable() {
            @Override
            public void run() {
              log.fatal("Lost ability to monitor tablet server lock, exiting.", e);
            }
          });

        }
      };

      byte[] lockContent = new ServerServices(getClientAddressString(), Service.TSERV_CLIENT).toString().getBytes(UTF_8);
      for (int i = 0; i < 120 / 5; i++) {
        zoo.putPersistentData(zPath, new byte[0], NodeExistsPolicy.SKIP);

        if (tabletServerLock.tryLock(lw, lockContent)) {
          log.debug("Obtained tablet server lock " + tabletServerLock.getLockPath());
          lockID = tabletServerLock.getLockID().serialize(ZooUtil.getRoot(instance) + Constants.ZTSERVERS + "/");
          return;
        }
        log.info("Waiting for tablet server lock");
        UtilWaitThread.sleep(5000);
      }
      String msg = "Too many retries, exiting.";
      log.info(msg);
      throw new RuntimeException(msg);
    } catch (Exception e) {
      log.info("Could not obtain tablet server lock, exiting.", e);
      throw new RuntimeException(e);
    }
  }

  // main loop listens for client requests
  public void run() {
    SecurityUtil.serverLogin(ServerConfiguration.getSiteConfiguration());

    try {
      clientAddress = startTabletClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the tablet client service", e1);
    }
    announceExistence();

    ThreadPoolExecutor distWorkQThreadPool = new SimpleThreadPool(getSystemConfiguration().getCount(Property.TSERV_WORKQ_THREADS), "distributed work queue");

    bulkFailedCopyQ = new DistributedWorkQueue(ZooUtil.getRoot(instance) + Constants.ZBULK_FAILED_COPYQ);
    try {
      bulkFailedCopyQ.startProcessing(new BulkFailedCopyProcessor(), distWorkQThreadPool);
    } catch (Exception e1) {
      throw new RuntimeException("Failed to start distributed work queue for copying ", e1);
    }

    try {
      logSorter.startWatchingForRecoveryLogs(distWorkQThreadPool);
    } catch (Exception ex) {
      log.error("Error setting watches for recoveries");
      throw new RuntimeException(ex);
    }

    try {
      OBJECT_NAME = new ObjectName("accumulo.server.metrics:service=TServerInfo,name=TabletServerMBean,instance=" + Thread.currentThread().getName());
      // Do this because interface not in same package.
      StandardMBean mbean = new StandardMBean(this, TabletServerMBean.class, false);
      this.register(mbean);
      mincMetrics.register();
    } catch (Exception e) {
      log.error("Error registering with JMX", e);
    }

    String masterHost;
    while (!serverStopRequested) {
      // send all of the pending messages
      try {
        MasterMessage mm = null;
        MasterClientService.Client iface = null;

        try {
          // wait until a message is ready to send, or a sever stop
          // was requested
          while (mm == null && !serverStopRequested) {
            mm = masterMessages.poll(1000, TimeUnit.MILLISECONDS);
          }

          // have a message to send to the master, so grab a
          // connection
          masterHost = getMasterAddress();
          iface = masterConnection(masterHost);
          TServiceClient client = iface;

          // if while loop does not execute at all and mm != null,
          // then
          // finally block should place mm back on queue
          while (!serverStopRequested && mm != null && client != null && client.getOutputProtocol() != null
              && client.getOutputProtocol().getTransport() != null && client.getOutputProtocol().getTransport().isOpen()) {
            try {
              mm.send(SystemCredentials.get().toThrift(instance), getClientAddressString(), iface);
              mm = null;
            } catch (TException ex) {
              log.warn("Error sending message: queuing message again");
              masterMessages.putFirst(mm);
              mm = null;
              throw ex;
            }

            // if any messages are immediately available grab em and
            // send them
            mm = masterMessages.poll();
          }

        } finally {

          if (mm != null) {
            masterMessages.putFirst(mm);
          }
          returnMasterConnection(iface);

          UtilWaitThread.sleep(1000);
        }
      } catch (InterruptedException e) {
        log.info("Interrupt Exception received, shutting down");
        serverStopRequested = true;

      } catch (Exception e) {
        // may have lost connection with master
        // loop back to the beginning and wait for a new one
        // this way we survive master failures
        log.error(getClientAddressString() + ": TServerInfo: Exception. Master down?", e);
      }
    }

    // wait for shutdown
    // if the main thread exits oldServer the master listener, the JVM will
    // kill the
    // other threads and finalize objects. We want the shutdown that is
    // running
    // in the master listener thread to complete oldServer this happens.
    // consider making other threads daemon threads so that objects don't
    // get prematurely finalized
    synchronized (this) {
      while (shutdownComplete == false) {
        try {
          this.wait(1000);
        } catch (InterruptedException e) {
          log.error(e.toString());
        }
      }
    }
    log.debug("Stopping Thrift Servers");
    TServerUtils.stopTServer(server);

    try {
      log.debug("Closing filesystem");
      fs.close();
    } catch (IOException e) {
      log.warn("Failed to close filesystem : " + e.getMessage(), e);
    }

    logGCInfo(getSystemConfiguration());

    log.info("TServerInfo: stop requested. exiting ... ");

    try {
      tabletServerLock.unlock();
    } catch (Exception e) {
      log.warn("Failed to release tablet server lock", e);
    }
  }

  private long totalMinorCompactions;

  private static Pair<Text,KeyExtent> verifyRootTablet(KeyExtent extent, TServerInstance instance) throws DistributedStoreException, AccumuloException {
    ZooTabletStateStore store = new ZooTabletStateStore();
    if (!store.iterator().hasNext()) {
      throw new AccumuloException("Illegal state: location is not set in zookeeper");
    }
    TabletLocationState next = store.iterator().next();
    if (!instance.equals(next.future)) {
      throw new AccumuloException("Future location is not to this server for the root tablet");
    }

    if (next.current != null) {
      throw new AccumuloException("Root tablet already has a location set");
    }

    try {
      return new Pair<Text,KeyExtent>(new Text(MetadataTableUtil.getRootTabletDir()), null);
    } catch (IOException e) {
      throw new AccumuloException(e);
    }
  }

  public static Pair<Text,KeyExtent> verifyTabletInformation(KeyExtent extent, TServerInstance instance, SortedMap<Key,Value> tabletsKeyValues,
      String clientAddress, ZooLock lock) throws AccumuloSecurityException, DistributedStoreException, AccumuloException {

    log.debug("verifying extent " + extent);
    if (extent.isRootTablet()) {
      return verifyRootTablet(extent, instance);
    }
    String tableToVerify = MetadataTable.ID;
    if (extent.isMeta())
      tableToVerify = RootTable.ID;

    List<ColumnFQ> columnsToFetch = Arrays.asList(new ColumnFQ[] {TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN,
        TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN, TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN,
        TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN, TabletsSection.ServerColumnFamily.TIME_COLUMN});

    ScannerImpl scanner = new ScannerImpl(HdfsZooInstance.getInstance(), SystemCredentials.get(), tableToVerify, Authorizations.EMPTY);
    scanner.setRange(extent.toMetadataRange());

    TreeMap<Key,Value> tkv = new TreeMap<Key,Value>();
    for (Entry<Key,Value> entry : scanner)
      tkv.put(entry.getKey(), entry.getValue());

    // only populate map after success
    if (tabletsKeyValues == null) {
      tabletsKeyValues = tkv;
    } else {
      tabletsKeyValues.clear();
      tabletsKeyValues.putAll(tkv);
    }

    Text metadataEntry = extent.getMetadataEntry();

    Value dir = checkTabletMetadata(extent, instance, tabletsKeyValues, metadataEntry);
    if (dir == null)
      return null;

    Value oldPrevEndRow = null;
    for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
      if (TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN.hasColumns(entry.getKey())) {
        oldPrevEndRow = entry.getValue();
      }
    }

    if (oldPrevEndRow != null) {
      SortedMap<Text,SortedMap<ColumnFQ,Value>> tabletEntries;
      tabletEntries = MetadataTableUtil.getTabletEntries(tabletsKeyValues, columnsToFetch);

      KeyExtent fke;
      try {
        fke = MasterMetadataUtil.fixSplit(metadataEntry, tabletEntries.get(metadataEntry), instance, SystemCredentials.get(), lock);
      } catch (IOException e) {
        log.error("Error fixing split " + metadataEntry);
        throw new AccumuloException(e.toString());
      }

      if (!fke.equals(extent)) {
        return new Pair<Text,KeyExtent>(null, fke);
      }

      // reread and reverify metadata entries now that metadata entries were fixed
      tabletsKeyValues.clear();
      return verifyTabletInformation(fke, instance, tabletsKeyValues, clientAddress, lock);
    }

    return new Pair<Text,KeyExtent>(new Text(dir.get()), null);
  }

  static Value checkTabletMetadata(KeyExtent extent, TServerInstance instance, SortedMap<Key,Value> tabletsKeyValues, Text metadataEntry)
      throws AccumuloException {

    TServerInstance future = null;
    Value prevEndRow = null;
    Value dir = null;
    Value time = null;
    for (Entry<Key,Value> entry : tabletsKeyValues.entrySet()) {
      Key key = entry.getKey();
      if (!metadataEntry.equals(key.getRow())) {
        log.info("Unexpected row in tablet metadata " + metadataEntry + " " + key.getRow());
        return null;
      }
      Text cf = key.getColumnFamily();
      if (cf.equals(TabletsSection.FutureLocationColumnFamily.NAME)) {
        if (future != null) {
          throw new AccumuloException("Tablet has multiple future locations " + extent);
        }
        future = new TServerInstance(entry.getValue(), key.getColumnQualifier());
      } else if (cf.equals(TabletsSection.CurrentLocationColumnFamily.NAME)) {
        log.info("Tablet seems to be already assigned to " + new TServerInstance(entry.getValue(), key.getColumnQualifier()));
        return null;
      } else if (TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(key)) {
        prevEndRow = entry.getValue();
      } else if (TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN.hasColumns(key)) {
        dir = entry.getValue();
      } else if (TabletsSection.ServerColumnFamily.TIME_COLUMN.hasColumns(key)) {
        time = entry.getValue();
      }
    }

    if (prevEndRow == null) {
      throw new AccumuloException("Metadata entry does not have prev row (" + metadataEntry + ")");
    } else {
      KeyExtent ke2 = new KeyExtent(metadataEntry, prevEndRow);
      if (!extent.equals(ke2)) {
        log.info("Tablet prev end row mismatch " + extent + " " + ke2.getPrevEndRow());
        return null;
      }
    }

    if (dir == null) {
      throw new AccumuloException("Metadata entry does not have directory (" + metadataEntry + ")");
    }

    if (time == null && !extent.equals(RootTable.OLD_EXTENT)) {
      throw new AccumuloException("Metadata entry does not have time (" + metadataEntry + ")");
    }

    if (future == null) {
      log.info("The master has not assigned " + extent + " to " + instance);
      return null;
    }

    if (!instance.equals(future)) {
      log.info("Table " + extent + " has been assigned to " + future + " which is not " + instance);
      return null;
    }

    return dir;
  }

  public String getClientAddressString() {
    if (clientAddress == null)
      return null;
    return clientAddress.getHostText() + ":" + clientAddress.getPort();
  }

  TServerInstance getTabletSession() {
    String address = getClientAddressString();
    if (address == null)
      return null;

    try {
      return new TServerInstance(address, tabletServerLock.getSessionId());
    } catch (Exception ex) {
      log.warn("Unable to read session from tablet server lock" + ex);
      return null;
    }
  }

  public void config(String hostname) {
    log.info("Tablet server starting on " + hostname);
    security = AuditedSecurityOperation.getInstance();
    clientAddress = HostAndPort.fromParts(hostname, 0);
    long walogMaxSize = getSystemConfiguration().getMemoryInBytes(Property.TSERV_WALOG_MAX_SIZE);
    long minBlockSize = CachedConfiguration.getInstance().getLong("dfs.namenode.fs-limits.min-block-size", 0);
    if (minBlockSize != 0 && minBlockSize > walogMaxSize)
      throw new RuntimeException("Unable to start TabletServer. Logger is set to use blocksize " + walogMaxSize + " but hdfs minimum block size is "
          + minBlockSize + ". Either increase the " + Property.TSERV_WALOG_MAX_SIZE + " or decrease dfs.namenode.fs-limits.min-block-size in hdfs-site.xml.");
    logger = new TabletServerLogger(this, walogMaxSize);

    try {
      AccumuloVFSClassLoader.getContextManager().setContextConfig(new ContextManager.DefaultContextsConfig(new Iterable<Entry<String,String>>() {
        @Override
        public Iterator<Entry<String,String>> iterator() {
          return getSystemConfiguration().iterator();
        }
      }));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // A task that cleans up unused classloader contexts
    Runnable contextCleaner = new Runnable() {
      @Override
      public void run() {
        ArrayList<KeyExtent> extents;

        synchronized (onlineTablets) {
          extents = new ArrayList<KeyExtent>(onlineTablets.keySet());
        }

        Set<Text> tables = new HashSet<Text>();

        for (KeyExtent keyExtent : extents) {
          tables.add(keyExtent.getTableId());
        }

        HashSet<String> contexts = new HashSet<String>();

        for (Text tableid : tables) {
          String context = getTableConfiguration(new KeyExtent(tableid, null, null)).get(Property.TABLE_CLASSPATH);
          if (!context.equals("")) {
            contexts.add(context);
          }
        }

        try {
          AccumuloVFSClassLoader.getContextManager().removeUnusedContexts(contexts);
        } catch (IOException e) {
          log.warn(e.getMessage(), e);
        }
      }
    };

    SimpleTimer.getInstance().schedule(contextCleaner, 60000, 60000);

    FileSystemMonitor.start(getSystemConfiguration(), Property.TSERV_MONITOR_FS);

    Runnable gcDebugTask = new Runnable() {
      @Override
      public void run() {
        logGCInfo(getSystemConfiguration());
      }
    };

    SimpleTimer.getInstance().schedule(gcDebugTask, 0, 1000);

    Runnable constraintTask = new Runnable() {

      @Override
      public void run() {
        ArrayList<Tablet> tablets;

        synchronized (onlineTablets) {
          tablets = new ArrayList<Tablet>(onlineTablets.values());
        }

        for (Tablet tablet : tablets) {
          tablet.checkConstraints();
        }
      }
    };

    SimpleTimer.getInstance().schedule(constraintTask, 0, 1000);

    this.resourceManager = new TabletServerResourceManager(instance, fs);

    lastPingTime = System.currentTimeMillis();

    currentMaster = null;

    statsKeeper = new TabletStatsKeeper();

    // start major compactor
    majorCompactorThread = new Daemon(new LoggingRunnable(log, new MajorCompactor(getSystemConfiguration())));
    majorCompactorThread.setName("Split/MajC initiator");
    majorCompactorThread.start();
  }

  public TabletServerStatus getStats(Map<String,MapCounter<ScanRunState>> scanCounts) {
    TabletServerStatus result = new TabletServerStatus();

    Map<KeyExtent,Tablet> onlineTabletsCopy;
    synchronized (this.onlineTablets) {
      onlineTabletsCopy = new HashMap<KeyExtent,Tablet>(this.onlineTablets);
    }
    Map<String,TableInfo> tables = new HashMap<String,TableInfo>();

    for (Entry<KeyExtent,Tablet> entry : onlineTabletsCopy.entrySet()) {
      String tableId = entry.getKey().getTableId().toString();
      TableInfo table = tables.get(tableId);
      if (table == null) {
        table = new TableInfo();
        table.minors = new Compacting();
        table.majors = new Compacting();
        tables.put(tableId, table);
      }
      Tablet tablet = entry.getValue();
      long recs = tablet.getNumEntries();
      table.tablets++;
      table.onlineTablets++;
      table.recs += recs;
      table.queryRate += tablet.queryRate();
      table.queryByteRate += tablet.queryByteRate();
      table.ingestRate += tablet.ingestRate();
      table.ingestByteRate += tablet.ingestByteRate();
      table.scanRate += tablet.scanRate();
      long recsInMemory = tablet.getNumEntriesInMemory();
      table.recsInMemory += recsInMemory;
      if (tablet.minorCompactionRunning())
        table.minors.running++;
      if (tablet.minorCompactionQueued())
        table.minors.queued++;
      if (tablet.majorCompactionRunning())
        table.majors.running++;
      if (tablet.majorCompactionQueued())
        table.majors.queued++;
    }

    for (Entry<String,MapCounter<ScanRunState>> entry : scanCounts.entrySet()) {
      TableInfo table = tables.get(entry.getKey());
      if (table == null) {
        table = new TableInfo();
        tables.put(entry.getKey(), table);
      }

      if (table.scans == null)
        table.scans = new Compacting();

      table.scans.queued += entry.getValue().get(ScanRunState.QUEUED);
      table.scans.running += entry.getValue().get(ScanRunState.RUNNING);
    }

    ArrayList<KeyExtent> offlineTabletsCopy = new ArrayList<KeyExtent>();
    synchronized (this.unopenedTablets) {
      synchronized (this.openingTablets) {
        offlineTabletsCopy.addAll(this.unopenedTablets);
        offlineTabletsCopy.addAll(this.openingTablets);
      }
    }

    for (KeyExtent extent : offlineTabletsCopy) {
      String tableId = extent.getTableId().toString();
      TableInfo table = tables.get(tableId);
      if (table == null) {
        table = new TableInfo();
        tables.put(tableId, table);
      }
      table.tablets++;
    }

    result.lastContact = RelativeTime.currentTimeMillis();
    result.tableMap = tables;
    result.osLoad = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
    result.name = getClientAddressString();
    result.holdTime = resourceManager.holdTime();
    result.lookups = seekCount.get();
    result.indexCacheHits = resourceManager.getIndexCache().getStats().getHitCount();
    result.indexCacheRequest = resourceManager.getIndexCache().getStats().getRequestCount();
    result.dataCacheHits = resourceManager.getDataCache().getStats().getHitCount();
    result.dataCacheRequest = resourceManager.getDataCache().getStats().getRequestCount();
    result.logSorts = logSorter.getLogSorts();
    return result;
  }

  public static void main(String[] args) throws IOException {
    try {
      SecurityUtil.serverLogin(ServerConfiguration.getSiteConfiguration());
      ServerOpts opts = new ServerOpts();
      final String app = "tserver";
      opts.parseArgs(app, args);
      String hostname = opts.getAddress();
      Accumulo.setupLogging(app);
      final Instance instance = HdfsZooInstance.getInstance();
      ServerConfiguration conf = new ServerConfiguration(instance);
      VolumeManager fs = VolumeManagerImpl.get();
      Accumulo.init(fs, conf, app);
      TabletServer server = new TabletServer(conf, fs);
      server.config(hostname);
      Accumulo.enableTracing(hostname, app);
      server.run();
    } catch (Exception ex) {
      log.error("Uncaught exception in TabletServer.main, exiting", ex);
      System.exit(1);
    }
  }

  public void minorCompactionFinished(CommitSession tablet, String newDatafile, int walogSeq) throws IOException {
    totalMinorCompactions++;
    logger.minorCompactionFinished(tablet, newDatafile, walogSeq);
  }

  public void minorCompactionStarted(CommitSession tablet, int lastUpdateSequence, String newMapfileLocation) throws IOException {
    logger.minorCompactionStarted(tablet, lastUpdateSequence, newMapfileLocation);
  }

  public void recover(VolumeManager fs, Tablet tablet, List<LogEntry> logEntries, Set<String> tabletFiles, MutationReceiver mutationReceiver)
      throws IOException {
    List<Path> recoveryLogs = new ArrayList<Path>();
    List<LogEntry> sorted = new ArrayList<LogEntry>(logEntries);
    Collections.sort(sorted, new Comparator<LogEntry>() {
      @Override
      public int compare(LogEntry e1, LogEntry e2) {
        return (int) (e1.timestamp - e2.timestamp);
      }
    });
    for (LogEntry entry : sorted) {
      Path recovery = null;
      for (String log : entry.logSet) {
        Path finished = RecoveryPath.getRecoveryPath(fs, fs.getFullPath(FileType.WAL, log));
        finished = SortedLogState.getFinishedMarkerPath(finished);
        TabletServer.log.info("Looking for " + finished);
        if (fs.exists(finished)) {
          recovery = finished.getParent();
          break;
        }
      }
      if (recovery == null)
        throw new IOException("Unable to find recovery files for extent " + tablet.getExtent() + " logEntry: " + entry);
      recoveryLogs.add(recovery);
    }
    logger.recover(fs, tablet, recoveryLogs, tabletFiles, mutationReceiver);
  }

  private final AtomicInteger logIdGenerator = new AtomicInteger();

  public int createLogId(KeyExtent tablet) {
    AccumuloConfiguration acuTableConf = getTableConfiguration(tablet);
    if (acuTableConf.getBoolean(Property.TABLE_WALOG_ENABLED)) {
      return logIdGenerator.incrementAndGet();
    }
    return -1;
  }

  // / JMX methods

  @Override
  public long getEntries() {
    if (this.isEnabled()) {
      long result = 0;
      for (Tablet tablet : Collections.unmodifiableCollection(onlineTablets.values())) {
        result += tablet.getNumEntries();
      }
      return result;
    }
    return 0;
  }

  @Override
  public long getEntriesInMemory() {
    if (this.isEnabled()) {
      long result = 0;
      for (Tablet tablet : Collections.unmodifiableCollection(onlineTablets.values())) {
        result += tablet.getNumEntriesInMemory();
      }
      return result;
    }
    return 0;
  }

  @Override
  public long getIngest() {
    if (this.isEnabled()) {
      long result = 0;
      for (Tablet tablet : Collections.unmodifiableCollection(onlineTablets.values())) {
        result += tablet.getNumEntriesInMemory();
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getMajorCompactions() {
    if (this.isEnabled()) {
      int result = 0;
      for (Tablet tablet : Collections.unmodifiableCollection(onlineTablets.values())) {
        if (tablet.majorCompactionRunning())
          result++;
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getMajorCompactionsQueued() {
    if (this.isEnabled()) {
      int result = 0;
      for (Tablet tablet : Collections.unmodifiableCollection(onlineTablets.values())) {
        if (tablet.majorCompactionQueued())
          result++;
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getMinorCompactions() {
    if (this.isEnabled()) {
      int result = 0;
      for (Tablet tablet : Collections.unmodifiableCollection(onlineTablets.values())) {
        if (tablet.minorCompactionRunning())
          result++;
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getMinorCompactionsQueued() {
    if (this.isEnabled()) {
      int result = 0;
      for (Tablet tablet : Collections.unmodifiableCollection(onlineTablets.values())) {
        if (tablet.minorCompactionQueued())
          result++;
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getOnlineCount() {
    if (this.isEnabled())
      return onlineTablets.size();
    return 0;
  }

  @Override
  public int getOpeningCount() {
    if (this.isEnabled())
      return openingTablets.size();
    return 0;
  }

  @Override
  public long getQueries() {
    if (this.isEnabled()) {
      long result = 0;
      for (Tablet tablet : Collections.unmodifiableCollection(onlineTablets.values())) {
        result += tablet.totalQueries();
      }
      return result;
    }
    return 0;
  }

  @Override
  public int getUnopenedCount() {
    if (this.isEnabled())
      return unopenedTablets.size();
    return 0;
  }

  @Override
  public String getName() {
    if (this.isEnabled())
      return getClientAddressString();
    return "";
  }

  @Override
  public long getTotalMinorCompactions() {
    if (this.isEnabled())
      return totalMinorCompactions;
    return 0;
  }

  @Override
  public double getHoldTime() {
    if (this.isEnabled())
      return this.resourceManager.holdTime() / 1000.;
    return 0;
  }

  @Override
  public double getAverageFilesPerTablet() {
    if (this.isEnabled()) {
      int count = 0;
      long result = 0;
      for (Tablet tablet : Collections.unmodifiableCollection(onlineTablets.values())) {
        result += tablet.getDatafiles().size();
        count++;
      }
      if (count == 0)
        return 0;
      return result / (double) count;
    }
    return 0;
  }

  @Override
  protected ObjectName getObjectName() {
    return OBJECT_NAME;
  }

  @Override
  protected String getMetricsPrefix() {
    return METRICS_PREFIX;
  }

  public TableConfiguration getTableConfiguration(KeyExtent extent) {
    return ServerConfiguration.getTableConfiguration(instance, extent.getTableId().toString());
  }

  public DfsLogger.ServerResources getServerConfig() {
    return new DfsLogger.ServerResources() {

      @Override
      public VolumeManager getFileSystem() {
        return fs;
      }

      @Override
      public Set<TServerInstance> getCurrentTServers() {
        return null;
      }

      @Override
      public AccumuloConfiguration getConfiguration() {
        return getSystemConfiguration();
      }
    };
  }

  public VolumeManager getFileSystem() {
    return fs;
  }

}

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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.apache.accumulo.server.problems.ProblemType.TABLET_LOAD;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
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
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Durability;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.CompressedIterators;
import org.apache.accumulo.core.client.impl.DurabilityImpl;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.ScannerImpl;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.impl.TabletLocator;
import org.apache.accumulo.core.client.impl.TabletType;
import org.apache.accumulo.core.client.impl.Translator;
import org.apache.accumulo.core.client.impl.Translator.TKeyExtentTranslator;
import org.apache.accumulo.core.client.impl.Translator.TRangeTranslator;
import org.apache.accumulo.core.client.impl.Translators;
import org.apache.accumulo.core.client.impl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ConstraintViolationSummary;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.InitialMultiScan;
import org.apache.accumulo.core.data.thrift.InitialScan;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.data.thrift.MapFileInfo;
import org.apache.accumulo.core.data.thrift.MultiScanResult;
import org.apache.accumulo.core.data.thrift.ScanResult;
import org.apache.accumulo.core.data.thrift.TCMResult;
import org.apache.accumulo.core.data.thrift.TCMStatus;
import org.apache.accumulo.core.data.thrift.TColumn;
import org.apache.accumulo.core.data.thrift.TConditionalMutation;
import org.apache.accumulo.core.data.thrift.TConditionalSession;
import org.apache.accumulo.core.data.thrift.TKeyExtent;
import org.apache.accumulo.core.data.thrift.TKeyValue;
import org.apache.accumulo.core.data.thrift.TMutation;
import org.apache.accumulo.core.data.thrift.TRange;
import org.apache.accumulo.core.data.thrift.TRowRange;
import org.apache.accumulo.core.data.thrift.TSummaries;
import org.apache.accumulo.core.data.thrift.TSummaryRequest;
import org.apache.accumulo.core.data.thrift.UpdateErrors;
import org.apache.accumulo.core.file.blockfile.cache.BlockCache;
import org.apache.accumulo.core.iterators.IterationInterruptedException;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.master.thrift.Compacting;
import org.apache.accumulo.core.master.thrift.MasterClientService;
import org.apache.accumulo.core.master.thrift.TableInfo;
import org.apache.accumulo.core.master.thrift.TabletLoadState;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.replication.ReplicationConstants;
import org.apache.accumulo.core.replication.thrift.ReplicationServicer;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.core.summary.Gatherer;
import org.apache.accumulo.core.summary.Gatherer.FileSystemResolver;
import org.apache.accumulo.core.summary.SummaryCollection;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.ActiveScan;
import org.apache.accumulo.core.tabletserver.thrift.ConstraintViolationException;
import org.apache.accumulo.core.tabletserver.thrift.NoSuchScanIDException;
import org.apache.accumulo.core.tabletserver.thrift.NotServingTabletException;
import org.apache.accumulo.core.tabletserver.thrift.TDurability;
import org.apache.accumulo.core.tabletserver.thrift.TSampleNotPresentException;
import org.apache.accumulo.core.tabletserver.thrift.TSamplerConfiguration;
import org.apache.accumulo.core.tabletserver.thrift.TUnloadTabletGoal;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Iface;
import org.apache.accumulo.core.tabletserver.thrift.TabletClientService.Processor;
import org.apache.accumulo.core.tabletserver.thrift.TabletStats;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.accumulo.core.util.Daemon;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.MapCounter;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.accumulo.core.util.ratelimit.SharedRateLimiterFactory;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.util.LoggingRunnable;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.RetryFactory;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockLossReason;
import org.apache.accumulo.fate.zookeeper.ZooLock.LockWatcher;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.AccumuloServerContext;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.TabletLevel;
import org.apache.accumulo.server.client.ClientServiceHandler;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.data.ServerMutation;
import org.apache.accumulo.server.fs.FileRef;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManager.FileType;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.log.SortedLogState;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.master.recovery.RecoveryPath;
import org.apache.accumulo.server.master.state.Assignment;
import org.apache.accumulo.server.master.state.DistributedStoreException;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.server.master.state.TabletLocationState.BadLocationStateException;
import org.apache.accumulo.server.master.state.TabletStateStore;
import org.apache.accumulo.server.master.state.ZooTabletStateStore;
import org.apache.accumulo.server.master.tableOps.UserCompactionConfig;
import org.apache.accumulo.server.metrics.Metrics;
import org.apache.accumulo.server.metrics.MetricsSystemHelper;
import org.apache.accumulo.server.problems.ProblemReport;
import org.apache.accumulo.server.problems.ProblemReports;
import org.apache.accumulo.server.replication.ZooKeeperInitialization;
import org.apache.accumulo.server.rpc.RpcWrapper;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TCredentialsUpdatingWrapper;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftServerType;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.accumulo.server.security.delegation.AuthenticationTokenSecretManager;
import org.apache.accumulo.server.security.delegation.ZooAuthenticationKeyWatcher;
import org.apache.accumulo.server.util.FileSystemMonitor;
import org.apache.accumulo.server.util.Halt;
import org.apache.accumulo.server.util.MasterMetadataUtil;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.util.ServerBulkImportStatus;
import org.apache.accumulo.server.util.time.RelativeTime;
import org.apache.accumulo.server.util.time.SimpleTimer;
import org.apache.accumulo.server.zookeeper.DistributedWorkQueue;
import org.apache.accumulo.server.zookeeper.TransactionWatcher;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.accumulo.start.classloader.vfs.ContextManager;
import org.apache.accumulo.tserver.ConditionCheckerContext.ConditionChecker;
import org.apache.accumulo.tserver.RowLocks.RowLock;
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
import org.apache.accumulo.tserver.metrics.TabletServerMetricsFactory;
import org.apache.accumulo.tserver.metrics.TabletServerScanMetrics;
import org.apache.accumulo.tserver.metrics.TabletServerUpdateMetrics;
import org.apache.accumulo.tserver.replication.ReplicationServicerHandler;
import org.apache.accumulo.tserver.replication.ReplicationWorker;
import org.apache.accumulo.tserver.scan.LookupTask;
import org.apache.accumulo.tserver.scan.NextBatchTask;
import org.apache.accumulo.tserver.scan.ScanRunState;
import org.apache.accumulo.tserver.session.ConditionalSession;
import org.apache.accumulo.tserver.session.MultiScanSession;
import org.apache.accumulo.tserver.session.ScanSession;
import org.apache.accumulo.tserver.session.Session;
import org.apache.accumulo.tserver.session.SessionManager;
import org.apache.accumulo.tserver.session.SummarySession;
import org.apache.accumulo.tserver.session.UpdateSession;
import org.apache.accumulo.tserver.tablet.BulkImportCacheCleaner;
import org.apache.accumulo.tserver.tablet.CommitSession;
import org.apache.accumulo.tserver.tablet.CompactionInfo;
import org.apache.accumulo.tserver.tablet.CompactionWatcher;
import org.apache.accumulo.tserver.tablet.Compactor;
import org.apache.accumulo.tserver.tablet.KVEntry;
import org.apache.accumulo.tserver.tablet.ScanBatch;
import org.apache.accumulo.tserver.tablet.Tablet;
import org.apache.accumulo.tserver.tablet.TabletClosedException;
import org.apache.accumulo.tserver.tablet.TabletData;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.server.TServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TabletServer extends AccumuloServerContext implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(TabletServer.class);
  private static final long MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS = 1000;
  private static final long RECENTLY_SPLIT_MILLIES = 60 * 1000;
  private static final long TIME_BETWEEN_GC_CHECKS = 5000;
  private static final long TIME_BETWEEN_LOCATOR_CACHE_CLEARS = 60 * 60 * 1000;

  private final GarbageCollectionLogger gcLogger = new GarbageCollectionLogger();
  private final TransactionWatcher watcher = new TransactionWatcher();
  private final ZooCache masterLockCache = new ZooCache();

  private final TabletServerLogger logger;

  private final TabletServerMetricsFactory metricsFactory;
  private final Metrics updateMetrics;
  private final Metrics scanMetrics;
  private final Metrics mincMetrics;

  public Metrics getScanMetrics() {
    return scanMetrics;
  }

  public Metrics getMinCMetrics() {
    return mincMetrics;
  }

  private final LogSorter logSorter;
  private ReplicationWorker replWorker = null;
  private final TabletStatsKeeper statsKeeper;
  private final AtomicInteger logIdGenerator = new AtomicInteger();

  private final AtomicLong flushCounter = new AtomicLong(0);
  private final AtomicLong syncCounter = new AtomicLong(0);

  private final VolumeManager fs;

  private final SortedMap<KeyExtent,Tablet> onlineTablets = Collections.synchronizedSortedMap(new TreeMap<KeyExtent,Tablet>());
  private final SortedSet<KeyExtent> unopenedTablets = Collections.synchronizedSortedSet(new TreeSet<KeyExtent>());
  private final SortedSet<KeyExtent> openingTablets = Collections.synchronizedSortedSet(new TreeSet<KeyExtent>());
  @SuppressWarnings("unchecked")
  private final Map<KeyExtent,Long> recentlyUnloadedCache = Collections.synchronizedMap(new LRUMap(1000));

  private final TabletServerResourceManager resourceManager;
  private final SecurityOperation security;

  private final BlockingDeque<MasterMessage> masterMessages = new LinkedBlockingDeque<>();

  private Thread majorCompactorThread;

  private HostAndPort replicationAddress;
  private HostAndPort clientAddress;

  private volatile boolean serverStopRequested = false;
  private volatile boolean shutdownComplete = false;

  private ZooLock tabletServerLock;

  private TServer server;
  private TServer replServer;

  private DistributedWorkQueue bulkFailedCopyQ;

  private String lockID;

  public static final AtomicLong seekCount = new AtomicLong(0);

  private final AtomicLong totalMinorCompactions = new AtomicLong(0);
  private final ServerConfigurationFactory confFactory;

  private final ZooAuthenticationKeyWatcher authKeyWatcher;
  private final WalStateManager walMarker;

  public TabletServer(Instance instance, ServerConfigurationFactory confFactory, VolumeManager fs) throws IOException {
    super(instance, confFactory);
    this.confFactory = confFactory;
    this.fs = fs;
    final AccumuloConfiguration aconf = getConfiguration();
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + instance.getInstanceID());
    this.sessionManager = new SessionManager(aconf);
    this.logSorter = new LogSorter(instance, fs, aconf);
    this.replWorker = new ReplicationWorker(this, fs);
    this.statsKeeper = new TabletStatsKeeper();
    SimpleTimer.getInstance(aconf).schedule(new Runnable() {
      @Override
      public void run() {
        synchronized (onlineTablets) {
          long now = System.currentTimeMillis();
          for (Tablet tablet : onlineTablets.values())
            try {
              tablet.updateRates(now);
            } catch (Exception ex) {
              log.error("Error updating rates for {}", tablet.getExtent(), ex);
            }
        }
      }
    }, 5000, 5000);

    final long walogMaxSize = aconf.getAsBytes(Property.TSERV_WALOG_MAX_SIZE);
    final long walogMaxAge = aconf.getTimeInMillis(Property.TSERV_WALOG_MAX_AGE);
    final long minBlockSize = CachedConfiguration.getInstance().getLong("dfs.namenode.fs-limits.min-block-size", 0);
    if (minBlockSize != 0 && minBlockSize > walogMaxSize)
      throw new RuntimeException("Unable to start TabletServer. Logger is set to use blocksize " + walogMaxSize + " but hdfs minimum block size is "
          + minBlockSize + ". Either increase the " + Property.TSERV_WALOG_MAX_SIZE + " or decrease dfs.namenode.fs-limits.min-block-size in hdfs-site.xml.");

    final long toleratedWalCreationFailures = aconf.getCount(Property.TSERV_WALOG_TOLERATED_CREATION_FAILURES);
    final long walCreationFailureRetryIncrement = aconf.getTimeInMillis(Property.TSERV_WALOG_TOLERATED_WAIT_INCREMENT);
    final long walCreationFailureRetryMax = aconf.getTimeInMillis(Property.TSERV_WALOG_TOLERATED_MAXIMUM_WAIT_DURATION);
    // Tolerate `toleratedWalCreationFailures` failures, waiting `walCreationFailureRetryIncrement` milliseconds after the first failure,
    // incrementing the next wait period by the same value, for a maximum of `walCreationFailureRetryMax` retries.
    final RetryFactory walCreationRetryFactory = new RetryFactory(toleratedWalCreationFailures, walCreationFailureRetryIncrement,
        walCreationFailureRetryIncrement, walCreationFailureRetryMax);

    logger = new TabletServerLogger(this, walogMaxSize, syncCounter, flushCounter, walCreationRetryFactory, walogMaxAge);
    this.resourceManager = new TabletServerResourceManager(this, fs);
    this.security = AuditedSecurityOperation.getInstance(this);

    metricsFactory = new TabletServerMetricsFactory(aconf);
    updateMetrics = metricsFactory.createUpdateMetrics();
    scanMetrics = metricsFactory.createScanMetrics();
    mincMetrics = metricsFactory.createMincMetrics();
    SimpleTimer.getInstance(aconf).schedule(new Runnable() {
      @Override
      public void run() {
        TabletLocator.clearLocators();
      }
    }, jitter(TIME_BETWEEN_LOCATOR_CACHE_CLEARS), jitter(TIME_BETWEEN_LOCATOR_CACHE_CLEARS));
    walMarker = new WalStateManager(instance, ZooReaderWriter.getInstance());

    // Create the secret manager
    setSecretManager(new AuthenticationTokenSecretManager(instance, aconf.getTimeInMillis(Property.GENERAL_DELEGATION_TOKEN_LIFETIME)));
    if (aconf.getBoolean(Property.INSTANCE_RPC_SASL_ENABLED)) {
      log.info("SASL is enabled, creating ZooKeeper watcher for AuthenticationKeys");
      // Watcher to notice new AuthenticationKeys which enable delegation tokens
      authKeyWatcher = new ZooAuthenticationKeyWatcher(getSecretManager(), ZooReaderWriter.getInstance(), ZooUtil.getRoot(instance)
          + Constants.ZDELEGATION_TOKEN_KEYS);
    } else {
      authKeyWatcher = null;
    }
  }

  public String getVersion() {
    return Constants.VERSION;
  }

  private static long jitter(long ms) {
    Random r = new Random();
    // add a random 10% wait
    return (long) ((1. + (r.nextDouble() / 10)) * ms);
  }

  private final SessionManager sessionManager;

  private final WriteTracker writeTracker = new WriteTracker();

  private final RowLocks rowLocks = new RowLocks();

  private final AtomicLong totalQueuedMutationSize = new AtomicLong(0);
  private final ReentrantLock recoveryLock = new ReentrantLock(true);
  private ThriftClientHandler clientHandler;
  private final ServerBulkImportStatus bulkImportStatus = new ServerBulkImportStatus();

  private class ThriftClientHandler extends ClientServiceHandler implements TabletClientService.Iface {

    ThriftClientHandler() {
      super(TabletServer.this, watcher, fs);
      log.debug("{} created", ThriftClientHandler.class.getName());
    }

    @Override
    public List<TKeyExtent> bulkImport(TInfo tinfo, TCredentials credentials, long tid, Map<TKeyExtent,Map<String,MapFileInfo>> files, boolean setTime)
        throws ThriftSecurityException {

      if (!security.canPerformSystemActions(credentials))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

      List<TKeyExtent> failures = new ArrayList<>();

      for (Entry<TKeyExtent,Map<String,MapFileInfo>> entry : files.entrySet()) {
        TKeyExtent tke = entry.getKey();
        Map<String,MapFileInfo> fileMap = entry.getValue();
        Map<FileRef,MapFileInfo> fileRefMap = new HashMap<>();
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
            log.info("files {} not imported to {}: {}", fileMap.keySet(), new KeyExtent(tke), ioe.getMessage());
            failures.add(tke);
          }
        }
      }
      return failures;
    }

    @Override
    public InitialScan startScan(TInfo tinfo, TCredentials credentials, TKeyExtent textent, TRange range, List<TColumn> columns, int batchSize,
        List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites, boolean isolated,
        long readaheadThreshold, TSamplerConfiguration tSamplerConfig, long batchTimeOut, String context) throws NotServingTabletException,
        ThriftSecurityException, org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException, TSampleNotPresentException {

      Table.ID tableId = Table.ID.of(new String(textent.getTable(), UTF_8));
      Namespace.ID namespaceId;
      try {
        namespaceId = Tables.getNamespaceId(getInstance(), tableId);
      } catch (TableNotFoundException e1) {
        throw new NotServingTabletException(textent);
      }
      if (!security.canScan(credentials, tableId, namespaceId, range, columns, ssiList, ssio, authorizations))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

      if (!security.userHasAuthorizations(credentials, authorizations))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_AUTHORIZATIONS);

      final KeyExtent extent = new KeyExtent(textent);

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

      Set<Column> columnSet = new HashSet<>();
      for (TColumn tcolumn : columns) {
        columnSet.add(new Column(tcolumn));
      }

      final ScanSession scanSession = new ScanSession(credentials, extent, columnSet, ssiList, ssio, new Authorizations(authorizations), readaheadThreshold,
          batchTimeOut, context);
      scanSession.scanner = tablet.createScanner(new Range(range), batchSize, scanSession.columnSet, scanSession.auths, ssiList, ssio, isolated,
          scanSession.interruptFlag, SamplerConfigurationImpl.fromThrift(tSamplerConfig), scanSession.batchTimeOut, scanSession.context);

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
        org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException, TSampleNotPresentException {
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
        org.apache.accumulo.core.tabletserver.thrift.TooManyFilesException, TSampleNotPresentException {

      if (scanSession.nextBatchTask == null) {
        scanSession.nextBatchTask = new NextBatchTask(TabletServer.this, scanID, scanSession.interruptFlag);
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
        else if (e.getCause() instanceof SampleNotPresentException)
          throw new TSampleNotPresentException(scanSession.extent.toThrift());
        else if (e.getCause() instanceof IOException) {
          sleepUninterruptibly(MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS, TimeUnit.MILLISECONDS);
          List<KVEntry> empty = Collections.emptyList();
          bresult = new ScanBatch(empty, true);
          scanSession.nextBatchTask = null;
        } else {
          throw new RuntimeException(e);
        }
      } catch (CancellationException ce) {
        sessionManager.removeSession(scanID);
        Tablet tablet = onlineTablets.get(scanSession.extent);
        if (tablet == null || tablet.isClosed())
          throw new NotServingTabletException(scanSession.extent.toThrift());
        else
          throw new NoSuchScanIDException();
      } catch (TimeoutException e) {
        List<TKeyValue> param = Collections.emptyList();
        long timeout = TabletServer.this.getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
        sessionManager.removeIfNotAccessed(scanID, timeout);
        return new ScanResult(param, true);
      } catch (Throwable t) {
        sessionManager.removeSession(scanID);
        log.warn("Failed to get next batch", t);
        throw new RuntimeException(t);
      }

      ScanResult scanResult = new ScanResult(Key.compress(bresult.getResults()), bresult.isMore());

      scanSession.entriesReturned += scanResult.results.size();

      scanSession.batchCount++;

      if (scanResult.more && scanSession.batchCount > scanSession.readaheadThreshold) {
        // start reading next batch while current batch is transmitted
        // to client
        scanSession.nextBatchTask = new NextBatchTask(TabletServer.this, scanID, scanSession.interruptFlag);
        resourceManager.executeReadAhead(scanSession.extent, scanSession.nextBatchTask);
      }

      if (!scanResult.more)
        closeScan(tinfo, scanID);

      return scanResult;
    }

    @Override
    public void closeScan(TInfo tinfo, long scanID) {
      final ScanSession ss = (ScanSession) sessionManager.removeSession(scanID);
      if (ss != null) {
        long t2 = System.currentTimeMillis();

        log.debug(String.format("ScanSess tid %s %s %,d entries in %.2f secs, nbTimes = [%s] ", TServerUtils.clientAddress.get(), ss.extent.getTableId(),
            ss.entriesReturned, (t2 - ss.startTime) / 1000.0, ss.nbTimes.toString()));
        if (scanMetrics.isEnabled()) {
          scanMetrics.add(TabletServerScanMetrics.SCAN, t2 - ss.startTime);
          scanMetrics.add(TabletServerScanMetrics.RESULT_SIZE, ss.entriesReturned);
        }
      }
    }

    @Override
    public InitialMultiScan startMultiScan(TInfo tinfo, TCredentials credentials, Map<TKeyExtent,List<TRange>> tbatch, List<TColumn> tcolumns,
        List<IterInfo> ssiList, Map<String,Map<String,String>> ssio, List<ByteBuffer> authorizations, boolean waitForWrites,
        TSamplerConfiguration tSamplerConfig, long batchTimeOut, String context) throws ThriftSecurityException, TSampleNotPresentException {
      // find all of the tables that need to be scanned
      final HashSet<Table.ID> tables = new HashSet<>();
      for (TKeyExtent keyExtent : tbatch.keySet()) {
        tables.add(Table.ID.of(new String(keyExtent.getTable(), UTF_8)));
      }

      if (tables.size() != 1)
        throw new IllegalArgumentException("Cannot batch scan over multiple tables");

      // check if user has permission to the tables
      for (Table.ID tableId : tables) {
        Namespace.ID namespaceId;
        try {
          namespaceId = Tables.getNamespaceId(getInstance(), tableId);
        } catch (TableNotFoundException e1) {
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
        }
        if (!security.canScan(credentials, tableId, namespaceId, tbatch, tcolumns, ssiList, ssio, authorizations))
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
      }

      try {
        if (!security.userHasAuthorizations(credentials, authorizations))
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_AUTHORIZATIONS);
      } catch (ThriftSecurityException tse) {
        log.error("{} is not authorized", credentials.getPrincipal(), tse);
        throw tse;
      }
      Map<KeyExtent,List<Range>> batch = Translator.translate(tbatch, new TKeyExtentTranslator(), new Translator.ListTranslator<>(new TRangeTranslator()));

      // This is used to determine which thread pool to use
      KeyExtent threadPoolExtent = batch.keySet().iterator().next();

      if (waitForWrites)
        writeTracker.waitForWrites(TabletType.type(batch.keySet()));

      final MultiScanSession mss = new MultiScanSession(credentials, threadPoolExtent, batch, ssiList, ssio, new Authorizations(authorizations),
          SamplerConfigurationImpl.fromThrift(tSamplerConfig), batchTimeOut, context);

      mss.numTablets = batch.size();
      for (List<Range> ranges : batch.values()) {
        mss.numRanges += ranges.size();
      }

      for (TColumn tcolumn : tcolumns)
        mss.columnSet.add(new Column(tcolumn));

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
    public MultiScanResult continueMultiScan(TInfo tinfo, long scanID) throws NoSuchScanIDException, TSampleNotPresentException {

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

    private MultiScanResult continueMultiScan(TInfo tinfo, long scanID, MultiScanSession session) throws NoSuchScanIDException, TSampleNotPresentException {

      if (session.lookupTask == null) {
        session.lookupTask = new LookupTask(TabletServer.this, scanID);
        resourceManager.executeReadAhead(session.threadPoolExtent, session.lookupTask);
      }

      try {
        MultiScanResult scanResult = session.lookupTask.get(MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS, TimeUnit.MILLISECONDS);
        session.lookupTask = null;
        return scanResult;
      } catch (ExecutionException e) {
        sessionManager.removeSession(scanID);
        if (e.getCause() instanceof SampleNotPresentException) {
          throw new TSampleNotPresentException();
        } else {
          log.warn("Failed to get multiscan result", e);
          throw new RuntimeException(e);
        }
      } catch (TimeoutException e1) {
        long timeout = TabletServer.this.getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
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
    public long startUpdate(TInfo tinfo, TCredentials credentials, TDurability tdurabilty) throws ThriftSecurityException {
      // Make sure user is real
      Durability durability = DurabilityImpl.fromThrift(tdurabilty);
      security.authenticateUser(credentials, credentials);
      if (updateMetrics.isEnabled())
        updateMetrics.add(TabletServerUpdateMetrics.PERMISSION_ERRORS, 0);

      UpdateSession us = new UpdateSession(new TservConstraintEnv(security, credentials), credentials, durability);
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

      Table.ID tableId = null;
      try {
        // if user has no permission to write to this table, add it to
        // the failures list
        boolean sameTable = us.currentTablet != null && (us.currentTablet.getExtent().getTableId().equals(keyExtent.getTableId()));
        tableId = keyExtent.getTableId();
        if (sameTable || security.canWrite(us.getCredentials(), tableId, Tables.getNamespaceId(getInstance(), tableId))) {
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
              updateMetrics.add(TabletServerUpdateMetrics.UNKNOWN_TABLET_ERRORS, 0);
          }
        } else {
          log.warn("Denying access to table {} for user {}", keyExtent.getTableId(), us.getUser());
          long t2 = System.currentTimeMillis();
          us.authTimes.addStat(t2 - t1);
          us.currentTablet = null;
          us.authFailures.put(keyExtent, SecurityErrorCode.PERMISSION_DENIED);
          if (updateMetrics.isEnabled())
            updateMetrics.add(TabletServerUpdateMetrics.PERMISSION_ERRORS, 0);
          return;
        }
      } catch (TableNotFoundException tnfe) {
        log.error("Table " + tableId + " not found ", tnfe);
        long t2 = System.currentTimeMillis();
        us.authTimes.addStat(t2 - t1);
        us.currentTablet = null;
        us.authFailures.put(keyExtent, SecurityErrorCode.TABLE_DOESNT_EXIST);
        if (updateMetrics.isEnabled())
          updateMetrics.add(TabletServerUpdateMetrics.UNKNOWN_TABLET_ERRORS, 0);
        return;
      } catch (ThriftSecurityException e) {
        log.error("Denying permission to check user " + us.getUser() + " with user " + e.getUser(), e);
        long t2 = System.currentTimeMillis();
        us.authTimes.addStat(t2 - t1);
        us.currentTablet = null;
        us.authFailures.put(keyExtent, e.getCode());
        if (updateMetrics.isEnabled())
          updateMetrics.add(TabletServerUpdateMetrics.PERMISSION_ERRORS, 0);
        return;
      }
    }

    @Override
    public void applyUpdates(TInfo tinfo, long updateID, TKeyExtent tkeyExtent, List<TMutation> tmutations) {
      UpdateSession us = (UpdateSession) sessionManager.reserveSession(updateID);
      if (us == null) {
        return;
      }

      boolean reserved = true;
      try {
        KeyExtent keyExtent = new KeyExtent(tkeyExtent);
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
          long totalQueued = updateTotalQueuedMutationSize(additionalMutationSize);
          long total = TabletServer.this.getConfiguration().getAsBytes(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX);
          if (totalQueued > total) {
            try {
              flush(us);
            } catch (HoldTimeoutException hte) {
              // Assumption is that the client has timed out and is gone. If thats not the case, then removing the session should cause the client to fail
              // in such a way that it retries.
              log.debug("HoldTimeoutException during applyUpdates, removing session");
              sessionManager.removeSession(updateID, true);
              reserved = false;
            }
          }
        }
      } finally {
        if (reserved) {
          sessionManager.unreserveSession(us);
        }
      }
    }

    private void flush(UpdateSession us) {

      int mutationCount = 0;
      Map<CommitSession,Mutations> sendables = new HashMap<>();
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
          Durability tabletDurability = tablet.getDurability();
          List<Mutation> mutations = entry.getValue();
          if (mutations.size() > 0) {
            try {
              if (updateMetrics.isEnabled())
                updateMetrics.add(TabletServerUpdateMetrics.MUTATION_ARRAY_SIZE, mutations.size());

              CommitSession commitSession = tablet.prepareMutationsForCommit(us.cenv, mutations);
              if (commitSession == null) {
                if (us.currentTablet == tablet) {
                  us.currentTablet = null;
                }
                us.failures.put(tablet.getExtent(), us.successfulCommits.get(tablet));
              } else {
                sendables.put(commitSession, new Mutations(DurabilityImpl.resolveDurabilty(us.durability, tabletDurability), mutations));
                mutationCount += mutations.size();
              }

            } catch (TConstraintViolationException e) {
              us.violations.add(e.getViolations());
              if (updateMetrics.isEnabled())
                updateMetrics.add(TabletServerUpdateMetrics.CONSTRAINT_VIOLATIONS, 0);

              if (e.getNonViolators().size() > 0) {
                // only log and commit mutations if there were some
                // that did not violate constraints... this is what
                // prepareMutationsForCommit() expects
                sendables.put(e.getCommitSession(), new Mutations(DurabilityImpl.resolveDurabilty(us.durability, tabletDurability), e.getNonViolators()));
              }

              mutationCount += mutations.size();

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
        for (Entry<CommitSession,Mutations> e : sendables.entrySet()) {
          e.getKey().abortCommit(e.getValue().getMutations());
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
          for (Entry<CommitSession,Mutations> entry : sendables.entrySet()) {
            CommitSession commitSession = entry.getKey();
            List<Mutation> mutations = entry.getValue().getMutations();

            commitSession.commit(mutations);

            KeyExtent extent = commitSession.getExtent();

            if (us.currentTablet != null && extent == us.currentTablet.getExtent()) {
              // because constraint violations may filter out some
              // mutations, for proper accounting with the client code,
              // need to increment the count based on the original
              // number of mutations from the client NOT the filtered number
              us.successfulCommits.increment(us.currentTablet, us.queuedMutations.get(us.currentTablet).size());
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
        updateTotalQueuedMutationSize(-us.queuedMutationSize);
        us.queuedMutationSize = 0;
      }
      us.totalUpdates += mutationCount;
    }

    private void updateWalogWriteTime(long time) {
      if (updateMetrics.isEnabled())
        updateMetrics.add(TabletServerUpdateMetrics.WALOG_WRITE_TIME, time);
    }

    private void updateAvgCommitTime(long time, int size) {
      if (updateMetrics.isEnabled())
        updateMetrics.add(TabletServerUpdateMetrics.COMMIT_TIME, (long) ((time) / (double) size));
    }

    private void updateAvgPrepTime(long time, int size) {
      if (updateMetrics.isEnabled())
        updateMetrics.add(TabletServerUpdateMetrics.COMMIT_PREP, (long) ((time) / (double) size));
    }

    @Override
    public UpdateErrors closeUpdate(TInfo tinfo, long updateID) throws NoSuchScanIDException {
      final UpdateSession us = (UpdateSession) sessionManager.removeSession(updateID);
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
        // Assumption is that the client has timed out and is gone. If thats not the case throw an exception that will cause it to retry.
        log.debug("HoldTimeoutException during closeUpdate, reporting no such session");
        throw new NoSuchScanIDException();
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
    public void update(TInfo tinfo, TCredentials credentials, TKeyExtent tkeyExtent, TMutation tmutation, TDurability tdurability)
        throws NotServingTabletException, ConstraintViolationException, ThriftSecurityException {

      final Table.ID tableId = Table.ID.of(new String(tkeyExtent.getTable(), UTF_8));
      Namespace.ID namespaceId;
      try {
        namespaceId = Tables.getNamespaceId(getInstance(), tableId);
      } catch (TableNotFoundException e1) {
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
      }
      if (!security.canWrite(credentials, tableId, namespaceId))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
      final KeyExtent keyExtent = new KeyExtent(tkeyExtent);
      final Tablet tablet = onlineTablets.get(new KeyExtent(keyExtent));
      if (tablet == null) {
        throw new NotServingTabletException(tkeyExtent);
      }
      Durability tabletDurability = tablet.getDurability();

      if (!keyExtent.isMeta()) {
        try {
          TabletServer.this.resourceManager.waitUntilCommitsAreEnabled();
        } catch (HoldTimeoutException hte) {
          // Major hack. Assumption is that the client has timed out and is gone. If thats not the case, then throwing the following will let client know there
          // was a failure and it should retry.
          throw new NotServingTabletException(tkeyExtent);
        }
      }

      final long opid = writeTracker.startWrite(TabletType.type(keyExtent));

      try {
        final Mutation mutation = new ServerMutation(tmutation);
        final List<Mutation> mutations = Collections.singletonList(mutation);

        final Span prep = Trace.start("prep");
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
            final Span wal = Trace.start("wal");
            try {
              logger.log(cs, cs.getWALogSeq(), mutation, DurabilityImpl.resolveDurabilty(DurabilityImpl.fromThrift(tdurability), tabletDurability));
            } finally {
              wal.stop();
            }
            break;
          } catch (IOException ex) {
            log.warn("Error writing mutations to log", ex);
          }
        }

        final Span commit = Trace.start("commit");
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

      final CompressedIterators compressedIters = new CompressedIterators(symbols);
      ConditionCheckerContext checkerContext = new ConditionCheckerContext(compressedIters, confFactory.getTableConfiguration(cs.tableId));

      while (iter.hasNext()) {
        final Entry<KeyExtent,List<ServerConditionalMutation>> entry = iter.next();
        final Tablet tablet = onlineTablets.get(entry.getKey());

        if (tablet == null || tablet.isClosed()) {
          for (ServerConditionalMutation scm : entry.getValue())
            results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
          iter.remove();
        } else {
          final List<ServerConditionalMutation> okMutations = new ArrayList<>(entry.getValue().size());
          final List<TCMResult> resultsSubList = results.subList(results.size(), results.size());

          ConditionChecker checker = checkerContext.newChecker(entry.getValue(), okMutations, resultsSubList);
          try {
            tablet.checkConditions(checker, cs.auths, cs.interruptFlag);

            if (okMutations.size() > 0) {
              entry.setValue(okMutations);
            } else {
              iter.remove();
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

    private void writeConditionalMutations(Map<KeyExtent,List<ServerConditionalMutation>> updates, ArrayList<TCMResult> results, ConditionalSession sess) {
      Set<Entry<KeyExtent,List<ServerConditionalMutation>>> es = updates.entrySet();

      Map<CommitSession,Mutations> sendables = new HashMap<>();

      boolean sessionCanceled = sess.interruptFlag.get();

      Span prepSpan = Trace.start("prep");
      try {
        long t1 = System.currentTimeMillis();
        for (Entry<KeyExtent,List<ServerConditionalMutation>> entry : es) {
          final Tablet tablet = onlineTablets.get(entry.getKey());
          if (tablet == null || tablet.isClosed() || sessionCanceled) {
            for (ServerConditionalMutation scm : entry.getValue())
              results.add(new TCMResult(scm.getID(), TCMStatus.IGNORED));
          } else {
            final Durability tabletDurability = tablet.getDurability();
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
                  sendables.put(cs, new Mutations(DurabilityImpl.resolveDurabilty(sess.durability, tabletDurability), mutations));
                }
              }
            } catch (TConstraintViolationException e) {
              if (e.getNonViolators().size() > 0) {
                sendables.put(e.getCommitSession(), new Mutations(DurabilityImpl.resolveDurabilty(sess.durability, tabletDurability), e.getNonViolators()));
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
        for (Entry<CommitSession,Mutations> entry : sendables.entrySet()) {
          CommitSession commitSession = entry.getKey();
          List<Mutation> mutations = entry.getValue().getMutations();

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

      Map<KeyExtent,List<ServerConditionalMutation>> deferred = new HashMap<>();

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
    public TConditionalSession startConditionalUpdate(TInfo tinfo, TCredentials credentials, List<ByteBuffer> authorizations, String tableIdStr,
        TDurability tdurabilty, String classLoaderContext) throws ThriftSecurityException, TException {

      Table.ID tableId = Table.ID.of(tableIdStr);
      Authorizations userauths = null;
      Namespace.ID namespaceId;
      try {
        namespaceId = Tables.getNamespaceId(getInstance(), tableId);
      } catch (TableNotFoundException e) {
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
      }
      if (!security.canConditionallyUpdate(credentials, tableId, namespaceId, authorizations))
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);

      userauths = security.getUserAuthorizations(credentials);
      for (ByteBuffer auth : authorizations)
        if (!userauths.contains(ByteBufferUtil.toBytes(auth)))
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.BAD_AUTHORIZATIONS);

      ConditionalSession cs = new ConditionalSession(credentials, new Authorizations(authorizations), tableId, DurabilityImpl.fromThrift(tdurabilty),
          classLoaderContext);

      long sid = sessionManager.createSession(cs, false);
      return new TConditionalSession(sid, lockID, sessionManager.getMaxIdleTime());
    }

    @Override
    public List<TCMResult> conditionalUpdate(TInfo tinfo, long sessID, Map<TKeyExtent,List<TConditionalMutation>> mutations, List<String> symbols)
        throws NoSuchScanIDException, TException {

      ConditionalSession cs = (ConditionalSession) sessionManager.reserveSession(sessID);

      if (cs == null || cs.interruptFlag.get())
        throw new NoSuchScanIDException();

      if (!cs.tableId.equals(MetadataTable.ID) && !cs.tableId.equals(RootTable.ID)) {
        try {
          TabletServer.this.resourceManager.waitUntilCommitsAreEnabled();
        } catch (HoldTimeoutException hte) {
          // Assumption is that the client has timed out and is gone. If thats not the case throw an exception that will cause it to retry.
          log.debug("HoldTimeoutException during conditionalUpdate, reporting no such session");
          throw new NoSuchScanIDException();
        }
      }

      Table.ID tid = cs.tableId;
      long opid = writeTracker.startWrite(TabletType.type(new KeyExtent(tid, null, null)));

      try {
        Map<KeyExtent,List<ServerConditionalMutation>> updates = Translator.translate(mutations, Translators.TKET, new Translator.ListTranslator<>(
            ServerConditionalMutation.TCMT));

        for (KeyExtent ke : updates.keySet())
          if (!ke.getTableId().equals(tid))
            throw new IllegalArgumentException("Unexpected table id " + tid + " != " + ke.getTableId());

        ArrayList<TCMResult> results = new ArrayList<>();

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

      Table.ID tableId = Table.ID.of(new String(ByteBufferUtil.toBytes(tkeyExtent.table)));
      Namespace.ID namespaceId;
      try {
        namespaceId = Tables.getNamespaceId(getInstance(), tableId);
      } catch (TableNotFoundException ex) {
        // tableOperationsImpl catches ThriftSeccurityException and checks for missing table
        throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.TABLE_DOESNT_EXIST);
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
        onlineTabletsCopy = new TreeMap<>(onlineTablets);
      }
      List<TabletStats> result = new ArrayList<>();
      Table.ID text = Table.ID.of(tableId);
      KeyExtent start = new KeyExtent(text, new Text(), null);
      for (Entry<KeyExtent,Tablet> entry : onlineTabletsCopy.tailMap(start).entrySet()) {
        KeyExtent ke = entry.getKey();
        if (ke.getTableId().compareTo(text) == 0) {
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

    private void checkPermission(TCredentials credentials, String lock, final String request) throws ThriftSecurityException {
      try {
        log.trace("Got {} message from user: {}", request, credentials.getPrincipal());
        if (!security.canPerformSystemActions(credentials)) {
          log.warn("Got {} message from user: {}", request, credentials.getPrincipal());
          throw new ThriftSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED);
        }
      } catch (ThriftSecurityException e) {
        log.warn("Got {} message from unauthenticatable user: {}", request, e.getUser());
        if (getCredentials().getToken().getClass().getName().equals(credentials.getTokenClassName())) {
          log.error("Got message from a service with a mismatched configuration. Please ensure a compatible configuration.", e);
        }
        throw e;
      }

      if (tabletServerLock == null || !tabletServerLock.wasLockAcquired()) {
        log.debug("Got {} message before my lock was acquired, ignoring...", request);
        throw new RuntimeException("Lock not acquired");
      }

      if (tabletServerLock != null && tabletServerLock.wasLockAcquired() && !tabletServerLock.isLocked()) {
        Halt.halt(1, new Runnable() {
          @Override
          public void run() {
            log.info("Tablet server no longer holds lock during checkPermission() : {}, exiting", request);
            gcLogger.logGCInfo(TabletServer.this.getConfiguration());
          }
        });
      }

      if (lock != null) {
        ZooUtil.LockID lid = new ZooUtil.LockID(ZooUtil.getRoot(getInstance()) + Constants.ZMASTER_LOCK, lock);

        try {
          if (!ZooLock.isLockHeld(masterLockCache, lid)) {
            // maybe the cache is out of date and a new master holds the
            // lock?
            masterLockCache.clear();
            if (!ZooLock.isLockHeld(masterLockCache, lid)) {
              log.warn("Got {} message from a master that does not hold the current lock {}", request, lock);
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
        log.error("Caller doesn't have permission to load a tablet", e);
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

            Set<KeyExtent> all = new HashSet<>();
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
                log.error("Tablet {} overlaps previously assigned {} {} {}", extent, unopenedOverlapping, openingOverlapping, onlineOverlapping + " " + all);
              }
              return;
            }

            unopenedTablets.add(extent);
          }
        }
      }

      // add the assignment job to the appropriate queue
      log.info("Loading tablet {}", extent);

      final AssignmentHandler ah = new AssignmentHandler(extent);
      // final Runnable ah = new LoggingRunnable(log, );
      // Root tablet assignment must take place immediately

      if (extent.isRootTablet()) {
        new Daemon("Root Tablet Assignment") {
          @Override
          public void run() {
            ah.run();
            if (onlineTablets.containsKey(extent)) {
              log.info("Root tablet loaded: {}", extent);
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
    public void unloadTablet(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent textent, TUnloadTabletGoal goal, long requestTime) {
      try {
        checkPermission(credentials, lock, "unloadTablet");
      } catch (ThriftSecurityException e) {
        log.error("Caller doesn't have permission to unload a tablet", e);
        throw new RuntimeException(e);
      }

      KeyExtent extent = new KeyExtent(textent);

      resourceManager.addMigration(extent, new LoggingRunnable(log, new UnloadTabletHandler(extent, goal, requestTime)));
    }

    @Override
    public void flush(TInfo tinfo, TCredentials credentials, String lock, String tableId, ByteBuffer startRow, ByteBuffer endRow) {
      try {
        checkPermission(credentials, lock, "flush");
      } catch (ThriftSecurityException e) {
        log.error("Caller doesn't have permission to flush a table", e);
        throw new RuntimeException(e);
      }

      ArrayList<Tablet> tabletsToFlush = new ArrayList<>();

      KeyExtent ke = new KeyExtent(Table.ID.of(tableId), ByteBufferUtil.toText(endRow), ByteBufferUtil.toText(startRow));

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
            log.info("Asked to flush table that has no flush id {} {}", ke, e.getMessage());
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
        log.error("Caller doesn't have permission to flush a tablet", e);
        throw new RuntimeException(e);
      }

      Tablet tablet = onlineTablets.get(new KeyExtent(textent));
      if (tablet != null) {
        log.info("Flushing {}", tablet.getExtent());
        try {
          tablet.flush(tablet.getFlushID());
        } catch (NoNodeException nne) {
          log.info("Asked to flush tablet that has no flush id {} {}", new KeyExtent(textent), nne.getMessage());
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
          gcLogger.logGCInfo(TabletServer.this.getConfiguration());
          serverStopRequested = true;
          try {
            tabletServerLock.unlock();
          } catch (Exception e) {
            log.error("Caught exception unlocking TabletServer lock", e);
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
        log.error("Caller doesn't have permission to get active scans", e);
        throw e;
      }

      return sessionManager.getActiveScans();
    }

    @Override
    public void chop(TInfo tinfo, TCredentials credentials, String lock, TKeyExtent textent) throws TException {
      try {
        checkPermission(credentials, lock, "chop");
      } catch (ThriftSecurityException e) {
        log.error("Caller doesn't have permission to chop extent", e);
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
        log.error("Caller doesn't have permission to compact a table", e);
        throw new RuntimeException(e);
      }

      KeyExtent ke = new KeyExtent(Table.ID.of(tableId), ByteBufferUtil.toText(endRow), ByteBufferUtil.toText(startRow));

      ArrayList<Tablet> tabletsToCompact = new ArrayList<>();
      synchronized (onlineTablets) {
        for (Tablet tablet : onlineTablets.values())
          if (ke.overlaps(tablet.getExtent()))
            tabletsToCompact.add(tablet);
      }

      Pair<Long,UserCompactionConfig> compactionInfo = null;

      for (Tablet tablet : tabletsToCompact) {
        // all for the same table id, so only need to read
        // compaction id once
        if (compactionInfo == null)
          try {
            compactionInfo = tablet.getCompactionID();
          } catch (NoNodeException e) {
            log.info("Asked to compact table with no compaction id {} {}", ke, e.getMessage());
            return;
          }
        tablet.compactAll(compactionInfo.getFirst(), compactionInfo.getSecond());
      }

    }

    @Override
    public List<ActiveCompaction> getActiveCompactions(TInfo tinfo, TCredentials credentials) throws ThriftSecurityException, TException {
      try {
        checkPermission(credentials, null, "getActiveCompactions");
      } catch (ThriftSecurityException e) {
        log.error("Caller doesn't have permission to get active compactions", e);
        throw e;
      }

      List<CompactionInfo> compactions = Compactor.getRunningCompactions();
      List<ActiveCompaction> ret = new ArrayList<>(compactions.size());

      for (CompactionInfo compactionInfo : compactions) {
        ret.add(compactionInfo.toThrift());
      }

      return ret;
    }

    @Override
    public List<String> getActiveLogs(TInfo tinfo, TCredentials credentials) throws TException {
      String log = logger.getLogFile();
      // Might be null if there no active logger
      if (null == log) {
        return Collections.emptyList();
      }
      return Collections.singletonList(log);
    }

    @Override
    public void removeLogs(TInfo tinfo, TCredentials credentials, List<String> filenames) throws TException {
      log.warn("Garbage collector is attempting to remove logs through the tablet server");
      log.warn("This is probably because your file Garbage Collector is an older version than your tablet servers.\n" + "Restart your file Garbage Collector.");
    }

    private TSummaries getSummaries(Future<SummaryCollection> future) throws TimeoutException {
      try {
        SummaryCollection sc = future.get(MAX_TIME_TO_WAIT_FOR_SCAN_RESULT_MILLIS, TimeUnit.MILLISECONDS);
        return sc.toThrift();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    private TSummaries handleTimeout(long sessionId) {
      long timeout = TabletServer.this.getConfiguration().getTimeInMillis(Property.TSERV_CLIENT_TIMEOUT);
      sessionManager.removeIfNotAccessed(sessionId, timeout);
      return new TSummaries(false, sessionId, -1, -1, null);
    }

    private TSummaries startSummaryOperation(TCredentials credentials, Future<SummaryCollection> future) {
      try {
        return getSummaries(future);
      } catch (TimeoutException e) {
        long sid = sessionManager.createSession(new SummarySession(credentials, future), false);
        while (sid == 0) {
          sessionManager.removeSession(sid);
          sid = sessionManager.createSession(new SummarySession(credentials, future), false);
        }
        return handleTimeout(sid);
      }
    }

    @Override
    public TSummaries startGetSummaries(TInfo tinfo, TCredentials credentials, TSummaryRequest request) throws ThriftSecurityException,
        ThriftTableOperationException, NoSuchScanIDException, TException {
      Namespace.ID namespaceId;
      Table.ID tableId = Table.ID.of(request.getTableId());
      try {
        namespaceId = Tables.getNamespaceId(TabletServer.this.getInstance(), tableId);
      } catch (TableNotFoundException e1) {
        throw new ThriftTableOperationException(tableId.canonicalID(), null, null, TableOperationExceptionType.NOTFOUND, null);
      }

      if (!security.canGetSummaries(credentials, tableId, namespaceId)) {
        throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED).asThriftException();
      }

      ServerConfigurationFactory factory = TabletServer.this.getServerConfigurationFactory();
      ExecutorService es = resourceManager.getSummaryPartitionExecutor();
      Future<SummaryCollection> future = new Gatherer(TabletServer.this, request, factory.getTableConfiguration(tableId)).gather(es);

      return startSummaryOperation(credentials, future);
    }

    @Override
    public TSummaries startGetSummariesForPartition(TInfo tinfo, TCredentials credentials, TSummaryRequest request, int modulus, int remainder)
        throws ThriftSecurityException, NoSuchScanIDException, TException {
      // do not expect users to call this directly, expect other tservers to call this method
      if (!security.canPerformSystemActions(credentials)) {
        throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED).asThriftException();
      }

      ServerConfigurationFactory factory = TabletServer.this.getServerConfigurationFactory();
      ExecutorService spe = resourceManager.getSummaryRemoteExecutor();
      Future<SummaryCollection> future = new Gatherer(TabletServer.this, request, factory.getTableConfiguration(Table.ID.of(request.getTableId())))
          .processPartition(spe, modulus, remainder);

      return startSummaryOperation(credentials, future);
    }

    @Override
    public TSummaries startGetSummariesFromFiles(TInfo tinfo, TCredentials credentials, TSummaryRequest request, Map<String,List<TRowRange>> files)
        throws ThriftSecurityException, NoSuchScanIDException, TException {
      // do not expect users to call this directly, expect other tservers to call this method
      if (!security.canPerformSystemActions(credentials)) {
        throw new AccumuloSecurityException(credentials.getPrincipal(), SecurityErrorCode.PERMISSION_DENIED).asThriftException();
      }

      ExecutorService srp = resourceManager.getSummaryRetrievalExecutor();
      TableConfiguration tableCfg = confFactory.getTableConfiguration(Table.ID.of(request.getTableId()));
      BlockCache summaryCache = resourceManager.getSummaryCache();
      BlockCache indexCache = resourceManager.getIndexCache();
      FileSystemResolver volMgr = p -> fs.getVolumeByPath(p).getFileSystem();
      Future<SummaryCollection> future = new Gatherer(TabletServer.this, request, tableCfg).processFiles(volMgr, files, summaryCache, indexCache, srp);

      return startSummaryOperation(credentials, future);
    }

    @Override
    public TSummaries contiuneGetSummaries(TInfo tinfo, long sessionId) throws NoSuchScanIDException, TException {
      SummarySession session = (SummarySession) sessionManager.getSession(sessionId);
      if (session == null) {
        throw new NoSuchScanIDException();
      }

      Future<SummaryCollection> future = session.getFuture();
      try {
        TSummaries tsums = getSummaries(future);
        sessionManager.removeSession(sessionId);
        return tsums;
      } catch (TimeoutException e) {
        return handleTimeout(sessionId);
      }
    }
  }

  private class SplitRunner implements Runnable {
    private final Tablet tablet;

    public SplitRunner(Tablet tablet) {
      this.tablet = tablet;
    }

    @Override
    public void run() {
      splitTablet(tablet);
    }
  }

  public long updateTotalQueuedMutationSize(long additionalMutationSize) {
    return totalQueuedMutationSize.addAndGet(additionalMutationSize);
  }

  public Tablet getOnlineTablet(KeyExtent extent) {
    return onlineTablets.get(extent);
  }

  public Session getSession(long sessionId) {
    return sessionManager.getSession(sessionId);
  }

  public void executeSplit(Tablet tablet) {
    resourceManager.executeSplit(tablet.getExtent(), new LoggingRunnable(log, new SplitRunner(tablet)));
  }

  private class MajorCompactor implements Runnable {

    public MajorCompactor(AccumuloConfiguration config) {
      CompactionWatcher.startWatching(config);
    }

    @Override
    public void run() {
      while (true) {
        try {
          sleepUninterruptibly(getConfiguration().getTimeInMillis(Property.TSERV_MAJC_DELAY), TimeUnit.MILLISECONDS);

          TreeMap<KeyExtent,Tablet> copyOnlineTablets = new TreeMap<>();

          synchronized (onlineTablets) {
            copyOnlineTablets.putAll(onlineTablets); // avoid
            // concurrent
            // modification
          }

          int numMajorCompactionsInProgress = 0;

          Iterator<Entry<KeyExtent,Tablet>> iter = copyOnlineTablets.entrySet().iterator();

          // bail early now if we're shutting down
          while (iter.hasNext()) {

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
              log.debug("Initiating minor compaction for {} because it has {} write ahead logs", tablet.getExtent(), tablet.getLogCount());
              tablet.initiateMinorCompaction(MinorCompactionReason.SYSTEM);
            }

            synchronized (tablet) {
              if (tablet.initiateMajorCompaction(MajorCompactionReason.NORMAL) || tablet.isMajorCompactionQueued() || tablet.isMajorCompactionRunning()) {
                numMajorCompactionsInProgress++;
                continue;
              }
            }
          }

          int idleCompactionsToStart = Math.max(1, getConfiguration().getCount(Property.TSERV_MAJC_MAXCONCURRENT) / 2);

          if (numMajorCompactionsInProgress < idleCompactionsToStart) {
            // system is not major compacting, can schedule some
            // idle compactions
            iter = copyOnlineTablets.entrySet().iterator();

            while (iter.hasNext() && numMajorCompactionsInProgress < idleCompactionsToStart) {
              Entry<KeyExtent,Tablet> entry = iter.next();
              Tablet tablet = entry.getValue();

              if (tablet.initiateMajorCompaction(MajorCompactionReason.IDLE)) {
                numMajorCompactionsInProgress++;
              }
            }
          }
        } catch (Throwable t) {
          log.error("Unexpected exception in {}", Thread.currentThread().getName(), t);
          sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
      }
    }
  }

  private void splitTablet(Tablet tablet) {
    try {

      TreeMap<KeyExtent,TabletData> tabletInfo = splitTablet(tablet, null);
      if (tabletInfo == null) {
        // either split or compact not both
        // were not able to split... so see if a major compaction is
        // needed
        tablet.initiateMajorCompaction(MajorCompactionReason.NORMAL);
      }
    } catch (IOException e) {
      statsKeeper.updateTime(Operation.SPLIT, 0, 0, true);
      log.error("split failed: {} for tablet {}", e.getMessage(), tablet.getExtent(), e);
    } catch (Exception e) {
      statsKeeper.updateTime(Operation.SPLIT, 0, 0, true);
      log.error("Unknown error on split:", e);
    }
  }

  private TreeMap<KeyExtent,TabletData> splitTablet(Tablet tablet, byte[] splitPoint) throws IOException {
    long t1 = System.currentTimeMillis();

    TreeMap<KeyExtent,TabletData> tabletInfo = tablet.split(splitPoint);
    if (tabletInfo == null) {
      return null;
    }

    log.info("Starting split: {}", tablet.getExtent());
    statsKeeper.incrementStatusSplit();
    long start = System.currentTimeMillis();

    Tablet[] newTablets = new Tablet[2];

    Entry<KeyExtent,TabletData> first = tabletInfo.firstEntry();
    TabletResourceManager newTrm0 = resourceManager.createTabletResourceManager(first.getKey(), getTableConfiguration(first.getKey()));
    newTablets[0] = new Tablet(TabletServer.this, first.getKey(), newTrm0, first.getValue());

    Entry<KeyExtent,TabletData> last = tabletInfo.lastEntry();
    TabletResourceManager newTrm1 = resourceManager.createTabletResourceManager(last.getKey(), getTableConfiguration(last.getKey()));
    newTablets[1] = new Tablet(TabletServer.this, last.getKey(), newTrm1, last.getValue());

    // roll tablet stats over into tablet server's statsKeeper object as
    // historical data
    statsKeeper.saveMajorMinorTimes(tablet.getTabletStats());

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
    log.info("Tablet split: {} size0 {} size1 {} time {}ms", tablet.getExtent(), newTablets[0].estimateTabletSize(), newTablets[1].estimateTabletSize(),
        (t2 - t1));

    return tabletInfo;
  }

  // add a message for the main thread to send back to the master
  public void enqueueMasterMessage(MasterMessage m) {
    masterMessages.addLast(m);
  }

  private class UnloadTabletHandler implements Runnable {
    private final KeyExtent extent;
    private final TUnloadTabletGoal goalState;
    private final long requestTimeSkew;

    public UnloadTabletHandler(KeyExtent extent, TUnloadTabletGoal goalState, long requestTime) {
      this.extent = extent;
      this.goalState = goalState;
      this.requestTimeSkew = requestTime - MILLISECONDS.convert(System.nanoTime(), NANOSECONDS);
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
          log.info("told to unload tablet that was not being served {}", extent);
          enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.UNLOAD_FAILURE_NOT_SERVING, extent));
        }
        return;
      }

      try {
        t.close(!goalState.equals(TUnloadTabletGoal.DELETED));
      } catch (Throwable e) {

        if ((t.isClosing() || t.isClosed()) && e instanceof IllegalStateException) {
          log.debug("Failed to unload tablet {}... it was already closing or closed : {}", extent, e.getMessage());
        } else {
          log.error("Failed to close tablet {}... Aborting migration", extent, e);
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
          tls = new TabletLocationState(extent, null, instance, null, null, null, false);
        } catch (BadLocationStateException e) {
          log.error("Unexpected error", e);
        }
        if (!goalState.equals(TUnloadTabletGoal.SUSPENDED) || extent.isRootTablet()
            || (extent.isMeta() && !getConfiguration().getBoolean(Property.MASTER_METADATA_SUSPENDABLE))) {
          log.debug("Unassigning {}", tls);
          TabletStateStore.unassign(TabletServer.this, tls, null);
        } else {
          log.debug("Suspending " + tls);
          TabletStateStore.suspend(TabletServer.this, tls, null, requestTimeSkew + MILLISECONDS.convert(System.nanoTime(), NANOSECONDS));
        }
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
      statsKeeper.saveMajorMinorTimes(t.getTabletStats());
      log.info("unloaded {}", extent);

    }
  }

  protected class AssignmentHandler implements Runnable {
    private final KeyExtent extent;
    private final int retryAttempt;

    public AssignmentHandler(KeyExtent extent) {
      this(extent, 0);
    }

    public AssignmentHandler(KeyExtent extent, int retryAttempt) {
      this.extent = extent;
      this.retryAttempt = retryAttempt;
    }

    @Override
    public void run() {
      log.info("{}: got assignment from master: {}", clientAddress, extent);

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
              log.info("assignment {} no longer in the unopened set", extent);
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

      log.debug("Loading extent: {}", extent);

      // check Metadata table before accepting assignment
      Text locationToOpen = null;
      SortedMap<Key,Value> tabletsKeyValues = new TreeMap<>();
      try {
        Pair<Text,KeyExtent> pair = verifyTabletInformation(TabletServer.this, extent, TabletServer.this.getTabletSession(), tabletsKeyValues,
            getClientAddressString(), getLock());
        if (pair != null) {
          locationToOpen = pair.getFirst();
          if (pair.getSecond() != null) {
            synchronized (openingTablets) {
              openingTablets.remove(extent);
              openingTablets.notifyAll();
              // it expected that the new extent will overlap the old one... if it does not, it should not be added to unopenedTablets
              if (!KeyExtent.findOverlapping(extent, new TreeSet<>(Arrays.asList(pair.getSecond()))).contains(pair.getSecond())) {
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
        log.debug("Reporting tablet {} assignment failure: unable to verify Tablet Information", extent);
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
        acquireRecoveryMemory(extent);

        TabletResourceManager trm = resourceManager.createTabletResourceManager(extent, getTableConfiguration(extent));
        TabletData data;
        if (extent.isRootTablet()) {
          data = new TabletData(fs, ZooReaderWriter.getInstance(), getTableConfiguration(extent));
        } else {
          data = new TabletData(extent, fs, tabletsKeyValues.entrySet().iterator());
        }

        tablet = new Tablet(TabletServer.this, extent, trm, data);
        // If a minor compaction starts after a tablet opens, this indicates a log recovery occurred. This recovered data must be minor compacted.
        // There are three reasons to wait for this minor compaction to finish before placing the tablet in online tablets.
        //
        // 1) The log recovery code does not handle data written to the tablet on multiple tablet servers.
        // 2) The log recovery code does not block if memory is full. Therefore recovering lots of tablets that use a lot of memory could run out of memory.
        // 3) The minor compaction finish event did not make it to the logs (the file will be in metadata, preventing replay of compacted data)... but do not
        // want a majc to wipe the file out from metadata and then have another process failure... this could cause duplicate data to replay.
        if (tablet.getNumEntriesInMemory() > 0 && !tablet.minorCompactNow(MinorCompactionReason.RECOVERY)) {
          throw new RuntimeException("Minor compaction after recovery fails for " + extent);
        }
        Assignment assignment = new Assignment(extent, getTabletSession());
        TabletStateStore.setLocation(TabletServer.this, assignment);

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
        log.warn("exception trying to assign tablet {} {}", extent, locationToOpen, e);

        if (e.getMessage() != null) {
          log.warn("{}", e.getMessage());
        }

        Table.ID tableId = extent.getTableId();
        ProblemReports.getInstance(TabletServer.this).report(new ProblemReport(tableId, TABLET_LOAD, extent.getUUID().toString(), getClientAddressString(), e));
      } finally {
        releaseRecoveryMemory(extent);
      }

      if (!successful) {
        synchronized (unopenedTablets) {
          synchronized (openingTablets) {
            openingTablets.remove(extent);
            unopenedTablets.add(extent);
            openingTablets.notifyAll();
          }
        }
        log.warn("failed to open tablet {} reporting failure to master", extent);
        enqueueMasterMessage(new TabletStatusMessage(TabletLoadState.LOAD_FAILURE, extent));
        long reschedule = Math.min((1l << Math.min(32, retryAttempt)) * 1000, 10 * 60 * 1000l);
        log.warn(String.format("rescheduling tablet load in %.2f seconds", reschedule / 1000.));
        SimpleTimer.getInstance(getConfiguration()).schedule(new TimerTask() {
          @Override
          public void run() {
            log.info("adding tablet {} back to the assignment pool (retry {})", extent, retryAttempt);
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

  private void acquireRecoveryMemory(KeyExtent extent) throws InterruptedException {
    if (!extent.isMeta()) {
      recoveryLock.lock();
    }
  }

  private void releaseRecoveryMemory(KeyExtent extent) {
    if (!extent.isMeta()) {
      recoveryLock.unlock();
    }
  }

  private HostAndPort startServer(AccumuloConfiguration conf, String address, Property portHint, TProcessor processor, String threadName)
      throws UnknownHostException {
    Property maxMessageSizeProperty = (conf.get(Property.TSERV_MAX_MESSAGE_SIZE) != null ? Property.TSERV_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(this, address, portHint, processor, this.getClass().getSimpleName(), threadName, Property.TSERV_PORTSEARCH,
        Property.TSERV_MINTHREADS, Property.TSERV_THREADCHECK, maxMessageSizeProperty);
    this.server = sp.server;
    return sp.address;
  }

  private HostAndPort getMasterAddress() {
    try {
      List<String> locations = getInstance().getMasterLocations();
      if (locations.size() == 0)
        return null;
      return HostAndPort.fromString(locations.get(0));
    } catch (Exception e) {
      log.warn("Failed to obtain master host " + e);
    }

    return null;
  }

  // Connect to the master for posting asynchronous results
  private MasterClientService.Client masterConnection(HostAndPort address) {
    try {
      if (address == null) {
        return null;
      }
      MasterClientService.Client client = ThriftUtil.getClient(new MasterClientService.Client.Factory(), address, this);
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
    clientHandler = new ThriftClientHandler();
    Iface rpcProxy = RpcWrapper.service(clientHandler);
    final Processor<Iface> processor;
    if (ThriftServerType.SASL == getThriftServerType()) {
      Iface tcredProxy = TCredentialsUpdatingWrapper.service(rpcProxy, ThriftClientHandler.class, getConfiguration());
      processor = new Processor<>(tcredProxy);
    } else {
      processor = new Processor<>(rpcProxy);
    }
    HostAndPort address = startServer(getServerConfigurationFactory().getSystemConfiguration(), clientAddress.getHost(), Property.TSERV_CLIENTPORT, processor,
        "Thrift Client Server");
    log.info("address = {}", address);
    return address;
  }

  private HostAndPort startReplicationService() throws UnknownHostException {
    final ReplicationServicerHandler handler = new ReplicationServicerHandler(this);
    ReplicationServicer.Iface rpcProxy = RpcWrapper.service(handler);
    ReplicationServicer.Iface repl = TCredentialsUpdatingWrapper.service(rpcProxy, handler.getClass(), getConfiguration());
    ReplicationServicer.Processor<ReplicationServicer.Iface> processor = new ReplicationServicer.Processor<>(repl);
    AccumuloConfiguration conf = getServerConfigurationFactory().getSystemConfiguration();
    Property maxMessageSizeProperty = (conf.get(Property.TSERV_MAX_MESSAGE_SIZE) != null ? Property.TSERV_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(this, clientAddress.getHost(), Property.REPLICATION_RECEIPT_SERVICE_PORT, processor,
        "ReplicationServicerHandler", "Replication Servicer", Property.TSERV_PORTSEARCH, Property.REPLICATION_MIN_THREADS, Property.REPLICATION_THREADCHECK,
        maxMessageSizeProperty);
    this.replServer = sp.server;
    log.info("Started replication service on {}", sp.address);

    try {
      // The replication service is unique to the thrift service for a tserver, not just a host.
      // Advertise the host and port for replication service given the host and port for the tserver.
      ZooReaderWriter.getInstance().putPersistentData(ZooUtil.getRoot(getInstance()) + ReplicationConstants.ZOO_TSERVERS + "/" + clientAddress.toString(),
          sp.address.toString().getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
    } catch (Exception e) {
      log.error("Could not advertise replication service port", e);
      throw new RuntimeException(e);
    }

    return sp.address;
  }

  public ZooLock getLock() {
    return tabletServerLock;
  }

  private void announceExistence() {
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    try {
      String zPath = ZooUtil.getRoot(getInstance()) + Constants.ZTSERVERS + "/" + getClientAddressString();

      try {
        zoo.putPersistentData(zPath, new byte[] {}, NodeExistsPolicy.SKIP);
      } catch (KeeperException e) {
        if (KeeperException.Code.NOAUTH == e.code()) {
          log.error("Failed to write to ZooKeeper. Ensure that accumulo-site.xml, specifically instance.secret, is consistent.");
        }
        throw e;
      }

      tabletServerLock = new ZooLock(zPath);

      LockWatcher lw = new LockWatcher() {

        @Override
        public void lostLock(final LockLossReason reason) {
          Halt.halt(serverStopRequested ? 0 : 1, new Runnable() {
            @Override
            public void run() {
              if (!serverStopRequested)
                log.error("Lost tablet server lock (reason = {}), exiting.", reason);
              gcLogger.logGCInfo(getConfiguration());
            }
          });
        }

        @Override
        public void unableToMonitorLockNode(final Throwable e) {
          Halt.halt(1, new Runnable() {
            @Override
            public void run() {
              log.error("Lost ability to monitor tablet server lock, exiting.", e);
            }
          });

        }
      };

      byte[] lockContent = new ServerServices(getClientAddressString(), Service.TSERV_CLIENT).toString().getBytes(UTF_8);
      for (int i = 0; i < 120 / 5; i++) {
        zoo.putPersistentData(zPath, new byte[0], NodeExistsPolicy.SKIP);

        if (tabletServerLock.tryLock(lw, lockContent)) {
          log.debug("Obtained tablet server lock {}", tabletServerLock.getLockPath());
          lockID = tabletServerLock.getLockID().serialize(ZooUtil.getRoot(getInstance()) + Constants.ZTSERVERS + "/");
          return;
        }
        log.info("Waiting for tablet server lock");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
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
  @Override
  public void run() {
    SecurityUtil.serverLogin(SiteConfiguration.getInstance());

    // To make things easier on users/devs, and to avoid creating an upgrade path to 1.7
    // We can just make the zookeeper paths before we try to use.
    try {
      ZooKeeperInitialization.ensureZooKeeperInitialized(ZooReaderWriter.getInstance(), ZooUtil.getRoot(getInstance()));
    } catch (KeeperException | InterruptedException e) {
      log.error("Could not ensure that ZooKeeper is properly initialized", e);
      throw new RuntimeException(e);
    }

    Metrics tserverMetrics = metricsFactory.createTabletServerMetrics(this);

    // Register MBeans
    try {
      tserverMetrics.register();
      mincMetrics.register();
      scanMetrics.register();
      updateMetrics.register();
    } catch (Exception e) {
      log.error("Error registering with JMX", e);
    }

    if (null != authKeyWatcher) {
      log.info("Seeding ZooKeeper watcher for authentication keys");
      try {
        authKeyWatcher.updateAuthKeys();
      } catch (KeeperException | InterruptedException e) {
        // TODO Does there need to be a better check? What are the error conditions that we'd fall out here? AUTH_FAILURE?
        // If we get the error, do we just put it on a timer and retry the exists(String, Watcher) call?
        log.error("Failed to perform initial check for authentication tokens in ZooKeeper. Delegation token authentication will be unavailable.", e);
      }
    }

    try {
      clientAddress = startTabletClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the tablet client service", e1);
    }
    announceExistence();
    try {
      walMarker.initWalMarker(getTabletSession());
    } catch (Exception e) {
      log.error("Unable to create WAL marker node in zookeeper", e);
      throw new RuntimeException(e);
    }

    ThreadPoolExecutor distWorkQThreadPool = new SimpleThreadPool(getConfiguration().getCount(Property.TSERV_WORKQ_THREADS), "distributed work queue");

    bulkFailedCopyQ = new DistributedWorkQueue(ZooUtil.getRoot(getInstance()) + Constants.ZBULK_FAILED_COPYQ, getConfiguration());
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

    // Start the thrift service listening for incoming replication requests
    try {
      replicationAddress = startReplicationService();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Failed to start replication service", e);
    }

    // Start the pool to handle outgoing replications
    final ThreadPoolExecutor replicationThreadPool = new SimpleThreadPool(getConfiguration().getCount(Property.REPLICATION_WORKER_THREADS), "replication task");
    replWorker.setExecutor(replicationThreadPool);
    replWorker.run();

    // Check the configuration value for the size of the pool and, if changed, resize the pool, every 5 seconds);
    final AccumuloConfiguration aconf = getConfiguration();
    Runnable replicationWorkThreadPoolResizer = new Runnable() {
      @Override
      public void run() {
        int maxPoolSize = aconf.getCount(Property.REPLICATION_WORKER_THREADS);
        if (replicationThreadPool.getMaximumPoolSize() != maxPoolSize) {
          log.info("Resizing thread pool for sending replication work from {} to {}", replicationThreadPool.getMaximumPoolSize(), maxPoolSize);
          replicationThreadPool.setMaximumPoolSize(maxPoolSize);
        }
      }
    };
    SimpleTimer.getInstance(aconf).schedule(replicationWorkThreadPoolResizer, 10000, 30000);

    final long CLEANUP_BULK_LOADED_CACHE_MILLIS = 15 * 60 * 1000;
    SimpleTimer.getInstance(aconf).schedule(new BulkImportCacheCleaner(this), CLEANUP_BULK_LOADED_CACHE_MILLIS, CLEANUP_BULK_LOADED_CACHE_MILLIS);

    HostAndPort masterHost;
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
          // then finally block should place mm back on queue
          while (!serverStopRequested && mm != null && client != null && client.getOutputProtocol() != null
              && client.getOutputProtocol().getTransport() != null && client.getOutputProtocol().getTransport().isOpen()) {
            try {
              mm.send(rpcCreds(), getClientAddressString(), iface);
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

          sleepUninterruptibly(1, TimeUnit.SECONDS);
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
    // kill the other threads and finalize objects. We want the shutdown that is
    // running in the master listener thread to complete oldServer this happens.
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
    log.debug("Stopping Replication Server");
    TServerUtils.stopTServer(this.replServer);
    log.debug("Stopping Thrift Servers");
    TServerUtils.stopTServer(server);

    try {
      log.debug("Closing filesystem");
      fs.close();
    } catch (IOException e) {
      log.warn("Failed to close filesystem : {}", e.getMessage(), e);
    }

    gcLogger.logGCInfo(getConfiguration());

    log.info("TServerInfo: stop requested. exiting ... ");

    try {
      tabletServerLock.unlock();
    } catch (Exception e) {
      log.warn("Failed to release tablet server lock", e);
    }
  }

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
      return new Pair<>(new Text(MetadataTableUtil.getRootTabletDir()), null);
    } catch (IOException e) {
      throw new AccumuloException(e);
    }
  }

  public static Pair<Text,KeyExtent> verifyTabletInformation(AccumuloServerContext context, KeyExtent extent, TServerInstance instance,
      SortedMap<Key,Value> tabletsKeyValues, String clientAddress, ZooLock lock) throws AccumuloSecurityException, DistributedStoreException, AccumuloException {

    log.debug("verifying extent {}", extent);
    if (extent.isRootTablet()) {
      return verifyRootTablet(extent, instance);
    }
    Table.ID tableToVerify = MetadataTable.ID;
    if (extent.isMeta())
      tableToVerify = RootTable.ID;

    List<ColumnFQ> columnsToFetch = Arrays.asList(new ColumnFQ[] {TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN,
        TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN, TabletsSection.TabletColumnFamily.SPLIT_RATIO_COLUMN,
        TabletsSection.TabletColumnFamily.OLD_PREV_ROW_COLUMN, TabletsSection.ServerColumnFamily.TIME_COLUMN});

    TreeMap<Key,Value> tkv = new TreeMap<>();
    try (ScannerImpl scanner = new ScannerImpl(context, tableToVerify, Authorizations.EMPTY)) {
      scanner.setRange(extent.toMetadataRange());
      for (Entry<Key,Value> entry : scanner)
        tkv.put(entry.getKey(), entry.getValue());
    }

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
        fke = MasterMetadataUtil.fixSplit(context, metadataEntry, tabletEntries.get(metadataEntry), instance, lock);
      } catch (IOException e) {
        log.error("Error fixing split {}", metadataEntry);
        throw new AccumuloException(e.toString());
      }

      if (!fke.equals(extent)) {
        return new Pair<>(null, fke);
      }

      // reread and reverify metadata entries now that metadata entries were fixed
      tabletsKeyValues.clear();
      return verifyTabletInformation(context, fke, instance, tabletsKeyValues, clientAddress, lock);
    }

    return new Pair<>(new Text(dir.get()), null);
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
        log.info("Unexpected row in tablet metadata {} {}", metadataEntry, key.getRow());
        return null;
      }
      Text cf = key.getColumnFamily();
      if (cf.equals(TabletsSection.FutureLocationColumnFamily.NAME)) {
        if (future != null) {
          throw new AccumuloException("Tablet has multiple future locations " + extent);
        }
        future = new TServerInstance(entry.getValue(), key.getColumnQualifier());
      } else if (cf.equals(TabletsSection.CurrentLocationColumnFamily.NAME)) {
        log.info("Tablet seems to be already assigned to {} {}", new TServerInstance(entry.getValue(), key.getColumnQualifier()));
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
        log.info("Tablet prev end row mismatch {} {}", extent, ke2.getPrevEndRow());
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
      log.info("The master has not assigned {} to ", extent, instance);
      return null;
    }

    if (!instance.equals(future)) {
      log.info("Table {} has been assigned to {} which is not {}", extent, future, instance);
      return null;
    }

    return dir;
  }

  public String getClientAddressString() {
    if (clientAddress == null)
      return null;
    return clientAddress.getHost() + ":" + clientAddress.getPort();
  }

  public String getReplicationAddressSTring() {
    if (null == replicationAddress) {
      return null;
    }
    return replicationAddress.getHost() + ":" + replicationAddress.getPort();
  }

  public TServerInstance getTabletSession() {
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
    log.info("Tablet server starting on {}", hostname);
    majorCompactorThread = new Daemon(new LoggingRunnable(log, new MajorCompactor(getConfiguration())));
    majorCompactorThread.setName("Split/MajC initiator");
    majorCompactorThread.start();

    clientAddress = HostAndPort.fromParts(hostname, 0);
    try {
      AccumuloVFSClassLoader.getContextManager().setContextConfig(new ContextManager.DefaultContextsConfig(new Iterable<Entry<String,String>>() {
        @Override
        public Iterator<Entry<String,String>> iterator() {
          return getConfiguration().iterator();
        }
      }));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // A task that cleans up unused classloader contexts
    Runnable contextCleaner = new Runnable() {
      @Override
      public void run() {
        Set<String> contextProperties = getServerConfigurationFactory().getSystemConfiguration()
            .getAllPropertiesWithPrefix(Property.VFS_CONTEXT_CLASSPATH_PROPERTY).keySet();
        Set<String> configuredContexts = new HashSet<>();
        for (String prop : contextProperties) {
          configuredContexts.add(prop.substring(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.name().length()));
        }

        try {
          AccumuloVFSClassLoader.getContextManager().removeUnusedContexts(configuredContexts);
        } catch (IOException e) {
          log.warn("{}", e.getMessage(), e);
        }
      }
    };

    AccumuloConfiguration aconf = getConfiguration();
    SimpleTimer.getInstance(aconf).schedule(contextCleaner, 60000, 60000);

    FileSystemMonitor.start(aconf, Property.TSERV_MONITOR_FS);

    Runnable gcDebugTask = new Runnable() {
      @Override
      public void run() {
        gcLogger.logGCInfo(getConfiguration());
      }
    };

    SimpleTimer.getInstance(aconf).schedule(gcDebugTask, 0, TIME_BETWEEN_GC_CHECKS);

    Runnable constraintTask = new Runnable() {

      @Override
      public void run() {
        ArrayList<Tablet> tablets;

        synchronized (onlineTablets) {
          tablets = new ArrayList<>(onlineTablets.values());
        }

        for (Tablet tablet : tablets) {
          tablet.checkConstraints();
        }
      }
    };

    SimpleTimer.getInstance(aconf).schedule(constraintTask, 0, 1000);
  }

  public TabletServerStatus getStats(Map<Table.ID,MapCounter<ScanRunState>> scanCounts) {
    long start = System.currentTimeMillis();
    TabletServerStatus result = new TabletServerStatus();

    Map<KeyExtent,Tablet> onlineTabletsCopy;
    synchronized (this.onlineTablets) {
      onlineTabletsCopy = new HashMap<>(this.onlineTablets);
    }
    Map<String,TableInfo> tables = new HashMap<>();

    for (Entry<KeyExtent,Tablet> entry : onlineTabletsCopy.entrySet()) {
      String tableId = entry.getKey().getTableId().canonicalID();
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
      if (tablet.isMinorCompactionRunning())
        table.minors.running++;
      if (tablet.isMinorCompactionQueued())
        table.minors.queued++;
      if (tablet.isMajorCompactionRunning())
        table.majors.running++;
      if (tablet.isMajorCompactionQueued())
        table.majors.queued++;
    }

    for (Entry<Table.ID,MapCounter<ScanRunState>> entry : scanCounts.entrySet()) {
      TableInfo table = tables.get(entry.getKey().canonicalID());
      if (table == null) {
        table = new TableInfo();
        tables.put(entry.getKey().canonicalID(), table);
      }

      if (table.scans == null)
        table.scans = new Compacting();

      table.scans.queued += entry.getValue().get(ScanRunState.QUEUED);
      table.scans.running += entry.getValue().get(ScanRunState.RUNNING);
    }

    ArrayList<KeyExtent> offlineTabletsCopy = new ArrayList<>();
    synchronized (this.unopenedTablets) {
      synchronized (this.openingTablets) {
        offlineTabletsCopy.addAll(this.unopenedTablets);
        offlineTabletsCopy.addAll(this.openingTablets);
      }
    }

    for (KeyExtent extent : offlineTabletsCopy) {
      String tableId = extent.getTableId().canonicalID();
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
    result.indexCacheHits = resourceManager.getIndexCache().getStats().hitCount();
    result.indexCacheRequest = resourceManager.getIndexCache().getStats().requestCount();
    result.dataCacheHits = resourceManager.getDataCache().getStats().hitCount();
    result.dataCacheRequest = resourceManager.getDataCache().getStats().requestCount();
    result.logSorts = logSorter.getLogSorts();
    result.flushs = flushCounter.get();
    result.syncs = syncCounter.get();
    result.bulkImports = new ArrayList<>();
    result.bulkImports.addAll(clientHandler.getBulkLoadStatus());
    result.bulkImports.addAll(bulkImportStatus.getBulkLoadStatus());
    result.version = getVersion();
    result.responseTime = System.currentTimeMillis() - start;
    return result;
  }

  public static void main(String[] args) throws IOException {
    try {
      final String app = "tserver";
      ServerOpts opts = new ServerOpts();
      opts.parseArgs(app, args);
      SecurityUtil.serverLogin(SiteConfiguration.getInstance());
      String hostname = opts.getAddress();
      Instance instance = HdfsZooInstance.getInstance();
      ServerConfigurationFactory conf = new ServerConfigurationFactory(instance);
      VolumeManager fs = VolumeManagerImpl.get();
      MetricsSystemHelper.configure(TabletServer.class.getSimpleName());
      Accumulo.init(fs, instance, conf, app);
      final TabletServer server = new TabletServer(instance, conf, fs);
      server.config(hostname);
      DistributedTrace.enable(hostname, app, conf.getSystemConfiguration());
      if (UserGroupInformation.isSecurityEnabled()) {
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        loginUser.doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() {
            server.run();
            return null;
          }
        });
      } else {
        server.run();
      }
    } catch (Exception ex) {
      log.error("Uncaught exception in TabletServer.main, exiting", ex);
      System.exit(1);
    } finally {
      DistributedTrace.disable();
    }
  }

  private Durability getMincEventDurability(KeyExtent extent) {
    TableConfiguration conf;
    if (extent.isMeta()) {
      conf = confFactory.getTableConfiguration(RootTable.ID);
    } else {
      conf = confFactory.getTableConfiguration(MetadataTable.ID);
    }
    Durability durability = DurabilityImpl.fromString(conf.get(Property.TABLE_DURABILITY));
    return durability;
  }

  public void minorCompactionFinished(CommitSession tablet, String newDatafile, int walogSeq) throws IOException {
    Durability durability = getMincEventDurability(tablet.getExtent());
    totalMinorCompactions.incrementAndGet();
    logger.minorCompactionFinished(tablet, newDatafile, walogSeq, durability);
    markUnusedWALs();
  }

  public void minorCompactionStarted(CommitSession tablet, int lastUpdateSequence, String newMapfileLocation) throws IOException {
    Durability durability = getMincEventDurability(tablet.getExtent());
    logger.minorCompactionStarted(tablet, lastUpdateSequence, newMapfileLocation, durability);
  }

  public void recover(VolumeManager fs, KeyExtent extent, TableConfiguration tconf, List<LogEntry> logEntries, Set<String> tabletFiles,
      MutationReceiver mutationReceiver) throws IOException {
    List<Path> recoveryLogs = new ArrayList<>();
    List<LogEntry> sorted = new ArrayList<>(logEntries);
    Collections.sort(sorted, new Comparator<LogEntry>() {
      @Override
      public int compare(LogEntry e1, LogEntry e2) {
        return (int) (e1.timestamp - e2.timestamp);
      }
    });
    for (LogEntry entry : sorted) {
      Path recovery = null;
      Path finished = RecoveryPath.getRecoveryPath(fs, fs.getFullPath(FileType.WAL, entry.filename));
      finished = SortedLogState.getFinishedMarkerPath(finished);
      TabletServer.log.info("Looking for " + finished);
      if (fs.exists(finished)) {
        recovery = finished.getParent();
      }
      if (recovery == null)
        throw new IOException("Unable to find recovery files for extent " + extent + " logEntry: " + entry);
      recoveryLogs.add(recovery);
    }
    logger.recover(fs, extent, tconf, recoveryLogs, tabletFiles, mutationReceiver);
  }

  public int createLogId(KeyExtent tablet) {
    AccumuloConfiguration acuTableConf = getTableConfiguration(tablet);
    if (DurabilityImpl.fromString(acuTableConf.get(Property.TABLE_DURABILITY)) != Durability.NONE) {
      return logIdGenerator.incrementAndGet();
    }
    return -1;
  }

  public TableConfiguration getTableConfiguration(KeyExtent extent) {
    return confFactory.getTableConfiguration(extent.getTableId());
  }

  public DfsLogger.ServerResources getServerConfig() {
    return new DfsLogger.ServerResources() {

      @Override
      public VolumeManager getFileSystem() {
        return fs;
      }

      @Override
      public AccumuloConfiguration getConfiguration() {
        return TabletServer.this.getConfiguration();
      }
    };
  }

  public Collection<Tablet> getOnlineTablets() {
    synchronized (onlineTablets) {
      return new ArrayList<>(onlineTablets.values());
    }
  }

  public VolumeManager getFileSystem() {
    return fs;
  }

  public int getOpeningCount() {
    return openingTablets.size();
  }

  public int getUnopenedCount() {
    return unopenedTablets.size();
  }

  public long getTotalMinorCompactions() {
    return totalMinorCompactions.get();
  }

  public double getHoldTimeMillis() {
    return resourceManager.holdTime();
  }

  public SecurityOperation getSecurityOperation() {
    return security;
  }

  // avoid unnecessary redundant markings to meta
  final ConcurrentHashMap<DfsLogger,EnumSet<TabletLevel>> metadataTableLogs = new ConcurrentHashMap<>();
  final Object levelLocks[] = new Object[TabletLevel.values().length];

  {
    for (int i = 0; i < levelLocks.length; i++) {
      levelLocks[i] = new Object();
    }
  }

  // remove any meta entries after a rolled log is no longer referenced
  Set<DfsLogger> closedLogs = new HashSet<>();

  private void markUnusedWALs() {
    Set<DfsLogger> candidates;
    synchronized (closedLogs) {
      candidates = new HashSet<>(closedLogs);
    }
    for (Tablet tablet : getOnlineTablets()) {
      candidates.removeAll(tablet.getCurrentLogFiles());
    }
    try {
      TServerInstance session = this.getTabletSession();
      for (DfsLogger candidate : candidates) {
        log.info("Marking " + candidate.getPath() + " as unreferenced");
        walMarker.walUnreferenced(session, candidate.getPath());
      }
      synchronized (closedLogs) {
        closedLogs.removeAll(candidates);
      }
    } catch (WalMarkerException ex) {
      log.info(ex.toString(), ex);
    }
  }

  public void addNewLogMarker(DfsLogger copy) throws WalMarkerException {
    log.info("Writing log marker for " + copy.getPath());
    walMarker.addNewWalMarker(getTabletSession(), copy.getPath());
  }

  public void walogClosed(DfsLogger currentLog) throws WalMarkerException {
    metadataTableLogs.remove(currentLog);
    synchronized (closedLogs) {
      closedLogs.add(currentLog);
    }
    log.info("Marking " + currentLog.getPath() + " as closed");
    walMarker.closeWal(getTabletSession(), currentLog.getPath());
  }

  public void updateBulkImportState(List<String> files, BulkImportState state) {
    bulkImportStatus.updateBulkImportStatus(files, state);
  }

  public void removeBulkImportState(List<String> files) {
    bulkImportStatus.removeBulkImportStatus(files);
  }

  private static final String MAJC_READ_LIMITER_KEY = "tserv_majc_read";
  private static final String MAJC_WRITE_LIMITER_KEY = "tserv_majc_write";
  private final SharedRateLimiterFactory.RateProvider rateProvider = new SharedRateLimiterFactory.RateProvider() {
    @Override
    public long getDesiredRate() {
      return getConfiguration().getAsBytes(Property.TSERV_MAJC_THROUGHPUT);
    }
  };

  /**
   * Get the {@link RateLimiter} for reads during major compactions on this tserver. All writes performed during major compactions are throttled to conform to
   * this RateLimiter.
   */
  public final RateLimiter getMajorCompactionReadLimiter() {
    return SharedRateLimiterFactory.getInstance().create(MAJC_READ_LIMITER_KEY, rateProvider);
  }

  /**
   * Get the RateLimiter for writes during major compations on this tserver. All reads performed during major compactions are throttled to conform to this
   * RateLimiter.
   */
  public final RateLimiter getMajorCompactionWriteLimiter() {
    return SharedRateLimiterFactory.getInstance().create(MAJC_WRITE_LIMITER_KEY, rateProvider);
  }
}

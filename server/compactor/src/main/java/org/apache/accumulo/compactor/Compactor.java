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
package org.apache.accumulo.compactor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.core.util.UtilWaitThread.sleepUninterruptibly;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService;
import org.apache.accumulo.core.compaction.thrift.CompactionCoordinatorService.Client;
import org.apache.accumulo.core.compaction.thrift.CompactorService;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.compaction.thrift.UnknownCompactionIdException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockLossReason;
import org.apache.accumulo.core.fate.zookeeper.ServiceLock.LockWatcher;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompaction;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.trace.thrift.TInfo;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.ServerServices;
import org.apache.accumulo.core.util.ServerServices.Service;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.GarbageCollectionLogger;
import org.apache.accumulo.server.ServerOpts;
import org.apache.accumulo.server.compaction.CompactionInfo;
import org.apache.accumulo.server.compaction.CompactionWatcher;
import org.apache.accumulo.server.compaction.FileCompactor;
import org.apache.accumulo.server.compaction.RetryableThriftCall;
import org.apache.accumulo.server.compaction.RetryableThriftCall.RetriesExceededException;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;

import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;

public class Compactor extends AbstractServer implements MetricsProducer, CompactorService.Iface {

  private static final SecureRandom random = new SecureRandom();

  public static class CompactorServerOpts extends ServerOpts {
    @Parameter(required = true, names = {"-q", "--queue"}, description = "compaction queue name")
    private String queueName = null;

    public String getQueueName() {
      return queueName;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(Compactor.class);
  private static final long TIME_BETWEEN_GC_CHECKS = 5000;
  private static final long TIME_BETWEEN_CANCEL_CHECKS = MINUTES.toMillis(5);

  private static final long TEN_MEGABYTES = 10485760;

  protected static final CompactionJobHolder JOB_HOLDER = new CompactionJobHolder();

  private final GarbageCollectionLogger gcLogger = new GarbageCollectionLogger();
  private final UUID compactorId = UUID.randomUUID();
  private final AccumuloConfiguration aconf;
  private final String queueName;
  protected final AtomicReference<ExternalCompactionId> currentCompactionId =
      new AtomicReference<>();
  private final CompactionWatcher watcher;

  private SecurityOperation security;
  private ServiceLock compactorLock;
  private ServerAddress compactorAddress = null;

  // Exposed for tests
  protected volatile boolean shutdown = false;

  private final AtomicBoolean compactionRunning = new AtomicBoolean(false);

  protected Compactor(CompactorServerOpts opts, String[] args) {
    this(opts, args, null);
  }

  protected Compactor(CompactorServerOpts opts, String[] args, AccumuloConfiguration conf) {
    super("compactor", opts, args);
    queueName = opts.getQueueName();
    aconf = conf == null ? super.getConfiguration() : conf;
    setupSecurity();
    watcher = new CompactionWatcher(aconf);
    var schedExecutor =
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(aconf);
    startGCLogger(schedExecutor);
    startCancelChecker(schedExecutor, TIME_BETWEEN_CANCEL_CHECKS);
    printStartupMsg();
  }

  @Override
  public AccumuloConfiguration getConfiguration() {
    return aconf;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    LongTaskTimer timer = LongTaskTimer.builder(METRICS_COMPACTOR_MAJC_STUCK)
        .description("Number and duration of stuck major compactions").register(registry);
    CompactionWatcher.setTimer(timer);
  }

  protected void setupSecurity() {
    security = getContext().getSecurityOperation();
  }

  protected void startGCLogger(ScheduledThreadPoolExecutor schedExecutor) {
    ScheduledFuture<?> future =
        schedExecutor.scheduleWithFixedDelay(() -> gcLogger.logGCInfo(getConfiguration()), 0,
            TIME_BETWEEN_GC_CHECKS, TimeUnit.MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  protected void startCancelChecker(ScheduledThreadPoolExecutor schedExecutor,
      long timeBetweenChecks) {
    ThreadPools.watchCriticalScheduledTask(schedExecutor.scheduleWithFixedDelay(
        this::checkIfCanceled, 0, timeBetweenChecks, TimeUnit.MILLISECONDS));
  }

  protected void checkIfCanceled() {
    TExternalCompactionJob job = JOB_HOLDER.getJob();
    if (job != null) {
      try {
        var extent = KeyExtent.fromThrift(job.getExtent());
        var ecid = ExternalCompactionId.of(job.getExternalCompactionId());

        TabletMetadata tabletMeta =
            getContext().getAmple().readTablet(extent, ColumnType.ECOMP, ColumnType.PREV_ROW);
        if (tabletMeta == null || !tabletMeta.getExternalCompactions().containsKey(ecid)) {
          // table was deleted OR tablet was split or merged OR tablet no longer thinks compaction
          // is running for some reason
          LOG.info("Cancelling compaction {} that no longer has a metadata entry at {}", ecid,
              extent);
          JOB_HOLDER.cancel(job.getExternalCompactionId());
          return;
        }

        if (job.getKind() == TCompactionKind.USER) {
          String zTablePath = Constants.ZROOT + "/" + getContext().getInstanceID()
              + Constants.ZTABLES + "/" + extent.tableId() + Constants.ZTABLE_COMPACT_CANCEL_ID;
          byte[] id = getContext().getZooCache().get(zTablePath);
          if (id == null) {
            // table probably deleted
            LOG.info("Cancelling compaction {} for table that no longer exists {}", ecid, extent);
            JOB_HOLDER.cancel(job.getExternalCompactionId());
          } else {
            var cancelId = Long.parseLong(new String(id, UTF_8));

            if (cancelId >= job.getUserCompactionId()) {
              LOG.info("Cancelling compaction {} because user compaction was canceled", ecid);
              JOB_HOLDER.cancel(job.getExternalCompactionId());
            }
          }
        }
      } catch (RuntimeException e) {
        LOG.warn("Failed to check if compaction {} for {} was canceled.",
            job.getExternalCompactionId(), KeyExtent.fromThrift(job.getExtent()), e);
      }
    }
  }

  protected void printStartupMsg() {
    LOG.info("Version " + Constants.VERSION);
    LOG.info("Instance " + getContext().getInstanceID());
  }

  /**
   * Set up nodes and locks in ZooKeeper for this Compactor
   *
   * @param clientAddress address of this Compactor
   * @throws KeeperException zookeeper error
   * @throws InterruptedException thread interrupted
   */
  protected void announceExistence(HostAndPort clientAddress)
      throws KeeperException, InterruptedException {

    String hostPort = ExternalCompactionUtil.getHostPortString(clientAddress);

    ZooReaderWriter zoo = getContext().getZooReaderWriter();
    String compactorQueuePath =
        getContext().getZooKeeperRoot() + Constants.ZCOMPACTORS + "/" + this.queueName;
    String zPath = compactorQueuePath + "/" + hostPort;

    try {
      zoo.mkdirs(compactorQueuePath);
      zoo.putPersistentData(zPath, new byte[] {}, NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NOAUTH) {
        LOG.error("Failed to write to ZooKeeper. Ensure that"
            + " accumulo.properties, specifically instance.secret, is consistent.");
      }
      throw e;
    }

    compactorLock = new ServiceLock(getContext().getZooReaderWriter().getZooKeeper(),
        ServiceLock.path(zPath), compactorId);
    LockWatcher lw = new LockWatcher() {
      @Override
      public void lostLock(final LockLossReason reason) {
        Halt.halt(1, () -> {
          LOG.error("Compactor lost lock (reason = {}), exiting.", reason);
          gcLogger.logGCInfo(getConfiguration());
        });
      }

      @Override
      public void unableToMonitorLockNode(final Exception e) {
        Halt.halt(1, () -> LOG.error("Lost ability to monitor Compactor lock, exiting.", e));
      }
    };

    try {
      byte[] lockContent =
          new ServerServices(hostPort, Service.COMPACTOR_CLIENT).toString().getBytes(UTF_8);
      for (int i = 0; i < 25; i++) {
        zoo.putPersistentData(zPath, new byte[0], NodeExistsPolicy.SKIP);

        if (compactorLock.tryLock(lw, lockContent)) {
          LOG.debug("Obtained Compactor lock {}", compactorLock.getLockPath());
          return;
        }
        LOG.info("Waiting for Compactor lock");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      String msg = "Too many retries, exiting.";
      LOG.info(msg);
      throw new RuntimeException(msg);
    } catch (Exception e) {
      LOG.info("Could not obtain tablet server lock, exiting.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Start this Compactors thrift service to handle incoming client requests
   *
   * @return address of this compactor client service
   * @throws UnknownHostException host unknown
   */
  protected ServerAddress startCompactorClientService() throws UnknownHostException {
    var processor = ThriftProcessorTypes.getCompactorTProcessor(this, getContext());
    Property maxMessageSizeProperty =
        (getConfiguration().get(Property.COMPACTOR_MAX_MESSAGE_SIZE) != null
            ? Property.COMPACTOR_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getContext(), getHostname(),
        Property.COMPACTOR_CLIENTPORT, processor, this.getClass().getSimpleName(),
        "Thrift Client Server", Property.COMPACTOR_PORTSEARCH, Property.COMPACTOR_MINTHREADS,
        Property.COMPACTOR_MINTHREADS_TIMEOUT, Property.COMPACTOR_THREADCHECK,
        maxMessageSizeProperty);
    LOG.info("address = {}", sp.address);
    return sp;
  }

  /**
   * Cancel the compaction with this id.
   *
   * @param externalCompactionId compaction id
   * @throws UnknownCompactionIdException if the externalCompactionId does not match the currently
   *         executing compaction
   * @throws TException thrift error
   */
  private void cancel(String externalCompactionId) throws TException {
    if (JOB_HOLDER.cancel(externalCompactionId)) {
      LOG.info("Cancel requested for compaction job {}", externalCompactionId);
    } else {
      throw new UnknownCompactionIdException();
    }
  }

  @Override
  public void cancel(TInfo tinfo, TCredentials credentials, String externalCompactionId)
      throws TException {
    TableId tableId = JOB_HOLDER.getTableId();
    try {
      NamespaceId nsId = getContext().getNamespaceId(tableId);
      if (!security.canCompact(credentials, tableId, nsId)) {
        throw new AccumuloSecurityException(credentials.getPrincipal(),
            SecurityErrorCode.PERMISSION_DENIED).asThriftException();
      }
    } catch (TableNotFoundException e) {
      throw new ThriftTableOperationException(tableId.canonical(), null,
          TableOperation.COMPACT_CANCEL, TableOperationExceptionType.NOTFOUND, e.getMessage());
    }

    cancel(externalCompactionId);
  }

  /**
   * Send an update to the CompactionCoordinator for this job
   *
   * @param job compactionJob
   * @param update status update
   * @throws RetriesExceededException thrown when retries have been exceeded
   */
  protected void updateCompactionState(TExternalCompactionJob job, TCompactionStatusUpdate update)
      throws RetriesExceededException {
    RetryableThriftCall<String> thriftCall =
        new RetryableThriftCall<>(1000, RetryableThriftCall.MAX_WAIT_TIME, 25, () -> {
          Client coordinatorClient = getCoordinatorClient();
          try {
            coordinatorClient.updateCompactionStatus(TraceUtil.traceInfo(), getContext().rpcCreds(),
                job.getExternalCompactionId(), update, System.currentTimeMillis());
            return "";
          } finally {
            ThriftUtil.returnClient(coordinatorClient, getContext());
          }
        });
    thriftCall.run();
  }

  /**
   * Notify the CompactionCoordinator the job failed
   *
   * @param job current compaction job
   * @throws RetriesExceededException thrown when retries have been exceeded
   */
  protected void updateCompactionFailed(TExternalCompactionJob job)
      throws RetriesExceededException {
    RetryableThriftCall<String> thriftCall =
        new RetryableThriftCall<>(1000, RetryableThriftCall.MAX_WAIT_TIME, 25, () -> {
          Client coordinatorClient = getCoordinatorClient();
          try {
            coordinatorClient.compactionFailed(TraceUtil.traceInfo(), getContext().rpcCreds(),
                job.getExternalCompactionId(), job.extent);
            return "";
          } finally {
            ThriftUtil.returnClient(coordinatorClient, getContext());
          }
        });
    thriftCall.run();
  }

  /**
   * Update the CompactionCoordinator with the stats from the completed job
   *
   * @param job current compaction job
   * @param stats compaction stats
   * @throws RetriesExceededException thrown when retries have been exceeded
   */
  protected void updateCompactionCompleted(TExternalCompactionJob job, TCompactionStats stats)
      throws RetriesExceededException {
    RetryableThriftCall<String> thriftCall =
        new RetryableThriftCall<>(1000, RetryableThriftCall.MAX_WAIT_TIME, 25, () -> {
          Client coordinatorClient = getCoordinatorClient();
          try {
            coordinatorClient.compactionCompleted(TraceUtil.traceInfo(), getContext().rpcCreds(),
                job.getExternalCompactionId(), job.extent, stats);
            return "";
          } finally {
            ThriftUtil.returnClient(coordinatorClient, getContext());
          }
        });
    thriftCall.run();
  }

  /**
   * Get the next job to run
   *
   * @param uuid uuid supplier
   * @return CompactionJob
   * @throws RetriesExceededException thrown when retries have been exceeded
   */
  protected TExternalCompactionJob getNextJob(Supplier<UUID> uuid) throws RetriesExceededException {
    RetryableThriftCall<TExternalCompactionJob> nextJobThriftCall =
        new RetryableThriftCall<>(1000, RetryableThriftCall.MAX_WAIT_TIME, 0, () -> {
          Client coordinatorClient = getCoordinatorClient();
          try {
            ExternalCompactionId eci = ExternalCompactionId.generate(uuid.get());
            LOG.trace("Attempting to get next job, eci = {}", eci);
            currentCompactionId.set(eci);
            return coordinatorClient.getCompactionJob(TraceUtil.traceInfo(),
                getContext().rpcCreds(), queueName,
                ExternalCompactionUtil.getHostPortString(compactorAddress.getAddress()),
                eci.toString());
          } catch (Exception e) {
            currentCompactionId.set(null);
            throw e;
          } finally {
            ThriftUtil.returnClient(coordinatorClient, getContext());
          }
        });
    return nextJobThriftCall.run();
  }

  /**
   * Get the client to the CompactionCoordinator
   *
   * @return compaction coordinator client
   * @throws TTransportException when unable to get client
   */
  protected CompactionCoordinatorService.Client getCoordinatorClient() throws TTransportException {
    var coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(getContext());
    if (coordinatorHost.isEmpty()) {
      throw new TTransportException("Unable to get CompactionCoordinator address from ZooKeeper");
    }
    LOG.trace("CompactionCoordinator address is: {}", coordinatorHost.get());
    return ThriftUtil.getClient(ThriftClientTypes.COORDINATOR, coordinatorHost.get(), getContext());
  }

  /**
   * Create compaction runnable
   *
   * @param job compaction job
   * @param totalInputEntries object to capture total entries
   * @param totalInputBytes object to capture input file size
   * @param started started latch
   * @param stopped stopped latch
   * @param err reference to error
   * @return Runnable compaction job
   */
  protected Runnable createCompactionJob(final TExternalCompactionJob job,
      final LongAdder totalInputEntries, final LongAdder totalInputBytes,
      final CountDownLatch started, final CountDownLatch stopped,
      final AtomicReference<Throwable> err) {

    return () -> {
      // Its only expected that a single compaction runs at a time. Multiple compactions running
      // at a time could cause odd behavior like out of order and unexpected thrift calls to the
      // coordinator. This is a sanity check to ensure the expectation is met. Should this check
      // ever fail, it means there is a bug elsewhere.
      Preconditions.checkState(compactionRunning.compareAndSet(false, true));
      try {
        LOG.info("Starting up compaction runnable for job: {}", job);
        TCompactionStatusUpdate update =
            new TCompactionStatusUpdate(TCompactionState.STARTED, "Compaction started", -1, -1, -1);
        updateCompactionState(job, update);
        var extent = KeyExtent.fromThrift(job.getExtent());
        final AccumuloConfiguration aConfig;
        final TableConfiguration tConfig = getContext().getTableConfiguration(extent.tableId());

        if (!job.getOverrides().isEmpty()) {
          aConfig = new ConfigurationCopy(tConfig);
          job.getOverrides().forEach(((ConfigurationCopy) aConfig)::set);
          LOG.debug("Overriding table properties with {}", job.getOverrides());
        } else {
          aConfig = tConfig;
        }

        final TabletFile outputFile = new TabletFile(new Path(job.getOutputFile()));

        final Map<StoredTabletFile,DataFileValue> files = new TreeMap<>();
        job.getFiles().forEach(f -> {
          files.put(new StoredTabletFile(f.getMetadataFileEntry()),
              new DataFileValue(f.getSize(), f.getEntries(), f.getTimestamp()));
          totalInputEntries.add(f.getEntries());
          totalInputBytes.add(f.getSize());
        });

        final List<IteratorSetting> iters = new ArrayList<>();
        job.getIteratorSettings().getIterators()
            .forEach(tis -> iters.add(SystemIteratorUtil.toIteratorSetting(tis)));

        ExtCEnv cenv = new ExtCEnv(JOB_HOLDER, queueName);
        FileCompactor compactor = new FileCompactor(getContext(), extent, files, outputFile,
            job.isPropagateDeletes(), cenv, iters, aConfig, tConfig.getCryptoService());

        LOG.trace("Starting compactor");
        started.countDown();

        org.apache.accumulo.server.compaction.CompactionStats stat = compactor.call();
        TCompactionStats cs = new TCompactionStats();
        cs.setEntriesRead(stat.getEntriesRead());
        cs.setEntriesWritten(stat.getEntriesWritten());
        cs.setFileSize(stat.getFileSize());
        JOB_HOLDER.setStats(cs);

        LOG.info("Compaction completed successfully {} ", job.getExternalCompactionId());
        // Update state when completed
        TCompactionStatusUpdate update2 = new TCompactionStatusUpdate(TCompactionState.SUCCEEDED,
            "Compaction completed successfully", -1, -1, -1);
        updateCompactionState(job, update2);
      } catch (FileCompactor.CompactionCanceledException cce) {
        LOG.debug("Compaction canceled {}", job.getExternalCompactionId());
      } catch (Exception e) {
        LOG.error("Compaction failed", e);
        err.set(e);
      } finally {
        stopped.countDown();
        Preconditions.checkState(compactionRunning.compareAndSet(true, false));
      }
    };
  }

  /**
   * Returns the number of seconds to wait in between progress checks based on input file sizes
   *
   * @param numBytes number of bytes in input file
   * @return number of seconds to wait between progress checks
   */
  static long calculateProgressCheckTime(long numBytes) {
    return Math.max(1, (numBytes / TEN_MEGABYTES));
  }

  protected Supplier<UUID> getNextId() {
    return UUID::randomUUID;
  }

  protected long getWaitTimeBetweenCompactionChecks() {
    // get the total number of compactors assigned to this queue
    int numCompactors = ExternalCompactionUtil.countCompactors(queueName, getContext());
    // Aim for around 3 compactors checking in every second
    long sleepTime = numCompactors * 1000L / 3;
    // Ensure a compactor sleeps at least around a second
    sleepTime = Math.max(1000, sleepTime);
    // Ensure a compactor sleep not too much more than 5 mins
    sleepTime = Math.min(300_000L, sleepTime);
    // Add some random jitter to the sleep time, that averages out to sleep time. This will spread
    // compactors out evenly over time.
    sleepTime = (long) (.9 * sleepTime + sleepTime * .2 * random.nextDouble());
    LOG.trace("Sleeping {}ms based on {} compactors", sleepTime, numCompactors);
    return sleepTime;
  }

  @Override
  public void run() {

    try {
      compactorAddress = startCompactorClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the compactor client service", e1);
    }
    final HostAndPort clientAddress = compactorAddress.getAddress();

    try {
      announceExistence(clientAddress);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Error registering compactor in ZooKeeper", e);
    }

    try {
      MetricsUtil.initializeMetrics(getContext().getConfiguration(), this.applicationName,
          clientAddress);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
        | SecurityException e1) {
      LOG.error("Error initializing metrics, metrics will not be emitted.", e1);
    }
    MetricsUtil.initializeProducers(this);

    LOG.info("Compactor started, waiting for work");
    try {

      final AtomicReference<Throwable> err = new AtomicReference<>();

      while (!shutdown) {
        currentCompactionId.set(null);
        err.set(null);
        JOB_HOLDER.reset();

        TExternalCompactionJob job;
        try {
          job = getNextJob(getNextId());
          if (!job.isSetExternalCompactionId()) {
            LOG.trace("No external compactions in queue {}", this.queueName);
            UtilWaitThread.sleep(getWaitTimeBetweenCompactionChecks());
            continue;
          }
          if (!job.getExternalCompactionId().equals(currentCompactionId.get().toString())) {
            throw new IllegalStateException("Returned eci " + job.getExternalCompactionId()
                + " does not match supplied eci " + currentCompactionId.get());
          }
        } catch (RetriesExceededException e2) {
          LOG.warn("Retries exceeded getting next job. Retrying...");
          continue;
        }
        LOG.debug("Received next compaction job: {}", job);

        final LongAdder totalInputEntries = new LongAdder();
        final LongAdder totalInputBytes = new LongAdder();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch stopped = new CountDownLatch(1);

        final Thread compactionThread = Threads.createThread(
            "Compaction job for tablet " + job.getExtent().toString(),
            createCompactionJob(job, totalInputEntries, totalInputBytes, started, stopped, err));

        JOB_HOLDER.set(job, compactionThread);

        try {
          compactionThread.start(); // start the compactionThread
          started.await(); // wait until the compactor is started
          final long inputEntries = totalInputEntries.sum();
          final long waitTime = calculateProgressCheckTime(totalInputBytes.sum());
          LOG.debug("Progress checks will occur every {} seconds", waitTime);
          String percentComplete = "unknown";

          while (!stopped.await(waitTime, TimeUnit.SECONDS)) {
            List<CompactionInfo> running =
                org.apache.accumulo.server.compaction.FileCompactor.getRunningCompactions();
            if (!running.isEmpty()) {
              // Compaction has started. There should only be one in the list
              CompactionInfo info = running.get(0);
              if (info != null) {
                if (inputEntries > 0) {
                  percentComplete =
                      Float.toString((info.getEntriesRead() / (float) inputEntries) * 100);
                }
                String message = String.format(
                    "Compaction in progress, read %d of %d input entries ( %s %s ), written %d entries",
                    info.getEntriesRead(), inputEntries, percentComplete, "%",
                    info.getEntriesWritten());
                watcher.run();
                try {
                  LOG.debug("Updating coordinator with compaction progress: {}.", message);
                  TCompactionStatusUpdate update =
                      new TCompactionStatusUpdate(TCompactionState.IN_PROGRESS, message,
                          inputEntries, info.getEntriesRead(), info.getEntriesWritten());
                  updateCompactionState(job, update);
                } catch (RetriesExceededException e) {
                  LOG.warn("Error updating coordinator with compaction progress, error: {}",
                      e.getMessage());
                }
              }
            } else {
              LOG.error("Waiting on compaction thread to finish, but no RUNNING compaction");
            }
          }
          compactionThread.join();
          LOG.trace("Compaction thread finished.");
          // Run the watcher again to clear out the finished compaction and set the
          // stuck count to zero.
          watcher.run();

          if (err.get() != null) {
            // maybe the error occured because the table was deleted or something like that, so
            // force a cancel check to possibly reduce noise in the logs
            checkIfCanceled();
          }

          if (compactionThread.isInterrupted() || JOB_HOLDER.isCancelled()
              || (err.get() != null && err.get().getClass().equals(InterruptedException.class))) {
            LOG.warn("Compaction thread was interrupted, sending CANCELLED state");
            try {
              TCompactionStatusUpdate update = new TCompactionStatusUpdate(
                  TCompactionState.CANCELLED, "Compaction cancelled", -1, -1, -1);
              updateCompactionState(job, update);
              updateCompactionFailed(job);
            } catch (RetriesExceededException e) {
              LOG.error("Error updating coordinator with compaction cancellation.", e);
            } finally {
              currentCompactionId.set(null);
            }
          } else if (err.get() != null) {
            try {
              LOG.info("Updating coordinator with compaction failure.");
              TCompactionStatusUpdate update = new TCompactionStatusUpdate(TCompactionState.FAILED,
                  "Compaction failed due to: " + err.get().getMessage(), -1, -1, -1);
              updateCompactionState(job, update);
              updateCompactionFailed(job);
            } catch (RetriesExceededException e) {
              LOG.error("Error updating coordinator with compaction failure.", e);
            } finally {
              currentCompactionId.set(null);
            }
          } else {
            try {
              LOG.trace("Updating coordinator with compaction completion.");
              updateCompactionCompleted(job, JOB_HOLDER.getStats());
            } catch (RetriesExceededException e) {
              LOG.error(
                  "Error updating coordinator with compaction completion, cancelling compaction.",
                  e);
              try {
                cancel(job.getExternalCompactionId());
              } catch (TException e1) {
                LOG.error("Error cancelling compaction.", e1);
              }
            } finally {
              currentCompactionId.set(null);
            }
          }
        } catch (RuntimeException e1) {
          LOG.error(
              "Compactor thread was interrupted waiting for compaction to start, cancelling job",
              e1);
          try {
            cancel(job.getExternalCompactionId());
          } catch (TException e2) {
            LOG.error("Error cancelling compaction.", e2);
          }
        } finally {
          currentCompactionId.set(null);
          // In the case where there is an error in the foreground code the background compaction
          // may still be running. Must cancel it before starting another iteration of the loop to
          // avoid multiple threads updating shared state.
          while (compactionThread.isAlive()) {
            compactionThread.interrupt();
            compactionThread.join(1000);
          }
        }

      }

    } catch (Exception e) {
      LOG.error("Unhandled error occurred in Compactor", e);
    } finally {
      // Shutdown local thrift server
      LOG.info("Stopping Thrift Servers");
      if (compactorAddress.server != null) {
        compactorAddress.server.stop();
      }

      try {
        LOG.debug("Closing filesystems");
        VolumeManager mgr = getContext().getVolumeManager();
        if (null != mgr) {
          mgr.close();
        }
      } catch (IOException e) {
        LOG.warn("Failed to close filesystem : {}", e.getMessage(), e);
      }

      gcLogger.logGCInfo(getConfiguration());
      LOG.info("stop requested. exiting ... ");
      try {
        if (null != compactorLock) {
          compactorLock.unlock();
        }
      } catch (Exception e) {
        LOG.warn("Failed to release compactor lock", e);
      }
    }

  }

  public static void main(String[] args) throws Exception {
    try (Compactor compactor = new Compactor(new CompactorServerOpts(), args)) {
      compactor.runServer();
    }
  }

  @Override
  public List<ActiveCompaction> getActiveCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    List<CompactionInfo> compactions =
        org.apache.accumulo.server.compaction.FileCompactor.getRunningCompactions();
    List<ActiveCompaction> ret = new ArrayList<>(compactions.size());

    for (CompactionInfo compactionInfo : compactions) {
      ret.add(compactionInfo.toThrift());
    }

    return ret;
  }

  /**
   * Called by a CompactionCoordinator to get the running compaction
   *
   * @param tinfo trace info
   * @param credentials caller credentials
   * @return current compaction job or empty compaction job is none running
   */
  @Override
  public TExternalCompactionJob getRunningCompaction(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    // Return what is currently running, does not wait for jobs in the process of reserving. This
    // method is called by a coordinator starting up to determine what is currently running on all
    // compactors.

    TExternalCompactionJob job = null;
    synchronized (JOB_HOLDER) {
      job = JOB_HOLDER.getJob();
    }

    if (null == job) {
      return new TExternalCompactionJob();
    } else {
      return job;
    }
  }

  @Override
  public String getRunningCompactionId(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    // Any returned id must cover the time period from before a job is reserved until after it
    // commits. This method is called to detect dead compactions and depends on this behavior.
    // For the purpose of detecting dead compactions its ok if ids are returned that never end up
    // being related to a running compaction.
    ExternalCompactionId eci = currentCompactionId.get();
    if (null == eci) {
      return "";
    } else {
      return eci.canonical();
    }
  }
}

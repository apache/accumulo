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
package org.apache.accumulo.tasks;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.accumulo.core.util.LazySingletons.RANDOM;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.lock.ServiceLock.LockLossReason;
import org.apache.accumulo.core.lock.ServiceLock.LockWatcher;
import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockData.ThriftService;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.ActiveCompactionList;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tasks.TaskMessage;
import org.apache.accumulo.core.tasks.TaskMessageType;
import org.apache.accumulo.core.tasks.compaction.ActiveCompactionTasks;
import org.apache.accumulo.core.tasks.compaction.CompactionTask;
import org.apache.accumulo.core.tasks.thrift.Task;
import org.apache.accumulo.core.tasks.thrift.TaskManager;
import org.apache.accumulo.core.tasks.thrift.TaskManager.Client;
import org.apache.accumulo.core.tasks.thrift.TaskRunnerInfo;
import org.apache.accumulo.core.tasks.thrift.WorkerType;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.compaction.CompactionInfo;
import org.apache.accumulo.server.compaction.CompactionWatcher;
import org.apache.accumulo.server.compaction.PausedCompactionMetrics;
import org.apache.accumulo.server.compaction.RetryableThriftCall;
import org.apache.accumulo.server.compaction.RetryableThriftCall.RetriesExceededException;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.rpc.TServerUtils;
import org.apache.accumulo.server.rpc.ThriftProcessorTypes;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.accumulo.tasks.jobs.CompactionJob;
import org.apache.accumulo.tasks.jobs.Job;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;

public class TaskRunner extends AbstractServer implements MetricsProducer,
    org.apache.accumulo.core.tasks.thrift.TaskRunner.Iface, TaskRunnerProcess {

  private static final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);
  private static final long TIME_BETWEEN_CANCEL_CHECKS = MINUTES.toMillis(5);
  private static final AtomicReference<Job<?>> CURRENTLY_EXECUTING_TASK = new AtomicReference<>();

  private final UUID taskRunnerId = UUID.randomUUID();
  private final AccumuloConfiguration aconf;
  protected final AtomicReference<ExternalCompactionId> currentTaskId = new AtomicReference<>();
  private final WorkerType workerType;

  private SecurityOperation security;
  private ServiceLock taskRunnerLock;
  private ServerAddress taskRunnerAddress = null;
  private PausedCompactionMetrics pausedMetrics;
  private CompactionWatcher watcher;

  // Exposed for tests
  protected volatile boolean shutdown = false;

  protected TaskRunner(ConfigOpts opts, String[] args) {
    this(opts, args, null);
  }

  protected TaskRunner(ConfigOpts opts, String[] args, AccumuloConfiguration conf) {
    super("TaskRunner", opts, args);
    String runnerType = getTaskWorkerTypePropertyValue();
    LOG.debug("Starting TaskRunner of type: {}", runnerType);
    System.out.println("TYPE: " + runnerType);
    workerType = WorkerType.valueOf(runnerType);
    aconf = conf == null ? super.getConfiguration() : conf;
    setupSecurity();
    switch (workerType) {
      case COMPACTION:
        watcher = new CompactionWatcher(aconf);
        var schedExecutor =
            ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(aconf);
        startCancelChecker(schedExecutor, TIME_BETWEEN_CANCEL_CHECKS);
        break;
      case LOG_SORTING:
        break;
      case SPLIT_POINT_CALCULATION:
        break;
      default:
        break;

    }
    printStartupMsg();
  }

  @Override
  protected String getResourceGroupPropertyValue(SiteConfiguration conf) {
    return conf.get(Property.TASK_RUNNER_GROUP_NAME);
  }

  protected String getTaskWorkerTypePropertyValue() {
    return getContext().getSiteConfiguration().get(Property.TASK_RUNNER_WORKER_TYPE);
  }

  @Override
  public AccumuloConfiguration getConfiguration() {
    return aconf;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    super.registerMetrics(registry);
    switch (workerType) {
      case COMPACTION:
        LongTaskTimer timer = LongTaskTimer.builder(METRICS_COMPACTOR_MAJC_STUCK)
            .description("Number and duration of stuck major compactions").register(registry);
        CompactionWatcher.setTimer(timer);
        break;
      case LOG_SORTING:
        break;
      case SPLIT_POINT_CALCULATION:
        break;
      default:
        break;

    }
  }

  protected void setupSecurity() {
    security = getContext().getSecurityOperation();
  }

  protected void startCancelChecker(ScheduledThreadPoolExecutor schedExecutor,
      long timeBetweenChecks) {
    ThreadPools.watchCriticalScheduledTask(
        schedExecutor.scheduleWithFixedDelay(() -> CompactionJob.checkIfCanceled(getContext()), 0,
            timeBetweenChecks, TimeUnit.MILLISECONDS));
  }

  public CompactionWatcher getCompactionWatcher() {
    return this.watcher;
  }

  public PausedCompactionMetrics getPausedCompactionMetrics() {
    return pausedMetrics;
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
        getContext().getZooKeeperRoot() + Constants.ZCOMPACTORS + "/" + this.getResourceGroup();
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

    taskRunnerLock = new ServiceLock(getContext().getZooReaderWriter().getZooKeeper(),
        ServiceLock.path(zPath), taskRunnerId);
    LockWatcher lw = new LockWatcher() {
      @Override
      public void lostLock(final LockLossReason reason) {
        Halt.halt(1, () -> {
          LOG.error("TaskRunner lost lock (reason = {}), exiting.", reason);
          getContext().getLowMemoryDetector().logGCInfo(getConfiguration());
        });
      }

      @Override
      public void unableToMonitorLockNode(final Exception e) {
        Halt.halt(1, () -> LOG.error("Lost ability to monitor TaskRunner lock, exiting.", e));
      }
    };

    try {
      for (int i = 0; i < 25; i++) {
        zoo.putPersistentData(zPath, new byte[0], NodeExistsPolicy.SKIP);

        if (taskRunnerLock.tryLock(lw, new ServiceLockData(taskRunnerId, hostPort,
            ThriftService.TASK_RUNNER, this.getResourceGroup()))) {
          LOG.debug("Obtained Compactor lock {}", taskRunnerLock.getLockPath());
          return;
        }
        LOG.info("Waiting for TaskRunner lock");
        sleepUninterruptibly(5, TimeUnit.SECONDS);
      }
      String msg = "Too many retries, exiting.";
      LOG.info(msg);
      throw new RuntimeException(msg);
    } catch (Exception e) {
      LOG.info("Could not obtain TaskRunner server lock, exiting.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Start this servers thrift service to handle incoming client requests
   *
   * @return address of this compactor client service
   * @throws UnknownHostException host unknown
   */
  protected ServerAddress startThriftClientService() throws UnknownHostException {
    var processor = ThriftProcessorTypes.getCompactorTProcessor(this, getContext());
    Property maxMessageSizeProperty =
        (getConfiguration().get(Property.TASK_RUNNER_MAX_MESSAGE_SIZE) != null
            ? Property.TASK_RUNNER_MAX_MESSAGE_SIZE : Property.GENERAL_MAX_MESSAGE_SIZE);
    ServerAddress sp = TServerUtils.startServer(getContext(), getHostname(),
        Property.TASK_RUNNER_CLIENTPORT, processor, this.getClass().getSimpleName(),
        "Thrift Client Server", Property.TASK_RUNNER_PORTSEARCH, Property.TASK_RUNNER_MINTHREADS,
        Property.TASK_RUNNER_MINTHREADS_TIMEOUT, Property.TASK_RUNNER_THREADCHECK,
        maxMessageSizeProperty);
    LOG.info("address = {}", sp.address);
    return sp;
  }

  @Override
  public void cancelTask(TInfo tinfo, TCredentials credentials, String externalCompactionId)
      throws TException {
    Job<?> job = CURRENTLY_EXECUTING_TASK.get();
    switch (workerType) {
      case COMPACTION:
        TableId tableId =
            KeyExtent.fromThrift(((CompactionJob) job).getJobDetails().getExtent()).tableId();
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

        job.cancel(externalCompactionId);
        break;
      case LOG_SORTING:
        // ELASTICITY_TODO
        break;
      case SPLIT_POINT_CALCULATION:
        // ELASTICITY_TODO
        break;
      default:
        break;

    }
  }

  /**
   * Get the next job to run
   *
   * @param uuid uuid supplier
   * @return CompactionJob
   * @throws RetriesExceededException thrown when retries have been exceeded
   */
  protected Job<?> getNextJob(Supplier<UUID> uuid) throws RetriesExceededException {
    final long startingWaitTime =
        getConfiguration().getTimeInMillis(Property.TASK_RUNNER_MIN_JOB_WAIT_TIME);
    final long maxWaitTime =
        getConfiguration().getTimeInMillis(Property.TASK_RUNNER_MAX_JOB_WAIT_TIME);

    RetryableThriftCall<? extends Job<?>> nextJobThriftCall =
        new RetryableThriftCall<>(startingWaitTime, maxWaitTime, 0, () -> {
          Client coordinatorClient = getCoordinatorClient();
          try {
            // ELASTICITY_TODO: Change ExternalCompactionId to a more generic task id
            ExternalCompactionId eci = ExternalCompactionId.generate(uuid.get());
            LOG.trace("Attempting to get next job, eci = {}", eci);
            TaskRunnerInfo runner = new TaskRunnerInfo(taskRunnerAddress.getAddress().getHost(),
                taskRunnerAddress.getAddress().getPort(), this.workerType, this.getResourceGroup());
            Task task = coordinatorClient.getTask(TraceUtil.traceInfo(), getContext().rpcCreds(),
                runner, eci.toString());

            switch (this.workerType) {
              case COMPACTION:
                final CompactionTask compactionTask =
                    TaskMessage.fromThiftTask(task, TaskMessageType.COMPACTION_TASK);
                currentTaskId.set(eci);
                LOG.debug("Received task for eci: {}, job:{}", eci.toString(),
                    compactionTask.getCompactionJob());
                return new CompactionJob(this, compactionTask, currentTaskId);
              case LOG_SORTING:
                // ELASTICITY_TODO
                return null;
              case SPLIT_POINT_CALCULATION:
                // ELASTICITY_TODO
                return null;
              default:
                return null;
            }

          } catch (Exception e) {
            currentTaskId.set(null);
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
  public TaskManager.Client getCoordinatorClient() throws TTransportException {
    var coordinatorHost = ExternalCompactionUtil.findCompactionCoordinator(getContext());
    if (coordinatorHost.isEmpty()) {
      throw new TTransportException("Unable to get TaskManager address from ZooKeeper");
    }
    LOG.trace("CompactionCoordinator address is: {}", coordinatorHost.orElseThrow());
    return ThriftUtil.getClient(ThriftClientTypes.TASK_MANAGER, coordinatorHost.orElseThrow(),
        getContext());
  }

  protected Supplier<UUID> getNextId() {
    return UUID::randomUUID;
  }

  protected long getWaitTimeBetweenCompactionChecks() {
    // get the total number of compactors assigned to this group
    int numCompactors =
        ExternalCompactionUtil.countCompactors(this.getResourceGroup(), getContext());
    // Aim for around 3 compactors checking in every second
    long sleepTime = numCompactors * 1000L / 3;
    // Ensure a compactor sleeps at least around a second
    sleepTime = Math.max(1000, sleepTime);
    // Ensure a compactor sleep not too much more than 5 mins
    sleepTime = Math.min(300_000L, sleepTime);
    // Add some random jitter to the sleep time, that averages out to sleep time. This will spread
    // compactors out evenly over time.
    sleepTime = (long) (.9 * sleepTime + sleepTime * .2 * RANDOM.get().nextDouble());
    LOG.trace("Sleeping {}ms based on {} compactors", sleepTime, numCompactors);
    return sleepTime;
  }

  @Override
  public void run() {

    try {
      taskRunnerAddress = startThriftClientService();
    } catch (UnknownHostException e1) {
      throw new RuntimeException("Failed to start the compactor client service", e1);
    }
    final HostAndPort clientAddress = taskRunnerAddress.getAddress();

    try {
      announceExistence(clientAddress);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Error registering compactor in ZooKeeper", e);
    }

    if (this.workerType == WorkerType.COMPACTION) {
      // ELASTICITY_TODO: There are no metrics for the other types
      try {
        MetricsUtil.initializeMetrics(getContext().getConfiguration(), this.applicationName,
            clientAddress);
        pausedMetrics = new PausedCompactionMetrics();
        MetricsUtil.initializeProducers(this, pausedMetrics);
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
          | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
          | SecurityException e1) {
        LOG.error("Error initializing metrics, metrics will not be emitted.", e1);
      }
    }

    LOG.info("TaskRunner started, waiting for {} work", workerType);
    try {

      while (!shutdown) {

        switch (this.workerType) {
          case COMPACTION: {
            currentTaskId.set(null);
            CURRENTLY_EXECUTING_TASK.set(null);
            CompactionJob job;
            try {
              job = (CompactionJob) getNextJob(getNextId());
              if (!job.getJobDetails().isSetExternalCompactionId()) {
                LOG.trace("No external compactions in group {}", this.getResourceGroup());
                UtilWaitThread.sleep(getWaitTimeBetweenCompactionChecks());
                continue;
              }
              if (!job.getJobDetails().getExternalCompactionId()
                  .equals(currentTaskId.get().toString())) {
                throw new IllegalStateException(
                    "Returned eci " + job.getJobDetails().getExternalCompactionId()
                        + " does not match supplied eci " + currentTaskId.get());
              }
            } catch (RetriesExceededException e2) {
              LOG.warn("Retries exceeded getting next job. Retrying...");
              continue;
            }
            LOG.debug("Received next compaction job: {}", job);

            final Thread compactionThread = Threads.createThread(
                "Compaction job for tablet " + job.getJobDetails().getExtent().toString(),
                job.createJob());

            CURRENTLY_EXECUTING_TASK.set(job);

            job.executeJob(compactionThread);
            break;
          }
          case LOG_SORTING:
            break;
          case SPLIT_POINT_CALCULATION:
            break;
          default:
            break;
        }
      }

    } catch (Exception e) {
      LOG.error("Unhandled error occurred in Compactor", e);
    } finally {
      // Shutdown local thrift server
      LOG.info("Stopping Thrift Servers");
      if (taskRunnerAddress.server != null) {
        taskRunnerAddress.server.stop();
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

      getContext().getLowMemoryDetector().logGCInfo(getConfiguration());
      LOG.info("stop requested. exiting ... ");
      try {
        if (null != taskRunnerLock) {
          taskRunnerLock.unlock();
        }
      } catch (Exception e) {
        LOG.warn("Failed to release compactor lock", e);
      }
    }

  }

  public static void main(String[] args) throws Exception {
    try (TaskRunner compactor = new TaskRunner(new ConfigOpts(), args)) {
      compactor.runServer();
    }
  }

  @Override
  public Task getActiveCompactions(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    List<CompactionInfo> compactions =
        org.apache.accumulo.server.compaction.FileCompactor.getRunningCompactions();

    ActiveCompactionList list = new ActiveCompactionList();
    compactions.forEach(c -> list.addToCompactions(c.toThrift()));

    ActiveCompactionTasks tasks = TaskMessageType.COMPACTION_TASK_LIST.getTaskMessage();
    tasks.setActiveCompactions(list);

    return tasks.toThriftTask();
  }

  @Override
  public Task getRunningTask(TInfo tinfo, TCredentials credentials)
      throws ThriftSecurityException, TException {
    // do not expect users to call this directly, expect other tservers to call this method
    if (!security.canPerformSystemActions(credentials)) {
      throw new AccumuloSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED).asThriftException();
    }

    // Return what is currently running, does not wait for jobs in the process of reserving. This
    // method is called by a coordinator starting up to determine what is currently running on all
    // compactors.

    Job<?> job = null;
    synchronized (CURRENTLY_EXECUTING_TASK) {
      job = CURRENTLY_EXECUTING_TASK.get();
    }

    switch (workerType) {
      case COMPACTION:
        CompactionTask task = TaskMessageType.COMPACTION_TASK.getTaskMessage();
        if (null == job) {
          task.setCompactionJob(new TExternalCompactionJob());
        } else {
          TExternalCompactionJob tjob = ((CompactionJob) job).getJobDetails();
          task.setTaskId(tjob.getExternalCompactionId());
          task.setCompactionJob(tjob);
        }
        return task.toThriftTask();
      case LOG_SORTING:
        // ELASTICITY_TODO
        return null;
      case SPLIT_POINT_CALCULATION:
        // ELASTICITY_TODO
        return null;
      default:
        return null;

    }
  }

  @Override
  public String getRunningTaskId(TInfo tinfo, TCredentials credentials)
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
    ExternalCompactionId eci = currentTaskId.get();
    if (null == eci) {
      return "";
    } else {
      return eci.canonical();
    }
  }

  @Override
  public void shutdown() {
    this.shutdown = true;
  }

}

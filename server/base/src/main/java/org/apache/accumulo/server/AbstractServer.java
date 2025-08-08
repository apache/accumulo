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
package org.apache.accumulo.server;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.net.UnknownHostException;
import java.util.OptionalInt;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.thrift.SecurityErrorCode;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.core.process.thrift.MetricSource;
import org.apache.accumulo.core.process.thrift.ServerProcessService;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.Timer;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.mem.LowMemoryDetector;
import org.apache.accumulo.server.metrics.MetricResponseWrapper;
import org.apache.accumulo.server.metrics.ProcessMetrics;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.server.security.SecurityUtil;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import com.google.flatbuffers.FlatBufferBuilder;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

public abstract class AbstractServer
    implements AutoCloseable, MetricsProducer, Runnable, ServerProcessService.Iface {

  public static interface ThriftServerSupplier {
    ServerAddress get() throws UnknownHostException;
  }

  private final MetricSource metricSource;
  private final ServerContext context;
  protected final String applicationName;
  private volatile ServerAddress thriftServer;
  private final AtomicReference<HostAndPort> advertiseAddress; // used for everything but the Thrift
                                                               // server (e.g. ZK, metadata, etc).
  private final String bindAddress; // used for the Thrift server
  private final ResourceGroupId resourceGroup;
  private final Logger log;
  private final ProcessMetrics processMetrics;
  protected final long idleReportingPeriodMillis;
  private volatile Timer idlePeriodTimer = null;
  private volatile Thread serverThread;
  private volatile Thread verificationThread;
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private final AtomicBoolean shutdownComplete = new AtomicBoolean(false);

  protected AbstractServer(ServerId.Type serverType, ConfigOpts opts,
      BiFunction<SiteConfiguration,ResourceGroupId,ServerContext> serverContextFactory,
      String[] args) {
    log = LoggerFactory.getLogger(getClass());
    this.applicationName = serverType.name();
    opts.parseArgs(applicationName, args);
    var siteConfig = opts.getSiteConfiguration();
    final String newBindParameter = siteConfig.get(Property.RPC_PROCESS_BIND_ADDRESS);
    // If new bind parameter passed on command line or in file, then use it.
    if (newBindParameter != null
        && !newBindParameter.equals(Property.RPC_PROCESS_BIND_ADDRESS.getDefaultValue())) {
      this.bindAddress = newBindParameter;
    } else {
      this.bindAddress = ConfigOpts.BIND_ALL_ADDRESSES;
    }
    String advertAddr = siteConfig.get(Property.RPC_PROCESS_ADVERTISE_ADDRESS);
    if (advertAddr != null && !advertAddr.isBlank()) {
      HostAndPort advertHP = HostAndPort.fromString(advertAddr);
      if (advertHP.getHost().equals(ConfigOpts.BIND_ALL_ADDRESSES)) {
        throw new IllegalArgumentException("Advertise address cannot be 0.0.0.0");
      }
      advertiseAddress = new AtomicReference<>(advertHP);
    } else {
      advertiseAddress = new AtomicReference<>();
    }
    log.info("Bind address: {}, advertise address: {}", bindAddress, getAdvertiseAddress());
    this.resourceGroup = ResourceGroupId.of(getResourceGroupPropertyValue(siteConfig));
    SecurityUtil.serverLogin(siteConfig);
    context = serverContextFactory.apply(siteConfig, resourceGroup);
    try {
      if (context.getZooSession().asReader().exists(Constants.ZPREPARE_FOR_UPGRADE)) {
        throw new IllegalStateException(
            "Instance has been prepared for upgrade to a minor or major version greater than "
                + Constants.VERSION + ", no servers can be started."
                + " To undo this state and abort upgrade preparations delete the zookeeper node: "
                + Constants.ZPREPARE_FOR_UPGRADE);
      }
    } catch (KeeperException | InterruptedException e) {
      throw new IllegalStateException("Error checking for upgrade preparation node ("
          + Constants.ZPREPARE_FOR_UPGRADE + ") in zookeeper", e);
    }
    log.info("Version " + Constants.VERSION);
    log.info("Instance " + context.getInstanceID());
    context.init(applicationName);
    ClassLoaderUtil.initContextFactory(context.getConfiguration());
    TraceUtil.setProcessTracing(
        context.getConfiguration().getBoolean(Property.GENERAL_OPENTELEMETRY_ENABLED));
    if (context.getSaslParams() != null) {
      // Server-side "client" check to make sure we're logged in as a user we expect to be
      context.enforceKerberosLogin();
    }
    final LowMemoryDetector lmd = context.getLowMemoryDetector();
    ScheduledFuture<?> future = context.getScheduledExecutor().scheduleWithFixedDelay(
        () -> lmd.logGCInfo(context.getConfiguration()), 0,
        lmd.getIntervalMillis(context.getConfiguration()), MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
    processMetrics = new ProcessMetrics(context);
    idleReportingPeriodMillis =
        context.getConfiguration().getTimeInMillis(Property.GENERAL_IDLE_PROCESS_INTERVAL);
    switch (serverType) {
      case COMPACTOR:
        metricSource = MetricSource.COMPACTOR;
        break;
      case GARBAGE_COLLECTOR:
        metricSource = MetricSource.GARBAGE_COLLECTOR;
        break;
      case MANAGER:
        metricSource = MetricSource.MANAGER;
        break;
      case MONITOR:
        metricSource = null;
        break;
      case SCAN_SERVER:
        metricSource = MetricSource.SCAN_SERVER;
        break;
      case TABLET_SERVER:
        metricSource = MetricSource.TABLET_SERVER;
        break;
      default:
        throw new IllegalArgumentException("Unhandled server type: " + serverType);
    }
  }

  /**
   * Updates the idle status of the server to set the idle process metric. The server must be idle
   * for multiple calls over a specified period for the metric to reflect the idle state. If the
   * server is busy or the idle period hasn't started, it resets the idle tracking.
   *
   * @param isIdle whether the server is idle
   */
  protected void updateIdleStatus(boolean isIdle) {
    boolean shouldResetIdlePeriod = !isIdle || idleReportingPeriodMillis == 0;
    boolean hasIdlePeriodStarted = idlePeriodTimer != null;
    boolean hasExceededIdlePeriod =
        hasIdlePeriodStarted && idlePeriodTimer.hasElapsed(idleReportingPeriodMillis, MILLISECONDS);

    if (shouldResetIdlePeriod) {
      // Reset idle period and set idle metric to false
      idlePeriodTimer = null;
      processMetrics.setIdleValue(false);
    } else if (!hasIdlePeriodStarted) {
      // Start tracking idle period
      idlePeriodTimer = Timer.startNew();
    } else if (hasExceededIdlePeriod) {
      // Set idle metric to true and reset the start of the idle period
      processMetrics.setIdleValue(true);
      idlePeriodTimer = null;
    }
  }

  protected String getResourceGroupPropertyValue(SiteConfiguration conf) {
    return Constants.DEFAULT_RESOURCE_GROUP_NAME;
  }

  public ResourceGroupId getResourceGroup() {
    return resourceGroup;
  }

  @Override
  public void gracefulShutdown(TCredentials credentials) {

    try {
      if (!context.getSecurityOperation().canPerformSystemActions(credentials)) {
        log.warn("Ignoring shutdown request, user " + credentials.getPrincipal()
            + " does not have the appropriate permissions.");
      }
    } catch (ThriftSecurityException e) {
      log.error(
          "Error trying to determine if user has permissions to shutdown server, ignoring request",
          e);
      return;
    }

    if (shutdownRequested.compareAndSet(false, true)) {
      // Don't interrupt the server thread, that will cause
      // IO operations to fail as the servers are finishing
      // their work.
      log.info("Graceful shutdown initiated.");
    } else {
      log.warn("Graceful shutdown previously requested.");
    }
  }

  public boolean isShutdownRequested() {
    return shutdownRequested.get();
  }

  public AtomicBoolean getShutdownComplete() {
    return shutdownComplete;
  }

  /**
   * Run this server in a main thread. The server's run method should set up the server, then wait
   * on isShutdownRequested() to return false, like so:
   *
   * <pre>
   * public void run() {
   *   // setup server and start threads
   *   while (!isShutdownRequested()) {
   *     if (Thread.currentThread().isInterrupted()) {
   *       LOG.info("Server process thread has been interrupted, shutting down");
   *       break;
   *     }
   *     try {
   *       // sleep or other things
   *     } catch (InterruptedException e) {
   *       gracefulShutdown();
   *     }
   *   }
   *   // shut down server
   *   getShutdownComplete().set(true);
   *   ServiceLock.unlock(serverLock);
   * }
   * </pre>
   */
  public void runServer() throws Exception {
    final AtomicReference<Throwable> err = new AtomicReference<>();
    serverThread = new Thread(TraceUtil.wrap(this), applicationName);
    serverThread.setUncaughtExceptionHandler((thread, exception) -> err.set(exception));
    serverThread.start();
    serverThread.join();
    if (verificationThread != null) {
      verificationThread.interrupt();
      verificationThread.join();
    }
    log.info(getClass().getSimpleName() + " process shut down.");
    Throwable thrown = err.get();
    if (thrown != null) {
      if (thrown instanceof Error) {
        throw (Error) thrown;
      }
      if (thrown instanceof Exception) {
        throw (Exception) thrown;
      }
      throw new IllegalStateException("Weird throwable type thrown", thrown);
    }
  }

  /**
   * Called
   */
  @Override
  public void registerMetrics(MeterRegistry registry) {
    // makes mocking subclasses easier
    if (processMetrics != null) {
      processMetrics.registerMetrics(registry);
    }
    getContext().setMeterRegistry(registry);
  }

  public HostAndPort getAdvertiseAddress() {
    return advertiseAddress.get();
  }

  public String getBindAddress() {
    return bindAddress;
  }

  protected TServer getThriftServer() {
    if (thriftServer == null) {
      return null;
    }
    return thriftServer.server;
  }

  protected ServerAddress getThriftServerAddress() {
    return thriftServer;
  }

  protected void updateAdvertiseAddress(HostAndPort thriftBindAddress) {
    advertiseAddress.accumulateAndGet(thriftBindAddress, (curr, update) -> {
      if (curr == null) {
        return thriftBindAddress;
      } else if (!curr.hasPort()) {
        return HostAndPort.fromParts(curr.getHost(), update.getPort());
      } else {
        return curr;
      }
    });
  }

  /**
   * Updates internal ThriftServer reference and optionally starts the Thrift server. Updates the
   * advertise address based on the address to which the ThriftServer is bound
   *
   * @param supplier ThriftServer
   * @param start true to start the server, else false
   * @throws UnknownHostException thrown from ThriftServer when binding to bad address
   */
  protected void updateThriftServer(ThriftServerSupplier supplier, boolean start)
      throws UnknownHostException {
    thriftServer = supplier.get();
    if (start) {
      thriftServer.startThriftServer("Thrift Client Server");
      log.info("Starting {} Thrift server, listening on {}", this.getClass().getSimpleName(),
          thriftServer.address);
    }
    updateAdvertiseAddress(thriftServer.address);
  }

  public ServerContext getContext() {
    return context;
  }

  public AccumuloConfiguration getConfiguration() {
    return getContext().getConfiguration();
  }

  public String getApplicationName() {
    return applicationName;
  }

  @Override
  public MetricResponse getMetrics(TInfo tinfo, TCredentials credentials) throws TException {

    if (!context.getSecurityOperation().authenticateUser(credentials, credentials)) {
      throw new ThriftSecurityException(credentials.getPrincipal(),
          SecurityErrorCode.PERMISSION_DENIED);
    }

    final FlatBufferBuilder builder = new FlatBufferBuilder(1024);
    final MetricResponseWrapper response = new MetricResponseWrapper(builder);

    if (getAdvertiseAddress() == null) {
      log.error(
          "Advertise address is not set, this should have been done after starting the Thrift service.");
      return response;
    }

    if (metricSource == null) {
      // Metrics not reported for Monitor type
      return response;
    }

    response.setServerType(metricSource);
    response.setServer(getAdvertiseAddress().toString());
    response.setResourceGroup(getResourceGroup().canonical());
    response.setTimestamp(System.currentTimeMillis());

    if (context.getMetricsInfo().isMetricsEnabled()) {
      Metrics.globalRegistry.getMeters().forEach(m -> {
        if (m.getId().getName().startsWith("accumulo.")) {
          m.match(response::writeMeter, response::writeMeter, response::writeTimer,
              response::writeDistributionSummary, response::writeLongTaskTimer,
              response::writeMeter, response::writeMeter, response::writeFunctionTimer,
              response::writeMeter);
        }
      });
    }

    builder.clear();
    return response;
  }

  /**
   * Get the ServiceLock for this server process. May return null if called before the lock is
   * acquired.
   *
   * @return lock ServiceLock or null
   */
  public abstract ServiceLock getLock();

  public void startServiceLockVerificationThread() {
    Preconditions.checkState(verificationThread == null,
        "verification thread not null, startServiceLockVerificationThread likely called twice");
    Preconditions.checkState(serverThread != null,
        "server thread is null, no server process is running");
    final long interval =
        getConfiguration().getTimeInMillis(Property.GENERAL_SERVER_LOCK_VERIFICATION_INTERVAL);
    if (interval > 0) {
      verificationThread = Threads.createCriticalThread("service-lock-verification-thread",
          OptionalInt.of(Thread.NORM_PRIORITY + 1), () -> {
            while (serverThread.isAlive()) {
              ServiceLock lock = getLock();
              try {
                log.trace(
                    "ServiceLockVerificationThread - checking ServiceLock existence in ZooKeeper");
                if (lock != null && !lock.verifyLockAtSource()) {
                  Halt.halt(-1, "Lock verification thread could not find lock");
                }
                // Need to sleep, not yield when the thread priority is greater than NORM_PRIORITY
                // so that this thread does not get immediately rescheduled.
                log.trace(
                    "ServiceLockVerificationThread - ServiceLock exists in ZooKeeper, sleeping for {}ms",
                    interval);
                Thread.sleep(interval);
              } catch (InterruptedException e) {
                if (serverThread.isAlive()) {
                  // throw an Error, which will cause this process to be terminated
                  throw new Error("Sleep interrupted in ServiceLock verification thread");
                }
              }
            }
          });
      verificationThread.start();
    } else {
      log.info("ServiceLockVerificationThread not started as "
          + Property.GENERAL_SERVER_LOCK_VERIFICATION_INTERVAL.getKey() + " is zero");
    }
  }

  @Override
  public void close() {
    if (context != null) {
      context.close();
    }
  }

  protected void waitForUpgrade() throws InterruptedException {
    while (AccumuloDataVersion.getCurrentVersion(getContext()) < AccumuloDataVersion.get()) {
      log.info("Waiting for upgrade to complete.");
      Thread.sleep(1000);
    }
  }

}

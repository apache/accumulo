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
package org.apache.accumulo.monitor.next;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.monitor.next.SystemInformation.MessageCategory.Monitor;
import static org.apache.accumulo.monitor.next.SystemInformation.MessageCategory.Table;
import static org.apache.accumulo.monitor.next.SystemInformation.MessagePriority.Critical;
import static org.apache.accumulo.monitor.next.SystemInformation.MessagePriority.Info;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;

import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.core.Response;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.RowRange;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.AdminUtil;
import org.apache.accumulo.core.fate.AdminUtil.FateStatus;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.fate.ReadOnlyFateStore;
import org.apache.accumulo.core.fate.user.UserFateStore;
import org.apache.accumulo.core.fate.zookeeper.LockRange;
import org.apache.accumulo.core.fate.zookeeper.MetaFateStore;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.lock.ServiceLockPaths.ResourceGroupPredicate;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.Location;
import org.apache.accumulo.core.metadata.schema.filters.NoCurrentLocationFilter;
import org.apache.accumulo.core.metadata.schema.filters.TabletMetadataFilter;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.core.process.thrift.ServerProcessService.Client;
import org.apache.accumulo.core.rpc.ThriftUtil;
import org.apache.accumulo.core.rpc.clients.ThriftClientTypes;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.CompactionPluginUtils;
import org.apache.accumulo.server.util.adminCommand.Fate;
import org.apache.zookeeper.KeeperException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.eclipse.jetty.util.NanoTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.net.HostAndPort;

public class InformationFetcher implements RemovalListener<ServerId,MetricResponse>, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(InformationFetcher.class);

  public static class InstanceSummary {
    private final String instanceName;
    private final String instanceUUID;
    private final Set<String> zooKeepers;
    private final Set<String> volumes;
    private final String version;

    public InstanceSummary(String instanceName, String instanceUUID, Set<String> zooKeepers,
        Set<String> volumes, String version) {
      super();
      this.instanceName = instanceName;
      this.instanceUUID = instanceUUID;
      this.zooKeepers = zooKeepers;
      this.volumes = volumes;
      this.version = version;
    }

    public String getInstanceName() {
      return instanceName;
    }

    public String getInstanceUUID() {
      return instanceUUID;
    }

    public Set<String> getZooKeepers() {
      return zooKeepers;
    }

    public Set<String> getVolumes() {
      return volumes;
    }

    public String getVersion() {
      return version;
    }
  }

  record UpdateTaskFuture(Future<?> future, UpdateTask<?> task) {
  }

  static class UpdateTasks {

    private final Comparator<UpdateTaskFuture> c = new Comparator<>() {

      @Override
      public int compare(UpdateTaskFuture o1, UpdateTaskFuture o2) {
        if (o1.future() == o2.future()) {
          return 0;
        } else {
          if (Objects.equals(o1.task(), o2.task())) {
            return 0;
          } else {
            return Integer.compare(o1.task().hashCode(), o2.task().hashCode());
          }
        }
      }

    };
    private final ConcurrentSkipListSet<UpdateTaskFuture> futures = new ConcurrentSkipListSet<>(c);
    private final AtomicBoolean stopTables = new AtomicBoolean(false);

    boolean isEmpty() {
      return futures.isEmpty();
    }

    Iterator<UpdateTaskFuture> iterator() {
      return futures.iterator();
    }

    int size() {
      return futures.size();
    }

    void add(UpdateTaskFuture f) {
      if (stopTables.get() && f.task().getType() == UpdateType.TABLE) {
        return;
      }
      futures.add(f);
    }
  }

  enum UpdateType {
    COMPACTION, COMPACTION_RGS, FATE, METRIC, TABLE;
  }

  interface UpdateTask<T extends Object> extends Runnable, Comparable<UpdateTask<T>> {

    UpdateType getType();

    T getResource();

    String getFailureMessage();

  }

  class MetricFetcher implements UpdateTask<ServerId> {

    private final ServerContext ctx;
    private final ServerId server;
    private final SystemInformation summary;
    private final UpdateTasks tasks;

    private MetricFetcher(ServerContext ctx, ServerId server, SystemInformation summary,
        UpdateTasks tasks) {
      this.ctx = ctx;
      this.server = server;
      this.summary = summary;
      this.tasks = tasks;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Objects.hash(getType());
      result = prime * result + Objects.hash(getResource());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      MetricFetcher other = (MetricFetcher) obj;
      return Objects.equals(getType(), other.getType())
          && Objects.equals(getResource(), other.getResource());
    }

    @Override
    public int compareTo(UpdateTask<ServerId> other) {
      int result = this.getType().compareTo(other.getType());
      if (result == 0) {
        result = getResource().compareTo(other.getResource());
      }
      return result;
    }

    @Override
    public UpdateType getType() {
      return UpdateType.METRIC;
    }

    @Override
    public ServerId getResource() {
      return server;
    }

    @Override
    public String getFailureMessage() {
      return "Failed to get metrics from server: " + server;
    }

    @Override
    public void run() {
      try {
        Client metricsClient = ThriftUtil.getClient(ThriftClientTypes.SERVER_PROCESS,
            HostAndPort.fromParts(server.getHost(), server.getPort()), ctx);
        try {
          MetricResponse response = metricsClient.getMetrics(TraceUtil.traceInfo(), ctx.rpcCreds());
          retainedProblemServers.invalidate(server);
          summary.processResponse(server, response, tasks);
        } finally {
          ThriftUtil.returnClient(metricsClient, ctx);
        }
      } catch (Exception e) {
        LOG.warn("Error trying to get metrics from server: {}", server, e);
        retainedProblemServers.put(server, Boolean.TRUE);
        summary.processMetricsError(server);
      }
    }
  }

  class TableInformationFetcher implements UpdateTask<TableId> {
    private final ServerContext ctx;
    private final TableId tableId;
    private final SystemInformation summary;

    private TableInformationFetcher(ServerContext ctx, TableId tableId, SystemInformation summary) {
      this.ctx = ctx;
      this.tableId = tableId;
      this.summary = summary;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Objects.hash(getType());
      result = prime * result + Objects.hash(getResource());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      TableInformationFetcher other = (TableInformationFetcher) obj;
      return Objects.equals(getType(), other.getType())
          && Objects.equals(getResource(), other.getResource());
    }

    @Override
    public int compareTo(UpdateTask<TableId> other) {
      int result = this.getType().compareTo(other.getType());
      if (result == 0) {
        result = getResource().compareTo(other.getResource());
      }
      return result;
    }

    @Override
    public UpdateType getType() {
      return UpdateType.TABLE;
    }

    @Override
    public TableId getResource() {
      return tableId;
    }

    @Override
    public String getFailureMessage() {
      return "Failed to get information for table: " + tableId;
    }

    @Override
    public void run() {
      try {
        final String tableName = ctx.getQualifiedTableName(tableId);
        try (Stream<TabletInformation> tablets =
            this.ctx.tableOperations().getTabletInformation(tableName, List.of(RowRange.all()))) {
          tablets.forEach(t -> summary.processTabletInformation(tableId, tableName, t));
        }
      } catch (TableNotFoundException e) {
        LOG.warn("TableNotFoundException thrown while trying to gather information for TableId: {}",
            tableId, e);
      } catch (Exception e) {
        LOG.warn("Interrupted while trying to gather information for TableId: {}", tableId, e);
      }
    }
  }

  class RunningCompactionFetcher implements UpdateTask<Void> {

    private final SystemInformation summary;
    private final ThreadPoolExecutor executor;

    public RunningCompactionFetcher(SystemInformation summary, ThreadPoolExecutor executor) {
      this.summary = summary;
      this.executor = executor;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Objects.hash(getType());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      RunningCompactionFetcher other = (RunningCompactionFetcher) obj;
      return Objects.equals(getType(), other.getType());
    }

    @Override
    public int compareTo(UpdateTask<Void> other) {
      return this.getType().compareTo(other.getType());
    }

    @Override
    public UpdateType getType() {
      return UpdateType.COMPACTION;
    }

    @Override
    public Void getResource() {
      return null;
    }

    @Override
    public String getFailureMessage() {
      return "Failed to get running compactions";
    }

    @Override
    public void run() {
      try {
        List<ServerId> failures = ExternalCompactionUtil.getCompactionsRunningOnCompactors(ctx,
            executor, (t) -> summary.processExternalCompaction(t));
        summary.getProblemHosts().addAll(failures);
      } catch (Exception e) {
        LOG.warn("Error gathering running compaction information.", e);
      }
    }
  }

  class FateTransactionFetcher implements UpdateTask<Void> {

    private final SystemInformation summary;
    private Map<FateId,List<String>> emptyLocks = new HashMap<>();
    private Map<FateId,LockRange> emptyRanges = new HashMap<>();

    public FateTransactionFetcher(SystemInformation summary) {
      this.summary = summary;
    }

    @Override
    public void run() {
      FateStatus status = AdminUtil.getTransactionStatus(stores, null, null, null, emptyLocks,
          emptyLocks, emptyRanges);
      summary.processFateTransactions(status.getTransactions());
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Objects.hash(getType());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      FateTransactionFetcher other = (FateTransactionFetcher) obj;
      return Objects.equals(getType(), other.getType());
    }

    @Override
    public int compareTo(UpdateTask<Void> other) {
      return this.getType().compareTo(other.getType());
    }

    @Override
    public UpdateType getType() {
      return UpdateType.FATE;
    }

    @Override
    public Void getResource() {
      return null;
    }

    @Override
    public String getFailureMessage() {
      return "Error fetching fate transaction details";
    }
  }

  class ConfiguredCompactionResourceGroupFetcher implements UpdateTask<Void> {

    private final SystemInformation summary;

    public ConfiguredCompactionResourceGroupFetcher(SystemInformation summary) {
      this.summary = summary;
    }

    @Override
    public void run() {
      try {
        summary.addConfiguredCompactionGroups(
            CompactionPluginUtils.getConfiguredCompactionResourceGroups(ctx));
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException(e);
      }
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Objects.hash(getType());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ConfiguredCompactionResourceGroupFetcher other =
          (ConfiguredCompactionResourceGroupFetcher) obj;
      return Objects.equals(getType(), other.getType());
    }

    @Override
    public int compareTo(UpdateTask<Void> other) {
      return this.getType().compareTo(other.getType());
    }

    @Override
    public UpdateType getType() {
      return UpdateType.COMPACTION_RGS;
    }

    @Override
    public Void getResource() {
      return null;
    }

    @Override
    public String getFailureMessage() {
      return "Error fetching configured compaction resource groups";
    }

  }

  private final String poolName = "MonitorMetricsThreadPool";
  private final ThreadPoolExecutor pool = ThreadPools.getServerThreadPools()
      .getPoolBuilder(poolName).numCoreThreads(10).withTimeOut(30, SECONDS).build();

  private final ServerContext ctx;
  private final Supplier<Long> connectionCount;
  private final AtomicBoolean newConnectionEvent = new AtomicBoolean(false);
  private final Cache<ServerId,MetricResponse> allMetrics;
  private final Cache<ServerId,Boolean> retainedProblemServers;
  private final AtomicReference<SystemInformation> summaryRef = new AtomicReference<>();
  private final ReadOnlyFateStore<Fate> readOnlyMFS;
  private final ReadOnlyFateStore<Fate> readOnlyUFS;
  private final Map<FateInstanceType,ReadOnlyFateStore<Fate>> stores;
  private final TabletMetadataFilter noLocation = new NoCurrentLocationFilter();

  public InformationFetcher(ServerContext ctx, Supplier<Long> connectionCount) {
    this.ctx = ctx;
    this.connectionCount = connectionCount;
    this.allMetrics = Caffeine.newBuilder().executor(pool).scheduler(Scheduler.systemScheduler())
        .expireAfterWrite(Duration.ofMinutes(10)).evictionListener(this::onRemoval).build();
    this.retainedProblemServers = Caffeine.newBuilder().executor(pool)
        .scheduler(Scheduler.systemScheduler()).expireAfterWrite(Duration.ofMinutes(10)).build();
    try {
      this.readOnlyMFS = new MetaFateStore<>(ctx.getZooSession(), null, null);
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException("Exception creating MetaFateStore", e);
    }
    this.readOnlyUFS = new UserFateStore<>(ctx, SystemTables.FATE.tableName(), null, null);
    this.stores = Map.of(FateInstanceType.META, readOnlyMFS, FateInstanceType.USER, readOnlyUFS);
  }

  public void newConnectionEvent() {
    this.newConnectionEvent.compareAndSet(false, true);
  }

  // Protect against NPE and wait for initial data gathering
  private SystemInformation getSummary() throws InterruptedException {
    while (summaryRef.get() == null) {
      Thread.sleep(100);
    }
    return summaryRef.get();
  }

  /**
   * {@link #getSummary()} but throws a 503 (Service Unavailable) server error to the web client if
   * an {@link InterruptedException} occurs.
   */
  public SystemInformation getSummaryForEndpoint() throws ServiceUnavailableException {
    try {
      return getSummary();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  public Cache<ServerId,MetricResponse> getAllMetrics() {
    return allMetrics;
  }

  @Override
  public void onRemoval(@Nullable ServerId server, @Nullable MetricResponse response,
      RemovalCause cause) {
    if (server == null) {
      return;
    }
    try {
      getSummary().processError(server);
      LOG.info("{} has been evicted", server);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("{} could not be evicted", server, e);
    }
  }

  /**
   * Obtains a count of the metadata tablets with no location. This work is done in a Thread because
   * the Scanner used by Ample will sit and wait for the tablets to be hosted.
   *
   * @return count of metadata tablets with no location
   */
  private long countMetadataTabletsNoLocation() {
    // If any Metadata tablet is not hosted, then don't look for table information
    // on other tables.
    AtomicLong metadataNoLocation = new AtomicLong(0);
    // This is a background task because the tserver could go down and
    // the scanner inside Ample will sit there and wait.
    Runnable countTask = () -> {
      metadataNoLocation.set(ctx.getAmple().readTablets().forTable(SystemTables.METADATA.tableId())
          .fetch(ColumnType.LOCATION).filter(noLocation).build().stream().count());
    };
    Thread countThread = new Thread(countTask, "Metadata-Tablets-Location-Thread");
    countThread.start();
    try {
      countThread.join(30_000);
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Interrupted while waiting for thread counting metadata tablet locations");
    }
    if (countThread.isAlive()) {
      countThread.interrupt();
    }
    return metadataNoLocation.get();
  }

  /**
   * Validates that tablet location is a tablet server that is alive and has its lock
   *
   * @param location TabletMetadata Location
   * @return true if location is valid
   */
  private boolean isLocationValid(Location location) {
    if (location != null) {
      // Verify location is alive
      Set<ServiceLockPath> servers = ctx.getServerPaths().getTabletServer(
          ResourceGroupPredicate.ANY, AddressSelector.exact(location.getHostAndPort()), true);
      if (servers != null && !servers.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  private void fetchTabletInformation(SystemInformation summary, UpdateTasks futures,
      Location rootTabletLocation) {

    // Fetch information about the root tablet, this is pulled from ZK so
    // it doesn't depend on the tablet being hosted.
    TableInformationFetcher rtif =
        new TableInformationFetcher(this.ctx, SystemTables.ROOT.tableId(), summary);
    Future<?> rtiff = this.pool.submit(rtif);
    futures.add(new UpdateTaskFuture(rtiff, rtif));

    if (isLocationValid(rootTabletLocation)) {

      // If the root tablet is hosted, then we can ask about the metadata table
      TableInformationFetcher tif =
          new TableInformationFetcher(this.ctx, SystemTables.METADATA.tableId(), summary);
      Future<?> tiff = this.pool.submit(tif);
      futures.add(new UpdateTaskFuture(tiff, tif));

      final long metadataNoLocation = countMetadataTabletsNoLocation();
      if (metadataNoLocation == 0) {

        // If the metadata table is fully hosted, then we can ask about
        // all of the other tables
        for (TableId tableId : this.ctx.createQualifiedTableNameToIdMap().values()) {
          if (tableId.equals(SystemTables.ROOT.tableId())
              || tableId.equals(SystemTables.METADATA.tableId())) {
            continue; // we already spawned a task
          }
          tif = new TableInformationFetcher(this.ctx, tableId, summary);
          tiff = this.pool.submit(tif);
          futures.add(new UpdateTaskFuture(tiff, tif));
        }

      } else {
        summary.addMessage(Critical, Table,
            metadataNoLocation + " metadata tablets are not hosted");
      }
    } else {
      summary.addMessage(Critical, Table, "The root tablet is not currently hosted");
    }
  }

  @Override
  public void run() {

    long lastRunTime = 0;
    while (true) {

      // Don't fetch new data if there are no connections.
      // On an initial connection, no data may be displayed.
      // If a connection has not been made in a while, stale data may be displayed.
      // Only refresh every 5s (old monitor logic).
      while (!newConnectionEvent.get() && connectionCount.get() == 0
          && NanoTime.millisElapsed(lastRunTime, NanoTime.now()) > 5000) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(
              "Thread " + Thread.currentThread().getName() + " interrupted", e);
        }
      }
      // reset the connection event flag
      newConnectionEvent.compareAndExchange(true, false);

      LOG.info("Fetching information from servers");

      final UpdateTasks futures = new UpdateTasks();
      final SystemInformation summary = new SystemInformation(allMetrics, this.ctx);

      // Fetch set of registered compactors
      Set<ServerId> compactors = this.ctx.instanceOperations().getServers(Type.COMPACTOR);
      summary.processExternalCompactionInventory(compactors);

      // Fetch Fate transaction information
      FateTransactionFetcher fateFetcher = new FateTransactionFetcher(summary);
      Future<?> fff = this.pool.submit(fateFetcher);
      futures.add(new UpdateTaskFuture(fff, fateFetcher));

      // Fetch metrics from the other server processes. This
      // makes an RPC call to AbstractServer.getMetrics
      for (ServerId.Type type : ServerId.Type.values()) {
        if (type == Type.MONITOR) {
          continue;
        }
        for (ServerId server : this.ctx.instanceOperations().getServers(type)) {
          MetricFetcher mf = new MetricFetcher(this.ctx, server, summary, futures);
          Future<?> mff = this.pool.submit(mf);
          futures.add(new UpdateTaskFuture(mff, mf));
        }
      }

      // Fetch external compaction information from the Compactors
      RunningCompactionFetcher rcf = new RunningCompactionFetcher(summary, pool);
      Future<?> rcff = this.pool.submit(rcf);
      futures.add(new UpdateTaskFuture(rcff, rcf));

      final TabletMetadata rootTabletMetadata =
          new RootTabletMetadata(new String(ctx.getZooCache().get(RootTable.ZROOT_TABLET), UTF_8))
              .toTabletMetadata();
      final Location rootTabletLocation = rootTabletMetadata.getLocation();
      fetchTabletInformation(summary, futures, rootTabletLocation);

      ConfiguredCompactionResourceGroupFetcher r =
          new ConfiguredCompactionResourceGroupFetcher(summary);
      Future<?> f = this.pool.submit(r);
      futures.add(new UpdateTaskFuture(f, r));

      ThreadPools.resizePool(pool, () -> Math.max(20, (futures.size() / 20)), poolName);

      final long monitorFetchTimeout =
          ctx.getConfiguration().getTimeInMillis(Property.MONITOR_FETCH_TIMEOUT);
      final long allFuturesAdded = NanoTime.now();
      boolean tookToLong = false;

      final List<UpdateTaskFuture> failures = new ArrayList<>();
      final List<UpdateTaskFuture> cancelled = new ArrayList<>();
      boolean firstIteration = true;
      while (!futures.isEmpty()) {

        if (NanoTime.millisElapsed(allFuturesAdded, NanoTime.now()) > monitorFetchTimeout) {
          String message =
              "Fetching information for Monitor has taken longer than %1$d ms. Cancelling all remaining tasks (%2$d) "
                  + "and monitor will display old information. Resolve issue causing this or increase property %3$s.";
          LOG.warn(String.format(message, monitorFetchTimeout, futures.size(),
              Property.MONITOR_FETCH_TIMEOUT.getKey()));
          tookToLong = true;
        }

        boolean isRootLocationValid = isLocationValid(rootTabletLocation);
        long unhostedMetadataTabletCount = 1;
        if (isRootLocationValid) {
          unhostedMetadataTabletCount = countMetadataTabletsNoLocation();
        }
        Iterator<UpdateTaskFuture> iter = futures.iterator();
        while (iter.hasNext()) {

          UpdateTaskFuture future = iter.next();

          if (future.future().isDone()) {
            iter.remove();
            try {
              future.future().get();
            } catch (CancellationException e) {
              if (!tookToLong) {
                cancelled.add(future);
              }
            } catch (InterruptedException | ExecutionException e) {
              failures.add(future);
              LOG.error("Error getting status from future", e);
            }
          } else if (future.task().getClass().equals(TableInformationFetcher.class)
              && (!isRootLocationValid || unhostedMetadataTabletCount > 0)) {
            TableInformationFetcher task = (TableInformationFetcher) future.task();
            TableId tid = task.getResource();
            if (!isRootLocationValid && !tid.equals(SystemTables.ROOT.tableId())) {
              LOG.warn(
                  "Cancelling TableInformationFetcher tasks for non-root tables as root tablet is unhosted. {}",
                  future.task().getFailureMessage());
              future.future().cancel(true);
              cancelled.add(future);
            } else if (unhostedMetadataTabletCount > 0 && (!tid.equals(SystemTables.ROOT.tableId())
                && !tid.equals(SystemTables.METADATA.tableId()))) {
              LOG.warn(
                  "Cancelling TableInformationFetcher tasks for user tables as metadata tablet has {} unhosted tablets. {}",
                  unhostedMetadataTabletCount, future.task().getFailureMessage());
              future.future().cancel(true);
              cancelled.add(future);
            }
          } else if (tookToLong && !future.future().isCancelled()) {
            LOG.warn("Cancelling task as it took too long. {}", future.task().getFailureMessage());
            future.future().cancel(true);
            cancelled.add(future);
          }
        }
        if (!firstIteration) {
          // Update current messages on the Monitor that we are
          // waiting on tasks to complete to complete a refresh
          final String waitingMsg = "Waiting on " + futures.size()
              + " tasks to complete. Time remaining before cancellation: "
              + (monitorFetchTimeout - NanoTime.millisElapsed(allFuturesAdded, NanoTime.now()))
                  / 1000
              + " seconds";
          SystemInformation currentSummary = summaryRef.get();
          if (currentSummary != null) {
            currentSummary.removeMessage(Info, Monitor,
                " tasks to complete. Time remaining before cancellation: ");
            currentSummary.addMessage(Info, Monitor, waitingMsg);
          }
        }

        if (!futures.isEmpty()) {
          UtilWaitThread.sleep(3_000);
        }
        firstIteration = false;
      }

      lastRunTime = NanoTime.now();

      retainedProblemServers.asMap().keySet().forEach(summary::retainProblemServer);
      summary.finish(failures, cancelled);

      LOG.info("Finished fetching metrics from servers");
      LOG.info(
          "All: {}, Managers: {}, Garbage Collector: {}, Compactors: {}, Scan Servers: {}, Tablet Servers: {}",
          allMetrics.estimatedSize(), summary.getManagers().size(),
          summary.getGarbageCollector() != null,
          summary.getCompactorAllMetricSummary().isEmpty() ? 0
              : summary.getCompactorAllMetricSummary().entrySet().iterator().next().getValue()
                  .count(),
          summary.getSServerAllMetricSummary().isEmpty() ? 0
              : summary.getSServerAllMetricSummary().entrySet().iterator().next().getValue()
                  .count(),
          summary.getTServerAllMetricSummary().isEmpty() ? 0 : summary.getTServerAllMetricSummary()
              .entrySet().iterator().next().getValue().count());

      SystemInformation oldSummary = summaryRef.getAndSet(summary);
      if (oldSummary != null) {
        oldSummary.clear();
      }
    }

  }

}

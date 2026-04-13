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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.TabletMergeabilityInfo;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.core.spi.balancer.TableLoadBalancer;
import org.apache.accumulo.core.util.compaction.RunningCompactionInfo;
import org.apache.accumulo.monitor.next.deployment.DeploymentOverview;
import org.apache.accumulo.monitor.next.sservers.ScanServerView;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.metrics.MetricResponseWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.net.HostAndPort;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

public class SystemInformation {

  public static class ObfuscatedTabletId extends TabletIdImpl {

    public ObfuscatedTabletId(KeyExtent ke) {
      super(ke);
    }

    @Override
    public String toString() {
      return this.toKeyExtent().obscured();
    }

  }

  public static class SanitizedTabletInformation implements TabletInformation {
    private final TabletInformation tabletInfo;

    public SanitizedTabletInformation(TabletInformation tabletInfo) {
      super();
      this.tabletInfo = tabletInfo;
    }

    @Override
    public TabletId getTabletId() {
      return new ObfuscatedTabletId(((TabletIdImpl) tabletInfo.getTabletId()).toKeyExtent());
    }

    @Override
    public int getNumFiles() {
      return tabletInfo.getNumFiles();
    }

    @Override
    public int getNumWalLogs() {
      return tabletInfo.getNumWalLogs();
    }

    @Override
    public long getEstimatedEntries() {
      return tabletInfo.getEstimatedEntries();
    }

    @Override
    public long getEstimatedSize() {
      return tabletInfo.getEstimatedSize();
    }

    @Override
    public String getTabletState() {
      return tabletInfo.getTabletState();
    }

    @Override
    public Optional<String> getLocation() {
      return tabletInfo.getLocation();
    }

    @Override
    public String getTabletDir() {
      return tabletInfo.getTabletDir();
    }

    @Override
    public TabletAvailability getTabletAvailability() {
      return tabletInfo.getTabletAvailability();
    }

    @Override
    public TabletMergeabilityInfo getTabletMergeabilityInfo() {
      return tabletInfo.getTabletMergeabilityInfo();
    }

  }

  public static class TableSummary {

    private final AtomicLong totalEntries = new AtomicLong();
    private final AtomicLong totalSizeOnDisk = new AtomicLong();
    private final AtomicLong totalFiles = new AtomicLong();
    private final AtomicLong totalWals = new AtomicLong();
    private final AtomicLong totalTablets = new AtomicLong();
    private final AtomicLong availableAlways = new AtomicLong();
    private final AtomicLong availableOnDemand = new AtomicLong();
    private final AtomicLong availableNever = new AtomicLong();
    private final AtomicLong totalAssignedTablets = new AtomicLong();
    private final AtomicLong totalAssignedToDeadServerTablets = new AtomicLong();
    private final AtomicLong totalHostedTablets = new AtomicLong();
    private final AtomicLong totalSuspendedTablets = new AtomicLong();
    private final AtomicLong totalUnassignedTablets = new AtomicLong();
    private String tableName;

    public TableSummary(String tableName) {
      this.tableName = tableName;
    }

    public long getTotalEntries() {
      return totalEntries.get();
    }

    public long getTotalSizeOnDisk() {
      return totalSizeOnDisk.get();
    }

    public long getTotalFiles() {
      return totalFiles.get();
    }

    public long getTotalWals() {
      return totalWals.get();
    }

    public long getTotalTablets() {
      return totalTablets.get();
    }

    public long getAvailableAlways() {
      return availableAlways.get();
    }

    public long getAvailableOnDemand() {
      return availableOnDemand.get();
    }

    public long getAvailableNever() {
      return availableNever.get();
    }

    public long getTotalAssignedTablets() {
      return totalAssignedTablets.get();
    }

    public long getTotalAssignedToDeadServerTablets() {
      return totalAssignedToDeadServerTablets.get();
    }

    public long getTotalHostedTablets() {
      return totalHostedTablets.get();
    }

    public long getTotalSuspendedTablets() {
      return totalSuspendedTablets.get();
    }

    public long getTotalUnassignedTablets() {
      return totalUnassignedTablets.get();
    }

    public String getTableName() {
      return tableName;
    }

    public void addTablet(TabletInformation info) {
      totalEntries.addAndGet(info.getEstimatedEntries());
      totalSizeOnDisk.addAndGet(info.getEstimatedSize());
      totalFiles.addAndGet(info.getNumFiles());
      totalWals.addAndGet(info.getNumWalLogs());
      totalTablets.addAndGet(1);
      switch (info.getTabletAvailability()) {
        case HOSTED:
          availableAlways.addAndGet(1);
          break;
        case ONDEMAND:
          availableOnDemand.addAndGet(1);
          break;
        case UNHOSTED:
          availableNever.addAndGet(1);
          break;
        default:
          throw new RuntimeException("Error processing TabletInformation, unknown availability: "
              + info.getTabletAvailability());
      }
      TabletState state = TabletState.valueOf(info.getTabletState());
      switch (state) {
        case ASSIGNED:
          totalAssignedTablets.addAndGet(1);
          break;
        case ASSIGNED_TO_DEAD_SERVER:
          totalAssignedToDeadServerTablets.addAndGet(1);
          break;
        case HOSTED:
          totalHostedTablets.addAndGet(1);
          break;
        case SUSPENDED:
          totalSuspendedTablets.addAndGet(1);
          break;
        case UNASSIGNED:
          totalUnassignedTablets.addAndGet(1);
          break;
        default:
          throw new RuntimeException(
              "Error processing TabletInformation, unknown state: " + info.getTabletState());
      }
    }
  }

  public static class ProcessSummary {
    private final Set<ServerId> responded = ConcurrentHashMap.newKeySet();
    private final Set<ServerId> notResponded = ConcurrentHashMap.newKeySet();

    public void addResponded(ServerId server) {
      notResponded.remove(server);
      responded.add(server);
    }

    public void addNotResponded(ServerId server) {
      responded.remove(server);
      notResponded.add(server);
    }

    public long getTotal() {
      return this.responded.size() + this.notResponded.size();
    }

    public long getResponded() {
      return this.responded.size();
    }

  }

  // Object that serves as a TopN view of the RunningCompactions, ordered by
  // RunningCompaction start time. The first entry in this Set should be the
  // oldest RunningCompaction.
  public static class TimeOrderedRunningCompactionSet {

    public static final Comparator<RunningCompactionInfo> OLDEST_FIRST_COMPARATOR =
        Comparator.comparingLong(RunningCompactionInfo::getStartTime).thenComparing(rc -> rc.ecid);

    private final ConcurrentSkipListSet<RunningCompactionInfo> compactions =
        new ConcurrentSkipListSet<>(OLDEST_FIRST_COMPARATOR);

    // Tracking size here as ConcurrentSkipListSet.size() is not constant time
    private final AtomicInteger size = new AtomicInteger(0);

    private final int limit;

    public TimeOrderedRunningCompactionSet(int limit) {
      this.limit = limit;
    }

    public int size() {
      return size.get();
    }

    public boolean add(RunningCompactionInfo e) {
      boolean added = compactions.add(e);
      if (added) {
        if (size.incrementAndGet() > this.limit) {
          this.remove(compactions.last());
        }
      }
      return added;
    }

    public boolean remove(Object o) {
      boolean removed = compactions.remove(o);
      if (removed) {
        size.decrementAndGet();
      }
      return removed;
    }

    public Iterator<RunningCompactionInfo> iterator() {
      return compactions.iterator();
    }

    public Stream<RunningCompactionInfo> stream() {
      return compactions.stream();
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(SystemInformation.class);

  private final DistributionStatisticConfig DSC =
      DistributionStatisticConfig.builder().percentilePrecision(1).minimumExpectedValue(0.1)
          .maximumExpectedValue(Double.POSITIVE_INFINITY).expiry(Duration.ofMinutes(10))
          .bufferLength(3).build();

  private final ServerContext ctx;
  private final Cache<ServerId,MetricResponse> allMetrics;

  private final Set<String> resourceGroups = ConcurrentHashMap.newKeySet();
  private final Set<ServerId> problemHosts = ConcurrentHashMap.newKeySet();
  private final Set<ServerId> metricProblemHosts = ConcurrentHashMap.newKeySet();
  private final AtomicReference<ServerId> manager = new AtomicReference<>();
  private final AtomicReference<ServerId> gc = new AtomicReference<>();

  // index of resource group name to set of servers
  private final Map<String,Set<ServerId>> compactors = new ConcurrentHashMap<>();
  private final Map<String,Set<ServerId>> sservers = new ConcurrentHashMap<>();
  private final Map<String,Set<ServerId>> tservers = new ConcurrentHashMap<>();

  // Summaries of metrics by server type
  // map of metric name to metric values
  private final Map<Id,CumulativeDistributionSummary> totalCompactorMetrics =
      new ConcurrentHashMap<>();
  private final Map<Id,CumulativeDistributionSummary> totalSServerMetrics =
      new ConcurrentHashMap<>();
  private final Map<Id,CumulativeDistributionSummary> totalTServerMetrics =
      new ConcurrentHashMap<>();

  // Summaries of metrics by server type and resource group
  // map of resource group to metric name to metric values
  private final Map<String,Map<Id,CumulativeDistributionSummary>> rgCompactorMetrics =
      new ConcurrentHashMap<>();
  private final Map<String,Map<Id,CumulativeDistributionSummary>> rgSServerMetrics =
      new ConcurrentHashMap<>();
  private final Map<String,Map<Id,CumulativeDistributionSummary>> rgTServerMetrics =
      new ConcurrentHashMap<>();

  // Compaction Information
  private final Map<String,List<FMetric>> queueMetrics = new ConcurrentHashMap<>();
  private volatile Set<ServerId> registeredCompactors = Set.of();
  private volatile HostAndPort coordinatorHost;

  protected final Map<String,TimeOrderedRunningCompactionSet> longRunningCompactionsByRg =
      new ConcurrentHashMap<>();

  protected final Map<TableId,LongAdder> runningCompactionsPerTable = new ConcurrentHashMap<>();
  protected final Map<String,LongAdder> runningCompactionsPerGroup = new ConcurrentHashMap<>();

  // Table Information
  private final Map<TableId,TableSummary> tables = new ConcurrentHashMap<>();
  private final Map<TableId,List<TabletInformation>> tablets = new ConcurrentHashMap<>();

  // Deployment Overview
  private final Map<ResourceGroupId,Map<ServerId.Type,ProcessSummary>> deployment =
      new ConcurrentHashMap<>();

  private final Set<String> suggestions = new ConcurrentSkipListSet<>();

  private final Set<String> configuredCompactionResourceGroups = ConcurrentHashMap.newKeySet();

  private long timestamp = 0;
  private ScanServerView scanServerView = new ScanServerView(0L, List.of(),
      new ScanServerView.Status(false, false, false, 0, 0, 0L, "OK", null));
  private DeploymentOverview deploymentOverview = new DeploymentOverview(0L, List.of());
  private final int rgLongRunningCompactionSize;

  public SystemInformation(Cache<ServerId,MetricResponse> allMetrics, ServerContext ctx) {
    this.allMetrics = allMetrics;
    this.ctx = ctx;
    this.rgLongRunningCompactionSize =
        this.ctx.getConfiguration().getCount(Property.MONITOR_LONG_RUNNING_COMPACTION_LIMIT);
  }

  public void clear() {
    resourceGroups.clear();
    problemHosts.clear();
    metricProblemHosts.clear();
    compactors.clear();
    sservers.clear();
    tservers.clear();
    totalCompactorMetrics.clear();
    totalSServerMetrics.clear();
    totalTServerMetrics.clear();
    rgCompactorMetrics.clear();
    rgSServerMetrics.clear();
    rgTServerMetrics.clear();
    queueMetrics.clear();
    registeredCompactors = Set.of();
    coordinatorHost = null;
    longRunningCompactionsByRg.clear();
    tables.clear();
    tablets.clear();
    deployment.clear();
    suggestions.clear();
    runningCompactionsPerGroup.clear();
    runningCompactionsPerTable.clear();
    configuredCompactionResourceGroups.clear();
  }

  private void updateAggregates(final MetricResponse response,
      final Map<Id,CumulativeDistributionSummary> total,
      final Map<String,Map<Id,CumulativeDistributionSummary>> rg) {
    if (response.getMetrics() == null) {
      return;
    }

    final Map<Id,CumulativeDistributionSummary> rgMetrics =
        rg.computeIfAbsent(response.getResourceGroup(), (k) -> new ConcurrentHashMap<>());

    response.getMetrics().forEach((bb) -> {
      final FMetric fm = FMetric.getRootAsFMetric(bb);
      final String name = fm.name();
      FTag statisticTag = null;
      for (int i = 0; i < fm.tagsLength(); i++) {
        FTag t = fm.tags(i);
        if (t.key().equals(MetricResponseWrapper.STATISTIC_TAG)) {
          statisticTag = t;
          break;
        }
      }
      Number value = getMetricValue(fm);
      final Id id = new Id(name,
          (statisticTag == null) ? Tags.empty() : Tags.of(statisticTag.key(), statisticTag.value()),
          null, null, Type.valueOf(fm.type()));
      total
          .computeIfAbsent(id,
              (k) -> new CumulativeDistributionSummary(id, Clock.SYSTEM, DSC, 1.0, false))
          .record(value.doubleValue());
      rgMetrics
          .computeIfAbsent(id,
              (k) -> new CumulativeDistributionSummary(id, Clock.SYSTEM, DSC, 1.0, false))
          .record(value.doubleValue());
    });

  }

  private void createCompactionSummary(MetricResponse response) {
    if (response.getMetrics() != null) {
      for (final ByteBuffer binary : response.getMetrics()) {
        FMetric fm = FMetric.getRootAsFMetric(binary);
        for (int i = 0; i < fm.tagsLength(); i++) {
          FTag t = fm.tags(i);
          if (t.key().equals("queue.id")) {
            queueMetrics
                .computeIfAbsent(t.value(), (k) -> Collections.synchronizedList(new ArrayList<>()))
                .add(fm);
          }
        }
      }
    }
  }

  public void processResponse(final ServerId server, final MetricResponse response) {
    problemHosts.remove(server);
    metricProblemHosts.remove(server);
    allMetrics.put(server, response);
    resourceGroups.add(response.getResourceGroup());
    deployment.computeIfAbsent(server.getResourceGroup(), g -> new ConcurrentHashMap<>())
        .computeIfAbsent(server.getType(), t -> new ProcessSummary()).addResponded(server);
    switch (response.serverType) {
      case COMPACTOR:
        compactors
            .computeIfAbsent(response.getResourceGroup(), (rg) -> ConcurrentHashMap.newKeySet())
            .add(server);
        updateAggregates(response, totalCompactorMetrics, rgCompactorMetrics);
        break;
      case GARBAGE_COLLECTOR:
        if (gc.get() == null || !gc.get().equals(server)) {
          gc.set(server);
        }
        break;
      case MANAGER:
        if (manager.get() == null || !manager.get().equals(server)) {
          manager.set(server);
        }
        createCompactionSummary(response);
        break;
      case SCAN_SERVER:
        sservers.computeIfAbsent(response.getResourceGroup(), (rg) -> ConcurrentHashMap.newKeySet())
            .add(server);
        updateAggregates(response, totalSServerMetrics, rgSServerMetrics);
        break;
      case TABLET_SERVER:
        tservers.computeIfAbsent(response.getResourceGroup(), (rg) -> ConcurrentHashMap.newKeySet())
            .add(server);
        updateAggregates(response, totalTServerMetrics, rgTServerMetrics);
        break;
      default:
        LOG.error("Unhandled server type in fetch metric response: {}", response.serverType);
        break;
    }
  }

  public void processExternalCompaction(TExternalCompaction tec) {
    var tableId = KeyExtent.fromThrift(tec.getJob().extent).tableId();
    runningCompactionsPerTable.computeIfAbsent(tableId, t -> new LongAdder()).increment();
    runningCompactionsPerGroup.computeIfAbsent(tec.getGroupName(), t -> new LongAdder())
        .increment();

    this.longRunningCompactionsByRg
        .computeIfAbsent(tec.getGroupName(),
            k -> new TimeOrderedRunningCompactionSet(rgLongRunningCompactionSize))
        .add(new RunningCompactionInfo(tec));
  }

  public void processExternalCompactionInventory(Set<ServerId> compactors, HostAndPort host) {
    if (compactors == null) {
      registeredCompactors = Set.of();
    } else {
      registeredCompactors = Set.copyOf(compactors);
    }
    coordinatorHost = host;
  }

  public void processTabletInformation(TableId tableId, String tableName, TabletInformation info) {
    final SanitizedTabletInformation sti = new SanitizedTabletInformation(info);
    tablets.computeIfAbsent(tableId, (t) -> Collections.synchronizedList(new ArrayList<>()))
        .add(sti);
    tables.computeIfAbsent(tableId, (t) -> new TableSummary(tableName)).addTablet(sti);
    if (sti.getEstimatedEntries() == 0) {
      suggestions.add("Tablet " + sti.getTabletId().toString() + " (tid: "
          + sti.getTabletId().getTable() + ") may have zero entries and could be merged.");
    }
  }

  public void processError(ServerId server) {
    problemHosts.add(server);
  }

  public void processMetricsError(ServerId server) {
    problemHosts.add(server);
    metricProblemHosts.add(server);
  }

  public void addConfiguredCompactionGroups(Set<String> groups) {
    configuredCompactionResourceGroups.addAll(groups);
  }

  public void finish() {
    // Update the deployment not-responded numbers based
    // on metric fetch failures for this refresh.
    metricProblemHosts.forEach(serverId -> {
      deployment.computeIfAbsent(serverId.getResourceGroup(), g -> new ConcurrentHashMap<>())
          .computeIfAbsent(serverId.getType(), t -> new ProcessSummary()).addNotResponded(serverId);
    });
    for (SystemTables table : SystemTables.values()) {
      TableConfiguration tconf = this.ctx.getTableConfiguration(table.tableId());
      String balancerRG = tconf.get(TableLoadBalancer.TABLE_ASSIGNMENT_GROUP_PROPERTY);
      balancerRG = balancerRG == null ? Constants.DEFAULT_RESOURCE_GROUP_NAME : balancerRG;
      if (!tservers.containsKey(balancerRG)) {
        suggestions.add("Table " + table.tableName() + " configured to balance tablets in resource"
            + " group " + balancerRG + ", but there are no TabletServers.");
      }
    }
    for (String rg : getResourceGroups()) {
      Set<ServerId> rgCompactors = getCompactorResourceGroupServers(rg);
      List<FMetric> metrics = queueMetrics.get(rg);
      if (metrics == null || metrics.isEmpty()) {
        continue;
      }
      Optional<FMetric> queued = metrics.stream()
          .filter(fm -> fm.name().equals(Metric.COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED.getName()))
          .findFirst();
      if (queued.isPresent()) {
        Number numQueued = getMetricValue(queued.orElseThrow());
        if (numQueued.longValue() > 0) {
          if (rgCompactors == null || rgCompactors.size() == 0) {
            suggestions.add("Compactor group " + rg + " has " + numQueued.longValue()
                + " queued compactions but no running compactors");
          } else {
            // Check for idle compactors.
            Map<Id,CumulativeDistributionSummary> rgMetrics =
                getCompactorResourceGroupMetricSummary(rg);
            if (rgMetrics == null || rgMetrics.isEmpty()) {
              continue;
            }
            Optional<Entry<Id,CumulativeDistributionSummary>> idleMetric = rgMetrics.entrySet()
                .stream().filter(e -> e.getKey().getName().equals(Metric.SERVER_IDLE.getName()))
                .findFirst();
            if (idleMetric.isPresent()) {
              var metric = idleMetric.orElseThrow().getValue();
              if (metric.max() == 1.0D) {
                suggestions.add("Compactor group " + rg + " has queued jobs and idle compactors.");
              }
            }

          }
        }
      }
    }

    for (var compactorGroup : compactors.keySet()) {
      if (!configuredCompactionResourceGroups.contains(compactorGroup)) {
        suggestions.add("Compactor group " + compactorGroup
            + " has running compactors, but no configuration uses them.");
      }
    }

    Set<ServerId> scanServers = new HashSet<>();
    sservers.values().forEach(scanServers::addAll);
    int problemScanServerCount = (int) problemHosts.stream()
        .filter(serverId -> serverId.getType() == ServerId.Type.SCAN_SERVER).count();
    var responses = allMetrics.getAllPresent(scanServers).values();
    timestamp = System.currentTimeMillis();
    deploymentOverview = DeploymentOverview.fromSummary(deployment, timestamp);
    scanServerView = ScanServerView.fromMetrics(responses, scanServers.size(),
        problemScanServerCount, timestamp);
  }

  public Set<String> getResourceGroups() {
    return this.resourceGroups;
  }

  public Set<ServerId> getProblemHosts() {
    return this.problemHosts;
  }

  public ServerId getManager() {
    return this.manager.get();
  }

  public ServerId getGarbageCollector() {
    return this.gc.get();
  }

  public Set<ServerId> getCompactorResourceGroupServers(String resourceGroup) {
    return this.compactors.get(resourceGroup);
  }

  public Map<Id,CumulativeDistributionSummary>
      getCompactorResourceGroupMetricSummary(String resourceGroup) {
    return this.rgCompactorMetrics.get(resourceGroup);
  }

  public Map<Id,CumulativeDistributionSummary> getCompactorAllMetricSummary() {
    return this.totalCompactorMetrics;
  }

  public Set<ServerId> getSServerResourceGroupServers(String resourceGroup) {
    return this.sservers.get(resourceGroup);
  }

  public Map<Id,CumulativeDistributionSummary>
      getSServerResourceGroupMetricSummary(String resourceGroup) {
    return this.rgSServerMetrics.get(resourceGroup);
  }

  public Map<Id,CumulativeDistributionSummary> getSServerAllMetricSummary() {
    return this.totalSServerMetrics;
  }

  public Set<ServerId> getTServerResourceGroupServers(String resourceGroup) {
    return this.tservers.get(resourceGroup);
  }

  public Map<Id,CumulativeDistributionSummary>
      getTServerResourceGroupMetricSummary(String resourceGroup) {
    return this.rgTServerMetrics.get(resourceGroup);
  }

  public Map<Id,CumulativeDistributionSummary> getTServerAllMetricSummary() {
    return this.totalTServerMetrics;
  }

  public Map<String,List<FMetric>> getCompactionMetricSummary() {
    return this.queueMetrics;
  }

  public Set<ServerId> getCompactorServers() {
    return registeredCompactors;
  }

  public HostAndPort getCoordinatorHost() {
    return coordinatorHost;
  }

  public Map<String,TimeOrderedRunningCompactionSet> getTopRunningCompactions() {
    return this.longRunningCompactionsByRg;
  }

  public Map<TableId,TableSummary> getTables() {
    return this.tables;
  }

  public List<TabletInformation> getTablets(TableId tableId) {
    return this.tablets.get(tableId);
  }

  public DeploymentOverview getDeploymentView() {
    return this.deploymentOverview;
  }

  public Set<String> getSuggestions() {
    return this.suggestions;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public ScanServerView getScanServerView() {
    return this.scanServerView;
  }

  public static Number getMetricValue(FMetric metric) {
    if (metric.ivalue() != 0) {
      return metric.ivalue();
    }
    if (metric.lvalue() != 0L) {
      return metric.lvalue();
    }
    if (metric.dvalue() != 0.0d) {
      return metric.dvalue();
    }
    return 0;
  }
}

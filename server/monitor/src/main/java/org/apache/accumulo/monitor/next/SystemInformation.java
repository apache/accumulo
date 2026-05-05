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

import static com.google.common.base.Suppliers.memoize;
import static org.apache.accumulo.core.metrics.MetricsInfo.QUEUE_TAG_KEY;
import static org.apache.accumulo.monitor.next.SystemInformation.MessageCategory.Configuration;
import static org.apache.accumulo.monitor.next.SystemInformation.MessageCategory.Monitor;
import static org.apache.accumulo.monitor.next.SystemInformation.MessageCategory.Resource;
import static org.apache.accumulo.monitor.next.SystemInformation.MessageCategory.Table;
import static org.apache.accumulo.monitor.next.SystemInformation.MessagePriority.Critical;
import static org.apache.accumulo.monitor.next.SystemInformation.MessagePriority.High;
import static org.apache.accumulo.monitor.next.SystemInformation.MessagePriority.Info;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNotFoundException;
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
import org.apache.accumulo.monitor.next.InformationFetcher.MetricFetcher;
import org.apache.accumulo.monitor.next.InformationFetcher.TableInformationFetcher;
import org.apache.accumulo.monitor.next.InformationFetcher.UpdateTaskFuture;
import org.apache.accumulo.monitor.next.InformationFetcher.UpdateTasks;
import org.apache.accumulo.monitor.next.deployment.DeploymentOverview;
import org.apache.accumulo.monitor.next.views.ColumnFactory;
import org.apache.accumulo.monitor.next.views.Status;
import org.apache.accumulo.monitor.next.views.TableData;
import org.apache.accumulo.monitor.next.views.TableData.Column;
import org.apache.accumulo.monitor.next.views.TableDataFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.metrics.MetricResponseWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;

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

  public record CompactionTableSummary(String tableId, String tableName, long running) {
  }

  public record CompactionGroupSummary(String groupId, long running) {
  }

  public enum MessagePriority {
    Critical, High, Info;
  }

  public enum MessageCategory {
    Configuration, Monitor, Resource, Table;
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
  private final Set<ServerId> retainedProblemHosts = ConcurrentHashMap.newKeySet();
  private final Set<ServerId> managers = ConcurrentHashMap.newKeySet();
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

  protected final Map<String,TimeOrderedRunningCompactionSet> longRunningCompactionsByRg =
      new ConcurrentHashMap<>();

  protected final Map<TableId,LongAdder> runningCompactionsPerTable = new ConcurrentHashMap<>();
  protected final Map<String,LongAdder> runningCompactionsPerGroup = new ConcurrentHashMap<>();

  private final List<CompactionTableSummary> tableCompactions = new ArrayList<>();
  private final List<CompactionGroupSummary> groupCompactions = new ArrayList<>();

  // Table Information
  private final Map<TableId,TableSummary> tables = new ConcurrentHashMap<>();
  private final Map<TableId,List<TabletInformation>> tablets = new ConcurrentHashMap<>();

  // Deployment Overview
  private final Map<ResourceGroupId,Map<ServerId.Type,ProcessSummary>> deployment =
      new ConcurrentHashMap<>();

  private final Map<MessagePriority,Map<MessageCategory,Set<String>>> messages =
      new EnumMap<>(MessagePriority.class);

  private final Set<String> configuredCompactionResourceGroups = ConcurrentHashMap.newKeySet();

  private final AtomicLong timestamp = new AtomicLong(0);
  private final EnumMap<ServerId.Type,Status> componentStatuses =
      new EnumMap<>(ServerId.Type.class);
  private final EnumMap<TableDataFactory.TableName,Supplier<TableData>> serverMetricsView =
      new EnumMap<>(TableDataFactory.TableName.class);
  private DeploymentOverview deploymentOverview = new DeploymentOverview(0L, List.of());
  private String managerGoalState;
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
    retainedProblemHosts.clear();
    managers.clear();
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
    longRunningCompactionsByRg.clear();
    tables.clear();
    tablets.clear();
    deployment.clear();
    messages.clear();
    runningCompactionsPerGroup.clear();
    runningCompactionsPerTable.clear();
    tableCompactions.clear();
    groupCompactions.clear();
    configuredCompactionResourceGroups.clear();
    componentStatuses.clear();
    managerGoalState = null;
    serverMetricsView.clear();
  }

  public void addMessage(MessagePriority pri, MessageCategory cat, String msg) {
    messages.computeIfAbsent(pri, k -> new EnumMap<>(MessageCategory.class))
        .computeIfAbsent(cat, k -> new TreeSet<>()).add(msg);
  }

  public void removeMessage(MessagePriority pri, MessageCategory cat, String part) {
    messages.getOrDefault(pri, new EnumMap<>(MessageCategory.class))
        .getOrDefault(cat, new HashSet<String>()).removeIf(s -> s.contains(part));
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

  private TableData createCompactionQueueSummary(final Set<ServerId> managers) {

    final Column COMPACTION_QUEUE_COL =
        new Column(TableDataFactory.RG_COL_KEY, "Compaction Queue", "Compaction Queue", "");

    List<ColumnFactory> cols =
        TableDataFactory.columnsFor(TableDataFactory.TableName.COORDINATOR_QUEUES);

    // Remove the column mapping for the resource group and replace it so that
    // the column header reads "Compaction Queue" instead of "Resource Group"
    int rgIdx = -1;
    for (int idx = 0; idx < cols.size(); idx++) {
      if (cols.get(idx).getColumn().key().equals(TableDataFactory.RG_COL_KEY)) {
        rgIdx = idx;
        break;
      }
    }

    if (rgIdx == -1) {
      LOG.warn("Did not find Resource Group column to replace column header");
    } else {
      cols.remove(rgIdx);
      cols.set(rgIdx, new ColumnFactory() {

        @Override
        public Column getColumn() {
          return COMPACTION_QUEUE_COL;
        }

        @Override
        public Object getRowData(ServerId sid, MetricResponse mr,
            Map<String,List<FMetric>> serverMetrics) {
          return sid.getResourceGroup().canonical();
        }
      });
    }

    // Construct a Map of MetricResponses by Queue. This method will take
    // the provided MetricResponse and construct new ones that contain
    // only the metrics with the "queue.id" tag in addition to the common
    // server information (address, resource group, etc.).
    Map<ServerId,MetricResponse> qm = new HashMap<>();

    for (ServerId manager : managers) {
      MetricResponse response = allMetrics.getIfPresent(manager);
      if (response.getMetrics() != null) {

        FMetric fm = new FMetric();
        FTag t = new FTag();
        for (final ByteBuffer binary : response.getMetrics()) {
          fm = FMetric.getRootAsFMetric(binary, fm);
          for (int i = 0; i < fm.tagsLength(); i++) {
            t = fm.tags(t, i);
            if (t.key().equals(QUEUE_TAG_KEY)) {
              String queueName = t.value();
              // For these MetricResponse objects we are going to put the queueId value
              // in the place of the resource group, we'll update the column information
              // for the resource group below.
              ServerId sid = new ServerId(manager.getType(), ResourceGroupId.of(queueName),
                  manager.getHost(), manager.getPort());
              qm.computeIfAbsent(sid, (k) -> new MetricResponse(response.getServerType(),
                  response.getServer(), queueName, response.getTimestamp(), new ArrayList<>()))
                  .addToMetrics(binary);
              break;
            }
          }
        }
      }
    }

    if (!qm.isEmpty()) {
      // Create a ServersView object from the MetricResponse for each queue
      return TableDataFactory.forColumns(qm.keySet(), qm, timestamp.get(), cols);
    }
    return TableDataFactory.forColumns(Set.of(), Map.of(), timestamp.get(), cols);
  }

  public void processResponse(final ServerId server, final MetricResponse response,
      final UpdateTasks callback) {
    problemHosts.remove(server);
    metricProblemHosts.remove(server);
    retainedProblemHosts.remove(server);
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
        managers.add(server);
        FMetric flatbuffer = new FMetric();
        for (ByteBuffer binary : response.getMetrics()) {
          flatbuffer = FMetric.getRootAsFMetric(binary, flatbuffer);
          final String metricName = flatbuffer.name();
          if (metricName.equals(Metric.MANAGER_ROOT_TGW_RECOVERY.getName())
              && getMetricValue(flatbuffer).longValue() > 0) {
            addMessage(Critical, Table, "The root table requires recovery");
          } else if (metricName.equals(Metric.MANAGER_META_TGW_RECOVERY.getName())) {
            long tablets = getMetricValue(flatbuffer).longValue();
            if (tablets > 0) {
              callback.stopCollectingTableInformation();
              addMessage(Critical, Table, tablets + " metadata table tablets require recovery");
            }
          } else if (metricName.equals(Metric.MANAGER_USER_TGW_RECOVERY.getName())) {
            long tablets = getMetricValue(flatbuffer).longValue();
            if (tablets > 0) {
              callback.stopCollectingTableInformation();
              addMessage(High, Table, tablets + " user table tablets require recovery");
            }
          }
        }
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

  public void processExternalCompactionInventory(Set<ServerId> compactors) {
    if (compactors == null) {
      registeredCompactors = Set.of();
    } else {
      registeredCompactors = Set.copyOf(compactors);
    }
  }

  public void processTabletInformation(TableId tableId, String tableName, TabletInformation info) {
    final SanitizedTabletInformation sti = new SanitizedTabletInformation(info);
    tablets.computeIfAbsent(tableId, (t) -> Collections.synchronizedList(new ArrayList<>()))
        .add(sti);
    tables.computeIfAbsent(tableId, (t) -> new TableSummary(tableName)).addTablet(sti);
  }

  public void processError(ServerId server) {
    problemHosts.add(server);
  }

  public void processMetricsError(ServerId server) {
    problemHosts.add(server);
    metricProblemHosts.add(server);
    allMetrics.invalidate(server);
  }

  public void retainProblemServer(ServerId server) {
    problemHosts.add(server);
    metricProblemHosts.add(server);
    retainedProblemHosts.add(server);
    resourceGroups.add(server.getResourceGroup().canonical());
  }

  public void addConfiguredCompactionGroups(Set<String> groups) {
    configuredCompactionResourceGroups.addAll(groups);
  }

  private void computeMessages(final List<UpdateTaskFuture> failures,
      final List<UpdateTaskFuture> cancelled) {

    if (failures.size() > 0) {
      addMessage(High, Monitor,
          "There were " + failures.size() + " failures in the last monitor update cycle."
              + " Information displayed may be out of date or missing.");
    }

    if (cancelled.size() > 0) {
      final long monitorFetchTimeout =
          ctx.getConfiguration().getTimeInMillis(Property.MONITOR_FETCH_TIMEOUT);
      String message =
          "Fetching information for Monitor has taken longer than %1$d ms. (%2$d) tasks were cancelled."
              + " Information displayed may be out of date or missing. Resolve the issue causing this or increase property `%3$s`."
                  .formatted(monitorFetchTimeout, cancelled.size(),
                      Property.MONITOR_FETCH_TIMEOUT.getKey());
      addMessage(High, Monitor, message);
    }

    Set<ServerId> failedOrCancelledServers = new HashSet<>();
    Set<TableId> failedOrCancelledTables = new HashSet<>();
    for (UpdateTaskFuture f : failures) {
      switch (f.task().getType()) {
        case COMPACTION:
          addMessage(Info, Monitor,
              "The task to get information about currently running compactions failed");
          break;
        case COMPACTION_RGS:
          addMessage(Info, Monitor,
              "The task to get information about configured compaction resource groups failed");
          break;
        case METRIC:
          ServerId s = ((MetricFetcher) f.task()).getResource();
          failedOrCancelledServers.add(s);
          break;
        case TABLE:
          TableId t = ((TableInformationFetcher) f.task()).getResource();
          failedOrCancelledTables.add(t);
          break;
        default:
          break;
      }
    }

    for (UpdateTaskFuture f : cancelled) {
      switch (f.task().getType()) {
        case COMPACTION:
          addMessage(Info, Monitor,
              "The task to get information about currently running compactions was cancelled");
          break;
        case COMPACTION_RGS:
          addMessage(Info, Monitor,
              "The task to get information about configured compaction resource groups was cancelled");
          break;
        case METRIC:
          ServerId s = ((MetricFetcher) f.task()).getResource();
          failedOrCancelledServers.add(s);
          break;
        case TABLE:
          TableId t = ((TableInformationFetcher) f.task()).getResource();
          failedOrCancelledTables.add(t);
          break;
        default:
          break;
      }
    }

    if (failedOrCancelledServers.size() > 0) {
      addMessage(High, Monitor, failedOrCancelledServers.size()
          + " tasks to get information from servers were failed or cancelled.");
      addMessage(Info, Monitor,
          "The Monitor is not displaying updated information for the following servers: "
              + failedOrCancelledServers);
    }

    if (failedOrCancelledTables.size() > 0) {
      addMessage(High, Monitor, failedOrCancelledTables.size()
          + " tasks to get information for tables were failed or cancelled.");
      addMessage(Info, Monitor,
          "The Monitor is not displaying updated information for the following tables: "
              + failedOrCancelledTables);
    }

    if (managers.isEmpty()) {
      addMessage(Critical, Resource, "No Managers are running");
    }

    if (gc.get() == null) {
      addMessage(Critical, Resource, "Garbage Collector is not running");
    }

    if (problemHosts.size() > 0) {
      addMessage(Info, Resource, "Monitor has not recevied a response from " + problemHosts.size()
          + " servers in the last 10 minutes");
    }

    if (metricProblemHosts.size() > 0) {
      addMessage(Info, Resource,
          "Unable to gather information from " + metricProblemHosts.size() + " servers");
    }

    for (ResourceGroupId rg : ctx.resourceGroupOperations().list()) {
      if (rg == ResourceGroupId.DEFAULT) {
        continue;
      }
      if (!compactors.containsKey(rg.canonical()) && !sservers.containsKey(rg.canonical())
          && !tservers.containsKey(rg.canonical())) {
        addMessage(Info, Configuration, "Resource Group " + rg
            + " exists, but no resources assigned. Consider removing the resource group with command `accumulo inst init --remove-resource-groups`");
      }
    }

    tablets.forEach((tid, tablets) -> {
      int empty = 0;
      for (TabletInformation tablet : tablets) {
        if (tablet.getEstimatedEntries() == 0) {
          empty++;
        }
      }
      if (empty > 0) {
        addMessage(Info, Table,
            "Table " + tid + " may have " + empty + " tablets that could be merged.");
      }
    });

    for (SystemTables table : SystemTables.values()) {
      TableConfiguration tconf = this.ctx.getTableConfiguration(table.tableId());
      String balancerRG = tconf.get(TableLoadBalancer.TABLE_ASSIGNMENT_GROUP_PROPERTY);
      balancerRG = balancerRG == null ? Constants.DEFAULT_RESOURCE_GROUP_NAME : balancerRG;
      if (!tservers.containsKey(balancerRG)) {
        addMessage(Critical, Table,
            "Table " + table.tableName() + " configured to balance tablets in resource group "
                + balancerRG + ", but there are no TabletServers.");
      }
    }

    FMetric flatbuffer = new FMetric();
    long serversWithZombieScans = 0;
    for (Entry<ServerId,MetricResponse> e : allMetrics.asMap().entrySet()) {
      ServerId sid = e.getKey();
      List<ByteBuffer> metrics = e.getValue().metrics;
      if (sid.getType() == ServerId.Type.SCAN_SERVER
          || sid.getType() == ServerId.Type.TABLET_SERVER) {
        for (ByteBuffer binary : metrics) {
          flatbuffer = FMetric.getRootAsFMetric(binary, flatbuffer);
          if (flatbuffer.name().equals(Metric.SCAN_ZOMBIE_THREADS.getName())) {
            if (getMetricValue(flatbuffer).longValue() > 0) {
              serversWithZombieScans++;
            }
          }
        }
      }
    }
    if (serversWithZombieScans > 0) {
      addMessage(High, Resource,
          "There are " + serversWithZombieScans + " servers with zombie scan threads");
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
            addMessage(Critical, Configuration, "Compactor group " + rg + " has "
                + numQueued.longValue() + " queued compactions but no running compactors");
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
                addMessage(High, Resource,
                    "Compactor group " + rg + " has queued jobs and idle compactors.");
              }
            }

          }
        }
      }
    }

    for (var compactorGroup : compactors.keySet()) {
      if (!configuredCompactionResourceGroups.contains(compactorGroup)) {
        addMessage(High, Configuration, "Compactor group " + compactorGroup
            + " has running compactors, but no configuration uses them.");
      }
    }

  }

  public void finish(final List<UpdateTaskFuture> failures,
      final List<UpdateTaskFuture> cancelled) {
    // Update the deployment not-responded numbers based
    // on metric fetch failures for this refresh.
    metricProblemHosts.forEach(serverId -> {
      deployment.computeIfAbsent(serverId.getResourceGroup(), g -> new ConcurrentHashMap<>())
          .computeIfAbsent(serverId.getType(), t -> new ProcessSummary()).addNotResponded(serverId);
    });

    computeMessages(failures, cancelled);

    timestamp.set(System.currentTimeMillis());
    componentStatuses.clear();
    for (final ServerId.Type type : ServerId.Type.values()) {
      if (type == ServerId.Type.MONITOR) {
        continue;
      }
      componentStatuses.put(type, computeServerStatus(type));
    }
    managerGoalState = computeManagerGoalState();

    for (Entry<TableId,LongAdder> e : runningCompactionsPerTable.entrySet()) {
      TableId tid = e.getKey();
      try {
        tableCompactions.add(new CompactionTableSummary(tid.canonical(),
            ctx.getQualifiedTableName(tid), e.getValue().sum()));
      } catch (TableNotFoundException e1) {
        LOG.warn("Error converting table id {} to table name, caught TableNotFoundException", tid);
      }
    }

    runningCompactionsPerGroup
        .forEach((k, v) -> groupCompactions.add(new CompactionGroupSummary(k, v.sum())));

    for (final ServerId.Type type : ServerId.Type.values()) {
      Set<ServerId> servers = getServers(type);
      switch (type) {
        case COMPACTOR:
          cacheServerProcessView(TableDataFactory.TableName.COMPACTORS, servers);
          break;
        case GARBAGE_COLLECTOR:
          cacheServerProcessView(TableDataFactory.TableName.GC_SUMMARY, servers);
          cacheServerProcessView(TableDataFactory.TableName.GC_FILES, servers);
          cacheServerProcessView(TableDataFactory.TableName.GC_WALS, servers);
          break;
        case MANAGER:
          cacheServerProcessView(TableDataFactory.TableName.MANAGERS, servers);
          cacheServerProcessView(TableDataFactory.TableName.MANAGER_FATE, servers);
          cacheServerProcessView(TableDataFactory.TableName.MANAGER_COMPACTIONS, servers);
          TableData coordinatorQueues = createCompactionQueueSummary(getActiveServers(type));
          serverMetricsView.put(TableDataFactory.TableName.COORDINATOR_QUEUES,
              memoize(() -> coordinatorQueues));
          break;
        case SCAN_SERVER:
          cacheServerProcessView(TableDataFactory.TableName.SCAN_SERVERS, servers);
          break;
        case TABLET_SERVER:
          cacheServerProcessView(TableDataFactory.TableName.TABLET_SERVERS, servers);
          break;
        case MONITOR:
        default:
          break;
      }
    }
    deploymentOverview = DeploymentOverview.fromSummary(deployment, timestamp);
  }

  public Set<String> getResourceGroups() {
    return this.resourceGroups;
  }

  public Set<ServerId> getProblemHosts() {
    return this.problemHosts;
  }

  public Set<ServerId> getManagers() {
    return this.managers;
  }

  public Status getServerStatus(ServerId.Type type) {
    return componentStatuses.get(type);
  }

  public Map<ServerId.Type,Status> getComponentStatuses() {
    return new EnumMap<>(componentStatuses);
  }

  public String getManagerGoalState() {
    return managerGoalState;
  }

  private Status computeServerStatus(ServerId.Type type) {
    Set<ServerId> servers = getServers(type);
    long problemHostCount =
        problemHosts.stream().filter(serverId -> serverId.getType() == type).count();
    int missingMetricCount = (int) servers.stream()
        .filter(serverId -> !TableDataFactory.hasMetricData(allMetrics.getIfPresent(serverId)))
        .count();
    return Status.buildStatus(servers.size(), problemHostCount, missingMetricCount,
        type == ServerId.Type.TABLET_SERVER);
  }

  private String computeManagerGoalState() {
    Integer goalState = managers.stream().map(allMetrics::getIfPresent)
        .map(response -> TableDataFactory.metricValuesByName(response)
            .get(Metric.MANAGER_GOAL_STATE.getName()))
        .filter(value -> value != null && !value.isEmpty())
        .map(value -> value.stream().map(SystemInformation::getMetricValue).filter(Objects::nonNull)
            .map(Number::intValue).min(Comparator.naturalOrder()).orElse(null))
        .filter(Objects::nonNull).min(Comparator.naturalOrder()).orElse(null);

    return switch (goalState == null ? -1 : goalState) {
      case 0 -> "CLEAN_STOP";
      case 1 -> "SAFE_MODE";
      case 2 -> "NORMAL";
      default -> null;
    };
  }

  private Set<ServerId> getServers(ServerId.Type type) {
    Set<ServerId> servers = new HashSet<>(getActiveServers(type));
    retainedProblemHosts.stream().filter(serverId -> serverId.getType() == type)
        .forEach(servers::add);
    return servers;
  }

  private Set<ServerId> getActiveServers(ServerId.Type type) {
    return switch (type) {
      case COMPACTOR -> getAll(compactors);
      case GARBAGE_COLLECTOR -> {
        final var gcServer = gc.get();
        yield gcServer == null ? Set.of() : Set.of(gcServer);
      }
      case MANAGER -> Set.copyOf(managers);
      case SCAN_SERVER -> getAll(sservers);
      case TABLET_SERVER -> getAll(tservers);
      case MONITOR -> Set.of();
    };
  }

  private static Set<ServerId> getAll(Map<String,Set<ServerId>> groupedServers) {
    return groupedServers.values().stream().flatMap(Set::stream).collect(HashSet::new, Set::add,
        Set::addAll);
  }

  public ServerId getGarbageCollector() {
    return this.gc.get();
  }

  public List<CompactionGroupSummary> getRunningCompactionsPerGroup() {
    return this.groupCompactions;
  }

  public List<CompactionTableSummary> getRunningCompactionsPerTable() {
    return this.tableCompactions;
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

  public Map<MessagePriority,Map<MessageCategory,Set<String>>> getMessages() {
    return this.messages;
  }

  public long getTimestamp() {
    return this.timestamp.get();
  }

  /**
   * Cache a ServersView for the given table and set of servers.
   */
  private void cacheServerProcessView(TableDataFactory.TableName table, Set<ServerId> servers) {
    serverMetricsView.put(table, memoize(
        () -> TableDataFactory.forTable(table, servers, allMetrics.asMap(), timestamp.get())));
  }

  public TableData getServerProcessView(TableDataFactory.TableName table) {
    Supplier<TableData> view = this.serverMetricsView.get(table);
    if (view != null) {
      return view.get();
    }
    return null;
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

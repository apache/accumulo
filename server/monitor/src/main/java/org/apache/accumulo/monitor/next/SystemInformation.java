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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TExternalCompactionList;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.metadata.TabletState;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.monitor.next.InformationFetcher.GcServerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
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
    private long configured = 0;
    private long responded = 0;
    private Set<String> notResponded = new HashSet<>();

    public void addResponded() {
      configured++;
      responded++;
    }

    public void addNotResponded(ServerId server) {
      notResponded.add(server.getHost() + ":" + server.getPort());
    }

    public long getConfigured() {
      return this.configured;
    }

    public long getResponded() {
      return this.responded;
    }

    public long getNotResponded() {
      return this.notResponded.size();
    }

    public Set<String> getNotRespondedHosts() {
      return this.notResponded;
    }

  }

  private static final Logger LOG = LoggerFactory.getLogger(SystemInformation.class);

  private final DistributionStatisticConfig DSC =
      DistributionStatisticConfig.builder().percentilePrecision(1).minimumExpectedValue(0.1)
          .maximumExpectedValue(Double.POSITIVE_INFINITY).expiry(Duration.ofMinutes(10))
          .bufferLength(3).build();

  private final Cache<ServerId,MetricResponse> allMetrics;

  private final Set<String> resourceGroups = new HashSet<>();
  private final Set<ServerId> problemHosts = new HashSet<>();
  private final AtomicReference<ServerId> manager = new AtomicReference<>();
  private final AtomicReference<ServerId> gc = new AtomicReference<>();

  // index of resource group name to set of servers
  private final Map<String,Set<ServerId>> compactors = new ConcurrentHashMap<>();
  private final Map<String,Set<ServerId>> sservers = new ConcurrentHashMap<>();
  private final Map<String,Set<ServerId>> tservers = new ConcurrentHashMap<>();

  // Summaries of metrics by server type
  // map of metric name to metric values
  private final Map<String,CumulativeDistributionSummary> totalCompactorMetrics =
      new ConcurrentHashMap<>();
  private final Map<String,CumulativeDistributionSummary> totalSServerMetrics =
      new ConcurrentHashMap<>();
  private final Map<String,CumulativeDistributionSummary> totalTServerMetrics =
      new ConcurrentHashMap<>();

  // Summaries of metrics by server type and resource group
  // map of resource group to metric name to metric values
  private final Map<String,Map<String,CumulativeDistributionSummary>> rgCompactorMetrics =
      new ConcurrentHashMap<>();
  private final Map<String,Map<String,CumulativeDistributionSummary>> rgSServerMetrics =
      new ConcurrentHashMap<>();
  private final Map<String,Map<String,CumulativeDistributionSummary>> rgTServerMetrics =
      new ConcurrentHashMap<>();

  // Compaction Information
  private final AtomicReference<Map<String,TExternalCompaction>> runningCompactions =
      new AtomicReference<>();
  private final AtomicReference<Map<Long,String>> runningCompactionsDurationIndex =
      new AtomicReference<>();

  // Table Information
  private final Map<String,TableSummary> tables = new ConcurrentHashMap<>();
  private final Map<String,List<TabletInformation>> tablets = new ConcurrentHashMap<>();

  // Deployment Overview
  private final Map<String,Map<String,ProcessSummary>> deployment = new HashMap<>();

  public SystemInformation(Cache<ServerId,MetricResponse> allMetrics) {
    this.allMetrics = allMetrics;
  }

  public void clear() {
    resourceGroups.clear();
    compactors.clear();
    sservers.clear();
    tservers.clear();
    totalCompactorMetrics.clear();
    totalSServerMetrics.clear();
    totalTServerMetrics.clear();
    rgCompactorMetrics.clear();
    rgSServerMetrics.clear();
    rgTServerMetrics.clear();
  }

  private void updateAggregates(final MetricResponse response,
      final Map<String,CumulativeDistributionSummary> total,
      final Map<String,Map<String,CumulativeDistributionSummary>> rg) {

    final Map<String,CumulativeDistributionSummary> rgMetrics =
        rg.computeIfAbsent(response.getResourceGroup(),
            (k) -> new ConcurrentHashMap<String,CumulativeDistributionSummary>());

    response.getMetrics().forEach((bb) -> {
      final FMetric fm = FMetric.getRootAsFMetric(bb);
      final String name = fm.name();
      double value = fm.dvalue();
      if (value == 0.0) {
        value = fm.ivalue();
        if (value == 0.0) {
          value = fm.lvalue();
        }
      }
      final Meter.Id id = new Meter.Id(name, Tags.empty(), null, null, Type.valueOf(fm.type()));
      total
          .computeIfAbsent(name,
              (k) -> new CumulativeDistributionSummary(id, Clock.SYSTEM, DSC, 1.0, false))
          .record(value);
      rgMetrics
          .computeIfAbsent(name,
              (k) -> new CumulativeDistributionSummary(id, Clock.SYSTEM, DSC, 1.0, false))
          .record(value);
    });

  }

  public void processResponse(final ServerId server, final MetricResponse response) {
    problemHosts.remove(server);
    allMetrics.put(server, response);
    resourceGroups.add(response.getResourceGroup());
    switch (response.serverType) {
      case COMPACTOR:
        compactors.computeIfAbsent(response.getResourceGroup(), (rg) -> new HashSet<>())
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
        break;
      case SCAN_SERVER:
        sservers.computeIfAbsent(response.getResourceGroup(), (rg) -> new HashSet<>()).add(server);
        updateAggregates(response, totalSServerMetrics, rgSServerMetrics);
        break;
      case TABLET_SERVER:
        tservers.computeIfAbsent(response.getResourceGroup(), (rg) -> new HashSet<>()).add(server);
        updateAggregates(response, totalTServerMetrics, rgTServerMetrics);
        break;
      default:
        LOG.error("Unhandled server type in fetch metric response: {}", response.serverType);
        break;
    }

  }

  public void processExternalCompactionList(TExternalCompactionList running) {
    // Create an index into the running compaction list that is
    // sorted by duration, meaning earliest start time first. This
    // will allow us to show topN longest running compactions.
    Map<Long,String> timeSortedEcids = new TreeMap<>();
    if (running.getCompactions() != null) {
      running.getCompactions().forEach((ecid, extComp) -> {
        if (extComp.getUpdates() != null && !extComp.getUpdates().isEmpty()) {
          Set<Long> orderedUpdateTimes = new TreeSet<>(extComp.getUpdates().keySet());
          timeSortedEcids.put(orderedUpdateTimes.iterator().next(), ecid);
        }
      });
    }
    runningCompactions.set(running.getCompactions());
    runningCompactionsDurationIndex.set(timeSortedEcids);
  }

  public void processTabletInformation(String tableName, TabletInformation info) {
    final SanitizedTabletInformation sti = new SanitizedTabletInformation(info);
    tablets.computeIfAbsent(tableName, (t) -> new ArrayList<>()).add(sti);
    tables.computeIfAbsent(tableName, (t) -> new TableSummary()).addTablet(sti);
  }

  public void processError(ServerId server) {
    problemHosts.add(server);
  }

  public void finish() {
    // Iterate over the metrics
    allMetrics.asMap().keySet().forEach(serverId -> {
      String typeName = serverId.getType().name();
      if (serverId instanceof GcServerId) {
        typeName = "GC";
      }
      deployment.computeIfAbsent(serverId.getResourceGroup(), g -> new HashMap<>())
          .computeIfAbsent(typeName, t -> new ProcessSummary()).addResponded();
    });
    problemHosts.forEach(serverId -> {
      String typeName = serverId.getType().name();
      if (serverId instanceof GcServerId) {
        typeName = "GC";
      }
      deployment.computeIfAbsent(serverId.getResourceGroup(), g -> new HashMap<>())
          .computeIfAbsent(typeName, t -> new ProcessSummary()).addNotResponded(serverId);
    });
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

  public Map<String,CumulativeDistributionSummary>
      getCompactorResourceGroupMetricSummary(String resourceGroup) {
    return this.rgCompactorMetrics.get(resourceGroup);
  }

  public Map<String,CumulativeDistributionSummary> getCompactorAllMetricSummary() {
    return this.totalCompactorMetrics;
  }

  public Set<ServerId> getSServerResourceGroupServers(String resourceGroup) {
    return this.sservers.get(resourceGroup);
  }

  public Map<String,CumulativeDistributionSummary>
      getSServerResourceGroupMetricSummary(String resourceGroup) {
    return this.rgSServerMetrics.get(resourceGroup);
  }

  public Map<String,CumulativeDistributionSummary> getSServerAllMetricSummary() {
    return this.totalSServerMetrics;
  }

  public Set<ServerId> getTServerResourceGroupServers(String resourceGroup) {
    return this.tservers.get(resourceGroup);
  }

  public Map<String,CumulativeDistributionSummary>
      getTServerResourceGroupMetricSummary(String resourceGroup) {
    return this.rgTServerMetrics.get(resourceGroup);
  }

  public Map<String,CumulativeDistributionSummary> getTServerAllMetricSummary() {
    return this.totalTServerMetrics;
  }

  public Collection<TExternalCompaction> getCompactions(int topN) {
    List<TExternalCompaction> results = new ArrayList<>();
    Map<String,TExternalCompaction> compactions = runningCompactions.get();
    Iterator<String> ecids = runningCompactionsDurationIndex.get().values().iterator();
    for (int i = 0; i < topN && ecids.hasNext(); i++) {
      results.add(compactions.get(ecids.next()));
    }
    return results;
  }

  public Map<String,TableSummary> getTables() {
    return this.tables;
  }

  public List<TabletInformation> getTablets(String table) {
    return this.tablets.get(table);
  }

  public Map<String,Map<String,ProcessSummary>> getDeploymentOverview() {
    return this.deployment;
  }

}

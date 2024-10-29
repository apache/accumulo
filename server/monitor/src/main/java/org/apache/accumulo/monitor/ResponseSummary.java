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
package org.apache.accumulo.monitor;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Type;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;

public class ResponseSummary {

  private static final Logger LOG = LoggerFactory.getLogger(ResponseSummary.class);

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
  private final Map<String,DistributionSummary> totalCompactorMetrics = new ConcurrentHashMap<>();
  private final Map<String,DistributionSummary> totalSServerMetrics = new ConcurrentHashMap<>();
  private final Map<String,DistributionSummary> totalTServerMetrics = new ConcurrentHashMap<>();

  // Summaries of metrics by server type and resource group
  // map of resource group to metric name to metric values
  private final Map<String,Map<String,DistributionSummary>> rgCompactorMetrics =
      new ConcurrentHashMap<>();
  private final Map<String,Map<String,DistributionSummary>> rgSServerMetrics =
      new ConcurrentHashMap<>();
  private final Map<String,Map<String,DistributionSummary>> rgTServerMetrics =
      new ConcurrentHashMap<>();

  public ResponseSummary(Cache<ServerId,MetricResponse> allMetrics) {
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
      final Map<String,DistributionSummary> total,
      final Map<String,Map<String,DistributionSummary>> rg) {

    final Map<String,DistributionSummary> rgMetrics = rg.computeIfAbsent(
        response.getResourceGroup(), (k) -> new ConcurrentHashMap<String,DistributionSummary>());

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
      total.computeIfAbsent(name, (k) -> new CumulativeDistributionSummary(id, Clock.SYSTEM,
          DistributionStatisticConfig.NONE, 1.0, false)).record(value);
      rgMetrics.computeIfAbsent(name, (k) -> new CumulativeDistributionSummary(id, Clock.SYSTEM,
          DistributionStatisticConfig.NONE, 1.0, false)).record(value);
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

  public void processError(ServerId server) {
    problemHosts.add(server);
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

  public Map<String,DistributionSummary>
      getCompactorResourceGroupMetricSummary(String resourceGroup) {
    return this.rgCompactorMetrics.get(resourceGroup);
  }

  public Map<String,DistributionSummary> getCompactorAllMetricSummary() {
    return this.totalCompactorMetrics;
  }

  public Set<ServerId> getSServerResourceGroupServers(String resourceGroup) {
    return this.sservers.get(resourceGroup);
  }

  public Map<String,DistributionSummary>
      getSServerResourceGroupMetricSummary(String resourceGroup) {
    return this.rgSServerMetrics.get(resourceGroup);
  }

  public Map<String,DistributionSummary> getSServerAllMetricSummary() {
    return this.totalSServerMetrics;
  }

  public Set<ServerId> getTServerResourceGroupServers(String resourceGroup) {
    return this.tservers.get(resourceGroup);
  }

  public Map<String,DistributionSummary>
      getTServerResourceGroupMetricSummary(String resourceGroup) {
    return this.rgTServerMetrics.get(resourceGroup);
  }

  public Map<String,DistributionSummary> getTServerAllMetricSummary() {
    return this.totalTServerMetrics;
  }

}

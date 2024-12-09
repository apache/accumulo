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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.next.InformationFetcher.InstanceSummary;
import org.apache.accumulo.monitor.next.SystemInformation.ProcessSummary;
import org.apache.accumulo.monitor.next.SystemInformation.TableSummary;

import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;

@Path("/metrics")
public class Endpoints {

  @Inject
  private Monitor monitor;

  private void validateResourceGroup(String resourceGroup) {
    if (monitor.getInformationFetcher().getSummary().getResourceGroups().contains(resourceGroup)) {
      return;
    }
    throw new NotFoundException("Resource Group " + resourceGroup + " not found");
  }

  @GET
  @Path("/groups")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<String> getResourceGroups() {
    return monitor.getInformationFetcher().getSummary().getResourceGroups();
  }

  @GET
  @Path("/problems")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<ServerId> getProblemHosts() {
    return monitor.getInformationFetcher().getSummary().getProblemHosts();
  }

  @GET
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<MetricResponse> getAll() {
    return monitor.getInformationFetcher().getAllMetrics().asMap().values();
  }

  @GET
  @Path("/manager")
  @Produces(MediaType.APPLICATION_JSON)
  public MetricResponse getManager() {
    final ServerId s = monitor.getInformationFetcher().getSummary().getManager();
    if (s == null) {
      throw new NotFoundException("Manager not found");
    }
    return monitor.getInformationFetcher().getAllMetrics().asMap().get(s);
  }

  @GET
  @Path("/gc")
  @Produces(MediaType.APPLICATION_JSON)
  public MetricResponse getGarbageCollector() {
    final ServerId s = monitor.getInformationFetcher().getSummary().getGarbageCollector();
    if (s == null) {
      throw new NotFoundException("Garbage Collector not found");
    }
    return monitor.getInformationFetcher().getAllMetrics().asMap().get(s);
  }

  @GET
  @Path("/instance")
  @Produces(MediaType.APPLICATION_JSON)
  public InstanceSummary getInstanceSummary() {
    return new InstanceSummary(monitor.getContext().getInstanceName(),
        monitor.getContext().instanceOperations().getInstanceId().canonical(),
        Set.of(monitor.getContext().getZooKeepers().split(",")),
        monitor.getContext().getVolumeManager().getVolumes().stream().map(v -> v.toString())
            .collect(Collectors.toSet()),
        Constants.VERSION);
  }

  @GET
  @Path("/compactors/detail/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<MetricResponse> getCompactors(@PathParam("group") String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Set<ServerId> servers = monitor.getInformationFetcher().getSummary()
        .getCompactorResourceGroupServers(resourceGroup);
    if (servers == null) {
      return List.of();
    }
    return monitor.getInformationFetcher().getAllMetrics().getAllPresent(servers).values();
  }

  @GET
  @Path("/compactors/summary/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Id,CumulativeDistributionSummary>
      getCompactorResourceGroupMetricSummary(@PathParam("group") String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Map<Id,CumulativeDistributionSummary> metrics = monitor.getInformationFetcher()
        .getSummary().getCompactorResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      return Map.of();
    }
    return metrics;
  }

  @GET
  @Path("/compactors/summary")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Id,CumulativeDistributionSummary> getCompactorAllMetricSummary() {
    return monitor.getInformationFetcher().getSummary().getCompactorAllMetricSummary();
  }

  @GET
  @Path("/sservers/detail/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<MetricResponse> getScanServers(@PathParam("group") String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Set<ServerId> servers =
        monitor.getInformationFetcher().getSummary().getSServerResourceGroupServers(resourceGroup);
    if (servers == null) {
      return List.of();
    }
    return monitor.getInformationFetcher().getAllMetrics().getAllPresent(servers).values();
  }

  @GET
  @Path("/sservers/summary/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Id,CumulativeDistributionSummary>
      getScanServerResourceGroupMetricSummary(@PathParam("group") String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Map<Id,CumulativeDistributionSummary> metrics = monitor.getInformationFetcher()
        .getSummary().getSServerResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      return Map.of();
    }
    return metrics;
  }

  @GET
  @Path("/sservers/summary")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Id,CumulativeDistributionSummary> getScanServerAllMetricSummary() {
    return monitor.getInformationFetcher().getSummary().getSServerAllMetricSummary();
  }

  @GET
  @Path("/tservers/detail/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  public Collection<MetricResponse> getTabletServers(@PathParam("group") String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Set<ServerId> servers =
        monitor.getInformationFetcher().getSummary().getTServerResourceGroupServers(resourceGroup);
    if (servers == null) {
      return List.of();
    }
    return monitor.getInformationFetcher().getAllMetrics().getAllPresent(servers).values();
  }

  @GET
  @Path("/tservers/summary/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Id,CumulativeDistributionSummary>
      getTabletServerResourceGroupMetricSummary(@PathParam("group") String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Map<Id,CumulativeDistributionSummary> metrics = monitor.getInformationFetcher()
        .getSummary().getTServerResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      return Map.of();
    }
    return metrics;
  }

  @GET
  @Path("/tservers/summary")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<Id,CumulativeDistributionSummary> getTabletServerAllMetricSummary() {
    return monitor.getInformationFetcher().getSummary().getTServerAllMetricSummary();
  }

  @GET
  @Path("/compactions/summary")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String,List<FMetric>> getCompactionMetricSummary() {
    return monitor.getInformationFetcher().getSummary().getCompactionMetricSummary();
  }

  @GET
  @Path("/compactions/detail")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String,List<TExternalCompaction>> getCompactions() {
    return monitor.getInformationFetcher().getSummary().getCompactions(25);
  }

  @GET
  @Path("/compactions/detail/{num}")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String,List<TExternalCompaction>> getCompactions(@PathParam("num") int topN) {
    return monitor.getInformationFetcher().getSummary().getCompactions(topN);
  }

  @GET
  @Path("/tables")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String,TableSummary> getTables() {
    return monitor.getInformationFetcher().getSummary().getTables();
  }

  @GET
  @Path("/tables/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public TableSummary getTable(@PathParam("name") String tableName) {
    TableSummary ts = monitor.getInformationFetcher().getSummary().getTables().get(tableName);
    if (ts == null) {
      throw new NotFoundException(tableName + " not found");
    }
    return ts;
  }

  @GET
  @Path("/tables/{name}/tablets")
  @Produces(MediaType.APPLICATION_JSON)
  public List<TabletInformation> getTablets(@PathParam("name") String tableName) {
    List<TabletInformation> ti = monitor.getInformationFetcher().getSummary().getTablets(tableName);
    if (ti == null) {
      throw new NotFoundException(tableName + " not found");
    }
    return ti;
  }

  @GET
  @Path("/deployment")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String,Map<String,ProcessSummary>> getDeploymentOverview() {
    return monitor.getInformationFetcher().getSummary().getDeploymentOverview();
  }

  @GET
  @Path("/suggestions")
  @Produces(MediaType.APPLICATION_JSON)
  public Set<String> getSuggestions() {
    return monitor.getInformationFetcher().getSummary().getSuggestions();
  }

  @GET
  @Path("/lastUpdate")
  @Produces(MediaType.APPLICATION_JSON)
  public long getTimestamp() {
    return monitor.getInformationFetcher().getSummary().getTimestamp();
  }

  @GET
  @Path("/stats")
  @Produces(MediaType.TEXT_PLAIN)
  public String getConnectionStatistics() {
    return monitor.getConnectionStatisticsBean().dump();
  }

}

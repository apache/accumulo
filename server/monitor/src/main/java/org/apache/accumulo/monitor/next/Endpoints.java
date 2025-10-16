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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.TabletInformation;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.next.InformationFetcher.InstanceSummary;
import org.apache.accumulo.monitor.next.SystemInformation.ProcessSummary;
import org.apache.accumulo.monitor.next.SystemInformation.TableSummary;

import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;

@Path("/")
public class Endpoints {
  /**
   * A {@code String} constant representing supplied resource group in path parameter.
   */
  private static final String GROUP_PARAM_KEY = "group";

  /**
   * A {@code String} constant representing supplied tableId in path parameter.
   */
  private static final String TABLEID_PARAM_KEY = "tableId";

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Description {
    String value();
  }

  @Inject
  private Monitor monitor;

  private void validateResourceGroup(String resourceGroup) {
    if (monitor.getInformationFetcher().getSummaryForEndpoint().getResourceGroups()
        .contains(resourceGroup)) {
      return;
    }
    throw new NotFoundException("Resource Group " + resourceGroup + " not found");
  }

  @GET
  @Path("endpoints")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of the available endpoints and a description for each")
  public Map<String,String> getEndpoints(@Context HttpServletRequest request) {

    /*
     * Attempted to use OpenAPI annotation for use with Swagger-UI, but ran into potential
     * dependency convergence issues as we were using newer version of some of the same
     * dependencies.
     */
    final String basePath = request.getRequestURL().toString();
    final Map<String,String> documentation = new TreeMap<>();

    for (Method m : Endpoints.class.getMethods()) {
      if (m.isAnnotationPresent(Path.class)) {
        Path pathAnnotation = m.getAnnotation(Path.class);
        String path = basePath + "/" + pathAnnotation.value();
        String description = "";
        if (m.isAnnotationPresent(Description.class)) {
          Description desc = m.getAnnotation(Description.class);
          description = desc.value();
        }
        documentation.put(path, description);
      }
    }

    return documentation;
  }

  @GET
  @Path("groups")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of the resource groups that are in use")
  public Set<String> getResourceGroups() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getResourceGroups();
  }

  @GET
  @Path("problems")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of the servers that are potentially down")
  public Collection<ServerId> getProblemHosts() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getProblemHosts();
  }

  @GET
  @Path("metrics")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric responses for all servers")
  public Collection<MetricResponse> getAll() {
    return monitor.getInformationFetcher().getAllMetrics().asMap().values();
  }

  @GET
  @Path("manager")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric response for the Manager")
  public MetricResponse getManager() {
    final ServerId s = monitor.getInformationFetcher().getSummaryForEndpoint().getManager();
    if (s == null) {
      throw new NotFoundException("Manager not found");
    }
    return monitor.getInformationFetcher().getAllMetrics().asMap().get(s);
  }

  @GET
  @Path("manager/metrics")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metrics for the Manager")
  public List<FMetric> getManagerMetrics() {
    var managerMetrics = getManager().getMetrics();
    if (managerMetrics != null) {
      return managerMetrics.stream().map(FMetric::getRootAsFMetric).collect(Collectors.toList());
    }
    return List.of();
  }

  @GET
  @Path("gc")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric response for the Garbage Collector")
  public MetricResponse getGarbageCollector() {
    final ServerId s =
        monitor.getInformationFetcher().getSummaryForEndpoint().getGarbageCollector();
    if (s == null) {
      throw new NotFoundException("Garbage Collector not found");
    }
    return monitor.getInformationFetcher().getAllMetrics().asMap().get(s);
  }

  @GET
  @Path("instance")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the instance name, instance id, version, zookeepers, and volumes")
  public InstanceSummary getInstanceSummary() {
    return new InstanceSummary(monitor.getContext().getInstanceName(),
        monitor.getContext().instanceOperations().getInstanceId().canonical(),
        Set.of(monitor.getContext().getZooKeepers().split(",")),
        monitor.getContext().getVolumeManager().getVolumes().stream().map(Object::toString)
            .collect(Collectors.toSet()),
        Constants.VERSION);
  }

  @GET
  @Path("compactors/detail/{" + GROUP_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric responses for the Compactors in the supplied resource group")
  public Collection<MetricResponse>
      getCompactors(@PathParam(GROUP_PARAM_KEY) String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Set<ServerId> servers = monitor.getInformationFetcher().getSummaryForEndpoint()
        .getCompactorResourceGroupServers(resourceGroup);
    if (servers == null) {
      return List.of();
    }
    return monitor.getInformationFetcher().getAllMetrics().getAllPresent(servers).values();
  }

  @GET
  @Path("compactors/summary/{" + GROUP_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for the Compactors in the supplied resource group")
  public Map<Id,CumulativeDistributionSummary>
      getCompactorResourceGroupMetricSummary(@PathParam(GROUP_PARAM_KEY) String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Map<Id,CumulativeDistributionSummary> metrics = monitor.getInformationFetcher()
        .getSummaryForEndpoint().getCompactorResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      return Map.of();
    }
    return metrics;
  }

  @GET
  @Path("compactors/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for all Compactors")
  public Map<Id,CumulativeDistributionSummary> getCompactorAllMetricSummary() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getCompactorAllMetricSummary();
  }

  @GET
  @Path("sservers/detail/{" + GROUP_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric responses for the ScanServers in the supplied resource group")
  public Collection<MetricResponse>
      getScanServers(@PathParam(GROUP_PARAM_KEY) String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Set<ServerId> servers = monitor.getInformationFetcher().getSummaryForEndpoint()
        .getSServerResourceGroupServers(resourceGroup);
    if (servers == null) {
      return List.of();
    }
    return monitor.getInformationFetcher().getAllMetrics().getAllPresent(servers).values();
  }

  @GET
  @Path("sservers/summary/{" + GROUP_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for the ScanServers in the supplied resource group")
  public Map<Id,CumulativeDistributionSummary>
      getScanServerResourceGroupMetricSummary(@PathParam(GROUP_PARAM_KEY) String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Map<Id,CumulativeDistributionSummary> metrics = monitor.getInformationFetcher()
        .getSummaryForEndpoint().getSServerResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      return Map.of();
    }
    return metrics;
  }

  @GET
  @Path("sservers/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for all ScanServers")
  public Map<Id,CumulativeDistributionSummary> getScanServerAllMetricSummary() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getSServerAllMetricSummary();
  }

  @GET
  @Path("tservers/detail/{" + GROUP_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric responses for the TabletServers in the supplied resource group")
  public Collection<MetricResponse>
      getTabletServers(@PathParam(GROUP_PARAM_KEY) String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Set<ServerId> servers = monitor.getInformationFetcher().getSummaryForEndpoint()
        .getTServerResourceGroupServers(resourceGroup);
    if (servers == null) {
      return List.of();
    }
    return monitor.getInformationFetcher().getAllMetrics().getAllPresent(servers).values();
  }

  @GET
  @Path("tservers/summary/{" + GROUP_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for the TabletServers in the supplied resource group")
  public Map<Id,CumulativeDistributionSummary>
      getTabletServerResourceGroupMetricSummary(@PathParam(GROUP_PARAM_KEY) String resourceGroup) {
    validateResourceGroup(resourceGroup);
    final Map<Id,CumulativeDistributionSummary> metrics = monitor.getInformationFetcher()
        .getSummaryForEndpoint().getTServerResourceGroupMetricSummary(resourceGroup);
    if (metrics == null) {
      return Map.of();
    }
    return metrics;
  }

  @GET
  @Path("tservers/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for all TabletServers")
  public Map<Id,CumulativeDistributionSummary> getTabletServerAllMetricSummary() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getTServerAllMetricSummary();
  }

  @GET
  @Path("compactions/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metrics for all compaction queues")
  public Map<String,List<FMetric>> getCompactionMetricSummary() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getCompactionMetricSummary();
  }

  @GET
  @Path("compactions/detail")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a map of Compactor resource group to the 50 oldest running compactions")
  public Map<String,List<TExternalCompaction>> getCompactions() {
    Map<String,List<TExternalCompaction>> all =
        monitor.getInformationFetcher().getSummaryForEndpoint().getCompactions();
    if (all == null) {
      return Map.of();
    }
    return all;
  }

  @GET
  @Path("compactions/detail/{" + GROUP_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of the 50 oldest running compactions in the supplied resource group")
  public List<TExternalCompaction>
      getCompactions(@PathParam(GROUP_PARAM_KEY) String resourceGroup) {
    validateResourceGroup(resourceGroup);
    List<TExternalCompaction> compactions =
        monitor.getInformationFetcher().getSummaryForEndpoint().getCompactions(resourceGroup);
    if (compactions == null) {
      return List.of();
    }
    return compactions;
  }

  @GET
  @Path("tables")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a map of TableId to table details")
  public Map<TableId,TableSummary> getTables() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getTables();
  }

  @GET
  @Path("tables/{" + TABLEID_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns table details for the supplied TableId")
  public TableSummary getTable(@PathParam(TABLEID_PARAM_KEY) String tableId) {
    TableSummary ts = monitor.getInformationFetcher().getSummaryForEndpoint().getTables()
        .get(TableId.of(tableId));
    if (ts == null) {
      throw new NotFoundException(tableId + " not found");
    }
    return ts;
  }

  @GET
  @Path("tables/{" + TABLEID_PARAM_KEY + "}/tablets")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns tablet details for the supplied table name")
  public List<TabletInformation> getTablets(@PathParam(TABLEID_PARAM_KEY) String tableId) {
    List<TabletInformation> ti =
        monitor.getInformationFetcher().getSummaryForEndpoint().getTablets(TableId.of(tableId));
    if (ti == null) {
      throw new NotFoundException(tableId + " not found");
    }
    return ti;
  }

  @GET
  @Path("deployment")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a map of resource group to server type to process summary."
      + " The process summary contains the number of configured, responding, and not responding servers")
  public Map<ResourceGroupId,Map<String,ProcessSummary>> getDeploymentOverview() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getDeploymentOverview();
  }

  @GET
  @Path("suggestions")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of suggestions")
  public Set<String> getSuggestions() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getSuggestions();
  }

  @GET
  @Path("lastUpdate")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the timestamp of when the monitor information was last refreshed")
  public long getTimestamp() {
    return monitor.getInformationFetcher().getSummaryForEndpoint().getTimestamp();
  }

  @GET
  @Path("stats")
  @Produces(MediaType.TEXT_PLAIN)
  @Description("Returns connection statistics for the Jetty server")
  public String getConnectionStatistics() {
    return monitor.getConnectionStatisticsBean().dump();
  }

}

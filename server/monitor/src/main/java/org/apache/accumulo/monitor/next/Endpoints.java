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
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.thrift.MetricResponse;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.monitor.next.InformationFetcher.InstanceSummary;
import org.apache.accumulo.monitor.next.SystemInformation.ProcessSummary;
import org.apache.accumulo.monitor.next.SystemInformation.TableSummary;

import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;

@Path("/")
public class Endpoints {

  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Description {
    String value();
  }

  @Inject
  private Monitor monitor;

  private void validateResourceGroup(String resourceGroup) {
    if (monitor.getInformationFetcher().getSummary().getResourceGroups().contains(resourceGroup)) {
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
    return monitor.getInformationFetcher().getSummary().getResourceGroups();
  }

  @GET
  @Path("problems")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of the servers that are potentially down")
  public Collection<ServerId> getProblemHosts() {
    return monitor.getInformationFetcher().getSummary().getProblemHosts();
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
    final ServerId s = monitor.getInformationFetcher().getSummary().getManager();
    if (s == null) {
      throw new NotFoundException("Manager not found");
    }
    return monitor.getInformationFetcher().getAllMetrics().asMap().get(s);
  }

  @GET
  @Path("gc")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric response for the Garbage Collector")
  public MetricResponse getGarbageCollector() {
    final ServerId s = monitor.getInformationFetcher().getSummary().getGarbageCollector();
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
  @Path("compactors/detail/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric responses for the Compactors in the supplied resource group")
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
  @Path("compactors/summary/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for the Compactors in the supplied resource group")
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
  @Path("compactors/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for all Compactors")
  public Map<Id,CumulativeDistributionSummary> getCompactorAllMetricSummary() {
    return monitor.getInformationFetcher().getSummary().getCompactorAllMetricSummary();
  }

  @GET
  @Path("sservers/detail/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric responses for the ScanServers in the supplied resource group")
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
  @Path("sservers/summary/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for the ScanServers in the supplied resource group")
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
  @Path("sservers/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for all ScanServers")
  public Map<Id,CumulativeDistributionSummary> getScanServerAllMetricSummary() {
    return monitor.getInformationFetcher().getSummary().getSServerAllMetricSummary();
  }

  @GET
  @Path("tservers/detail/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric responses for the TabletServers in the supplied resource group")
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
  @Path("tservers/summary/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for the TabletServers in the supplied resource group")
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
  @Path("tservers/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns an aggregate view of the metric responses for all TabletServers")
  public Map<Id,CumulativeDistributionSummary> getTabletServerAllMetricSummary() {
    return monitor.getInformationFetcher().getSummary().getTServerAllMetricSummary();
  }

  @GET
  @Path("compactions/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metrics for all compaction queues")
  public Map<String,List<FMetric>> getCompactionMetricSummary() {
    return monitor.getInformationFetcher().getSummary().getCompactionMetricSummary();
  }

  @GET
  @Path("compactions/detail")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a map of Compactor resource group to the 50 oldest running compactions")
  public Map<String,List<TExternalCompaction>> getCompactions() {
    Map<String,List<TExternalCompaction>> all =
        monitor.getInformationFetcher().getSummary().getCompactions();
    if (all == null) {
      return Map.of();
    }
    return all;
  }

  @GET
  @Path("compactions/detail/{group}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of the 50 oldest running compactions in the supplied resource group")
  public List<TExternalCompaction> getCompactions(@PathParam("group") String resourceGroup) {
    validateResourceGroup(resourceGroup);
    List<TExternalCompaction> compactions =
        monitor.getInformationFetcher().getSummary().getCompactions(resourceGroup);
    if (compactions == null) {
      return List.of();
    }
    return compactions;
  }

  @GET
  @Path("tables")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a map of table name to table details")
  public Map<String,TableSummary> getTables() {
    return monitor.getInformationFetcher().getSummary().getTables();
  }

  @GET
  @Path("tables/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns table details for the supplied table name")
  public TableSummary getTable(@PathParam("name") String tableName) {
    TableSummary ts = monitor.getInformationFetcher().getSummary().getTables().get(tableName);
    if (ts == null) {
      throw new NotFoundException(tableName + " not found");
    }
    return ts;
  }

  @GET
  @Path("tables/{name}/tablets")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns tablet details for the supplied table name")
  public List<TabletInformation> getTablets(@PathParam("name") String tableName) {
    List<TabletInformation> ti = monitor.getInformationFetcher().getSummary().getTablets(tableName);
    if (ti == null) {
      throw new NotFoundException(tableName + " not found");
    }
    return ti;
  }

  @GET
  @Path("deployment")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a map of resource group to server type to process summary."
      + " The process summary contains the number of configured, responding, and not responding servers")
  public Map<String,Map<String,ProcessSummary>> getDeploymentOverview() {
    return monitor.getInformationFetcher().getSummary().getDeploymentOverview();
  }

  @GET
  @Path("suggestions")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of suggestions")
  public Set<String> getSuggestions() {
    return monitor.getInformationFetcher().getSummary().getSuggestions();
  }

  @GET
  @Path("lastUpdate")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the timestamp of when the monitor information was last refreshed")
  public long getTimestamp() {
    return monitor.getInformationFetcher().getSummary().getTimestamp();
  }

  @GET
  @Path("stats")
  @Produces(MediaType.TEXT_PLAIN)
  @Description("Returns connection statistics for the Jetty server")
  public String getConnectionStatistics() {
    return monitor.getConnectionStatisticsBean().dump();
  }

}

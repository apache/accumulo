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
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

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

  private void validateResourceGroup(String resourceGroup) throws InterruptedException {
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
    try {
      return monitor.getInformationFetcher().getSummary().getResourceGroups();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  @GET
  @Path("problems")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of the servers that are potentially down")
  public Collection<ServerId> getProblemHosts() {
    try {
      return monitor.getInformationFetcher().getSummary().getProblemHosts();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    final ServerId s;
    try {
      s = monitor.getInformationFetcher().getSummary().getManager();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(Response.status(Response.Status.SERVICE_UNAVAILABLE)
          .entity("Service unavailable, request was interrupted. Please try again").build(), e);
    }
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
    final ServerId s;
    try {
      s = monitor.getInformationFetcher().getSummary().getGarbageCollector();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    final Set<ServerId> servers;
    try {
      validateResourceGroup(resourceGroup);
      servers = monitor.getInformationFetcher().getSummary()
          .getCompactorResourceGroupServers(resourceGroup);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    final Map<Id,CumulativeDistributionSummary> metrics;
    try {
      validateResourceGroup(resourceGroup);
      metrics = monitor.getInformationFetcher().getSummary()
          .getCompactorResourceGroupMetricSummary(resourceGroup);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    try {
      return monitor.getInformationFetcher().getSummary().getCompactorAllMetricSummary();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  @GET
  @Path("sservers/detail/{" + GROUP_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric responses for the ScanServers in the supplied resource group")
  public Collection<MetricResponse>
      getScanServers(@PathParam(GROUP_PARAM_KEY) String resourceGroup) {
    final Set<ServerId> servers;
    try {
      validateResourceGroup(resourceGroup);
      servers = monitor.getInformationFetcher().getSummary()
          .getSServerResourceGroupServers(resourceGroup);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    final Map<Id,CumulativeDistributionSummary> metrics;
    try {
      validateResourceGroup(resourceGroup);
      metrics = monitor.getInformationFetcher().getSummary()
          .getSServerResourceGroupMetricSummary(resourceGroup);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    try {
      return monitor.getInformationFetcher().getSummary().getSServerAllMetricSummary();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  @GET
  @Path("tservers/detail/{" + GROUP_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metric responses for the TabletServers in the supplied resource group")
  public Collection<MetricResponse>
      getTabletServers(@PathParam(GROUP_PARAM_KEY) String resourceGroup) {
    final Set<ServerId> servers;
    try {
      validateResourceGroup(resourceGroup);
      servers = monitor.getInformationFetcher().getSummary()
          .getTServerResourceGroupServers(resourceGroup);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    final Map<Id,CumulativeDistributionSummary> metrics;
    try {
      validateResourceGroup(resourceGroup);
      metrics = monitor.getInformationFetcher().getSummary()
          .getTServerResourceGroupMetricSummary(resourceGroup);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    try {
      return monitor.getInformationFetcher().getSummary().getTServerAllMetricSummary();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  @GET
  @Path("compactions/summary")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the metrics for all compaction queues")
  public Map<String,List<FMetric>> getCompactionMetricSummary() {
    try {
      return monitor.getInformationFetcher().getSummary().getCompactionMetricSummary();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  @GET
  @Path("compactions/detail")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a map of Compactor resource group to the 50 oldest running compactions")
  public Map<String,List<TExternalCompaction>> getCompactions() {
    Map<String,List<TExternalCompaction>> all;
    try {
      all = monitor.getInformationFetcher().getSummary().getCompactions();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    List<TExternalCompaction> compactions;
    try {
      validateResourceGroup(resourceGroup);
      compactions = monitor.getInformationFetcher().getSummary().getCompactions(resourceGroup);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    try {
      return monitor.getInformationFetcher().getSummary().getTables();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  @GET
  @Path("tables/{" + TABLEID_PARAM_KEY + "}")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns table details for the supplied TableId")
  public TableSummary getTable(@PathParam(TABLEID_PARAM_KEY) String tableId) {
    final TableSummary ts;
    try {
      ts = monitor.getInformationFetcher().getSummary().getTables().get(TableId.of(tableId));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    final List<TabletInformation> ti;
    try {
      ti = monitor.getInformationFetcher().getSummary().getTablets(TableId.of(tableId));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
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
    try {
      return monitor.getInformationFetcher().getSummary().getDeploymentOverview();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  @GET
  @Path("suggestions")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns a list of suggestions")
  public Set<String> getSuggestions() {
    try {
      return monitor.getInformationFetcher().getSummary().getSuggestions();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  @GET
  @Path("lastUpdate")
  @Produces(MediaType.APPLICATION_JSON)
  @Description("Returns the timestamp of when the monitor information was last refreshed")
  public long getTimestamp() {
    try {
      return monitor.getInformationFetcher().getSummary().getTimestamp();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ServiceUnavailableException(
          Response.status(Response.Status.SERVICE_UNAVAILABLE).build(), e);
    }
  }

  @GET
  @Path("stats")
  @Produces(MediaType.TEXT_PLAIN)
  @Description("Returns connection statistics for the Jetty server")
  public String getConnectionStatistics() {
    return monitor.getConnectionStatisticsBean().dump();
  }

}

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
package org.apache.accumulo.monitor.next.deployment;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.monitor.next.SystemInformation.ProcessSummary;

/**
 * Data Transfer Object for the Monitor Overview page deployment tables. It packages the total,
 * responding, and not responding server counts for each process in each resource group into a
 * UI-ready JSON response.
 */
public record DeploymentOverview(long lastUpdate, List<ResourceGroupDeployment> groups) {

  /**
   * Data Transfer Object for a resource group and its associated server counts
   */
  public record ResourceGroupDeployment(String resourceGroup,
      List<ServerTypeDeployment> processes) {
  }

  /**
   * Data Transfer Object for a server type and its associated counts
   */
  public record ServerTypeDeployment(String serverType, long total, long responding,
      long notResponding) {
  }

  public static DeploymentOverview fromSummary(
      Map<ResourceGroupId,Map<ServerId.Type,ProcessSummary>> deployment, long lastUpdate) {
    if (deployment == null || deployment.isEmpty()) {
      return new DeploymentOverview(lastUpdate, List.of());
    }

    var groups = deployment.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(entry -> {
      String resourceGroup = entry.getKey().canonical();
      List<ServerTypeDeployment> processes = buildProcesses(entry.getValue());
      return new ResourceGroupDeployment(resourceGroup, processes);
    }).toList();

    return new DeploymentOverview(lastUpdate, groups);
  }

  private static List<ServerTypeDeployment>
      buildProcesses(Map<ServerId.Type,ProcessSummary> processes) {
    if (processes == null || processes.isEmpty()) {
      return List.of();
    }

    return processes.entrySet().stream()
        .sorted(Map.Entry.comparingByKey(Comparator.comparingInt(Enum::ordinal))).map(entry -> {
          ServerId.Type key = entry.getKey();
          ProcessSummary value = entry.getValue();
          return new ServerTypeDeployment(processLabel(key), value.getTotal(), value.getResponded(),
              value.getNotResponded());
        }).toList();
  }

  private static String processLabel(ServerId.Type process) {
    return switch (process) {
      case COMPACTOR -> "Compactor";
      case GARBAGE_COLLECTOR -> "Garbage Collector";
      case MANAGER -> "Manager";
      case MONITOR -> "Monitor";
      case SCAN_SERVER -> "Scan Server";
      case TABLET_SERVER -> "Tablet Server";
    };
  }
}

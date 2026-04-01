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
 * Data Transfer Object for the Monitor Deployment page.
 */
public record DeploymentOverview(long lastUpdate, List<DeploymentRow> breakdown) {

  /**
   * Data Transfer Object containing counts of total and responding servers for a given resource
   * group and server type.
   */
  public record DeploymentRow(String resourceGroup, String serverType, long total,
      long responding) {
  }

  public static DeploymentOverview fromSummary(
      Map<ResourceGroupId,Map<ServerId.Type,ProcessSummary>> deployment, long lastUpdate) {
    if (deployment == null || deployment.isEmpty()) {
      return new DeploymentOverview(lastUpdate, List.of());
    }

    var breakdown =
        deployment.entrySet().stream().sorted(Map.Entry.comparingByKey()).flatMap(entry -> {
          var resourceGroup = entry.getKey().canonical();
          var processes = entry.getValue();
          if (processes == null || processes.isEmpty()) {
            return java.util.stream.Stream.empty();
          }

          return processes.entrySet().stream()
              .sorted(Map.Entry.comparingByKey(Comparator.comparingInt(Enum::ordinal)))
              .map(processEntry -> {
                ServerId.Type type = processEntry.getKey();
                ProcessSummary processSummary = processEntry.getValue();
                return new DeploymentRow(resourceGroup, processLabel(type),
                    processSummary.getTotal(), processSummary.getResponded());
              });
        }).toList();

    return new DeploymentOverview(lastUpdate, breakdown);
  }

  /**
   * @return String representation of the server type
   */
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

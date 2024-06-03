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
package org.apache.accumulo.server.util.serviceStatus;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class StatusSummary {

  private final ServiceStatusReport.ReportKey serviceType;
  private final Set<String> resourceGroups;
  private final Map<String,Set<String>> serviceByGroups;
  private final int serviceCount;
  private final int errorCount;

  public StatusSummary(ServiceStatusReport.ReportKey serviceType, final Set<String> resourceGroups,
      final Map<String,Set<String>> serviceByGroups, final int errorCount) {
    this.serviceType = serviceType;
    this.resourceGroups = resourceGroups;
    this.serviceByGroups = serviceByGroups;
    this.serviceCount =
        serviceByGroups.values().stream().map(Set::size).reduce(Integer::sum).orElse(0);
    this.errorCount = errorCount;
  }

  public ServiceStatusReport.ReportKey getServiceType() {
    return serviceType;
  }

  public String getDisplayName() {
    return serviceType.getDisplayName();
  }

  public Set<String> getResourceGroups() {
    return resourceGroups;
  }

  public Map<String,Set<String>> getServiceByGroups() {
    return serviceByGroups;
  }

  public int getServiceCount() {
    return serviceCount;
  }

  public int getErrorCount() {
    return errorCount;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StatusSummary)) {
      return false;
    }
    StatusSummary that = (StatusSummary) o;
    return serviceCount == that.serviceCount && errorCount == that.errorCount
        && serviceType == that.serviceType && Objects.equals(resourceGroups, that.resourceGroups)
        && Objects.equals(serviceByGroups, that.serviceByGroups);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceType, resourceGroups, serviceCount, serviceByGroups, errorCount);
  }

  @Override
  public String toString() {
    return "StatusSummary{serviceName=" + serviceType + ", resourceGroups=" + resourceGroups
        + ", serviceCount=" + serviceCount + ", names=" + serviceByGroups + ", errorCount="
        + errorCount + '}';
  }
}

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

import java.util.Objects;
import java.util.Set;

public class StatusSummary {

  private final ServiceStatusReport.ReportKey reportKey;
  private final Set<String> resourceGroups;
  private final Set<String> serviceNames;
  private final int serviceCount;
  private final int errorCount;

  public StatusSummary(ServiceStatusReport.ReportKey reportKey, final Set<String> resourceGroups,
      final Set<String> serviceNames, final int errorCount) {
    this.reportKey = reportKey;
    this.resourceGroups = resourceGroups;
    this.serviceNames = serviceNames;
    this.serviceCount = serviceNames.size();
    this.errorCount = errorCount;
  }

  public ServiceStatusReport.ReportKey getReportKey() {
    return reportKey;
  }

  public String getDisplayName() {
    return reportKey.getDisplayName();
  }

  public Set<String> getResourceGroups() {
    return resourceGroups;
  }

  public Set<String> getServiceNames() {
    return serviceNames;
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
        && reportKey == that.reportKey && Objects.equals(resourceGroups, that.resourceGroups)
        && Objects.equals(serviceNames, that.serviceNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(reportKey, resourceGroups, serviceCount, serviceNames, errorCount);
  }

  @Override
  public String toString() {
    return "StatusSummary{serviceName=" + reportKey + ", resourceGroups=" + resourceGroups
        + ", serviceCount=" + serviceCount + ", names=" + serviceNames + ", errorCount="
        + errorCount + '}';
  }
}

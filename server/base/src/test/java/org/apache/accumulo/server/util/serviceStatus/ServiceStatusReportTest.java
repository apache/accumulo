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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceStatusReportTest {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceStatusReport.class);

  @Test
  public void printOutputCountTest() {
    final Map<ServiceStatusReport.ReportKey,StatusSummary> services = buildHostStatus();
    ServiceStatusReport report = new ServiceStatusReport(services, true);
    StringBuilder sb = new StringBuilder(8192);
    report.report(sb);
    LOG.info("Report: \n{}", sb);
    assertTrue(sb.length() > 0);
  }

  @Test
  public void printOutputHostTest() {
    final Map<ServiceStatusReport.ReportKey,StatusSummary> services = buildHostStatus();
    ServiceStatusReport report = new ServiceStatusReport(services, false);
    StringBuilder sb = new StringBuilder(8192);
    report.report(sb);
    LOG.info("Report: \n{}", sb);
    assertTrue(sb.length() > 0);
  }

  @Test
  public void printJsonHostTest() {
    final Map<ServiceStatusReport.ReportKey,StatusSummary> services = buildHostStatus();
    ServiceStatusReport report = new ServiceStatusReport(services, false);
    var output = report.toJson();
    LOG.info("{}", output);
    assertTrue(!output.isEmpty());
  }

  /**
   * validate reduce / sum is correct
   */
  @Test
  public void sumTest() {
    final Map<ServiceStatusReport.ReportKey,StatusSummary> services = buildHostStatus();
    int count =
        services.values().stream().map(StatusSummary::getErrorCount).reduce(Integer::sum).orElse(0);
    assertEquals(4, count);
  }

  private Map<ServiceStatusReport.ReportKey,StatusSummary> buildHostStatus() {
    final Map<ServiceStatusReport.ReportKey,StatusSummary> services = new TreeMap<>();

    StatusSummary managerSummary = new StatusSummary(ServiceStatusReport.ReportKey.MANAGER,
        Set.of(), Set.of("host1:8080", "host2:9090"), 1);
    services.put(ServiceStatusReport.ReportKey.MANAGER, managerSummary);

    StatusSummary monitorSummary = new StatusSummary(ServiceStatusReport.ReportKey.MONITOR,
        Set.of(), Set.of("host1:8080", "host2:9090"), 0);
    services.put(ServiceStatusReport.ReportKey.MONITOR, monitorSummary);

    StatusSummary gcSummary = new StatusSummary(ServiceStatusReport.ReportKey.GC, Set.of(),
        Set.of("host1:8080", "host2:9090"), 0);
    services.put(ServiceStatusReport.ReportKey.GC, gcSummary);

    StatusSummary tserverSummary =
        new StatusSummary(ServiceStatusReport.ReportKey.T_SERVER, Set.of(),
            new TreeSet<>(List.of("host2:9090", "host4:9091", "host1:8080", "host3:9091")), 1);
    services.put(ServiceStatusReport.ReportKey.T_SERVER, tserverSummary);

    StatusSummary scanServerSummary = new StatusSummary(ServiceStatusReport.ReportKey.S_SERVER,
        new TreeSet<>(List.of("default", "rg1", "rg2")), new TreeSet<>(List
            .of("default, host2:9090", "rg2, host4:9091", "rg1, host1:8080", "rg1, host3:9091")),
        2);
    services.put(ServiceStatusReport.ReportKey.S_SERVER, scanServerSummary);

    StatusSummary coordinatorSummary = new StatusSummary(ServiceStatusReport.ReportKey.COORDINATOR,
        Set.of(), new TreeSet<>(List.of("host4:9090", "host2:9091")), 0);
    services.put(ServiceStatusReport.ReportKey.COORDINATOR, coordinatorSummary);

    StatusSummary compactorSummary = new StatusSummary(ServiceStatusReport.ReportKey.COMPACTOR,
        new TreeSet<>(List.of("q2", "q1")), new TreeSet<>(List.of("q2: host2:9090",
            "q2: host4:9091", "q1: host3:8080", "q1: host1:9091", "q2: host5:8080")),
        0);
    services.put(ServiceStatusReport.ReportKey.COMPACTOR, compactorSummary);

    return services;
  }

}

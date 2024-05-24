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

import static org.apache.accumulo.server.util.ServiceStatusCmd.NO_GROUP_TAG;
import static org.apache.accumulo.server.util.serviceStatus.ServiceStatusReport.ReportKey.MANAGER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    assertFalse(output.isEmpty());
  }

  @Test
  public void jsonRoundTripTest() {
    final Map<ServiceStatusReport.ReportKey,StatusSummary> services = new TreeMap<>();

    Map<String,Set<String>> managerByGroup = new TreeMap<>();
    managerByGroup.put(NO_GROUP_TAG, new TreeSet<>(List.of("hostZ:8080", "hostA:9090")));
    StatusSummary managerSummary = new StatusSummary(MANAGER, Set.of(), managerByGroup, 1);
    services.put(MANAGER, managerSummary);
    ServiceStatusReport report = new ServiceStatusReport(services, false);
    var encoded = report.toJson();

    ServiceStatusReport decoded = ServiceStatusReport.fromJson(encoded);
    assertNotNull(decoded.getReportTime());
    assertEquals(1, decoded.getTotalZkReadErrors());
    assertEquals(1, report.getSummaries().size());

    var byGroup = report.getSummaries().get(MANAGER).getServiceByGroups();
    assertEquals(new TreeSet<>(List.of("hostZ:8080", "hostA:9090")), byGroup.get(NO_GROUP_TAG));
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

    Map<String,Set<String>> managerByGroup = new TreeMap<>();
    managerByGroup.put(NO_GROUP_TAG, new TreeSet<>(List.of("host1:8080", "host2:9090")));
    StatusSummary managerSummary = new StatusSummary(MANAGER, Set.of(), managerByGroup, 1);
    services.put(MANAGER, managerSummary);

    Map<String,Set<String>> monitorByGroup = new TreeMap<>();
    monitorByGroup.put(NO_GROUP_TAG, new TreeSet<>(List.of("host1:8080", "host2:9090")));
    StatusSummary monitorSummary =
        new StatusSummary(ServiceStatusReport.ReportKey.MONITOR, Set.of(), monitorByGroup, 0);
    services.put(ServiceStatusReport.ReportKey.MONITOR, monitorSummary);

    Map<String,Set<String>> gcByGroup = new TreeMap<>();
    gcByGroup.put(NO_GROUP_TAG, new TreeSet<>(List.of("host1:8080", "host2:9090")));

    StatusSummary gcSummary =
        new StatusSummary(ServiceStatusReport.ReportKey.GC, Set.of(), gcByGroup, 0);
    services.put(ServiceStatusReport.ReportKey.GC, gcSummary);

    Map<String,Set<String>> tserverByGroup = new TreeMap<>();
    tserverByGroup.put(NO_GROUP_TAG,
        new TreeSet<>(List.of("host2:9090", "host4:9091", "host1:8080", "host3:9091")));

    StatusSummary tserverSummary =
        new StatusSummary(ServiceStatusReport.ReportKey.T_SERVER, Set.of(), tserverByGroup, 1);
    services.put(ServiceStatusReport.ReportKey.T_SERVER, tserverSummary);

    Map<String,Set<String>> sserverByGroup = new TreeMap<>();
    sserverByGroup.put("default", new TreeSet<>(List.of("host2:9090")));
    sserverByGroup.put("rg1", new TreeSet<>(List.of("host1:8080", "host3:9091")));
    sserverByGroup.put("rg2", new TreeSet<>(List.of("host4:9091")));

    StatusSummary scanServerSummary = new StatusSummary(ServiceStatusReport.ReportKey.S_SERVER,
        new TreeSet<>(List.of("default", "rg1", "rg2")), sserverByGroup, 2);
    services.put(ServiceStatusReport.ReportKey.S_SERVER, scanServerSummary);

    Map<String,Set<String>> coordinatorByGroup = new TreeMap<>();
    coordinatorByGroup.put(NO_GROUP_TAG, new TreeSet<>(List.of("host4:9090", "host2:9091")));
    StatusSummary coordinatorSummary = new StatusSummary(ServiceStatusReport.ReportKey.COORDINATOR,
        Set.of(), coordinatorByGroup, 0);
    services.put(ServiceStatusReport.ReportKey.COORDINATOR, coordinatorSummary);

    Map<String,Set<String>> compactorByGroup = new TreeMap<>();
    compactorByGroup.put("q2", new TreeSet<>(List.of("host5:8080", "host2:9090", "host4:9091")));
    compactorByGroup.put("q1", new TreeSet<>(List.of("host3:8080", "host1:9091")));

    StatusSummary compactorSummary = new StatusSummary(ServiceStatusReport.ReportKey.COMPACTOR,
        new TreeSet<>(List.of("q2", "q1")), compactorByGroup, 0);
    services.put(ServiceStatusReport.ReportKey.COMPACTOR, compactorSummary);

    return services;
  }

}

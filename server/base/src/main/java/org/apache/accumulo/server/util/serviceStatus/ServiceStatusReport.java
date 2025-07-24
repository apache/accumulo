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

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * Wrapper for JSON formatted report.
 */
public class ServiceStatusReport {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceStatusReport.class);

  private static final Gson gson = new Gson();

  private static final DateTimeFormatter rptTimeFmt =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  private static final String I2 = "  ";
  private static final String I4 = "    ";
  private static final String I6 = "      ";

  private final String reportTime;
  private final int zkReadErrors;
  private final boolean showHosts;
  private final Map<ReportKey,StatusSummary> summaries;

  public ServiceStatusReport(final Map<ReportKey,StatusSummary> summaries,
      final boolean showHosts) {
    reportTime = rptTimeFmt.format(ZonedDateTime.now(ZoneId.of("UTC")));
    zkReadErrors = summaries.values().stream().map(StatusSummary::getErrorCount)
        .reduce(Integer::sum).orElse(0);
    this.showHosts = showHosts;
    this.summaries = summaries;
  }

  public String getReportTime() {
    return reportTime;
  }

  public int getTotalZkReadErrors() {
    return zkReadErrors;
  }

  public Map<ReportKey,StatusSummary> getSummaries() {
    return summaries;
  }

  public String toJson() {
    // return gson.toJson(this, ServiceStatusReport.class);

    Map<ReportKey,StatusSummary> noHostSummaries = summaries.entrySet().stream().collect(Collectors
        .toMap(Map.Entry::getKey, e -> e.getValue().withoutHosts(), (a, b) -> b, TreeMap::new));
    ServiceStatusReport noHostReport = new ServiceStatusReport(noHostSummaries, false);
    return gson.toJson(noHostReport, ServiceStatusReport.class);
  }

  public static ServiceStatusReport fromJson(final String json) {
    return gson.fromJson(json, ServiceStatusReport.class);
  }

  public String toCsv() {
    StringBuilder sb = new StringBuilder();
    sb.append("Service,Resource Group,Host Count,Hosts,Error Count\n");

    for (Map.Entry<ReportKey,StatusSummary> entry : summaries.entrySet()) {
      ReportKey reportKey = entry.getKey();
      StatusSummary summary = entry.getValue();

      if (summary == null || summary.getServiceByGroups() == null) {
        continue;
      }

      Map<String,Set<String>> groupMap = summary.getServiceByGroups();
      int errorCount = summary.getErrorCount();

      for (Map.Entry<String,Set<String>> groupEntry : groupMap.entrySet()) {
        String group = groupEntry.getKey();
        Set<String> hosts = groupEntry.getValue();
        String hostList = String.join(";", hosts);
        sb.append(reportKey.name()).append(",").append(group).append(",").append(hosts.size())
            .append(",").append(hostList).append(",").append(errorCount).append("\n");
      }
    }

    return sb.toString();
  }

  public String report(final StringBuilder sb) {
    sb.append("Report time: ").append(rptTimeFmt.format(ZonedDateTime.now(ZoneId.of("UTC"))))
        .append("\n");
    int zkErrors = summaries.values().stream().map(StatusSummary::getErrorCount)
        .reduce(Integer::sum).orElse(0);
    sb.append("ZooKeeper read errors: ").append(zkErrors).append("\n");

    fmtServiceStatus(sb, ReportKey.MANAGER, summaries.get(ReportKey.MANAGER), showHosts);
    fmtServiceStatus(sb, ReportKey.MONITOR, summaries.get(ReportKey.MONITOR), showHosts);
    fmtServiceStatus(sb, ReportKey.GC, summaries.get(ReportKey.GC), showHosts);
    fmtServiceStatus(sb, ReportKey.T_SERVER, summaries.get(ReportKey.T_SERVER), showHosts);
    fmtResourceGroups(sb, ReportKey.S_SERVER, summaries.get(ReportKey.S_SERVER), showHosts);
    fmtServiceStatus(sb, ReportKey.COORDINATOR, summaries.get(ReportKey.COORDINATOR), showHosts);
    fmtResourceGroups(sb, ReportKey.COMPACTOR, summaries.get(ReportKey.COMPACTOR), showHosts);

    sb.append("\n");
    LOG.trace("fmtStatus - with hosts: {}", summaries);
    return sb.toString();
  }

  private void fmtServiceStatus(final StringBuilder sb, final ReportKey displayNames,
      final StatusSummary summary, boolean showHosts) {
    if (summary == null) {
      sb.append(displayNames).append(": unavailable").append("\n");
      return;
    }

    fmtCounts(sb, summary);

    // skip host info if NOT showing hosts
    if (!showHosts) {
      return;
    }

    if (summary.getServiceCount() > 0) {
      var hosts = summary.getServiceByGroups();
      hosts.values().forEach(s -> s.forEach(h -> sb.append(I2).append(h).append("\n")));
    }
  }

  private void fmtCounts(StringBuilder sb, StatusSummary summary) {
    sb.append(summary.getDisplayName()).append(": count: ").append(summary.getServiceCount());
    if (summary.getErrorCount() > 0) {
      sb.append(", (ZooKeeper errors: ").append(summary.getErrorCount()).append(")\n");
    } else {
      sb.append("\n");
    }
  }

  private void fmtResourceGroups(final StringBuilder sb, final ReportKey reportKey,
      final StatusSummary summary, boolean showHosts) {
    if (summary == null) {
      sb.append(reportKey).append(": unavailable").append("\n");
      return;
    }

    fmtCounts(sb, summary);

    // skip host info if NOT showing hosts
    if (!showHosts) {
      return;
    }

    if (!summary.getResourceGroups().isEmpty()) {
      sb.append(I2).append("resource groups:\n");
      summary.getResourceGroups().forEach(g -> sb.append(I4).append(g).append("\n"));

      if (summary.getServiceCount() > 0) {
        sb.append(I2).append("hosts (by group):\n");
        var groups = summary.getServiceByGroups();
        groups.forEach((g, h) -> {
          sb.append(I4).append(g).append(" (").append(h.size()).append(")").append(":\n");
          h.forEach(n -> {
            sb.append(I6).append(n).append("\n");
          });
        });
      }
    }
  }

  @Override
  public String toString() {
    return "ServiceStatusReport{reportTime='" + reportTime + '\'' + ", zkReadErrors=" + zkReadErrors
        + ", Hosts=" + showHosts + ", status=" + summaries + '}';
  }

  public enum ReportKey {
    COMPACTOR("Compactors"),
    COORDINATOR("Coordinators"),
    GC("Garbage Collectors"),
    MANAGER("Managers"),
    MONITOR("Monitors"),
    S_SERVER("Scan Servers"),
    T_SERVER("Tablet Servers");

    private final String displayName;

    ReportKey(final String name) {
      this.displayName = name;
    }

    public String getDisplayName() {
      return displayName;
    }
  }
}

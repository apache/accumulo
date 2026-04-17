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
package org.apache.accumulo.monitor.next.views;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.Metric.MetricDocSection;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.monitor.next.SystemInformation;
import org.apache.accumulo.server.metrics.MetricResponseWrapper;

import com.github.benmanes.caffeine.cache.Cache;

/**
 * Generic Data Transfer Object (DTO) for a set of Accumulo server processes of the same type. The
 * response object contains several fields:
 *
 *
 * <pre>
 * columns - contains an array of column definitions that can be used to create the table headers
 *           and Data Table columns
 * data    - an array of objects that can be used for the Data Table data definition
 * status  - overall status information, counts, warnings, etc.
 * </pre>
 *
 * Each server-process table is identified by {@link ServerTable}. The table-specific metric methods
 * define the metrics for each table, {@link #columnsFor(ServerTable)} converts those metrics to
 * column definitions. The frontend uses the returned column definitions to build the table headers
 * and DataTables column configuration.
 */
public class ServersView {

  /**
   * all the data needed for the Server status indicator(s)
   */
  public record Status(boolean hasServers, boolean hasProblemServers, boolean hasMissingMetrics,
      long serverCount, long problemServerCount, long missingMetricServerCount, String level,
      String message) {
  }

  /**
   * Definition of a column to be rendered in the UI
   */
  public record Column(String key, String label, String description, String uiClass) {
  }

  private record ServerMetricRow(ServerId server, MetricResponse response,
      Map<String,Number> metrics) {
  }

  /**
   * Server-process table identifiers accepted by /rest-v2/servers/view. These enum names are used
   * directly as the frontend table parameter values.
   */
  public enum ServerTable {
    COMPACTORS,
    GC_SUMMARY,
    GC_FILES,
    GC_WALS,
    MANAGERS,
    MANAGER_FATE,
    MANAGER_COMPACTIONS,
    SCAN_SERVERS,
    TABLET_SERVERS
  }

  private static final String LEVEL_OK = "OK";
  private static final String LEVEL_WARN = "WARN";

  public static final String RG_COL_KEY = "resourceGroup";
  public static final String ADDR_COL_KEY = "serverAddress";
  public static final String TIME_COL_KEY = "lastContact";

  /**
   * Common columns that are included in every ServersView table
   */
  private static final List<Column> COMMON_COLUMNS = List.of(
      new Column(TIME_COL_KEY, "Last Contact",
          "Time since the server last responded to the monitor", "duration"),
      new Column(RG_COL_KEY, "Resource Group", "Resource Group", ""),
      new Column(ADDR_COL_KEY, "Server Address", "Server address", ""));

  public final List<Map<String,Object>> data = new ArrayList<>();
  public final List<Column> columns;
  public final Status status;
  public final long timestamp;

  public ServersView(final Set<ServerId> servers, final long problemServerCount,
      final Cache<ServerId,MetricResponse> allMetrics, final long timestamp,
      final List<Column> requestedColumns) {

    AtomicInteger serversMissingMetrics = new AtomicInteger(0);
    // Grab the current metrics for each server
    List<ServerMetricRow> serverMetricRows = servers.stream().sorted().map(serverId -> {
      MetricResponse metricResponse = allMetrics.getIfPresent(serverId);
      boolean hasMetricData = hasMetricData(metricResponse);
      Map<String,Number> serverMetrics =
          hasMetricData ? metricValuesByName(metricResponse) : Map.of();

      if (!hasMetricData) {
        serversMissingMetrics.incrementAndGet();
      }

      return new ServerMetricRow(serverId, metricResponse, serverMetrics);
    }).toList();

    Set<String> presentMetricNames = serverMetricRows.stream()
        .flatMap(row -> row.metrics().keySet().stream()).collect(Collectors.toSet());

    // Keep common columns and filter out metric columns that have no values
    this.columns = requestedColumns.stream()
        .filter(col -> isCommonColumn(col.key()) || presentMetricNames.contains(col.key()))
        .toList();

    serverMetricRows.forEach(serverMetricRow -> {
      Map<String,Object> row = new LinkedHashMap<>();
      for (Column col : columns) {
        row.put(col.key(), valueForColumn(col.key(), serverMetricRow.server(),
            serverMetricRow.response(), serverMetricRow.metrics()));
      }
      data.add(row);
    });
    status = buildStatus(servers.size(), problemServerCount, serversMissingMetrics.get());
    this.timestamp = timestamp;
  }

  private static Object valueForColumn(String key, ServerId sid, MetricResponse mr,
      Map<String,Number> serverMetrics) {
    return switch (key) {
      case TIME_COL_KEY -> mr == null ? null : System.currentTimeMillis() - mr.getTimestamp();
      case RG_COL_KEY -> sid.getResourceGroup().canonical();
      case ADDR_COL_KEY -> sid.toHostPortString();
      default -> serverMetrics.get(key);
    };
  }

  private static Status buildStatus(int serverCount, long problemServerCount,
      int serversMissingMetrics) {
    final boolean hasServers = serverCount > 0;
    final boolean hasProblemServers = problemServerCount > 0;
    final boolean hasMissingMetrics = serversMissingMetrics > 0;

    List<String> warnings = new ArrayList<>(2);
    if (hasProblemServers) {
      warnings.add("One or more servers are unavailable");
    }
    if (hasMissingMetrics) {
      warnings.add("Metrics are not present (are metrics enabled?)");
    }

    if (warnings.isEmpty()) {
      // no warnings, set status to OK
      return new Status(hasServers, false, false, serverCount, 0, 0, LEVEL_OK, null);
    }

    final String message = "WARN: " + String.join("; ", warnings) + ".";
    return new Status(hasServers, hasProblemServers, hasMissingMetrics, serverCount,
        problemServerCount, serversMissingMetrics, LEVEL_WARN, message);
  }

  private static boolean hasMetricData(MetricResponse mr) {
    return mr != null && mr.getMetrics() != null && !mr.getMetrics().isEmpty();
  }

  private static boolean isCommonColumn(String key) {
    return COMMON_COLUMNS.stream().anyMatch(col -> col.key().equals(key));
  }

  /**
   * Builds the final ordered columns for a table. First adds the common columns, then adds the
   * table specific metrics.
   */
  public static List<Column> columnsFor(ServerTable table) {
    List<Column> cols = new ArrayList<>(COMMON_COLUMNS);
    cols.addAll(metricsForTable(table).stream().map(ServersView::metricColumn).toList());
    return cols;
  }

  private static List<Metric> metricsForTable(ServerTable table) {
    return switch (table) {
      case COMPACTORS -> compactorMetrics();
      case GC_SUMMARY -> gcSummaryMetrics();
      case GC_FILES -> gcFileMetrics();
      case GC_WALS -> gcWalMetrics();
      case MANAGERS -> managerMetrics();
      case MANAGER_FATE -> managerFateMetrics();
      case MANAGER_COMPACTIONS -> managerCompactionMetrics();
      case SCAN_SERVERS -> scanServerMetrics();
      case TABLET_SERVERS -> tabletServerMetrics();
    };
  }

  /**
   * The following helper methods are where the metrics included in each table are defined as well
   * as their order.
   */
  private static List<Metric> compactorMetrics() {
    return metricList(MetricDocSection.GENERAL_SERVER, MetricDocSection.COMPACTION,
        MetricDocSection.COMPACTOR);
  }

  private static List<Metric> gcSummaryMetrics() {
    return metricList(MetricDocSection.GENERAL_SERVER);
  }

  private static List<Metric> gcFileMetrics() {
    return Arrays.stream(Metric.values()).filter(metric -> {
      String name = metric.getName();
      return metric.getDocSection() == MetricDocSection.GARBAGE_COLLECTION
          && !name.startsWith("accumulo.gc.wal.");
    }).toList();
  }

  private static List<Metric> gcWalMetrics() {
    return Arrays.stream(Metric.values())
        .filter(metric -> metric.getName().startsWith("accumulo.gc.wal."))
        .collect(Collectors.toList());
  }

  private static List<Metric> managerMetrics() {
    return metricList(MetricDocSection.GENERAL_SERVER, MetricDocSection.MANAGER);
  }

  private static List<Metric> managerFateMetrics() {
    return metricList(MetricDocSection.FATE);
  }

  private static List<Metric> managerCompactionMetrics() {
    return metricList(MetricDocSection.COMPACTION);
  }

  private static List<Metric> scanServerMetrics() {
    return metricList(MetricDocSection.GENERAL_SERVER, MetricDocSection.SCAN_SERVER,
        MetricDocSection.SCAN, MetricDocSection.BLOCK_CACHE);
  }

  private static List<Metric> tabletServerMetrics() {
    return metricList(MetricDocSection.GENERAL_SERVER, MetricDocSection.TABLET_SERVER,
        MetricDocSection.SCAN, MetricDocSection.COMPACTION, MetricDocSection.BLOCK_CACHE);
  }

  /**
   * @return all the metrics for the given sections
   */
  private static List<Metric> metricList(MetricDocSection... sections) {
    Set<MetricDocSection> requestedSections = Set.of(sections);
    return Arrays.stream(Metric.values())
        .filter(metric -> requestedSections.contains(metric.getDocSection()))
        .collect(Collectors.toList());
  }

  /**
   * @return a Column definition converted from the given Metric
   */
  private static Column metricColumn(Metric metric) {
    String classes = Arrays.stream(metric.getColumnClasses())
        .map(Metric.MonitorCssClass::getCssClass).collect(Collectors.joining(" "));
    return new Column(metric.getName(), metric.getColumnHeader(), metric.getColumnDescription(),
        classes);
  }

  public static Map<String,Number> metricValuesByName(MetricResponse response) {
    var values = new HashMap<String,Number>();
    if (response == null || response.getMetrics() == null || response.getMetrics().isEmpty()) {
      return values;
    }

    for (var binary : response.getMetrics()) {
      var metric = FMetric.getRootAsFMetric(binary);
      var metricStatistic = extractStatistic(metric);
      if (metricStatistic == null || metricStatistic.equals("value")
          || metricStatistic.equals("count")) {
        // For metrics with the same name, but different tags, compute a sum
        Number val = SystemInformation.getMetricValue(metric);
        values.compute(metric.name(), (k, v) -> {
          if (v == null) {
            return val;
          } else if (v instanceof Integer i) {
            return i + val.intValue();
          } else if (v instanceof Long l) {
            return l + val.longValue();
          } else if (v instanceof Double d) {
            return d + val.doubleValue();
          } else {
            throw new RuntimeException("Unexpected value type: " + val.getClass());
          }
        });
      }
    }
    return values;
  }

  private static String extractStatistic(FMetric metric) {
    for (int i = 0; i < metric.tagsLength(); i++) {
      FTag tag = metric.tags(i);
      if (MetricResponseWrapper.STATISTIC_TAG.equals(tag.key())) {
        return normalizeStatistic(tag.value());
      }
    }
    return null;
  }

  private static String normalizeStatistic(String statistic) {
    if (statistic == null) {
      return null;
    }
    return statistic.toLowerCase();
  }

}

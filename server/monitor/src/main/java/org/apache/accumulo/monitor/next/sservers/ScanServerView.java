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
package org.apache.accumulo.monitor.next.sservers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.monitor.next.SystemInformation;
import org.apache.accumulo.server.metrics.MetricResponseWrapper;

/**
 * Data Transfer Object (DTO) for the Monitor Scan Servers page. It transforms backend metrics into
 * a UI-ready JSON response consumed by the frontend; each record component is serialized as a JSON
 * field.
 */
public record ScanServerView(long lastUpdate, List<Row> servers, Status status) {

  /**
   * all the data needed for a row in the table in monitor
   */
  public record Row(String host, String resourceGroup, long lastContact, boolean metricsAvailable,
      Number openFiles, Number queries, Number scannedEntries, Number queryResults,
      Number queryResultBytes, Number busyTimeouts, Number reservationConflicts,
      Number zombieThreads, Number serverIdle, Number lowMemoryDetected,
      Number scansPausedForMemory, Number scansReturnedEarlyForMemory) {
  }

  /**
   * all the data needed for the ScanServer status indicator(s)
   */
  public record Status(boolean hasScanServers, boolean hasProblemScanServers,
      boolean hasMissingMetrics, int scanServerCount, int problemScanServerCount,
      long missingMetricServerCount, String level, String message) {
  }

  private static final String LEVEL_OK = "OK";
  private static final String LEVEL_WARN = "WARN";

  public static ScanServerView fromMetrics(Collection<MetricResponse> responses,
      int scanServerCount, int problemScanServerCount, long snapshotTime) {
    var rows = rows(responses, snapshotTime);
    Status status = buildStatus(scanServerCount, problemScanServerCount, rows);
    return new ScanServerView(snapshotTime, rows, status);
  }

  private static List<Row> rows(Collection<MetricResponse> responses, long nowMs) {
    if (responses == null) {
      return List.of();
    }
    return responses.stream().map(response -> toRow(response, nowMs))
        .sorted(Comparator.comparing(Row::resourceGroup).thenComparing(Row::host)).toList();
  }

  private static Status buildStatus(int scanServerCount, int problemScanServerCount,
      List<Row> rows) {
    long missingMetricServerCount = rows.stream().filter(row -> !row.metricsAvailable()).count();
    boolean hasScanServers = scanServerCount > 0;
    boolean hasProblemScanServers = problemScanServerCount > 0;
    boolean hasMissingMetrics = missingMetricServerCount > 0;

    List<String> warnings = new ArrayList<>(2);
    if (hasProblemScanServers) {
      warnings.add("one or more scan servers are unavailable");
    }
    if (hasMissingMetrics) {
      warnings.add("ScanServer metrics are not present (are metrics enabled?)");
    }

    if (warnings.isEmpty()) {
      // no warnings, set status to OK
      return new Status(hasScanServers, false, false, scanServerCount, 0, 0, LEVEL_OK, null);
    }

    final String message = "WARN: " + String.join("; ", warnings) + ".";
    return new Status(hasScanServers, hasProblemScanServers, hasMissingMetrics, scanServerCount,
        problemScanServerCount, missingMetricServerCount, LEVEL_WARN, message);
  }

  private static Row toRow(MetricResponse response, long nowMs) {
    if (response == null) {
      return new Row(null, null, 0, false, null, null, null, null, null, null, null, null, null,
          null, null, null);
    }

    var values = metricValuesByName(response);
    var openFiles = values.get(Metric.SCAN_OPEN_FILES.getName());
    var queries = values.get(Metric.SCAN_QUERIES.getName());
    var scannedEntries = values.get(Metric.SCAN_SCANNED_ENTRIES.getName());
    var queryResults = values.get(Metric.SCAN_QUERY_SCAN_RESULTS.getName());
    var queryResultBytes = values.get(Metric.SCAN_QUERY_SCAN_RESULTS_BYTES.getName());
    var busyTimeouts = values.get(Metric.SCAN_BUSY_TIMEOUT_COUNT.getName());
    var reservationConflicts = values.get(Metric.SCAN_RESERVATION_CONFLICT_COUNTER.getName());
    var zombieThreads = values.get(Metric.SCAN_ZOMBIE_THREADS.getName());
    var serverIdle = values.get(Metric.SERVER_IDLE.getName());
    var lowMemoryDetected = values.get(Metric.LOW_MEMORY.getName());
    var scansPausedForMemory = values.get(Metric.SCAN_PAUSED_FOR_MEM.getName());
    var scansReturnedEarlyForMemory = values.get(Metric.SCAN_RETURN_FOR_MEM.getName());

    boolean allMetricsPresent =
        Stream.of(openFiles, queries, scannedEntries, queryResults, queryResultBytes, busyTimeouts,
            reservationConflicts, zombieThreads, serverIdle, lowMemoryDetected,
            scansPausedForMemory, scansReturnedEarlyForMemory).allMatch(Objects::nonNull);

    long lastContact = Math.max(0, nowMs - response.getTimestamp());

    return new Row(response.getServer(), response.getResourceGroup(), lastContact,
        allMetricsPresent, openFiles, queries, scannedEntries, queryResults, queryResultBytes,
        busyTimeouts, reservationConflicts, zombieThreads, serverIdle, lowMemoryDetected,
        scansPausedForMemory, scansReturnedEarlyForMemory);
  }

  private static Map<String,Number> metricValuesByName(MetricResponse response) {
    var values = new HashMap<String,Number>();
    if (response == null || response.getMetrics() == null || response.getMetrics().isEmpty()) {
      return values;
    }

    for (var binary : response.getMetrics()) {
      var metric = FMetric.getRootAsFMetric(binary);
      var metricStatistic = extractStatistic(metric);
      if (metricStatistic == null || metricStatistic.equals("value")
          || metricStatistic.equals("count")) {
        values.putIfAbsent(metric.name(), SystemInformation.getMetricValue(metric));
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

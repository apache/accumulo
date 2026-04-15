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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.admin.servers.ServerId;
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
 */
public class ServersView {

  /**
   * all the data needed for the Server status indicator(s)
   */
  public record Status(boolean hasServers, boolean hasProblemServers, boolean hasMissingMetrics,
      long serverCount, long problemServerCount, long missingMetricServerCount, String level,
      String message) {
  }

  public static final String TYPE_COL_NAME = "Server Type";
  public static final String RG_COL_NAME = "Resource Group";
  public static final String ADDR_COL_NAME = "Server Address";
  public static final String TIME_COL_NAME = "Last Contact";

  private static final String LEVEL_OK = "OK";
  private static final String LEVEL_WARN = "WARN";

  public final List<Map<String,Object>> data = new ArrayList<>();
  public final Set<String> columns = new TreeSet<>();
  public final Status status;
  public final long timestamp;

  public ServersView(final Set<ServerId> servers, final long problemServerCount,
      final Cache<ServerId,MetricResponse> allMetrics, final long timestamp) {

    AtomicInteger serversMissingMetrics = new AtomicInteger(0);
    servers.forEach(sid -> {
      Map<String,Object> metrics = new TreeMap<>();

      columns.add(TYPE_COL_NAME);
      metrics.put(TYPE_COL_NAME, sid.getType().name());
      columns.add(RG_COL_NAME);
      metrics.put(RG_COL_NAME, sid.getResourceGroup().canonical());
      columns.add(ADDR_COL_NAME);
      metrics.put(ADDR_COL_NAME, sid.toHostPortString());

      MetricResponse mr = allMetrics.getIfPresent(sid);
      if (mr != null) {
        // Don't use the timestamp for the last contact duration,
        // use the current time.
        columns.add(TIME_COL_NAME);
        metrics.put(TIME_COL_NAME, System.currentTimeMillis() - mr.getTimestamp());

        Map<String,Number> serverMetrics = metricValuesByName(mr);
        for (Entry<String,Number> e : serverMetrics.entrySet()) {
          columns.add(e.getKey());
          metrics.put(e.getKey(), e.getValue());
        }
        data.add(metrics);
      } else {
        serversMissingMetrics.incrementAndGet();
      }
    });
    status = buildStatus(servers.size(), problemServerCount, serversMissingMetrics.get());
    this.timestamp = timestamp;
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
          } else {
            if (v instanceof Integer i) {
              return i + val.intValue();
            } else if (v instanceof Long l) {
              return l + val.longValue();
            } else if (v instanceof Double d) {
              return d + val.doubleValue();
            } else {
              throw new RuntimeException("Unexpected value type: " + val.getClass());
            }
          }
        });
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

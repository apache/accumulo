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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.Metric.MetricDocSection;
import org.apache.accumulo.core.metrics.MonitorMeterRegistry;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.core.util.threads.ThreadPoolNames;
import org.apache.accumulo.monitor.next.SystemInformation;
import org.apache.accumulo.server.metrics.MetricResponseWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
      Map<String,List<FMetric>> metrics) {
  }

  /**
   * Server-process table identifiers accepted by /rest-v2/servers/view. These enum names are used
   * directly as the frontend table parameter values.
   */
  public enum ServerTable {
    COORDINATOR_QUEUES,
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

  public static final Column LAST_CONTACT_COLUMN = new Column(TIME_COL_KEY, "Last Contact",
      "Time since the server last responded to the monitor", "duration");
  public static final Column RG_COLUMN =
      new Column(RG_COL_KEY, "Resource Group", "Resource Group", "");
  public static final Column ADDR_COLUMN =
      new Column(ADDR_COL_KEY, "Server Address", "Server address", "");

  /**
   * Common columns that are included in every ServersView table
   */
  private static final List<ColumnFactory> COMMON_COLUMNS = List.of(new ColumnFactory() {

    @Override
    public Column getColumn() {
      return LAST_CONTACT_COLUMN;
    }

    @Override
    public Object getRowData(ServerId sid, MetricResponse mr,
        Map<String,List<FMetric>> serverMetrics) {
      return mr == null ? null : System.currentTimeMillis() - mr.getTimestamp();
    }
  }, new ColumnFactory() {

    @Override
    public Column getColumn() {
      return RG_COLUMN;
    }

    @Override
    public Object getRowData(ServerId sid, MetricResponse mr,
        Map<String,List<FMetric>> serverMetrics) {
      return sid.getResourceGroup().canonical();
    }
  }, new ColumnFactory() {

    @Override
    public Column getColumn() {
      return ADDR_COLUMN;
    }

    @Override
    public Object getRowData(ServerId sid, MetricResponse mr,
        Map<String,List<FMetric>> serverMetrics) {
      return sid.toHostPortString();
    }
  });

  public final List<Map<String,Object>> data = new ArrayList<>();
  public final List<Column> columns;
  public final Status status;
  public final long timestamp;

  public ServersView(final Set<ServerId> servers, final long problemServerCount,
      final Map<ServerId,MetricResponse> allMetrics, final long timestamp,
      final List<ColumnFactory> requestedColumns) {

    AtomicInteger serversMissingMetrics = new AtomicInteger(0);
    // Grab the current metrics for each server
    List<ServerMetricRow> serverMetricRows = servers.stream().sorted().map(serverId -> {
      MetricResponse metricResponse = allMetrics.get(serverId);
      boolean hasMetricData = hasMetricData(metricResponse);
      Map<String,List<FMetric>> serverMetrics =
          hasMetricData ? metricValuesByName(metricResponse) : Map.of();

      if (!hasMetricData) {
        serversMissingMetrics.incrementAndGet();
      }

      return new ServerMetricRow(serverId, metricResponse, serverMetrics);
    }).toList();

    this.columns = requestedColumns.stream().map(ColumnFactory::getColumn).toList();

    serverMetricRows.forEach(serverMetricRow -> {
      Map<String,Object> row = new LinkedHashMap<>();
      for (ColumnFactory colf : requestedColumns) {
        row.put(colf.getColumn().key(), colf.getRowData(serverMetricRow.server(),
            serverMetricRow.response(), serverMetricRow.metrics()));
      }
      data.add(row);
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

  private static boolean hasMetricData(MetricResponse mr) {
    return mr != null && mr.getMetrics() != null && !mr.getMetrics().isEmpty();
  }

  public interface ColumnFactory {
    Column getColumn();

    Object getRowData(ServerId sid, MetricResponse mr, Map<String,List<FMetric>> serverMetrics);
  }

  private static class RatioColumnFactory implements ColumnFactory {

    private final Column column;
    private final Metric numerator;
    private final Metric denominator;

    RatioColumnFactory(String label, String description, Metric numerator, Metric denominator) {
      this.column = new Column(numerator.getName() + "/" + denominator.getName(), label,
          description, Metric.MonitorCssClass.PERCENT.getCssClass());
      this.numerator = numerator;
      this.denominator = denominator;
    }

    @Override
    public Column getColumn() {
      return column;
    }

    @Override
    public Object getRowData(ServerId sid, MetricResponse mr,
        Map<String,List<FMetric>> serverMetrics) {
      var n = serverMetrics.get(numerator.getName());
      var d = serverMetrics.get(denominator.getName());

      if (n == null || d == null) {
        return null;
      }

      var numeratorSum = sum(n).doubleValue();
      var denominatorSum = sum(n).doubleValue();

      if (denominatorSum == 0) {
        return null;
      }

      return numeratorSum / denominatorSum;
    }
  }

  private static class MetricColumnFactory implements ColumnFactory {

    private final Column column;
    private final boolean computeRate;

    MetricColumnFactory(Metric metric) {
      String classes;
      if (metric.getType() == Metric.MetricType.FUNCTION_COUNTER) {
        if (Arrays.asList(metric.getColumnClasses()).contains(Metric.MonitorCssClass.BYTES)) {
          classes = Metric.MonitorCssClass.BYTES_RATE.getCssClass();
        } else {
          classes = Metric.MonitorCssClass.RATE.getCssClass();
        }
        computeRate = true;
      } else {
        classes = Arrays.stream(metric.getColumnClasses()).map(Metric.MonitorCssClass::getCssClass)
            .collect(Collectors.joining(" "));
        computeRate = false;
      }
      this.column = new Column(metric.getName(), metric.getColumnHeader(),
          metric.getColumnDescription(), classes);
    }

    @Override
    public Column getColumn() {
      return column;
    }

    @Override
    public Object getRowData(ServerId sid, MetricResponse mr,
        Map<String,List<FMetric>> serverMetrics) {
      var sum = sum(serverMetrics.getOrDefault(column.key, List.of()));
      if (computeRate) {
        return computeRate(sum);
      } else {
        return sum;
      }
    }
  }

  private static class ExecutorColumnFactory implements ColumnFactory {

    private static final Logger log = LoggerFactory.getLogger(ExecutorColumnFactory.class);

    private final Column column;
    private final String metricName;
    private final Predicate<String> tagPredicate;
    private final Type type;

    enum Type {
      COMPLETED, QUEUED
    }

    String getMetricName(Type t) {
      return switch (t) {
        case COMPLETED -> Metric.EXECUTOR_COMPLETED.getName();
        case QUEUED -> Metric.EXECUTOR_QUEUED.getName();
      };
    }

    String getCssClass(Type t) {
      return switch (t) {
        case COMPLETED -> Metric.MonitorCssClass.RATE.getCssClass();
        case QUEUED -> Metric.MonitorCssClass.NUMBER.getCssClass();
      };
    }

    ExecutorColumnFactory(Type type, String theadPoolPrefix, String label, String description) {
      this.type = type;
      this.metricName = getMetricName(type);
      this.tagPredicate = s -> s.startsWith(theadPoolPrefix);
      this.column =
          new Column(metricName + "-" + theadPoolPrefix, label, description, getCssClass(type));
    }

    ExecutorColumnFactory(Type type, String keySuffix, Predicate<String> tagPredicate, String label,
        String description) {
      this.type = type;
      this.metricName = getMetricName(type);
      this.tagPredicate = tagPredicate;
      this.column = new Column(metricName + "-" + keySuffix, label, description, getCssClass(type));
    }

    @Override
    public Column getColumn() {
      return column;
    }

    @Override
    public Object getRowData(ServerId sid, MetricResponse mr,
        Map<String,List<FMetric>> serverMetrics) {

      var metrics = serverMetrics.getOrDefault(metricName, List.of());

      Number sum = null;

      FTag ftag = new FTag();
      for (var metric : metrics) {
        boolean foundTag = false;
        String tag = null;
        for (int i = 0; i < metric.tagsLength(); i++) {
          metric.tags(ftag, i);
          var key = ftag.key();
          var value = ftag.value();
          if (key != null && value != null && key.equals("name") && tagPredicate.test(value)) {
            foundTag = true;
            tag = value;
            break;
          }
        }

        var metricStatistic = extractStatistic(metric);
        if (foundTag && (metricStatistic == null || metricStatistic.equals("value")
            || metricStatistic.equals("count"))) {
          var val = SystemInformation.getMetricValue(metric);
          log.trace("adding {}+{} for {} {} {} {}", sum, val, metric.name(), tag,
              sid.toHostPortString(), sid.getResourceGroup());
          sum = add(sum, SystemInformation.getMetricValue(metric));

        }
      }

      if (type == Type.COMPLETED) {
        // Convert to a rate
        return computeRate(sum);
      }

      return sum;
    }

  }

  private static Number computeRate(Number sum) {
    if (sum == null) {
      return null;
    }
    return sum.doubleValue() / MonitorMeterRegistry.STEP.toSeconds();
  }

  /**
   * Builds the final ordered columns for a table. First adds the common columns, then adds the
   * table specific metrics.
   */
  public static List<ColumnFactory> columnsFor(ServerTable table) {
    List<ColumnFactory> cols = new ArrayList<>(COMMON_COLUMNS);

    switch (table) {
      case COORDINATOR_QUEUES ->
        coordinatorQueueMetrics().forEach(m -> cols.add(new MetricColumnFactory(m)));
      case COMPACTORS -> compactorMetrics().forEach(m -> cols.add(new MetricColumnFactory(m)));
      case GC_SUMMARY -> gcSummaryMetrics().forEach(m -> cols.add(new MetricColumnFactory(m)));
      case GC_FILES -> gcFileMetrics().forEach(m -> cols.add(new MetricColumnFactory(m)));
      case GC_WALS -> gcWalMetrics().forEach(m -> cols.add(new MetricColumnFactory(m)));
      case MANAGERS -> managerMetrics().forEach(m -> cols.add(new MetricColumnFactory(m)));
      case MANAGER_FATE -> managerFateMetrics().forEach(m -> cols.add(new MetricColumnFactory(m)));
      case MANAGER_COMPACTIONS ->
        managerCompactionMetrics().forEach(m -> cols.add(new MetricColumnFactory(m)));
      case SCAN_SERVERS -> scanServerMetrics().forEach(m -> cols.add(new MetricColumnFactory(m)));
      case TABLET_SERVERS -> tabletServerColumns(cols);
    }
    return cols;
  }

  private static class ColumnFactoryList {
    private final Map<String,ColumnFactory> colFactories;

    public ColumnFactoryList(int size) {
      colFactories = new HashMap<>(size);
    }

    public void add(ColumnFactory factory) {
      colFactories.put(factory.getColumn().key(), factory);
    }

    public Collection<ColumnFactory> list() {
      return colFactories.values();
    }
  }

  private static void tabletServerColumns(List<ColumnFactory> cols) {

    List<Metric> tabletServerMetrics = tabletServerMetrics();
    ColumnFactoryList cfl = new ColumnFactoryList(tabletServerMetrics.size());
    tabletServerMetrics.forEach(tsm -> cfl.add(new MetricColumnFactory(tsm)));

    cfl.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.COMPLETED,
        ThreadPoolNames.RPC_POOL.poolName, "Completed RPCs",
        "Task completed by the Thrift thread pool"));
    cfl.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.QUEUED,
        ThreadPoolNames.RPC_POOL.poolName, "Queued RPCs",
        "Task queued for the Thrift thread pool"));

    // Scan columns
    cfl.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.COMPLETED,
        ThreadPoolNames.SCAN_EXECUTOR_PREFIX.poolName, "Completed scans",
        "Scan task completed by all scan thread pools"));
    cfl.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.QUEUED,
        ThreadPoolNames.SCAN_EXECUTOR_PREFIX.poolName, "Queued scans",
        "Scan task queued on all scan thread pools"));
    cfl.add(new RatioColumnFactory("Index cache hit",
        "Ratio of hits/total request for the index block cache", Metric.BLOCKCACHE_INDEX_HITCOUNT,
        Metric.BLOCKCACHE_INDEX_REQUESTCOUNT));
    cfl.add(new RatioColumnFactory("Data cache hit",
        "Ratio of hits/total request for the data block cache", Metric.BLOCKCACHE_DATA_HITCOUNT,
        Metric.BLOCKCACHE_DATA_REQUESTCOUNT));

    // Ingest and minc
    cfl.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.COMPLETED,
        ThreadPoolNames.TSERVER_MINOR_COMPACTOR_POOL.poolName, "Completed MinC",
        "Task completed by the minor compaction thread pool"));
    cfl.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.QUEUED,
        ThreadPoolNames.TSERVER_MINOR_COMPACTOR_POOL.poolName, "Queued MinC",
        "Task queued for the minor compaction thread pool"));

    // conditional update
    Predicate<String> condTagPredicate =
        tag -> ThreadPoolNames.TSERVER_CONDITIONAL_UPDATE_ROOT_POOL.poolName.equals(tag)
            || ThreadPoolNames.TSERVER_CONDITIONAL_UPDATE_META_POOL.poolName.equals(tag)
            || ThreadPoolNames.TSERVER_CONDITIONAL_UPDATE_USER_POOL.poolName.equals(tag);
    cfl.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.COMPLETED, "conditional.update",
        condTagPredicate, "Completed Conditional",
        "Task completed by the conditional update thread pool"));
    cfl.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.QUEUED, "conditional.update",
        condTagPredicate, "Queued Conditional",
        "Task queued for the conditional update thread pool"));

    cols.addAll(cfl.list());
    Comparator<ColumnFactory> comp =
        Comparator.<ColumnFactory,String>comparing(cf -> cf.getColumn().key())
            .thenComparing(COMMON_COLUMNS::contains);
    Collections.sort(cols, comp);

    // TODO create scan problems that is a sum of zombie and low memory
  }

  /**
   * The following helper methods are where the metrics included in each table are defined as well
   * as their order.
   */
  private static List<Metric> coordinatorQueueMetrics() {
    return metricList(m -> !m.getName().startsWith("accumulo.minc")
        && !m.getName().startsWith("accumulo.compaction.minc"), MetricDocSection.COMPACTION);
  }

  private static List<Metric> compactorMetrics() {
    return metricList(null, MetricDocSection.GENERAL_SERVER, MetricDocSection.COMPACTION,
        MetricDocSection.COMPACTOR);
  }

  private static List<Metric> gcSummaryMetrics() {
    return metricList(null, MetricDocSection.GENERAL_SERVER);
  }

  private static List<Metric> gcFileMetrics() {
    return metricList((m) -> !m.getName().startsWith("accumulo.gc.wal."),
        MetricDocSection.GARBAGE_COLLECTION);
  }

  private static List<Metric> gcWalMetrics() {
    return Arrays.stream(Metric.values()).filter(m -> m.getName().startsWith("accumulo.gc.wal."))
        .collect(Collectors.toList());
  }

  private static List<Metric> managerMetrics() {
    return metricList(null, MetricDocSection.GENERAL_SERVER, MetricDocSection.MANAGER);
  }

  private static List<Metric> managerFateMetrics() {
    return metricList(null, MetricDocSection.FATE);
  }

  private static List<Metric> managerCompactionMetrics() {
    return metricList(m -> !m.getName().startsWith("accumulo.minc")
        && !m.getName().startsWith("accumulo.compaction.minc"), MetricDocSection.COMPACTION);
  }

  private static List<Metric> scanServerMetrics() {
    return metricList(null, MetricDocSection.GENERAL_SERVER, MetricDocSection.SCAN_SERVER,
        MetricDocSection.SCAN, MetricDocSection.BLOCK_CACHE);
  }

  private static List<Metric> tabletServerMetrics() {
    return metricList(null, MetricDocSection.GENERAL_SERVER, MetricDocSection.TABLET_SERVER,
        MetricDocSection.SCAN, MetricDocSection.COMPACTION, MetricDocSection.BLOCK_CACHE);
  }

  /**
   * @return all the metrics for the given sections
   */
  private static List<Metric> metricList(Predicate<Metric> filter, MetricDocSection... sections) {
    Predicate<Metric> sectionPredicate = null;
    if (sections == null) {
      sectionPredicate = m -> false;
    } else {
      Set<MetricDocSection> requestedSections = Set.of(sections);
      sectionPredicate = m -> requestedSections.contains(m.getDocSection());
    }
    return Arrays.stream(Metric.values()).filter(sectionPredicate)
        .filter(filter == null ? m -> true : filter).collect(Collectors.toList());
  }

  public static Number add(Number n1, Number n2) {
    if (n1 == null && n2 == null) {
      return null;
    } else if (n1 == null) {
      return n2;
    } else if (n2 == null) {
      return n1;
    } else if (n1 instanceof Double || n2 instanceof Double) {
      return n1.doubleValue() + n2.doubleValue();
    } else if (n1 instanceof Long || n2 instanceof Long) {
      return n1.longValue() + n2.longValue();
    } else if (n1 instanceof Integer || n2 instanceof Integer) {
      return n1.intValue() + n2.intValue();
    } else {
      throw new IllegalArgumentException(
          "Unexpected value type: " + n1.getClass().getName() + " " + n2.getClass().getName());
    }
  }

  private static Number sum(List<FMetric> metrics) {
    Number sum = null;
    for (var metric : metrics) {
      sum = add(sum, SystemInformation.getMetricValue(metric));
    }
    return sum;
  }

  public static Map<String,List<FMetric>> metricValuesByName(MetricResponse response) {
    var values = new HashMap<String,List<FMetric>>();
    if (response == null || response.getMetrics() == null || response.getMetrics().isEmpty()) {
      return values;
    }

    for (var binary : response.getMetrics()) {
      var metric = FMetric.getRootAsFMetric(binary);
      var metricStatistic = extractStatistic(metric);
      if (metricStatistic == null || metricStatistic.equals("value")
          || metricStatistic.equals("count")) {
        values.computeIfAbsent(metric.name(), m -> new ArrayList<>()).add(metric);
      }
    }
    return values;
  }

  private static String extractStatistic(FMetric metric) {
    FTag tag = new FTag();
    for (int i = 0; i < metric.tagsLength(); i++) {
      tag = metric.tags(tag, i);
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

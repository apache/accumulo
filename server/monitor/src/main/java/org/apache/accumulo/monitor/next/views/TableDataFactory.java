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

import static org.apache.accumulo.monitor.next.views.TableDataFactory.MetricFilterPrefixes.GC_WAL;
import static org.apache.accumulo.monitor.next.views.TableDataFactory.MetricFilterPrefixes.MINC;
import static org.apache.accumulo.monitor.next.views.TableDataFactory.MetricFilterPrefixes.MINC_COMPACTION;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.Metric.MetricDocSection;
import org.apache.accumulo.core.metrics.flatbuffers.FMetric;
import org.apache.accumulo.core.metrics.flatbuffers.FTag;
import org.apache.accumulo.core.process.thrift.MetricResponse;
import org.apache.accumulo.core.util.threads.ThreadPoolNames;
import org.apache.accumulo.monitor.next.views.TableData.Column;
import org.apache.accumulo.server.metrics.MetricResponseWrapper;

/**
 * A factory for creating {@code TableData} Data Transfer Objects identified by a
 * {@link TableDataFactory.TableName}. The {@link TableDataFactory.TableName} defines the metrics
 * for each table, {@link #columnsFor(TableName)} converts those metrics to column definitions. The
 * frontend uses the returned column definitions to build the table headers and DataTables column
 * configuration.
 */
public class TableDataFactory {

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
   * Server-process table identifiers accepted by /rest-v2/servers/view. These enum names are used
   * directly as the frontend table parameter values.
   */
  public enum TableName {
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

  public static class StatType {
    public static final String COUNT = "count";
    public static final String VALUE = "value";
    public static final String AVERAGE = "avg";

    public static final Predicate<String> COUNT_OR_VALUE = s -> COUNT.equals(s) || VALUE.equals(s);
  }

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

  private record ServerMetricRow(ServerId server, MetricResponse response,
      Map<String,List<FMetric>> metrics) {
  }

  public static boolean hasMetricData(MetricResponse mr) {
    return mr != null && mr.getMetrics() != null && !mr.getMetrics().isEmpty();
  }

  public static TableData forColumns(final Set<ServerId> servers,
      final Map<ServerId,MetricResponse> allMetrics, final long timestamp,
      List<ColumnFactory> requestedColumns) {

    List<Column> columns = requestedColumns.stream().map(ColumnFactory::getColumn).toList();

    // Grab the current metrics for each server
    List<ServerMetricRow> serverMetricRows = servers.stream().sorted().map(serverId -> {
      MetricResponse metricResponse = allMetrics.get(serverId);
      boolean hasMetricData = hasMetricData(metricResponse);
      Map<String,List<FMetric>> serverMetrics =
          hasMetricData ? metricValuesByName(metricResponse) : Map.of();

      return new ServerMetricRow(serverId, metricResponse, serverMetrics);
    }).toList();

    List<Map<String,Object>> data = new ArrayList<>();
    serverMetricRows.forEach(serverMetricRow -> {
      Map<String,Object> row = new LinkedHashMap<>();
      for (ColumnFactory colf : requestedColumns) {
        row.put(colf.getColumn().key(), colf.getRowData(serverMetricRow.server(),
            serverMetricRow.response(), serverMetricRow.metrics()));
      }
      data.add(row);
    });

    return new TableData(columns, data, timestamp);

  }

  public static TableData forTable(final TableName table, final Set<ServerId> servers,
      final Map<ServerId,MetricResponse> allMetrics, final long timestamp) {

    List<ColumnFactory> requestedColumns = columnsFor(table);
    return forColumns(servers, allMetrics, timestamp, requestedColumns);
  }

  /**
   * Builds the final ordered columns for a table. First adds the common columns, then adds the
   * table specific metrics.
   */
  public static List<ColumnFactory> columnsFor(TableName table) {
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
      case SCAN_SERVERS -> scanServerColumns(cols);
      case TABLET_SERVERS -> tabletServerColumns(cols);
    }
    return cols;
  }

  private static void tabletServerColumns(List<ColumnFactory> cols) {

    commonColumns(cols);

    cols.add(new MetricColumnFactory(Metric.TSERVER_TABLETS_ONLINE));
    cols.add(new MetricColumnFactory(Metric.TSERVER_TABLETS_LONG_ASSIGNMENTS));

    scanColumns(cols);

    // Ingest and minc
    cols.add(new MetricColumnFactory(Metric.TSERVER_INGEST_ENTRIES));
    cols.add(new MetricColumnFactory(Metric.TSERVER_INGEST_BYTES));
    cols.add(new MetricColumnFactory(Metric.TSERVER_HOLD));
    cols.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.COMPLETED,
        ThreadPoolNames.TSERVER_MINOR_COMPACTOR_POOL.poolName, "Completed MinC",
        "Task completed by the minor compaction thread pool"));
    cols.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.QUEUED,
        ThreadPoolNames.TSERVER_MINOR_COMPACTOR_POOL.poolName, "Queued MinC",
        "Task queued for the minor compaction thread pool"));

    // conditional update
    Predicate<String> condTagPredicate =
        tag -> ThreadPoolNames.TSERVER_CONDITIONAL_UPDATE_ROOT_POOL.poolName.equals(tag)
            || ThreadPoolNames.TSERVER_CONDITIONAL_UPDATE_META_POOL.poolName.equals(tag)
            || ThreadPoolNames.TSERVER_CONDITIONAL_UPDATE_USER_POOL.poolName.equals(tag);
    cols.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.COMPLETED, "conditional.update",
        condTagPredicate, "Completed Conditional",
        "Task completed by the conditional update thread pool"));
    cols.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.QUEUED, "conditional.update",
        condTagPredicate, "Queued Conditional",
        "Task queued for the conditional update thread pool"));

    // TODO create scan problems that is a sum of zombie and low memory
  }

  private static void commonColumns(List<ColumnFactory> cols) {
    cols.add(new MetricColumnFactory(Metric.SERVER_IDLE));
    cols.add(new MetricColumnFactory(Metric.LOW_MEMORY));

    cols.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.COMPLETED,
        ThreadPoolNames.RPC_POOL.poolName, "Completed RPCs",
        "Task completed by the Thrift thread pool"));
    cols.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.QUEUED,
        ThreadPoolNames.RPC_POOL.poolName, "Queued RPCs",
        "Task queued for the Thrift thread pool"));
  }

  private static void scanColumns(List<ColumnFactory> cols) {
    cols.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.COMPLETED,
        ThreadPoolNames.SCAN_EXECUTOR_PREFIX.poolName, "Completed scans",
        "Scan task completed by all scan thread pools"));
    cols.add(new ExecutorColumnFactory(ExecutorColumnFactory.Type.QUEUED,
        ThreadPoolNames.SCAN_EXECUTOR_PREFIX.poolName, "Queued scans",
        "Scan task queued on all scan thread pools"));
    cols.add(new MetricColumnFactory(Metric.SCAN_ERRORS));
    cols.add(new MetricColumnFactory(Metric.SCAN_SCANNED_ENTRIES));
    cols.add(new MetricColumnFactory(Metric.SCAN_QUERY_SCAN_RESULTS));
    cols.add(new MetricColumnFactory(Metric.SCAN_QUERY_SCAN_RESULTS_BYTES));
    cols.add(new RatioColumnFactory("Index cache hit",
        "Ratio of hits/total request for the index block cache", Metric.BLOCKCACHE_INDEX_HITCOUNT,
        Metric.BLOCKCACHE_INDEX_REQUESTCOUNT));
    cols.add(new RatioColumnFactory("Data cache hit",
        "Ratio of hits/total request for the data block cache", Metric.BLOCKCACHE_DATA_HITCOUNT,
        Metric.BLOCKCACHE_DATA_REQUESTCOUNT));
  }

  private static void scanServerColumns(List<ColumnFactory> cols) {
    commonColumns(cols);
    scanColumns(cols);
    cols.add(new CacheSizeColumnFactory(Metric.SCAN_TABLET_METADATA_CACHE.getName(),
        "Tablets Cached", "The number of tablets for which a scan server has cached metadata."));
    cols.add(new MetricColumnFactory(Metric.SCAN_RESERVATION_FILES));
    cols.add(new MetricColumnFactory(Metric.SCAN_BUSY_TIMEOUT_COUNT));
    cols.add(new TimerColumnFactory(Metric.SCAN_RESERVATION_TOTAL_TIMER));
  }

  public static Map<String,List<FMetric>> metricValuesByName(MetricResponse response) {
    var values = new HashMap<String,List<FMetric>>();
    if (response == null || response.getMetrics() == null || response.getMetrics().isEmpty()) {
      return values;
    }

    FTag tag = new FTag();
    for (var binary : response.getMetrics()) {
      var metric = FMetric.getRootAsFMetric(binary);
      var metricStatistic = extractStatistic(metric, tag);
      if (metricStatistic == null || metricStatistic.equals(StatType.VALUE)
          || metricStatistic.equals(StatType.COUNT) || metricStatistic.equals(StatType.AVERAGE)) {
        values.computeIfAbsent(metric.name(), m -> new ArrayList<>()).add(metric);
      }
    }
    return values;
  }

  public static String extractStatistic(FMetric metric, FTag tag) {
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

  // Filters to use with the metricList method
  public static enum MetricFilterPrefixes {

    GC_WAL("accumulo.gc.wal."), MINC("accumulo.minc"), MINC_COMPACTION("accumulo.compaction.minc");

    String prefix;

    MetricFilterPrefixes(String prefix) {
      this.prefix = prefix;
    }

    public String getPrefix() {
      return prefix;
    }
  }

  /**
   * The following helper methods are where the metrics included in each table are defined as well
   * as their order.
   */
  private static List<Metric> coordinatorQueueMetrics() {
    return metricList(m -> !m.getName().startsWith(MINC.getPrefix())
        && !m.getName().startsWith(MINC_COMPACTION.getPrefix()), MetricDocSection.COMPACTION);
  }

  private static List<Metric> compactorMetrics() {
    return metricList(null, MetricDocSection.GENERAL_SERVER, MetricDocSection.COMPACTION,
        MetricDocSection.COMPACTOR);
  }

  private static List<Metric> gcSummaryMetrics() {
    return metricList(null, MetricDocSection.GENERAL_SERVER);
  }

  private static List<Metric> gcFileMetrics() {
    return metricList((m) -> !m.getName().startsWith(GC_WAL.getPrefix()),
        MetricDocSection.GARBAGE_COLLECTION);
  }

  private static List<Metric> gcWalMetrics() {
    return Arrays.stream(Metric.values()).filter(m -> m.getName().startsWith(GC_WAL.getPrefix()))
        .collect(Collectors.toList());
  }

  private static List<Metric> managerMetrics() {
    return metricList(null, MetricDocSection.GENERAL_SERVER, MetricDocSection.MANAGER);
  }

  private static List<Metric> managerFateMetrics() {
    return metricList(null, MetricDocSection.FATE);
  }

  private static List<Metric> managerCompactionMetrics() {
    return metricList(m -> !m.getName().startsWith(MINC.getPrefix())
        && !m.getName().startsWith(MINC_COMPACTION.getPrefix()), MetricDocSection.COMPACTION);
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

}

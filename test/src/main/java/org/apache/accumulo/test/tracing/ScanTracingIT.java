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
package org.apache.accumulo.test.tracing;

import static org.apache.accumulo.core.trace.TraceAttributes.EXECUTOR_KEY;
import static org.apache.accumulo.core.trace.TraceAttributes.EXTENT_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.trace.TraceAttributes;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

class ScanTracingIT extends ConfigurableMacBase {

  private static final int OTLP_PORT = PortUtils.getRandomFreePort();

  private static List<String> getJvmArgs() {
    String javaAgent = null;
    for (var cpi : System.getProperty("java.class.path").split(":")) {
      if (cpi.contains("opentelemetry-javaagent")) {
        javaAgent = cpi;
      }
    }

    Objects.requireNonNull(javaAgent);

    return List.of("-Dotel.traces.exporter=otlp", "-Dotel.exporter.otlp.protocol=http/protobuf",
        "-Dotel.exporter.otlp.endpoint=http://localhost:" + OTLP_PORT,
        "-Dotel.metrics.exporter=none", "-Dotel.logs.exporter=none", "-javaagent:" + javaAgent);
  }

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.getJvmOptions().addAll(getJvmArgs());
    // sized such that full table scans will not fit in the cache
    cfg.setProperty(Property.TSERV_DATACACHE_SIZE.getKey(), "8M");
    cfg.setProperty(Property.TSERV_SCAN_EXECUTORS_PREFIX.getKey() + "pool1.threads", "8");
  }

  private TraceCollector collector;

  @BeforeEach
  public void startCollector() throws Exception {
    collector = new TraceCollector("localhost", OTLP_PORT);
  }

  @AfterEach
  public void stopCollector() throws Exception {
    collector.stop();
  }

  @Test
  public void test() throws Exception {
    var names = getUniqueNames(8);
    runTest(names[0], 0, false, false, -1, -1, -1);
    runTest(names[1], 10, false, false, -1, -1, -1);
    // when the tables tablets are spread across two tablet servers, then all the tables data will
    // fit in cache
    runTest(names[2], 10, true, true, -1, -1, -1);
    runTest(names[3], 0, true, false, -1, -1, -1);
    runTest(names[4], 0, false, false, -1, -1, 2);
    runTest(names[5], 0, false, false, 32, 256, -1);
    runTest(names[6], 0, true, true, 32, 256, -1);
    runTest(names[7], 0, true, false, -1, -1, 2);
  }

  private void runTest(String tableName, int numSplits, boolean cacheData,
      boolean secondScanFitsInCache, int startRow, int endRow, int column) throws Exception {

    var ingestParams = new TestIngest.IngestParams(getClientProperties(), tableName);
    ingestParams.createTable = false;
    ingestParams.rows = 1000;
    ingestParams.cols = 10;

    try (var client = Accumulo.newClient().from(getClientProperties()).build()) {
      var ntc = new NewTableConfiguration();
      if (numSplits > 0) {
        var splits = TestIngest.getSplitPoints(0, 1000, numSplits);
        ntc.withSplits(splits);
      }

      var props = new HashMap<String,String>();

      if (cacheData) {
        props.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
      }

      // use a different executor for batch scans
      props.put("table.scan.dispatcher.opts.multi_executor", "pool1");

      ntc.setProperties(props);

      client.tableOperations().create(tableName, ntc);

      TestIngest.ingest(client, ingestParams);
      client.tableOperations().flush(tableName, null, null, true);
    }

    long expectedRows = ingestParams.rows;

    var options = new ScanTraceClient.Options(tableName);
    if (startRow != -1 && endRow != -1) {
      options.startRow = TestIngest.generateRow(startRow, 0).toString();
      options.endRow = TestIngest.generateRow(endRow, 0).toString();
      expectedRows = IntStream.range(startRow, endRow).count();
    }

    int expectedColumns = ingestParams.cols;

    if (column != -1) {
      var col = TestIngest.generateColumn(ingestParams, column);
      options.family = col.getColumnFamily().toString();
      options.qualifier = col.getColumnQualifier().toString();
      expectedColumns = 1;
    }

    var results = run(options);
    System.out.println(results);

    var tableId = getServerContext().getTableId(tableName).canonical();

    var expectedServers = getServerContext().getAmple().readTablets().forTable(TableId.of(tableId))
        .fetch(TabletMetadata.ColumnType.LOCATION).build().stream()
        .map(tm -> tm.getLocation().getHostAndPort().toString()).collect(Collectors.toSet());

    ScanTraceStats scanStats = new ScanTraceStats(false);
    ScanTraceStats batchScanStats = new ScanTraceStats(true);
    Set<String> extents1 = new TreeSet<>();
    Set<String> extents2 = new TreeSet<>();

    while (scanStats.getEntriesReturned() < expectedRows * expectedColumns
        || batchScanStats.getEntriesReturned() < expectedRows * expectedColumns) {
      var span = collector.take();
      var stats = ScanTraceStats.create(span);
      if (stats != null
          && span.stringAttributes.get(TraceAttributes.TABLE_ID_KEY.getKey()).equals(tableId)
          && (results.traceId1.equals(span.traceId) || results.traceId2.equals(span.traceId))) {
        assertTrue(
            expectedServers
                .contains(span.stringAttributes.get(TraceAttributes.SERVER_KEY.getKey())),
            () -> expectedServers + " " + span);
        if (stats.isBatchScan()) {
          assertEquals("pool1", span.stringAttributes.get(EXECUTOR_KEY.getKey()));
        } else {
          assertEquals("default", span.stringAttributes.get(EXECUTOR_KEY.getKey()));
        }
        if (numSplits == 0) {
          assertEquals(tableId + "<<", span.stringAttributes.get(EXTENT_KEY.getKey()));
        } else {
          var extent = span.stringAttributes.get(EXTENT_KEY.getKey());
          assertTrue(extent.startsWith(tableId + ";") || extent.startsWith(tableId + "<"));
        }
        assertEquals(1, stats.getSeeks());
        if (stats.isBatchScan()) {
          assertEquals(results.traceId1, span.traceId);
          extents1.add(span.stringAttributes.get(EXTENT_KEY.getKey()));
        } else {
          assertEquals(results.traceId2, span.traceId);
          extents2.add(span.stringAttributes.get(EXTENT_KEY.getKey()));
        }
      } else {
        continue;
      }

      if (stats.isBatchScan()) {
        batchScanStats.merge(stats);
      } else {
        scanStats.merge(stats);
      }
    }

    if (numSplits > 0) {
      assertEquals(numSplits, extents1.size());
      assertEquals(numSplits, extents2.size());
    }

    System.out.println(scanStats);
    System.out.println(batchScanStats);

    assertEquals(expectedRows * expectedColumns, results.scanCount, results::toString);

    var statsList = List.of(batchScanStats, scanStats);
    for (int i = 0; i < statsList.size(); i++) {
      var stats = statsList.get(i);
      assertEquals(expectedRows * 10, stats.getEntriesRead(), stats::toString);
      assertEquals(results.scanCount, stats.getEntriesReturned(), stats::toString);
      // When filtering on columns will read more data than we return
      double colMultiplier = 10.0 / expectedColumns;
      assertClose((long) (results.scanSize * colMultiplier), stats.getBytesRead(), .05);
      assertClose(results.scanSize, stats.getBytesReturned(), .05);
      if (secondScanFitsInCache && i == 1) {
        assertEquals(0, stats.getFileBytesRead(), stats::toString);
        assertEquals(0, stats.getDataCacheMisses(), stats::toString);
      } else {
        assertTrue(
            stats.getFileBytesRead() > 0 && stats.getFileBytesRead() < stats.getBytesRead() * .01,
            stats::toString);
      }
      if (cacheData) {
        assertEquals(0, stats.getDataCacheBypasses(), stats::toString);
        assertTrue(stats.getDataCacheHits() + stats.getDataCacheMisses() > 0, stats::toString);
        if (stats.getFileBytesRead() == 0) {
          assertEquals(0L, stats.getDataCacheMisses(), stats::toString);
        }
      } else {
        assertEquals(0, stats.getDataCacheHits(), stats::toString);
        assertEquals(0, stats.getDataCacheMisses(), stats::toString);
        assertTrue(stats.getDataCacheBypasses() > stats.getSeeks(), stats::toString);
      }
      // May see rfile metadata reads for each tablet
      var cacheSum = stats.getIndexCacheHits() + stats.getIndexCacheMisses();
      assertTrue(cacheSum <= (numSplits + 1) * 2L, stats::toString);
      assertEquals(0, stats.getIndexCacheBypasses(), stats::toString);
    }

  }

  public void assertClose(long expected, long value, double e) {
    assertTrue(Math.abs(1 - (double) expected / (double) value) < e,
        () -> expected + " " + value + " " + e);
  }

  /**
   * Runs ScanTraceClient in an external process so it can be instrumented with the open telemetry
   * java agent. Use json to get data to/from external process.
   */
  public ScanTraceClient.Results run(ScanTraceClient.Options opts)
      throws IOException, InterruptedException {
    opts.clientPropsPath = getCluster().getClientPropsPath();
    var proc = getCluster().exec(ScanTraceClient.class, getJvmArgs(), new Gson().toJson(opts));
    assertEquals(0, proc.getProcess().waitFor());
    var out = proc.readStdOut();
    var result = Arrays.stream(out.split("\\n")).filter(line -> line.startsWith("RESULT:"))
        .findFirst().orElse("RESULT:{}");
    result = result.substring("RESULT:".length());
    return new Gson().fromJson(result, ScanTraceClient.Results.class);
  }

  /**
   * Helper class that encapsulates data from a scan trace making it easier to access and
   * centralizing the code for accessing data from a span.
   */
  static class ScanTraceStats {
    final Map<String,Long> scanStats;
    final boolean isBatchScan;

    ScanTraceStats(SpanData spanData) {
      this.scanStats = spanData.integerAttributes;
      this.isBatchScan = spanData.name.contains("multiscan-batch");
    }

    ScanTraceStats(boolean isBatchScan) {
      scanStats = new TreeMap<>();
      this.isBatchScan = isBatchScan;
    }

    void merge(ScanTraceStats other) {
      Preconditions.checkArgument(isBatchScan == other.isBatchScan);
      other.scanStats.forEach((k, v) -> {
        scanStats.merge(k, v, Long::sum);
      });
    }

    /**
     * @return a ScanTrace if span is from a scan batch, otherwise return null
     */
    static ScanTraceStats create(SpanData data) {
      if (data.name.contains("scan-batch")) {
        return new ScanTraceStats(data);
      }
      return null;
    }

    boolean isBatchScan() {
      return isBatchScan;
    }

    long getEntriesRead() {
      return scanStats.getOrDefault(TraceAttributes.ENTRIES_READ_KEY.getKey(), 0L);
    }

    long getEntriesReturned() {
      return scanStats.getOrDefault(TraceAttributes.ENTRIES_RETURNED_KEY.getKey(), 0L);
    }

    long getFileBytesRead() {
      return scanStats.getOrDefault(TraceAttributes.BYTES_READ_FILE_KEY.getKey(), 0L);
    }

    long getBytesRead() {
      return scanStats.getOrDefault(TraceAttributes.BYTES_READ_KEY.getKey(), 0L);
    }

    long getBytesReturned() {
      return scanStats.getOrDefault(TraceAttributes.BYTES_RETURNED_KEY.getKey(), 0L);
    }

    long getDataCacheHits() {
      return scanStats.getOrDefault(TraceAttributes.DATA_HITS_KEY.getKey(), 0L);
    }

    long getDataCacheMisses() {
      return scanStats.getOrDefault(TraceAttributes.DATA_MISSES_KEY.getKey(), 0L);
    }

    long getDataCacheBypasses() {
      return scanStats.getOrDefault(TraceAttributes.DATA_BYPASSES_KEY.getKey(), 0L);
    }

    long getIndexCacheHits() {
      return scanStats.getOrDefault(TraceAttributes.INDEX_HITS_KEY.getKey(), 0L);
    }

    long getIndexCacheMisses() {
      return scanStats.getOrDefault(TraceAttributes.INDEX_MISSES_KEY.getKey(), 0L);
    }

    long getIndexCacheBypasses() {
      return scanStats.getOrDefault(TraceAttributes.INDEX_BYPASSES_KEY.getKey(), 0L);
    }

    long getSeeks() {
      return scanStats.getOrDefault(TraceAttributes.SEEKS_KEY.getKey(), 0L);
    }

    @Override
    public String toString() {
      return "ScanTraceStats{isBatchScan=" + isBatchScan + ", scanStats=" + scanStats + '}';
    }
  }
}

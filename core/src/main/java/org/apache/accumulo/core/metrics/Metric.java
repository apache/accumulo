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
package org.apache.accumulo.core.metrics;

import java.util.HashMap;
import java.util.Map;

public enum Metric {
  // General Server Metrics
  SERVER_IDLE("accumulo.server.idle", MetricType.GAUGE,
      "Indicates if the server is idle or not. The value will be 1 when idle and 0 when not idle.",
      MetricCategory.GENERAL_SERVER),
  LOW_MEMORY("accumulo.detected.low.memory", MetricType.GAUGE,
      "Reports 1 when process memory usage is above threshold, 0 when memory is okay.",
      MetricCategory.GENERAL_SERVER),

  // Compactor Metrics
  COMPACTOR_MAJC_STUCK("accumulo.compactor.majc.stuck", MetricType.LONG_TASK_TIMER,
      "Number and duration of stuck major compactions", MetricCategory.COMPACTOR),
  COMPACTOR_ENTRIES_READ("accumulo.compactor.entries.read", MetricType.FUNCTION_COUNTER,
      "Number of entries read by all compactions that have run on this compactor",
      MetricCategory.COMPACTOR),
  COMPACTOR_ENTRIES_WRITTEN("accumulo.compactor.entries.written", MetricType.FUNCTION_COUNTER,
      "Number of entries written by all compactions that have run on this compactor",
      MetricCategory.COMPACTOR),

  // Fate Metrics
  FATE_TYPE_IN_PROGRESS("accumulo.fate.ops.in.progress.by.type", MetricType.GAUGE,
      "Number of FATE operations in progress. The type is designated by the `op.type` tag.",
      MetricCategory.FATE),
  FATE_OPS("accumulo.fate.ops", MetricType.GAUGE, "Tracks all the current FATE ops in any state.",
      MetricCategory.FATE),
  FATE_OPS_ACTIVITY("accumulo.fate.ops.activity", MetricType.GAUGE, "", MetricCategory.FATE),
  FATE_ERRORS("accumulo.fate.errors", MetricType.GAUGE, "", MetricCategory.FATE),
  FATE_TX("accumulo.fate.tx", MetricType.GAUGE,
      "The state is now in a tag (e.g., state=new, state=in.progress, state=failed, etc.).",
      MetricCategory.FATE),

  // Garbage Collection Metrics
  GC_STARTED("accumulo.gc.started", MetricType.GAUGE, "Timestamp GC file collection cycle started",
      MetricCategory.GARBAGE_COLLECTION),
  GC_FINISHED("accumulo.gc.finished", MetricType.GAUGE, "Timestamp GC file collect cycle finished",
      MetricCategory.GARBAGE_COLLECTION),
  GC_CANDIDATES("accumulo.gc.candidates", MetricType.GAUGE,
      "Number of files that are candidates for deletion", MetricCategory.GARBAGE_COLLECTION),
  GC_IN_USE("accumulo.gc.in.use", MetricType.GAUGE, "Number of candidate files still in use",
      MetricCategory.GARBAGE_COLLECTION),
  GC_DELETED("accumulo.gc.deleted", MetricType.GAUGE, "Number of candidate files deleted",
      MetricCategory.GARBAGE_COLLECTION),
  GC_ERRORS("accumulo.gc.errors", MetricType.GAUGE, "Number of candidate deletion errors",
      MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_STARTED("accumulo.gc.wal.started", MetricType.GAUGE,
      "Timestamp GC WAL collection cycle started", MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_FINISHED("accumulo.gc.wal.finished", MetricType.GAUGE,
      "Timestamp GC WAL collect cycle finished", MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_CANDIDATES("accumulo.gc.wal.candidates", MetricType.GAUGE,
      "Number of files that are candidates for deletion", MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_IN_USE("accumulo.gc.wal.in.use", MetricType.GAUGE,
      "Number of wal file candidates that are still in use", MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_DELETED("accumulo.gc.wal.deleted", MetricType.GAUGE,
      "Number of candidate wal files deleted", MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_ERRORS("accumulo.gc.wal.errors", MetricType.GAUGE,
      "Number candidate wal file deletion errors", MetricCategory.GARBAGE_COLLECTION),
  GC_POST_OP_DURATION("accumulo.gc.post.op.duration", MetricType.GAUGE,
      "GC metadata table post operation duration in milliseconds",
      MetricCategory.GARBAGE_COLLECTION),
  GC_RUN_CYCLE("accumulo.gc.run.cycle", MetricType.GAUGE,
      "gauge incremented each gc cycle run, rest on process start",
      MetricCategory.GARBAGE_COLLECTION),

  // Tablet Server Metrics
  TSERVER_ENTRIES("accumulo.tserver.entries", MetricType.GAUGE, "Number of entries",
      MetricCategory.TABLET_SERVER),
  TSERVER_MEM_ENTRIES("accumulo.tserver.entries.mem", MetricType.GAUGE,
      "Number of entries in memory", MetricCategory.TABLET_SERVER),
  TSERVER_MAJC_RUNNING("accumulo.tserver.majc.running", MetricType.GAUGE,
      "Number of active major compactions", MetricCategory.TABLET_SERVER),
  TSERVER_MAJC_STUCK("accumulo.tserver.majc.stuck", MetricType.GAUGE,
      "Number and duration of stuck major compactions", MetricCategory.TABLET_SERVER),
  TSERVER_MAJC_QUEUED("accumulo.tserver.majc.queued", MetricType.GAUGE,
      "Number of queued major compactions", MetricCategory.TABLET_SERVER),
  TSERVER_MINC_QUEUED("accumulo.tserver.minc.queued", MetricType.GAUGE,
      "Number of queued minor compactions", MetricCategory.TABLET_SERVER),
  TSERVER_MINC_RUNNING("accumulo.tserver.minc.running", MetricType.GAUGE,
      "Number of active minor compactions", MetricCategory.TABLET_SERVER),
  TSERVER_MINC_TOTAL("accumulo.tserver.minc.total", MetricType.GAUGE,
      "Total number of minor compactions performed", MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_ONLINE("accumulo.tserver.tablets.online", MetricType.GAUGE,
      "Number of online tablets", MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_LONG_ASSIGNMENTS("accumulo.tserver.tablets.assignments.warning", MetricType.GAUGE,
      "Number of tablet assignments that are taking a long time", MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_OPENING("accumulo.tserver.tablets.opening", MetricType.GAUGE,
      "Number of opening tablets", MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_UNOPENED("accumulo.tserver.tablets.unopened", MetricType.GAUGE,
      "Number of unopened tablets", MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_FILES("accumulo.tserver.tablets.files", MetricType.GAUGE,
      "Number of files per tablet", MetricCategory.TABLET_SERVER),
  TSERVER_INGEST_MUTATIONS("accumulo.tserver.ingest.mutations", MetricType.GAUGE,
      "Ingest mutation count. The rate can be derived from this metric.",
      MetricCategory.TABLET_SERVER),
  TSERVER_INGEST_BYTES("accumulo.tserver.ingest.bytes", MetricType.GAUGE,
      "Ingest byte count. The rate can be derived from this metric.", MetricCategory.TABLET_SERVER),
  TSERVER_HOLD("accumulo.tserver.hold", MetricType.GAUGE, "Time commits held",
      MetricCategory.TABLET_SERVER),

  // Scan Metrics
  SCAN_RESERVATION_TOTAL_TIMER("accumulo.scan.reservation.total.timer", MetricType.TIMER,
      "Time to reserve a tablet's files for scan.", MetricCategory.SCAN_SERVER),
  SCAN_RESERVATION_WRITEOUT_TIMER("accumulo.scan.reservation.writeout.timer", MetricType.TIMER,
      "Time to write out a tablets file reservations for scan", MetricCategory.SCAN_SERVER),
  SCAN_RESERVATION_CONFLICT_COUNTER("accumulo.scan.reservation.conflict.count", MetricType.COUNTER,
      "Counts instances where file reservation attempts for scans encountered conflicts",
      MetricCategory.SCAN_SERVER),
  SCAN_BUSY_TIMEOUT_COUNT("accumulo.scan.busy.timeout.count", MetricType.COUNTER,
      "Count of the scans where a busy timeout happened.", MetricCategory.SCAN_SERVER),
  SCAN_TABLET_METADATA_CACHE("accumulo.scan.tablet.metadata.cache", MetricType.CACHE,
      "Scan server tablet cache metrics.", MetricCategory.SCAN_SERVER),
  SCAN_TIMES("accumulo.scan.times", MetricType.TIMER,
      "scan session life time from creation to close", MetricCategory.SCAN_SERVER),
  SCAN_OPEN_FILES("accumulo.scan.files.open", MetricType.GAUGE, "Number of files open for scans",
      MetricCategory.SCAN_SERVER),
  SCAN_RESULTS("accumulo.scan.result", MetricType.GAUGE, "Results per scan",
      MetricCategory.SCAN_SERVER),
  SCAN_YIELDS("accumulo.scan.yields", MetricType.GAUGE, "", MetricCategory.SCAN_SERVER),
  SCAN_START("accumulo.scan.start", MetricType.COUNTER, "calls to start a scan / multiscan",
      MetricCategory.SCAN_SERVER),
  SCAN_CONTINUE("accumulo.scan.continue", MetricType.COUNTER,
      "calls to continue a scan / multiscan", MetricCategory.SCAN_SERVER),
  SCAN_CLOSE("accumulo.scan.close", MetricType.COUNTER, "calls to close a scan / multiscan",
      MetricCategory.SCAN_SERVER),
  SCAN_QUERIES("accumulo.scan.queries", MetricType.GAUGE, "Number of queries",
      MetricCategory.SCAN_SERVER),
  SCAN_SCANNED_ENTRIES("accumulo.scan.query.scanned.entries", MetricType.GAUGE,
      "Count of scanned entries. The rate can be derived from this metric.",
      MetricCategory.SCAN_SERVER),
  SCAN_QUERY_SCAN_RESULTS("accumulo.scan.query.results", MetricType.GAUGE,
      "Query count. The rate can be derived from this metric.", MetricCategory.SCAN_SERVER),
  SCAN_QUERY_SCAN_RESULTS_BYTES("accumulo.scan.query.results.bytes", MetricType.GAUGE,
      "Query byte count. The rate can be derived from this metric.", MetricCategory.SCAN_SERVER),
  SCAN_PAUSED_FOR_MEM("accumulo.scan.paused.for.memory", MetricType.COUNTER,
      "scan paused due to server being low on memory", MetricCategory.SCAN_SERVER),
  SCAN_RETURN_FOR_MEM("accumulo.scan.return.early.for.memory", MetricType.COUNTER,
      "scan returned results early due to server being low on memory", MetricCategory.SCAN_SERVER),
  SCAN_ZOMBIE_THREADS("accumulo.scan.zombie.threads", MetricType.GAUGE,
      "Number of scan threads that have no associated client session", MetricCategory.SCAN_SERVER),

  // Major Compaction Metrics
  MAJC_QUEUED("accumulo.tserver.compactions.majc.queued", MetricType.GAUGE,
      "The compaction service information is in a tag: `id={i|e}_{compactionServiceName}_{executor_name}`.",
      MetricCategory.TABLET_SERVER),
  MAJC_RUNNING("accumulo.tserver.compactions.majc.running", MetricType.GAUGE,
      "The compaction service information is in a tag: `id={i|e}_{compactionServiceName}_{executor_name}`.",
      MetricCategory.TABLET_SERVER),
  MAJC_PAUSED("accumulo.tserver.compactions.majc.paused", MetricType.COUNTER,
      "major compaction pause count", MetricCategory.TABLET_SERVER),

  // Minor Compaction Metrics
  MINC_QUEUED("accumulo.tserver.compactions.minc.queued", MetricType.TIMER,
      "Queued minor compactions time queued", MetricCategory.TABLET_SERVER),
  MINC_RUNNING("accumulo.tserver.compactions.minc.running", MetricType.TIMER,
      "Minor compactions time active", MetricCategory.TABLET_SERVER),
  MINC_PAUSED("accumulo.tserver.compactions.minc.paused", MetricType.COUNTER,
      "minor compactor pause count", MetricCategory.TABLET_SERVER),

  // Updates (Ingest) Metrics
  UPDATE_ERRORS("accumulo.tserver.updates.error", MetricType.GAUGE,
      "Count of errors during tablet updates. Type/reason for error is stored in a tag (e.g., type=permission, type=unknown.tablet, type=constraint.violation).",
      MetricCategory.TABLET_SERVER),
  UPDATE_COMMIT("accumulo.tserver.updates.commit", MetricType.TIMER, "committing a single mutation",
      MetricCategory.TABLET_SERVER),
  UPDATE_COMMIT_PREP("accumulo.tserver.updates.commit.prep", MetricType.TIMER,
      "preparing to commit a single mutation", MetricCategory.TABLET_SERVER),
  UPDATE_WALOG_WRITE("accumulo.tserver.updates.walog.write", MetricType.TIMER,
      "writing batch of mutations to WAL", MetricCategory.TABLET_SERVER),
  UPDATE_MUTATION_ARRAY_SIZE("accumulo.tserver.updates.mutation.arrays.size",
      MetricType.DISTRIBUTION_SUMMARY, "batch size of mutations from client",
      MetricCategory.TABLET_SERVER),

  // Thrift Metrics

  THRIFT_IDLE("accumulo.thrift.idle", MetricType.DISTRIBUTION_SUMMARY,
      "time waiting to execute a RPC request", MetricCategory.THRIFT),
  THRIFT_EXECUTE("accumulo.thrift.execute", MetricType.DISTRIBUTION_SUMMARY,
      "time to execute a RPC request", MetricCategory.THRIFT),

  // Block Cache Metrics
  BLOCKCACHE_INDEX_HITCOUNT("accumulo.blockcache.index.hitcount", MetricType.FUNCTION_COUNTER,
      "Index block cache hit count", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_INDEX_REQUESTCOUNT("accumulo.blockcache.index.requestcount",
      MetricType.FUNCTION_COUNTER, "Index block cache request count", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_INDEX_EVICTIONCOUNT("accumulo.blockcache.index.evictioncount",
      MetricType.FUNCTION_COUNTER, "Index block cache eviction count", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_DATA_HITCOUNT("accumulo.blockcache.data.hitcount", MetricType.FUNCTION_COUNTER,
      "Data block cache hit count", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_DATA_REQUESTCOUNT("accumulo.blockcache.data.requestcount", MetricType.FUNCTION_COUNTER,
      "Data block cache request count", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_DATA_EVICTIONCOUNT("accumulo.blockcache.data.evictioncount",
      MetricType.FUNCTION_COUNTER, "Data block cache eviction count", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_SUMMARY_HITCOUNT("accumulo.blockcache.summary.hitcount", MetricType.FUNCTION_COUNTER,
      "Summary block cache hit count", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_SUMMARY_REQUESTCOUNT("accumulo.blockcache.summary.requestcount",
      MetricType.FUNCTION_COUNTER, "Summary block cache request count", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_SUMMARY_EVICTIONCOUNT("accumulo.blockcache.summary.evictioncount",
      MetricType.FUNCTION_COUNTER, "Summary block cache eviction count",
      MetricCategory.BLOCK_CACHE),

  // Manager Metrics
  MANAGER_BALANCER_MIGRATIONS_NEEDED("accumulo.manager.balancer.migrations.needed",
      MetricType.GAUGE,
      "The total number of migrations that need to complete before the system is balanced.",
      MetricCategory.MANAGER);

  private final String name;
  private final MetricType type;
  private final String description;
  private final MetricCategory category;

  private static final Map<String,Metric> NAME_TO_ENUM_MAP = new HashMap<>();

  static {
    for (Metric metric : values()) {
      NAME_TO_ENUM_MAP.put(metric.name, metric);
    }
  }

  Metric(String name, MetricType type, String description, MetricCategory category) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.category = category;
  }

  public String getName() {
    return name;
  }

  public MetricType getType() {
    return type;
  }

  public String getDescription() {
    return description;
  }

  public MetricCategory getCategory() {
    return category;
  }

  public enum MetricType {
    GAUGE, COUNTER, TIMER, LONG_TASK_TIMER, DISTRIBUTION_SUMMARY, FUNCTION_COUNTER, CACHE
  }

  public enum MetricCategory {
    GENERAL_SERVER("General Server Metrics"),
    COMPACTOR("Compactor Metrics"),
    FATE("Fate Metrics"),
    GARBAGE_COLLECTION("Garbage Collection Metrics"),
    TABLET_SERVER("Tablet Server Metrics"),
    SCAN_SERVER("Scan Server Metrics"),
    THRIFT("Thrift Metrics"),
    BLOCK_CACHE("Block Cache Metrics"),
    MANAGER("Manager Metrics");

    private final String sectionTitle;

    MetricCategory(String sectionTitle) {
      this.sectionTitle = sectionTitle;
    }

    public String getSectionTitle() {
      return sectionTitle;
    }
  }

  public static Metric fromName(String name) {
    Metric metric = NAME_TO_ENUM_MAP.get(name);
    if (metric == null) {
      throw new IllegalArgumentException("No enum constant for metric name: " + name);
    }
    return metric;
  }

}

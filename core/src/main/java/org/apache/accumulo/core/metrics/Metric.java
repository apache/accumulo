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
  COMPACTOR_MAJC_STUCK("accumulo.compactor.majc.stuck", MetricType.LONG_TASK_TIMER, "",
      MetricCategory.COMPACTOR),
  COMPACTOR_ENTRIES_READ("accumulo.compactor.entries.read", MetricType.FUNCTION_COUNTER,
      "Number of entries read by all threads performing compactions.", MetricCategory.COMPACTOR),
  COMPACTOR_ENTRIES_WRITTEN("accumulo.compactor.entries.written", MetricType.FUNCTION_COUNTER,
      "Number of entries written by all threads performing compactions.", MetricCategory.COMPACTOR),

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
  GC_STARTED("accumulo.gc.started", MetricType.GAUGE, "", MetricCategory.GARBAGE_COLLECTION),
  GC_FINISHED("accumulo.gc.finished", MetricType.GAUGE, "", MetricCategory.GARBAGE_COLLECTION),
  GC_CANDIDATES("accumulo.gc.candidates", MetricType.GAUGE, "", MetricCategory.GARBAGE_COLLECTION),
  GC_IN_USE("accumulo.gc.in.use", MetricType.GAUGE, "", MetricCategory.GARBAGE_COLLECTION),
  GC_DELETED("accumulo.gc.deleted", MetricType.GAUGE, "", MetricCategory.GARBAGE_COLLECTION),
  GC_ERRORS("accumulo.gc.errors", MetricType.GAUGE, "", MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_STARTED("accumulo.gc.wal.started", MetricType.GAUGE, "",
      MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_FINISHED("accumulo.gc.wal.finished", MetricType.GAUGE, "",
      MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_CANDIDATES("accumulo.gc.wal.candidates", MetricType.GAUGE, "",
      MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_IN_USE("accumulo.gc.wal.in.use", MetricType.GAUGE, "", MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_DELETED("accumulo.gc.wal.deleted", MetricType.GAUGE, "",
      MetricCategory.GARBAGE_COLLECTION),
  GC_WAL_ERRORS("accumulo.gc.wal.errors", MetricType.GAUGE, "", MetricCategory.GARBAGE_COLLECTION),
  GC_POST_OP_DURATION("accumulo.gc.post.op.duration", MetricType.GAUGE, "",
      MetricCategory.GARBAGE_COLLECTION),
  GC_RUN_CYCLE("accumulo.gc.run.cycle", MetricType.GAUGE, "", MetricCategory.GARBAGE_COLLECTION),

  // Tablet Server Metrics
  TSERVER_ENTRIES("accumulo.tserver.entries", MetricType.GAUGE, "", MetricCategory.TABLET_SERVER),
  TSERVER_MEM_ENTRIES("accumulo.tserver.entries.mem", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_MAJC_RUNNING("accumulo.tserver.majc.running", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_MAJC_STUCK("accumulo.tserver.majc.stuck", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_MAJC_QUEUED("accumulo.tserver.majc.queued", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_MINC_QUEUED("accumulo.tserver.minc.queued", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_MINC_RUNNING("accumulo.tserver.minc.running", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_MINC_TOTAL("accumulo.tserver.minc.total", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_ONLINE("accumulo.tserver.tablets.online", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_ASSIGNMENTS_WARNING("accumulo.tserver.tablets.assignments.warning",
      MetricType.GAUGE, "", MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_OPENING("accumulo.tserver.tablets.opening", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_UNOPENED("accumulo.tserver.tablets.unopened", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_TABLETS_FILES("accumulo.tserver.tablets.files", MetricType.GAUGE, "",
      MetricCategory.TABLET_SERVER),
  TSERVER_INGEST_MUTATIONS("accumulo.tserver.ingest.mutations", MetricType.GAUGE,
      "Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be derived.",
      MetricCategory.TABLET_SERVER),
  TSERVER_INGEST_BYTES("accumulo.tserver.ingest.bytes", MetricType.GAUGE,
      "Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be derived.",
      MetricCategory.TABLET_SERVER),
  TSERVER_HOLD("accumulo.tserver.hold", MetricType.GAUGE, "", MetricCategory.TABLET_SERVER),

  // Scan Metrics
  SCAN_RESERVATION_TOTAL_TIMER("accumulo.scan.reservation.total.timer", MetricType.TIMER,
      "Time to reserve a tablet's files for scan.", MetricCategory.SCAN_SERVER),
  SCAN_RESERVATION_WRITEOUT_TIMER("accumulo.scan.reservation.writeout.timer", MetricType.TIMER,
      "Time to write out a tablets file reservations for scan", MetricCategory.SCAN_SERVER),
  SCAN_RESERVATION_CONFLICT_TIMER("accumulo.scan.reservation.conflict.timer", MetricType.TIMER, "",
      MetricCategory.SCAN_SERVER),
  SCAN_BUSY_TIMEOUT_COUNT("accumulo.scan.busy.timeout.count", MetricType.COUNTER,
      "Count of the scans where a busy timeout happened.", MetricCategory.SCAN_SERVER),
  SCAN_TABLET_METADATA_CACHE("accumulo.scan.tablet.metadata.cache", MetricType.CACHE,
      "Scan server tablet cache metrics.", MetricCategory.SCAN_SERVER),
  SCAN_TIMES("accumulo.scan.times", MetricType.TIMER, "", MetricCategory.SCAN_SERVER),
  SCAN_OPEN_FILES("accumulo.scan.files.open", MetricType.GAUGE, "", MetricCategory.SCAN_SERVER),
  SCAN_RESULTS("accumulo.scan.result", MetricType.GAUGE, "", MetricCategory.SCAN_SERVER),
  SCAN_YIELDS("accumulo.scan.yields", MetricType.GAUGE, "", MetricCategory.SCAN_SERVER),
  SCAN_START("accumulo.scan.start", MetricType.COUNTER, "", MetricCategory.SCAN_SERVER),
  SCAN_CONTINUE("accumulo.scan.continue", MetricType.COUNTER, "", MetricCategory.SCAN_SERVER),
  SCAN_CLOSE("accumulo.scan.close", MetricType.COUNTER, "", MetricCategory.SCAN_SERVER),
  SCAN_QUERIES("accumulo.scan.queries", MetricType.GAUGE, "", MetricCategory.SCAN_SERVER),
  SCAN_SCANNED_ENTRIES("accumulo.scan.query.scanned.entries", MetricType.GAUGE,
      "Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be derived.",
      MetricCategory.SCAN_SERVER),
  SCAN_QUERY_SCAN_RESULTS("accumulo.scan.query.results", MetricType.GAUGE,
      "Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be derived.",
      MetricCategory.SCAN_SERVER),
  SCAN_QUERY_SCAN_RESULTS_BYTES("accumulo.scan.query.results.bytes", MetricType.GAUGE,
      "Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be derived.",
      MetricCategory.SCAN_SERVER),
  SCAN_PAUSED_FOR_MEM("accumulo.scan.paused.for.memory", MetricType.COUNTER, "",
      MetricCategory.SCAN_SERVER),
  SCAN_RETURN_FOR_MEM("accumulo.scan.return.early.for.memory", MetricType.COUNTER, "",
      MetricCategory.SCAN_SERVER),
  SCAN_ZOMBIE_THREADS("accumulo.scan.zombie.threads", MetricType.GAUGE, "",
      MetricCategory.SCAN_SERVER),

  // Major Compaction Metrics
  MAJC_QUEUED("accumulo.tserver.compactions.majc.queued", MetricType.GAUGE,
      "The compaction service information is in a tag: id={i|e}_{compactionServiceName}_{executor_name}.",
      MetricCategory.TABLET_SERVER),
  MAJC_RUNNING("accumulo.tserver.compactions.majc.running", MetricType.GAUGE,
      "The compaction service information is in a tag: id={i|e}_{compactionServiceName}_{executor_name}.",
      MetricCategory.TABLET_SERVER),
  MAJC_PAUSED("accumulo.tserver.compactions.majc.paused", MetricType.COUNTER, "",
      MetricCategory.TABLET_SERVER),

  // Minor Compaction Metrics
  MINC_QUEUED("accumulo.tserver.compactions.minc.queued", MetricType.TIMER, "",
      MetricCategory.TABLET_SERVER),
  MINC_RUNNING("accumulo.tserver.compactions.minc.running", MetricType.TIMER, "",
      MetricCategory.TABLET_SERVER),
  MINC_PAUSED("accumulo.tserver.compactions.minc.paused", MetricType.COUNTER, "",
      MetricCategory.TABLET_SERVER),

  // Updates (Ingest) Metrics
  UPDATE_ERRORS("accumulo.tserver.updates.error", MetricType.GAUGE,
      "Type is stored in a tag (e.g., type=permission, type=unknown.tablet, type=constraint.violation).",
      MetricCategory.TABLET_SERVER),
  UPDATE_COMMIT("accumulo.tserver.updates.commit", MetricType.TIMER, "",
      MetricCategory.TABLET_SERVER),
  UPDATE_COMMIT_PREP("accumulo.tserver.updates.commit.prep", MetricType.TIMER, "",
      MetricCategory.TABLET_SERVER),
  UPDATE_WALOG_WRITE("accumulo.tserver.updates.walog.write", MetricType.TIMER, "",
      MetricCategory.TABLET_SERVER),
  UPDATE_MUTATION_ARRAY_SIZE("accumulo.tserver.updates.mutation.arrays.size",
      MetricType.DISTRIBUTION_SUMMARY, "", MetricCategory.TABLET_SERVER),

  // Thrift Metrics

  THRIFT_IDLE("accumulo.thrift.idle", MetricType.DISTRIBUTION_SUMMARY, "", MetricCategory.THRIFT),
  THRIFT_EXECUTE("accumulo.thrift.execute", MetricType.DISTRIBUTION_SUMMARY, "",
      MetricCategory.THRIFT),

  // Block Cache Metrics
  BLOCKCACHE_INDEX_HITCOUNT("accumulo.blockcache.index.hitcount", MetricType.FUNCTION_COUNTER, "",
      MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_INDEX_REQUESTCOUNT("accumulo.blockcache.index.requestcount",
      MetricType.FUNCTION_COUNTER, "", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_INDEX_EVICTIONCOUNT("accumulo.blockcache.index.evictioncount",
      MetricType.FUNCTION_COUNTER, "", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_DATA_HITCOUNT("accumulo.blockcache.data.hitcount", MetricType.FUNCTION_COUNTER, "",
      MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_DATA_REQUESTCOUNT("accumulo.blockcache.data.requestcount", MetricType.FUNCTION_COUNTER,
      "", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_DATA_EVICTIONCOUNT("accumulo.blockcache.data.evictioncount",
      MetricType.FUNCTION_COUNTER, "", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_SUMMARY_HITCOUNT("accumulo.blockcache.summary.hitcount", MetricType.FUNCTION_COUNTER,
      "", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_SUMMARY_REQUESTCOUNT("accumulo.blockcache.summary.requestcount",
      MetricType.FUNCTION_COUNTER, "", MetricCategory.BLOCK_CACHE),
  BLOCKCACHE_SUMMARY_EVICTIONCOUNT("accumulo.blockcache.summary.evictioncount",
      MetricType.FUNCTION_COUNTER, "", MetricCategory.BLOCK_CACHE),

  // Manager Metrics
  MANAGER_BALANCER_MIGRATIONS_NEEDED("accumulo.manager.balancer.migrations.needed",
      MetricType.GAUGE,
      "The number of migrations that need to complete before the system is balanced.",
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
    GENERAL_SERVER,
    COMPACTOR,
    FATE,
    GARBAGE_COLLECTION,
    TABLET_SERVER,
    SCAN_SERVER,
    THRIFT,
    BLOCK_CACHE,
    MANAGER
  }

  public static Metric fromName(String name) {
    Metric metric = NAME_TO_ENUM_MAP.get(name);
    if (metric == null) {
      throw new IllegalArgumentException("No enum constant for metric name: " + name);
    }
    return metric;
  }

}

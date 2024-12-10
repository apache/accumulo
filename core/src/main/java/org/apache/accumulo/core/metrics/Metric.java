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
      MetricDocSection.GENERAL_SERVER),
  LOW_MEMORY("accumulo.detected.low.memory", MetricType.GAUGE,
      "Reports 1 when process memory usage is above the threshold, reports 0 when memory is okay.",
      MetricDocSection.GENERAL_SERVER),
  THRIFT_IDLE("accumulo.thrift.idle", MetricType.DISTRIBUTION_SUMMARY,
      "Time waiting to execute an RPC request.", MetricDocSection.GENERAL_SERVER),
  THRIFT_EXECUTE("accumulo.thrift.execute", MetricType.DISTRIBUTION_SUMMARY,
      "Time to execute an RPC request.", MetricDocSection.GENERAL_SERVER),

  // Compactor Metrics
  COMPACTION_SVC_ERRORS("accumulo.compaction.svc.misconfigured", MetricType.GAUGE,
      "A value of 1 indicates a misconfiguration in the compaction service, while a value of 0 indicates that the configuration is valid.",
      MetricDocSection.COMPACTION),
  COMPACTOR_MAJC_IN_PROGRESS("accumulo.compaction.majc.in_progress", MetricType.GAUGE,
      "Indicator of whether a compaction is in-progress (value: 1) or not (value: 0). An"
          + " in-progress compaction could also be stuck.",
      MetricDocSection.COMPACTION),
  COMPACTOR_MAJC_STUCK("accumulo.compaction.majc.stuck", MetricType.LONG_TASK_TIMER,
      "Number and duration of stuck major compactions.", MetricDocSection.COMPACTION),
  COMPACTOR_MINC_STUCK("accumulo.compaction.minc.stuck", MetricType.LONG_TASK_TIMER,
      "Number and duration of stuck minor compactions.", MetricDocSection.COMPACTION),
  COMPACTOR_ENTRIES_READ("accumulo.compaction.entries.read", MetricType.FUNCTION_COUNTER,
      "Number of entries read by all compactions that have run on this compactor (majc) or tserver (minc).",
      MetricDocSection.COMPACTION),
  COMPACTOR_ENTRIES_WRITTEN("accumulo.compaction.entries.written", MetricType.FUNCTION_COUNTER,
      "Number of entries written by all compactions that have run on this compactor (majc) or tserver (minc).",
      MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUES("accumulo.compaction.queue.count", MetricType.GAUGE,
      "Number of priority queues for compaction jobs.", MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUE_LENGTH("accumulo.compaction.queue.length", MetricType.GAUGE,
      "Length of priority queue.", MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_DEQUEUED("accumulo.compaction.queue.jobs.dequeued",
      MetricType.GAUGE, "Count of dequeued jobs.", MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED("accumulo.compaction.queue.jobs.queued",
      MetricType.GAUGE, "Count of queued jobs.", MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_REJECTED("accumulo.compaction.queue.jobs.rejected",
      MetricType.GAUGE, "Count of rejected jobs.", MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_PRIORITY("accumulo.compaction.queue.jobs.priority",
      MetricType.GAUGE, "Lowest priority queued job.", MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_MIN_AGE("accumulo.compaction.queue.jobs.min.age",
      MetricType.GAUGE, "Minimum age of currently queued jobs in seconds.",
      MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_MAX_AGE("accumulo.compaction.queue.jobs.max.age",
      MetricType.GAUGE, "Maximum age of currently queued jobs in seconds.",
      MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_AVG_AGE("accumulo.compaction.queue.jobs.avg.age",
      MetricType.GAUGE, "Average age of currently queued jobs in seconds.",
      MetricDocSection.COMPACTION),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_POLL_TIMER("accumulo.compaction.queue.jobs.exit.time",
      MetricType.TIMER, "Tracks time a job spent in the queue before exiting the queue.",
      MetricDocSection.COMPACTION),

  // Fate Metrics
  FATE_TYPE_IN_PROGRESS("accumulo.fate.ops.in.progress.by.type", MetricType.GAUGE,
      "Number of FATE operations in progress. The op type is designated by the `op.type` tag.",
      MetricDocSection.FATE),
  FATE_OPS("accumulo.fate.ops", MetricType.GAUGE,
      "Number of all the current FATE ops in any state.", MetricDocSection.FATE),
  FATE_OPS_ACTIVITY("accumulo.fate.ops.activity", MetricType.GAUGE,
      "Count of the total number of times fate operations are added, updated, and removed.",
      MetricDocSection.FATE),
  FATE_ERRORS("accumulo.fate.errors", MetricType.GAUGE,
      "Count of errors that occurred when attempting to gather fate metrics.",
      MetricDocSection.FATE),
  FATE_TX("accumulo.fate.tx", MetricType.GAUGE,
      "The state is now in a tag (e.g., state=new, state=in.progress, state=failed, etc.).",
      MetricDocSection.FATE),

  // Garbage Collection Metrics
  GC_STARTED("accumulo.gc.started", MetricType.GAUGE, "Timestamp GC file collection cycle started.",
      MetricDocSection.GARBAGE_COLLECTION),
  GC_FINISHED("accumulo.gc.finished", MetricType.GAUGE, "Timestamp GC file collect cycle finished.",
      MetricDocSection.GARBAGE_COLLECTION),
  GC_CANDIDATES("accumulo.gc.candidates", MetricType.GAUGE,
      "Number of files that are candidates for deletion.", MetricDocSection.GARBAGE_COLLECTION),
  GC_IN_USE("accumulo.gc.in.use", MetricType.GAUGE, "Number of candidate files still in use.",
      MetricDocSection.GARBAGE_COLLECTION),
  GC_DELETED("accumulo.gc.deleted", MetricType.GAUGE, "Number of candidate files deleted.",
      MetricDocSection.GARBAGE_COLLECTION),
  GC_ERRORS("accumulo.gc.errors", MetricType.GAUGE, "Number of candidate deletion errors.",
      MetricDocSection.GARBAGE_COLLECTION),
  GC_WAL_STARTED("accumulo.gc.wal.started", MetricType.GAUGE,
      "Timestamp GC WAL collection cycle started.", MetricDocSection.GARBAGE_COLLECTION),
  GC_WAL_FINISHED("accumulo.gc.wal.finished", MetricType.GAUGE,
      "Timestamp GC WAL collect cycle finished.", MetricDocSection.GARBAGE_COLLECTION),
  GC_WAL_CANDIDATES("accumulo.gc.wal.candidates", MetricType.GAUGE,
      "Number of files that are candidates for deletion.", MetricDocSection.GARBAGE_COLLECTION),
  GC_WAL_IN_USE("accumulo.gc.wal.in.use", MetricType.GAUGE,
      "Number of wal file candidates that are still in use.", MetricDocSection.GARBAGE_COLLECTION),
  GC_WAL_DELETED("accumulo.gc.wal.deleted", MetricType.GAUGE,
      "Number of candidate wal files deleted.", MetricDocSection.GARBAGE_COLLECTION),
  GC_WAL_ERRORS("accumulo.gc.wal.errors", MetricType.GAUGE,
      "Number candidate wal file deletion errors.", MetricDocSection.GARBAGE_COLLECTION),
  GC_POST_OP_DURATION("accumulo.gc.post.op.duration", MetricType.GAUGE,
      "GC metadata table post operation duration in milliseconds.",
      MetricDocSection.GARBAGE_COLLECTION),
  GC_RUN_CYCLE("accumulo.gc.run.cycle", MetricType.GAUGE,
      "Count of gc cycle runs. Value is reset on process start.",
      MetricDocSection.GARBAGE_COLLECTION),

  // Tablet Server Metrics
  TSERVER_ENTRIES("accumulo.tserver.entries", MetricType.GAUGE,
      "Number of entries assigned to a TabletServer.", MetricDocSection.TABLET_SERVER),
  TSERVER_MEM_ENTRIES("accumulo.tserver.entries.mem", MetricType.GAUGE,
      "Number of entries in memory.", MetricDocSection.TABLET_SERVER),
  TSERVER_MINC_QUEUED("accumulo.minc.queued", MetricType.GAUGE,
      "Number of queued minor compactions.", MetricDocSection.COMPACTION),
  TSERVER_MINC_RUNNING("accumulo.minc.running", MetricType.GAUGE,
      "Number of active minor compactions.", MetricDocSection.COMPACTION),
  TSERVER_MINC_TOTAL("accumulo.minc.total", MetricType.GAUGE,
      "Total number of minor compactions performed.", MetricDocSection.COMPACTION),
  TSERVER_TABLETS_ONLINE("accumulo.tablets.online", MetricType.GAUGE, "Number of online tablets.",
      MetricDocSection.TABLET_SERVER),
  TSERVER_TABLETS_LONG_ASSIGNMENTS("accumulo.tablets.assignments.warning", MetricType.GAUGE,
      "Number of tablet assignments that are taking longer than the configured warning duration.",
      MetricDocSection.TABLET_SERVER),
  TSERVER_TABLETS_OPENING("accumulo.tablets.opening", MetricType.GAUGE,
      "Number of opening tablets.", MetricDocSection.TABLET_SERVER),
  TSERVER_TABLETS_UNOPENED("accumulo.tablets.unopened", MetricType.GAUGE,
      "Number of unopened tablets.", MetricDocSection.TABLET_SERVER),
  TSERVER_TABLETS_FILES("accumulo.tablets.files", MetricType.GAUGE, "Number of files per tablet.",
      MetricDocSection.TABLET_SERVER),
  TSERVER_INGEST_ENTRIES("accumulo.ingest.entries", MetricType.GAUGE,
      "Ingest entry (a key/value) count. The rate can be derived from this metric.",
      MetricDocSection.TABLET_SERVER),
  TSERVER_INGEST_BYTES("accumulo.ingest.bytes", MetricType.GAUGE,
      "Ingest byte count. The rate can be derived from this metric.",
      MetricDocSection.TABLET_SERVER),
  TSERVER_HOLD("accumulo.ingest.hold", MetricType.GAUGE,
      "Duration for which commits have been held in milliseconds.", MetricDocSection.TABLET_SERVER),
  TSERVER_TABLETS_ONLINE_ONDEMAND("accumulo.tablets.ondemand.online", MetricType.GAUGE,
      "Number of online on-demand tablets", MetricDocSection.TABLET_SERVER),
  TSERVER_TABLETS_ONDEMAND_UNLOADED_FOR_MEM("accumulo.tablets.ondemand.unloaded.lowmem",
      MetricType.GAUGE, "Number of online on-demand tablets unloaded due to low memory",
      MetricDocSection.TABLET_SERVER),

  // Scan Server Metrics
  SCAN_RESERVATION_TOTAL_TIMER("accumulo.scan.reservation.total.timer", MetricType.TIMER,
      "Time to reserve a tablet's files for scan.", MetricDocSection.SCAN_SERVER),
  SCAN_RESERVATION_WRITEOUT_TIMER("accumulo.scan.reservation.writeout.timer", MetricType.TIMER,
      "Time to write out a tablets file reservations for scan.", MetricDocSection.SCAN_SERVER),
  SCAN_RESERVATION_CONFLICT_COUNTER("accumulo.scan.reservation.conflict.count", MetricType.COUNTER,
      "Count of instances where file reservation attempts for scans encountered conflicts.",
      MetricDocSection.SCAN_SERVER),
  SCAN_TABLET_METADATA_CACHE("accumulo.scan.tablet.metadata.cache", MetricType.CACHE,
      "Scan server tablet cache metrics.", MetricDocSection.SCAN_SERVER),

  // Scan Metrics
  SCAN_BUSY_TIMEOUT_COUNT("accumulo.scan.busy.timeout.count", MetricType.COUNTER,
      "Count of the scans where a busy timeout happened.", MetricDocSection.SCAN),
  SCAN_TIMES("accumulo.scan.times", MetricType.TIMER, "Scan session lifetime (creation to close).",
      MetricDocSection.SCAN),
  SCAN_OPEN_FILES("accumulo.scan.files.open", MetricType.GAUGE, "Number of files open for scans.",
      MetricDocSection.SCAN),
  SCAN_RESULTS("accumulo.scan.result", MetricType.GAUGE, "Results per scan.",
      MetricDocSection.SCAN),
  SCAN_YIELDS("accumulo.scan.yields", MetricType.GAUGE, "Counts scans that have yielded.",
      MetricDocSection.SCAN),
  SCAN_START("accumulo.scan.start", MetricType.COUNTER,
      "Number of calls to start a scan or multiscan.", MetricDocSection.SCAN),
  SCAN_CONTINUE("accumulo.scan.continue", MetricType.COUNTER,
      "Number of calls to continue a scan or multiscan.", MetricDocSection.SCAN),
  SCAN_CLOSE("accumulo.scan.close", MetricType.COUNTER,
      "Number of calls to close a scan or multiscan.", MetricDocSection.SCAN),
  SCAN_QUERIES("accumulo.scan.queries", MetricType.GAUGE, "Number of queries made during scans.",
      MetricDocSection.SCAN),
  SCAN_SCANNED_ENTRIES("accumulo.scan.query.scanned.entries", MetricType.GAUGE,
      "Count of scanned entries. The rate can be derived from this metric.", MetricDocSection.SCAN),
  SCAN_QUERY_SCAN_RESULTS("accumulo.scan.query.results", MetricType.GAUGE,
      "Query count. The rate can be derived from this metric.", MetricDocSection.SCAN),
  SCAN_QUERY_SCAN_RESULTS_BYTES("accumulo.scan.query.results.bytes", MetricType.GAUGE,
      "Query byte count. The rate can be derived from this metric.", MetricDocSection.SCAN),
  SCAN_PAUSED_FOR_MEM("accumulo.scan.paused.for.memory", MetricType.COUNTER,
      "Count of scans paused due to server being low on memory.", MetricDocSection.SCAN),
  SCAN_RETURN_FOR_MEM("accumulo.scan.return.early.for.memory", MetricType.COUNTER,
      "Count of scans that returned results early due to server being low on memory.",
      MetricDocSection.SCAN),
  SCAN_ZOMBIE_THREADS("accumulo.scan.zombie.threads", MetricType.GAUGE,
      "Number of scan threads that have no associated client session.", MetricDocSection.SCAN),

  // Major Compaction Metrics
  MAJC_PAUSED("accumulo.compaction.majc.paused", MetricType.COUNTER,
      "Number of paused major compactions.", MetricDocSection.COMPACTOR),

  // Minor Compaction Metrics
  MINC_QUEUED("accumulo.compaction.minc.queued", MetricType.TIMER,
      "Queued minor compactions time queued.", MetricDocSection.COMPACTION),
  MINC_RUNNING("accumulo.compaction.minc.running", MetricType.TIMER,
      "Minor compactions time active.", MetricDocSection.COMPACTION),
  MINC_PAUSED("accumulo.compaction.minc.paused", MetricType.COUNTER,
      "Number of paused minor compactions.", MetricDocSection.COMPACTION),

  // Updates (Ingest) Metrics
  UPDATE_ERRORS("accumulo.updates.error", MetricType.GAUGE,
      "Count of errors during tablet updates. Type/reason for error is stored in the `type` tag (e.g., type=permission, type=unknown.tablet, type=constraint.violation).",
      MetricDocSection.TABLET_SERVER),
  UPDATE_LOCK("accumulo.updates.lock", MetricType.TIMER,
      "Average time taken for conditional mutation to get a row lock.",
      MetricDocSection.TABLET_SERVER),
  UPDATE_CHECK("accumulo.updates.check", MetricType.TIMER,
      "Average time taken for conditional mutation to check conditions.",
      MetricDocSection.TABLET_SERVER),
  UPDATE_COMMIT("accumulo.updates.commit", MetricType.TIMER,
      "Average time taken to commit a mutation.", MetricDocSection.TABLET_SERVER),
  UPDATE_COMMIT_PREP("accumulo.updates.commit.prep", MetricType.TIMER,
      "Average time taken to prepare to commit a single mutation.", MetricDocSection.TABLET_SERVER),
  UPDATE_WALOG_WRITE("accumulo.updates.walog.write", MetricType.TIMER,
      "Time taken to write a batch of mutations to WAL.", MetricDocSection.TABLET_SERVER),
  UPDATE_MUTATION_ARRAY_SIZE("accumulo.updates.mutation.arrays.size",
      MetricType.DISTRIBUTION_SUMMARY, "Batch size of mutations from client.",
      MetricDocSection.TABLET_SERVER),

  // Block Cache Metrics
  BLOCKCACHE_INDEX_HITCOUNT("accumulo.blockcache.index.hitcount", MetricType.FUNCTION_COUNTER,
      "Index block cache hit count.", MetricDocSection.BLOCK_CACHE),
  BLOCKCACHE_INDEX_REQUESTCOUNT("accumulo.blockcache.index.requestcount",
      MetricType.FUNCTION_COUNTER, "Index block cache request count.",
      MetricDocSection.BLOCK_CACHE),
  BLOCKCACHE_INDEX_EVICTIONCOUNT("accumulo.blockcache.index.evictioncount",
      MetricType.FUNCTION_COUNTER, "Index block cache eviction count.",
      MetricDocSection.BLOCK_CACHE),
  BLOCKCACHE_DATA_HITCOUNT("accumulo.blockcache.data.hitcount", MetricType.FUNCTION_COUNTER,
      "Data block cache hit count.", MetricDocSection.BLOCK_CACHE),
  BLOCKCACHE_DATA_REQUESTCOUNT("accumulo.blockcache.data.requestcount", MetricType.FUNCTION_COUNTER,
      "Data block cache request count.", MetricDocSection.BLOCK_CACHE),
  BLOCKCACHE_DATA_EVICTIONCOUNT("accumulo.blockcache.data.evictioncount",
      MetricType.FUNCTION_COUNTER, "Data block cache eviction count.",
      MetricDocSection.BLOCK_CACHE),
  BLOCKCACHE_SUMMARY_HITCOUNT("accumulo.blockcache.summary.hitcount", MetricType.FUNCTION_COUNTER,
      "Summary block cache hit count.", MetricDocSection.BLOCK_CACHE),
  BLOCKCACHE_SUMMARY_REQUESTCOUNT("accumulo.blockcache.summary.requestcount",
      MetricType.FUNCTION_COUNTER, "Summary block cache request count.",
      MetricDocSection.BLOCK_CACHE),
  BLOCKCACHE_SUMMARY_EVICTIONCOUNT("accumulo.blockcache.summary.evictioncount",
      MetricType.FUNCTION_COUNTER, "Summary block cache eviction count.",
      MetricDocSection.BLOCK_CACHE),

  // Manager Metrics
  MANAGER_BALANCER_MIGRATIONS_NEEDED("accumulo.balancer.migrations.needed", MetricType.GAUGE,
      "The number of migrations that need to complete before the system is balanced.",
      MetricDocSection.MANAGER),
  MANAGER_ROOT_TGW_ERRORS("accumulo.tabletmgmt.root.errors", MetricType.GAUGE,
      "Error count encountered by the TabletGroupWatcher for the ROOT data level.",
      MetricDocSection.MANAGER),
  MANAGER_META_TGW_ERRORS("accumulo.tabletmgmt.meta.errors", MetricType.GAUGE,
      "Error count encountered by the TabletGroupWatcher for the META data level.",
      MetricDocSection.MANAGER),
  MANAGER_USER_TGW_ERRORS("accumulo.tabletmgmt.user.errors", MetricType.GAUGE,
      "Error count encountered by the TabletGroupWatcher for the USER data level.",
      MetricDocSection.MANAGER);

  private final String name;
  private final MetricType type;
  private final String description;
  private final MetricDocSection section;

  private static final Map<String,Metric> NAME_TO_ENUM_MAP = new HashMap<>();

  static {
    for (Metric metric : values()) {
      NAME_TO_ENUM_MAP.put(metric.name, metric);
    }
  }

  Metric(String name, MetricType type, String description, MetricDocSection section) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.section = section;
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

  public MetricDocSection getDocSection() {
    return section;
  }

  public enum MetricType {
    GAUGE, COUNTER, TIMER, LONG_TASK_TIMER, DISTRIBUTION_SUMMARY, FUNCTION_COUNTER, CACHE
  }

  public enum MetricDocSection {
    GENERAL_SERVER("General Server Metrics", "Metrics that are generated across all server types."),
    COMPACTION("Compaction Metrics",
        "Metrics specific to compactions, both minor and major. Metrics for major compactions"
            + " will likely have a 'queue.id' tag. The CompactionCoordinator component in the Manager creates a queue for each CompactionService"
            + " in the configuration. The 'queue.id' tag may map directly to the name of a Compactor resource group."),
    COMPACTOR("Compactor Metrics", "Metrics that are generated by the Compactor processes."),
    FATE("Fate Metrics",
        "Metrics that are generated by the Fate component in the Manager process."),
    GARBAGE_COLLECTION("Garbage Collection Metrics",
        "Metrics that are generated by the Garbage Collector process."),
    TABLET_SERVER("Tablet Server Metrics",
        "Metrics that are generated by the TabletServer processes."),
    SCAN("Scan Metrics",
        "Metrics specific to scans, which can be executed in the ScanServer or the TabletServer."),
    SCAN_SERVER("Scan Server Metrics", "Metrics that are generated by the ScanServer processes."),
    BLOCK_CACHE("Block Cache Metrics",
        "Metrics specific to RFile block cache usage in the ScanServer and TabletServer processes."),
    MANAGER("Manager Metrics", "Metrics that are generated by the Manager process.");

    private final String sectionTitle;
    private final String description;

    MetricDocSection(String sectionTitle, String description) {
      this.sectionTitle = sectionTitle;
      this.description = description;
    }

    public String getSectionTitle() {
      return sectionTitle;
    }

    public String getDescription() {
      return description;
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

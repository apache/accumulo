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

import static org.apache.accumulo.core.metrics.Metric.MonitorCssClass.BYTES;
import static org.apache.accumulo.core.metrics.Metric.MonitorCssClass.DATE_END;
import static org.apache.accumulo.core.metrics.Metric.MonitorCssClass.DATE_START;
import static org.apache.accumulo.core.metrics.Metric.MonitorCssClass.DURATION;
import static org.apache.accumulo.core.metrics.Metric.MonitorCssClass.IDLE_STATE;
import static org.apache.accumulo.core.metrics.Metric.MonitorCssClass.MEMORY_STATE;
import static org.apache.accumulo.core.metrics.Metric.MonitorCssClass.NUMBER;
import static org.apache.accumulo.core.metrics.Metric.MonitorCssClass.PERCENT;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.fate.FateExecutorMetrics;

public enum Metric {

  // General Server Metrics
  SERVER_IDLE("accumulo.server.idle", MetricType.GAUGE,
      "Indicates if the server is idle or not. The value will be 1 when idle and 0 when not idle.",
      MetricDocSection.GENERAL_SERVER, "Server Idle", null, IDLE_STATE),
  LOW_MEMORY("accumulo.detected.low.memory", MetricType.GAUGE,
      "Reports 1 when process memory usage is above the threshold, reports 0 when memory is okay.",
      MetricDocSection.GENERAL_SERVER, "Low Memory", null, MEMORY_STATE),
  THRIFT_IDLE("accumulo.thrift.idle", MetricType.DISTRIBUTION_SUMMARY,
      "Time waiting to execute an RPC request.", MetricDocSection.GENERAL_SERVER,
      "Thrift Idle Time", null, NUMBER),
  THRIFT_EXECUTE("accumulo.thrift.execute", MetricType.DISTRIBUTION_SUMMARY,
      "Time to execute an RPC request.", MetricDocSection.GENERAL_SERVER, "Thrift Execution Time",
      null, NUMBER),

  // Compactor Metrics
  COMPACTION_ROOT_SVC_ERRORS("accumulo.compaction.svc.root.misconfigured", MetricType.GAUGE,
      "A value of 1 indicates a misconfiguration in the compaction service, while a value of 0 indicates that the configuration is valid.",
      MetricDocSection.COMPACTION, "Root Compaction Svc Errors", null, NUMBER),
  COMPACTION_META_SVC_ERRORS("accumulo.compaction.svc.meta.misconfigured", MetricType.GAUGE,
      "A value of 1 indicates a misconfiguration in the compaction service, while a value of 0 indicates that the configuration is valid.",
      MetricDocSection.COMPACTION, "Meta Compaction Svc Errors", null, NUMBER),
  COMPACTION_USER_SVC_ERRORS("accumulo.compaction.svc.user.misconfigured", MetricType.GAUGE,
      "A value of 1 indicates a misconfiguration in the compaction service, while a value of 0 indicates that the configuration is valid.",
      MetricDocSection.COMPACTION, "User Compaction Svc Errors", null, NUMBER),
  COMPACTOR_MAJC_CANCELLED("accumulo.compaction.majc.cancelled", MetricType.FUNCTION_COUNTER,
      "Number compactions that have been cancelled on this compactor", MetricDocSection.COMPACTION,
      "Majc Cancelled", null, NUMBER),
  COMPACTOR_MAJC_COMPLETED("accumulo.compaction.majc.completed", MetricType.FUNCTION_COUNTER,
      "Number compactions that have succeeded on this compactor", MetricDocSection.COMPACTION,
      "Majc Completed", null, NUMBER),
  COMPACTOR_MAJC_FAILED("accumulo.compaction.majc.failed", MetricType.FUNCTION_COUNTER,
      "Number compactions that have failed on this compactor", MetricDocSection.COMPACTION,
      "Majc Failed", null, NUMBER),
  COMPACTOR_MAJC_FAILURES_CONSECUTIVE("accumulo.compaction.majc.failures.consecutive",
      MetricType.GAUGE,
      "Number of consecutive compaction failures. Resets to zero on a successful compaction",
      MetricDocSection.COMPACTION, "Majc Consecutive Failures", null, NUMBER),
  COMPACTOR_MAJC_FAILURES_TERMINATION("accumulo.compaction.process.terminated",
      MetricType.FUNCTION_COUNTER,
      "Will report 1 if the Compactor terminates due to consecutive failures, else 0. Emitting this metric is a best effort before the process terminates",
      MetricDocSection.COMPACTION, "Majc Consecutive Failure Termination", null, NUMBER),
  COMPACTOR_MAJC_IN_PROGRESS("accumulo.compaction.majc.in_progress", MetricType.GAUGE,
      "Indicator of whether a compaction is in-progress (value: 1) or not (value: 0). An"
          + " in-progress compaction could also be stuck.",
      MetricDocSection.COMPACTION, "Majc In Progress", null, NUMBER),
  COMPACTOR_MAJC_STUCK("accumulo.compaction.majc.stuck", MetricType.LONG_TASK_TIMER,
      "Number and duration of stuck major compactions.", MetricDocSection.COMPACTION, "Majc Stuck",
      null, NUMBER),
  COMPACTOR_MINC_STUCK("accumulo.compaction.minc.stuck", MetricType.LONG_TASK_TIMER,
      "Number and duration of stuck minor compactions.", MetricDocSection.COMPACTION, "Minc Stuck",
      null, NUMBER),
  COMPACTOR_ENTRIES_READ("accumulo.compaction.entries.read", MetricType.FUNCTION_COUNTER,
      "Number of entries read by all compactions that have run on this compactor (majc) or tserver (minc).",
      MetricDocSection.COMPACTION, "Compaction Entries Read", null, NUMBER),
  COMPACTOR_ENTRIES_WRITTEN("accumulo.compaction.entries.written", MetricType.FUNCTION_COUNTER,
      "Number of entries written by all compactions that have run on this compactor (majc) or tserver (minc).",
      MetricDocSection.COMPACTION, "Compaction Entries Written", null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUES("accumulo.compaction.queue.count", MetricType.GAUGE,
      "Number of priority queues for compaction jobs.", MetricDocSection.COMPACTION,
      "Compaction Queue Count", null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_DEQUEUED("accumulo.compaction.queue.jobs.dequeued",
      MetricType.GAUGE, "Count of dequeued jobs.", MetricDocSection.COMPACTION,
      "Compaction Jobs Dequeued", null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED("accumulo.compaction.queue.jobs.queued",
      MetricType.GAUGE, "Count of queued jobs.", MetricDocSection.COMPACTION,
      "Compaction Jobs Queued", null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_SIZE("accumulo.compaction.queue.jobs.size", MetricType.GAUGE,
      "Size of queued jobs in bytes.", MetricDocSection.COMPACTION, "Compaction Queued Jobs Size",
      null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_REJECTED("accumulo.compaction.queue.jobs.rejected",
      MetricType.GAUGE, "Count of rejected jobs.", MetricDocSection.COMPACTION,
      "Compaction Queue Jobs Rejected", null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_PRIORITY("accumulo.compaction.queue.jobs.priority",
      MetricType.GAUGE, "Lowest priority queued job.", MetricDocSection.COMPACTION,
      "Compaction Queue Lowest Priority", null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_MIN_AGE("accumulo.compaction.queue.jobs.min.age",
      MetricType.GAUGE, "Minimum age of currently queued jobs in seconds.",
      MetricDocSection.COMPACTION, "Compaction Queue Min Job Age", null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_MAX_AGE("accumulo.compaction.queue.jobs.max.age",
      MetricType.GAUGE, "Maximum age of currently queued jobs in seconds.",
      MetricDocSection.COMPACTION, "Compaction Queue Max Job Age", null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_AVG_AGE("accumulo.compaction.queue.jobs.avg.age",
      MetricType.GAUGE, "Average age of currently queued jobs in seconds.",
      MetricDocSection.COMPACTION, "Compaction Queue Avg Job Age", null, NUMBER),
  COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_POLL_TIMER("accumulo.compaction.queue.jobs.exit.time",
      MetricType.TIMER, "Tracks time a job spent in the queue before exiting the queue.",
      MetricDocSection.COMPACTION, "Compaction Queue Job Time Queued", null, NUMBER),

  // Fate Metrics
  FATE_TYPE_IN_PROGRESS("accumulo.fate.ops.in.progress.by.type", MetricType.GAUGE,
      "Number of FATE operations in progress. The op type is designated by the `op.type` tag.",
      MetricDocSection.FATE, "Fate Ops In Progess Count", null, NUMBER),
  FATE_OPS("accumulo.fate.ops", MetricType.GAUGE,
      "Number of all the current FATE ops in any state.", MetricDocSection.FATE,
      "Current Fate Ops Count", null, NUMBER),
  FATE_OPS_ACTIVITY("accumulo.fate.ops.activity", MetricType.GAUGE,
      "Count of the total number of times fate operations are added, updated, and removed.",
      MetricDocSection.FATE, "Total Fate Ops Count", null, NUMBER),
  FATE_ERRORS("accumulo.fate.errors", MetricType.GAUGE,
      "Count of errors that occurred when attempting to gather fate metrics.",
      MetricDocSection.FATE, "Fate Errors Count", null, NUMBER),
  FATE_TX("accumulo.fate.tx", MetricType.GAUGE,
      "Count of FATE operations in a certain state. The state is now in a tag "
          + "(e.g., state=new, state=in.progress, state=failed, etc.).",
      MetricDocSection.FATE, "Fate Ops State Count", null, NUMBER),
  FATE_OPS_THREADS_INACTIVE("accumulo.fate.ops.threads.inactive", MetricType.GAUGE,
      "Keeps track of the number of idle threads (not working on a fate operation) in the thread "
          + "pool. The pool name can be found in the " + FateExecutorMetrics.POOL_NAME_TAG_KEY
          + " tag. The fate instance type can be found in the "
          + FateExecutorMetrics.INSTANCE_TYPE_TAG_KEY + " tag.",
      MetricDocSection.FATE, "Fate Threads Inactive", null, NUMBER),
  FATE_OPS_THREADS_TOTAL("accumulo.fate.ops.threads.total", MetricType.GAUGE,
      "Keeps track of the total number of threads in the thread pool. The pool name can be found in the "
          + FateExecutorMetrics.POOL_NAME_TAG_KEY
          + " tag. The fate instance type can be found in the "
          + FateExecutorMetrics.INSTANCE_TYPE_TAG_KEY + " tag.",
      MetricDocSection.FATE, "Fate Threads Total", null, NUMBER),

  // Garbage Collection Metrics
  GC_STARTED("accumulo.gc.started", MetricType.GAUGE, "Timestamp GC file collection cycle started.",
      MetricDocSection.GARBAGE_COLLECTION, "GC File Cycle Start Time", null, DATE_START),
  GC_FINISHED("accumulo.gc.finished", MetricType.GAUGE, "Timestamp GC file collect cycle finished.",
      MetricDocSection.GARBAGE_COLLECTION, "GC File Cycle End Time", null, DATE_END),
  GC_CANDIDATES("accumulo.gc.candidates", MetricType.GAUGE,
      "Number of files that are candidates for deletion.", MetricDocSection.GARBAGE_COLLECTION,
      "GC File Deletion Candidate Count", null, NUMBER),
  GC_IN_USE("accumulo.gc.in.use", MetricType.GAUGE, "Number of candidate files still in use.",
      MetricDocSection.GARBAGE_COLLECTION, "GC File Deletion Candidates In Use", null, NUMBER),
  GC_DELETED("accumulo.gc.deleted", MetricType.GAUGE, "Number of candidate files deleted.",
      MetricDocSection.GARBAGE_COLLECTION, "GC File Candidates Deleted", null, NUMBER),
  GC_ERRORS("accumulo.gc.errors", MetricType.GAUGE, "Number of candidate deletion errors.",
      MetricDocSection.GARBAGE_COLLECTION, "GC File Candidate Deletion Errors", null, NUMBER),
  GC_WAL_STARTED("accumulo.gc.wal.started", MetricType.GAUGE,
      "Timestamp GC WAL collection cycle started.", MetricDocSection.GARBAGE_COLLECTION,
      "GC WAL Cycle Start Time", null, DATE_START),
  GC_WAL_FINISHED("accumulo.gc.wal.finished", MetricType.GAUGE,
      "Timestamp GC WAL collect cycle finished.", MetricDocSection.GARBAGE_COLLECTION,
      "GC WAL Cycle End Time", null, DATE_END),
  GC_WAL_CANDIDATES("accumulo.gc.wal.candidates", MetricType.GAUGE,
      "Number of files that are candidates for deletion.", MetricDocSection.GARBAGE_COLLECTION,
      "GC WAL Deletion Candidate Count", null, NUMBER),
  GC_WAL_IN_USE("accumulo.gc.wal.in.use", MetricType.GAUGE,
      "Number of wal file candidates that are still in use.", MetricDocSection.GARBAGE_COLLECTION,
      "GC WAL Deletion Candidaets In Use", null, NUMBER),
  GC_WAL_DELETED("accumulo.gc.wal.deleted", MetricType.GAUGE,
      "Number of candidate wal files deleted.", MetricDocSection.GARBAGE_COLLECTION,
      "GC WAL Candidates Deleted", null, NUMBER),
  GC_WAL_ERRORS("accumulo.gc.wal.errors", MetricType.GAUGE,
      "Number candidate wal file deletion errors.", MetricDocSection.GARBAGE_COLLECTION,
      "GC WAL Candidate Deletion Errors", null, NUMBER),
  GC_POST_OP_DURATION("accumulo.gc.post.op.duration", MetricType.GAUGE,
      "GC metadata table post operation duration in milliseconds.",
      MetricDocSection.GARBAGE_COLLECTION, "GC Post Ops Duration", null, DURATION),
  GC_RUN_CYCLE("accumulo.gc.run.cycle", MetricType.GAUGE,
      "Count of gc cycle runs. Value is reset on process start.",
      MetricDocSection.GARBAGE_COLLECTION, "GC Cycle Count", null, NUMBER),

  // Tablet Server Metrics
  TSERVER_ENTRIES("accumulo.tserver.entries", MetricType.GAUGE,
      "Number of entries assigned to a TabletServer.", MetricDocSection.TABLET_SERVER,
      "Entries Assigned", null, NUMBER),
  TSERVER_MEM_ENTRIES("accumulo.tserver.entries.mem", MetricType.GAUGE,
      "Number of entries in memory.", MetricDocSection.TABLET_SERVER, "Entries In Memory", null,
      NUMBER),
  TSERVER_MINC_QUEUED("accumulo.minc.queued", MetricType.GAUGE,
      "Number of queued minor compactions.", MetricDocSection.COMPACTION, "Minc Queued", null,
      NUMBER),
  TSERVER_MINC_RUNNING("accumulo.minc.running", MetricType.GAUGE,
      "Number of active minor compactions.", MetricDocSection.COMPACTION, "Minc Running", null,
      NUMBER),
  TSERVER_MINC_TOTAL("accumulo.minc.total", MetricType.GAUGE,
      "Total number of minor compactions performed.", MetricDocSection.COMPACTION, "Minc Completed",
      null, NUMBER),
  TSERVER_TABLETS_ONLINE("accumulo.tablets.online", MetricType.GAUGE, "Number of online tablets.",
      MetricDocSection.TABLET_SERVER, "Tablets", null, NUMBER),
  TSERVER_TABLETS_LONG_ASSIGNMENTS("accumulo.tablets.assignments.warning", MetricType.GAUGE,
      "Number of tablet assignments that are taking longer than the configured warning duration.",
      MetricDocSection.TABLET_SERVER, "Slow Assignments", null, NUMBER),
  TSERVER_TABLETS_OPENING("accumulo.tablets.opening", MetricType.GAUGE,
      "Number of opening tablets.", MetricDocSection.TABLET_SERVER, "Tablets Opening", null,
      NUMBER),
  TSERVER_TABLETS_UNOPENED("accumulo.tablets.unopened", MetricType.GAUGE,
      "Number of unopened tablets.", MetricDocSection.TABLET_SERVER, "Tablets Unopened", null,
      NUMBER),
  TSERVER_TABLETS_FILES("accumulo.tablets.files", MetricType.GAUGE, "Number of files per tablet.",
      MetricDocSection.TABLET_SERVER, "Tablet File Avg", null, NUMBER),
  TSERVER_INGEST_ENTRIES("accumulo.ingest.entries", MetricType.GAUGE,
      "Ingest entry (a key/value) count. The rate can be derived from this metric.",
      MetricDocSection.TABLET_SERVER, "Entries Ingested", null, NUMBER),
  TSERVER_INGEST_BYTES("accumulo.ingest.bytes", MetricType.GAUGE,
      "Ingest byte count. The rate can be derived from this metric.",
      MetricDocSection.TABLET_SERVER, "Bytes Ingested", null, BYTES),
  // TODO does this duration expect millis in javascript?
  TSERVER_HOLD("accumulo.ingest.hold", MetricType.GAUGE,
      "Duration for which commits have been held in milliseconds.", MetricDocSection.TABLET_SERVER,
      "Hold Time", null, DURATION),
  TSERVER_TABLETS_ONLINE_ONDEMAND("accumulo.tablets.ondemand.online", MetricType.GAUGE,
      "Number of online on-demand tablets", MetricDocSection.TABLET_SERVER,
      "Online On-Demand Tablets", null, NUMBER),
  TSERVER_TABLETS_ONDEMAND_UNLOADED_FOR_MEM("accumulo.tablets.ondemand.unloaded.lowmem",
      MetricType.GAUGE, "Number of online on-demand tablets unloaded due to low memory",
      MetricDocSection.TABLET_SERVER, "On-Demand Tablets Unloaded For Memory", null, NUMBER),

  // Scan Server Metrics
  SCAN_RESERVATION_TOTAL_TIMER("accumulo.scan.reservation.total.timer", MetricType.TIMER,
      "Time to reserve a tablet's files for scan.", MetricDocSection.SCAN_SERVER,
      "Scan Reservation Total Time", null, NUMBER),
  SCAN_RESERVATION_WRITEOUT_TIMER("accumulo.scan.reservation.writeout.timer", MetricType.TIMER,
      "Time to write out a tablets file reservations for scan.", MetricDocSection.SCAN_SERVER,
      "Scan Reservation Write Time", null, NUMBER),
  SCAN_RESERVATION_CONFLICT_COUNTER("accumulo.scan.reservation.conflict.count", MetricType.COUNTER,
      "Count of instances where file reservation attempts for scans encountered conflicts.",
      MetricDocSection.SCAN_SERVER, "Scan Reservation Conflicts", null, NUMBER),
  SCAN_TABLET_METADATA_CACHE("accumulo.scan.tablet.metadata.cache", MetricType.CACHE,
      "Scan server tablet cache metrics.", MetricDocSection.SCAN_SERVER, "Scan Server Tablet Cache",
      null, NUMBER),

  // Scan Metrics
  SCAN_BUSY_TIMEOUT_COUNT("accumulo.scan.busy.timeout.count", MetricType.FUNCTION_COUNTER,
      "Count of the scans where a busy timeout happened.", MetricDocSection.SCAN, "Scan Busy Count",
      null, NUMBER),
  SCAN_TIMES("accumulo.scan.times", MetricType.TIMER, "Scan session lifetime (creation to close).",
      MetricDocSection.SCAN, "Scan Session Total Time", null, NUMBER),
  SCAN_OPEN_FILES("accumulo.scan.files.open", MetricType.GAUGE, "Number of files open for scans.",
      MetricDocSection.SCAN, "Scan Files Open", null, NUMBER),
  SCAN_RESULTS("accumulo.scan.result", MetricType.DISTRIBUTION_SUMMARY, "Results per scan.",
      MetricDocSection.SCAN, "Scan Result Count", null, NUMBER),
  SCAN_YIELDS("accumulo.scan.yields", MetricType.DISTRIBUTION_SUMMARY,
      "Counts scans that have yielded.", MetricDocSection.SCAN, "Scan Yield Count", null, NUMBER),
  SCAN_START("accumulo.scan.start", MetricType.FUNCTION_COUNTER,
      "Number of calls to start a scan or multiscan.", MetricDocSection.SCAN, "Scan Start Count",
      null, NUMBER),
  SCAN_CONTINUE("accumulo.scan.continue", MetricType.FUNCTION_COUNTER,
      "Number of calls to continue a scan or multiscan.", MetricDocSection.SCAN,
      "Scan Continue Count", null, NUMBER),
  SCAN_CLOSE("accumulo.scan.close", MetricType.FUNCTION_COUNTER,
      "Number of calls to close a scan or multiscan.", MetricDocSection.SCAN, "Scan Close Count",
      null, NUMBER),
  SCAN_QUERIES("accumulo.scan.queries", MetricType.FUNCTION_COUNTER,
      "Number of queries made during scans.", MetricDocSection.SCAN, "Tablet Lookup Count", null,
      NUMBER),
  SCAN_SCANNED_ENTRIES("accumulo.scan.query.scanned.entries", MetricType.FUNCTION_COUNTER,
      "Count of scanned entries. The rate can be derived from this metric.", MetricDocSection.SCAN,
      "Scanned Entries", null, NUMBER),
  SCAN_QUERY_SCAN_RESULTS("accumulo.scan.query.results", MetricType.FUNCTION_COUNTER,
      "Query count. The rate can be derived from this metric.", MetricDocSection.SCAN,
      "Returned Entries", null, NUMBER),
  SCAN_QUERY_SCAN_RESULTS_BYTES("accumulo.scan.query.results.bytes", MetricType.FUNCTION_COUNTER,
      "Query byte count. The rate can be derived from this metric.", MetricDocSection.SCAN,
      "Returned Bytes", null, BYTES),
  SCAN_PAUSED_FOR_MEM("accumulo.scan.paused.for.memory", MetricType.FUNCTION_COUNTER,
      "Count of scans paused due to server being low on memory.", MetricDocSection.SCAN,
      "Scans Paused For Low Memory", null, NUMBER),
  SCAN_RETURN_FOR_MEM("accumulo.scan.return.early.for.memory", MetricType.FUNCTION_COUNTER,
      "Count of scans that returned results early due to server being low on memory.",
      MetricDocSection.SCAN, "Scans Returned Early For Low Memory", null, NUMBER),
  SCAN_ZOMBIE_THREADS("accumulo.scan.zombie.threads", MetricType.GAUGE,
      "Number of scan threads that have no associated client session.", MetricDocSection.SCAN,
      "Scan Zombie Thread Count", null, NUMBER),
  SCAN_ERRORS("accumulo.scan.errors", MetricType.FUNCTION_COUNTER,
      "Number of scan task that had an exception.", MetricDocSection.SCAN, "Failed scans", null,
      NUMBER),

  // Major Compaction Metrics
  MAJC_PAUSED("accumulo.compaction.majc.paused", MetricType.COUNTER,
      "Number of paused major compactions.", MetricDocSection.COMPACTOR, "Majc Paused", null,
      NUMBER),

  // Minor Compaction Metrics
  MINC_QUEUED("accumulo.compaction.minc.queued", MetricType.TIMER,
      "Queued minor compactions time queued.", MetricDocSection.COMPACTION, "Minc Queued", null,
      NUMBER),
  MINC_RUNNING("accumulo.compaction.minc.running", MetricType.TIMER,
      "Minor compactions time active.", MetricDocSection.COMPACTION, "Minc Running", null, NUMBER),
  MINC_PAUSED("accumulo.compaction.minc.paused", MetricType.COUNTER,
      "Number of paused minor compactions.", MetricDocSection.COMPACTION, "Minc Paused", null,
      NUMBER),

  // Updates (Ingest) Metrics
  UPDATE_ERRORS("accumulo.updates.error", MetricType.GAUGE,
      "Count of errors during tablet updates. Type/reason for error is stored in the `type` tag (e.g., type=permission, type=unknown.tablet, type=constraint.violation).",
      MetricDocSection.TABLET_SERVER, "Ingest Errors", null, NUMBER),
  UPDATE_LOCK("accumulo.updates.lock", MetricType.TIMER,
      "Average time taken for conditional mutation to get a row lock.",
      MetricDocSection.TABLET_SERVER, "Conditional Mutation Row Lock Wait Time", null, NUMBER),
  UPDATE_CHECK("accumulo.updates.check", MetricType.TIMER,
      "Average time taken for conditional mutation to check conditions.",
      MetricDocSection.TABLET_SERVER, "Conditional Mutation Condition Check Time", null, NUMBER),
  UPDATE_COMMIT("accumulo.updates.commit", MetricType.TIMER,
      "Average time taken to commit a mutation.", MetricDocSection.TABLET_SERVER,
      "Mutation Commit Avg Total Time", null, NUMBER),
  UPDATE_COMMIT_PREP("accumulo.updates.commit.prep", MetricType.TIMER,
      "Average time taken to prepare to commit a single mutation.", MetricDocSection.TABLET_SERVER,
      "Mutation Commit Avg Prep Time", null, NUMBER),
  UPDATE_WALOG_WRITE("accumulo.updates.walog.write", MetricType.TIMER,
      "Time taken to write a batch of mutations to WAL.", MetricDocSection.TABLET_SERVER,
      "WAL Write Time", null, NUMBER),
  UPDATE_MUTATION_ARRAY_SIZE("accumulo.updates.mutation.arrays.size",
      MetricType.DISTRIBUTION_SUMMARY, "Batch size of mutations from client.",
      MetricDocSection.TABLET_SERVER, "Mutation Batch Size", null, NUMBER),

  // Block Cache Metrics
  BLOCKCACHE_INDEX_HITCOUNT("accumulo.blockcache.index.hitcount", MetricType.FUNCTION_COUNTER,
      "Index block cache hit count.", MetricDocSection.BLOCK_CACHE, "Index Cache Hit", null,
      NUMBER),
  BLOCKCACHE_INDEX_REQUESTCOUNT("accumulo.blockcache.index.requestcount",
      MetricType.FUNCTION_COUNTER, "Index block cache request count.", MetricDocSection.BLOCK_CACHE,
      "Index Cache Request", null, NUMBER),
  BLOCKCACHE_INDEX_EVICTIONCOUNT("accumulo.blockcache.index.evictioncount",
      MetricType.FUNCTION_COUNTER, "Index block cache eviction count.",
      MetricDocSection.BLOCK_CACHE, "Index Cache Eviction", null, NUMBER),
  BLOCKCACHE_DATA_HITCOUNT("accumulo.blockcache.data.hitcount", MetricType.FUNCTION_COUNTER,
      "Data block cache hit count.", MetricDocSection.BLOCK_CACHE, "Data Cache Hit", null, NUMBER),
  BLOCKCACHE_DATA_REQUESTCOUNT("accumulo.blockcache.data.requestcount", MetricType.FUNCTION_COUNTER,
      "Data block cache request count.", MetricDocSection.BLOCK_CACHE, "Data Cache Request", null,
      NUMBER),
  BLOCKCACHE_DATA_EVICTIONCOUNT("accumulo.blockcache.data.evictioncount",
      MetricType.FUNCTION_COUNTER, "Data block cache eviction count.", MetricDocSection.BLOCK_CACHE,
      "Data Cache Eviction", null, NUMBER),
  BLOCKCACHE_SUMMARY_HITCOUNT("accumulo.blockcache.summary.hitcount", MetricType.FUNCTION_COUNTER,
      "Summary block cache hit count.", MetricDocSection.BLOCK_CACHE,
      "Summary Block Cache Hit Count", null, NUMBER),
  BLOCKCACHE_SUMMARY_REQUESTCOUNT("accumulo.blockcache.summary.requestcount",
      MetricType.FUNCTION_COUNTER, "Summary block cache request count.",
      MetricDocSection.BLOCK_CACHE, "Summary Block Cache Request Count", null, NUMBER),
  BLOCKCACHE_SUMMARY_EVICTIONCOUNT("accumulo.blockcache.summary.evictioncount",
      MetricType.FUNCTION_COUNTER, "Summary block cache eviction count.",
      MetricDocSection.BLOCK_CACHE, "Summary Block Cache Eviction Count", null, NUMBER),

  // Manager Metrics
  MANAGER_BALANCER_MIGRATIONS_NEEDED("accumulo.balancer.migrations.needed", MetricType.GAUGE,
      "The number of migrations that need to complete before the system is balanced.",
      MetricDocSection.MANAGER, "Balancer Migrations Needed", null, NUMBER),
  MANAGER_ROOT_TGW_ERRORS("accumulo.tabletmgmt.root.errors", MetricType.GAUGE,
      "Error count encountered by the TabletGroupWatcher for the ROOT data level.",
      MetricDocSection.MANAGER, "Root Tablet Watcher Errors", null, NUMBER),
  MANAGER_META_TGW_ERRORS("accumulo.tabletmgmt.meta.errors", MetricType.GAUGE,
      "Error count encountered by the TabletGroupWatcher for the META data level.",
      MetricDocSection.MANAGER, "Meta Tablet Watcher Errors", null, NUMBER),
  MANAGER_USER_TGW_ERRORS("accumulo.tabletmgmt.user.errors", MetricType.GAUGE,
      "Error count encountered by the TabletGroupWatcher for the USER data level.",
      MetricDocSection.MANAGER, "User Tablet Watcher Errors", null, NUMBER),
  MANAGER_GOAL_STATE("accumulo.manager.goal.state", MetricType.GAUGE,
      "Manager goal state: -1=unknown, 0=CLEAN_STOP, 1=SAFE_MODE, 2=NORMAL.",
      MetricDocSection.MANAGER, "Manager Goal State", null, NUMBER),

  // Recovery Metrics
  RECOVERIES_IN_PROGRESS("accumulo.recoveries.in.progress", MetricType.GAUGE,
      "The number of recoveries in progress.", MetricDocSection.GENERAL_SERVER,
      "Tablet Recoveries In Progress", null, NUMBER),
  RECOVERIES_LONGEST_RUNTIME("accumulo.recoveries.runtime.longest", MetricType.GAUGE,
      "The time (in milliseconds) of the longest running recovery.",
      MetricDocSection.GENERAL_SERVER, "Tablet Recovery Longest Time", null, DURATION),
  RECOVERIES_AVG_PROGRESS("accumulo.recoveries.avg.progress", MetricType.GAUGE,
      "The average percentage (0.0 - 99.9) of the in progress recoveries.",
      MetricDocSection.GENERAL_SERVER, "Tablet Recovery Avg Percent Complete", null, PERCENT),

  // Executor metrics
  EXECUTOR_COMPLETED("executor.completed", MetricType.FUNCTION_COUNTER,
      "Task completed by a thread pool. Each thread pool emits this metric w/ a different tag.",
      MetricDocSection.GENERAL_SERVER, "Completed task", null, NUMBER),
  EXECUTOR_QUEUED("executor.queued", MetricType.GAUGE,
      "Task queued for a thread pool. Each thread pool emits this metric w/ a different tag.",
      MetricDocSection.GENERAL_SERVER, "Queued task", null, NUMBER);

  public enum MonitorCssClass {
    BYTES("big-size"),
    BYTES_RATE("rate-size"),
    DATE_END("end-date"),
    DATE_START("start-date"),
    DURATION("duration"),
    IDLE_STATE("idle-state"),
    MEMORY_STATE("memory-state"),
    NUMBER("big-num"),
    RATE("rate-num"),
    PERCENT("percent");

    private final String cssClass;

    private MonitorCssClass(String cssClass) {
      this.cssClass = cssClass;
    }

    public String getCssClass() {
      return this.cssClass;
    }
  }

  private final String name;
  private final MetricType type;
  private final String description;
  private final MetricDocSection section;
  /* Name for HTML table header element in Monitor UI */
  private final String monitorColumnHeader;
  /* Description for HTML table header element in Monitor UI, displayed on hover */
  private final String monitorColumnDescription;
  /* CSS class for HTML table header element in Monitor UI */
  private final MonitorCssClass[] monitorColumnClasses;

  private static final Map<String,Metric> NAME_TO_ENUM_MAP = new HashMap<>();

  static {
    for (Metric metric : values()) {
      NAME_TO_ENUM_MAP.put(metric.name, metric);
    }
  }

  Metric(String name, MetricType type, String description, MetricDocSection section,
      String colHeader, String colDescription, MonitorCssClass... colClasses) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.section = section;
    this.monitorColumnHeader = colHeader;
    this.monitorColumnDescription = colDescription;
    this.monitorColumnClasses = colClasses;
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

  public String getColumnHeader() {
    return monitorColumnHeader;
  }

  public MonitorCssClass[] getColumnClasses() {
    return monitorColumnClasses;
  }

  public String getColumnDescription() {
    if (monitorColumnDescription == null) {
      return description;
    }
    return monitorColumnDescription;
  }

  public enum MetricType {
    GAUGE, COUNTER, TIMER, LONG_TASK_TIMER, DISTRIBUTION_SUMMARY, FUNCTION_COUNTER, CACHE
  }

  public enum MetricDocSection {
    GENERAL_SERVER("General Server Metrics", "Metrics that are generated across all server types."),
    COMPACTION("Compaction Metrics",
        "Metrics specific to compactions, both minor and major. Metrics for major compactions"
            + " will likely have a '" + MetricsInfo.QUEUE_TAG_KEY
            + "' tag. The CompactionCoordinator component in the Manager creates a queue for each CompactionService"
            + " in the configuration. The '" + MetricsInfo.QUEUE_TAG_KEY
            + "' tag may map directly to the name of a Compactor resource group."),
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

  /**
   * Returns a set of Metric names that should not be sent to the Monitor from
   * {@code AbstractServer.getMetrics}
   *
   * @param serverType server type
   * @return set of metrics not to send to Monitor
   */
  public static Set<String> getMonitorExclusions(ServerId.Type serverType) {
    switch (serverType) {
      case COMPACTOR:
        return Set.of(MINC_PAUSED.getName(), RECOVERIES_AVG_PROGRESS.getName(),
            RECOVERIES_IN_PROGRESS.getName(), RECOVERIES_LONGEST_RUNTIME.getName());
      case GARBAGE_COLLECTOR:
        return Set.of();
      case MANAGER:
        return Set.of();
      case MONITOR:
        return Set.of();
      case SCAN_SERVER:
        return Set.of(RECOVERIES_AVG_PROGRESS.getName(), RECOVERIES_IN_PROGRESS.getName(),
            RECOVERIES_LONGEST_RUNTIME.getName());
      case TABLET_SERVER:
        return Set.of(MAJC_PAUSED.getName());
      default:
        throw new IllegalArgumentException("Unhandled server type: " + serverType);
    }
  }

}

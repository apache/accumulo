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

import static org.apache.accumulo.core.metrics.Metric.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.metrics.Metric;

public class MetricColumnMappings {

  public static record ColumnInformation(String name, String description, String uiInfo)
      implements Comparable<ColumnInformation> {
    @Override
    public int compareTo(ColumnInformation other) {
      int result = this.name.compareTo(other.name);
      if (result == 0) {
        result = this.description.compareTo(other.description);
        if (result == 0) {
          result = this.uiInfo.compareTo(other.uiInfo);
        }
      }
      return result;
    }
  }

  private static final Map<Metric,ColumnInformation> mappings = new HashMap<>();

  static {
    mappings.put(SERVER_IDLE, new ColumnInformation("Server Idle",
        "Indicates if the server is idle or not. The value will be 1 when idle and 0 when not idle.",
        ""));
    mappings.put(LOW_MEMORY, new ColumnInformation("Low Memory",
        "Reports 1 when process memory usage is above the threshold, reports 0 when memory is okay.",
        ""));
    mappings.put(THRIFT_IDLE,
        new ColumnInformation("Thrift Idle Time", "Time waiting to execute an RPC request.", ""));
    mappings.put(THRIFT_EXECUTE,
        new ColumnInformation("Thrift Execution Time", "Time to execute an RPC request.", ""));
    mappings.put(COMPACTION_ROOT_SVC_ERRORS, new ColumnInformation("Root Compaction Service Errors",
        "A value of 1 indicates a misconfiguration in the compaction service, while a value of 0 indicates that the configuration is valid.",
        ""));
    mappings.put(COMPACTION_META_SVC_ERRORS, new ColumnInformation("Meta Compaction Service Errors",
        "A value of 1 indicates a misconfiguration in the compaction service, while a value of 0 indicates that the configuration is valid.",
        ""));
    mappings.put(COMPACTION_USER_SVC_ERRORS, new ColumnInformation("User Compaction Service Errors",
        "A value of 1 indicates a misconfiguration in the compaction service, while a value of 0 indicates that the configuration is valid.",
        ""));
    mappings.put(COMPACTOR_MAJC_CANCELLED, new ColumnInformation("Majc Cancelled",
        "Number compactions that have been cancelled on this compactor", ""));
    mappings.put(COMPACTOR_MAJC_COMPLETED, new ColumnInformation("Majc Completed",
        "Number compactions that have succeeded on this compactor", ""));
    mappings.put(COMPACTOR_MAJC_FAILED, new ColumnInformation("Majc Failed",
        "Number compactions that have failed on this compactor", ""));
    mappings.put(COMPACTOR_MAJC_FAILURES_CONSECUTIVE,
        new ColumnInformation("Majc Consecutive Failures",
            "Number of consecutive compaction failures. Resets to zero on a successful compaction",
            ""));
    mappings.put(COMPACTOR_MAJC_FAILURES_TERMINATION, new ColumnInformation(
        "Majc Consecutive Failure Termination",
        "Will report 1 if the Compactor terminates due to consecutive failures, else 0. Emitting this metric is a best effort before the process terminates",
        ""));
    mappings.put(COMPACTOR_MAJC_IN_PROGRESS, new ColumnInformation("Majc In Progress",
        "Indicator of whether a compaction is in-progress (value: 1) or not (value: 0). An in-progress compaction could also be stuck.",
        ""));
    mappings.put(COMPACTOR_MAJC_STUCK,
        new ColumnInformation("Majc Stuck", "Number and duration of stuck major compactions.", ""));
    mappings.put(COMPACTOR_MINC_STUCK,
        new ColumnInformation("Minc Stuck", "Number and duration of stuck minor compactions.", ""));
    mappings.put(COMPACTOR_ENTRIES_READ, new ColumnInformation("Compaction Entries Read",
        "Number of entries read by all compactions that have run on this compactor (majc) or tserver (minc).",
        ""));
    mappings.put(COMPACTOR_ENTRIES_WRITTEN, new ColumnInformation("Compaction Entries Written",
        "Number of entries written by all compactions that have run on this compactor (majc) or tserver (minc).",
        ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUES, new ColumnInformation("Compaction Queue Count",
        "Number of priority queues for compaction jobs.", ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_DEQUEUED,
        new ColumnInformation("Compaction Jobs Dequeued", "Count of dequeued jobs.", ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED,
        new ColumnInformation("Compaction Jobs Queued", "Count of queued jobs.", ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_SIZE,
        new ColumnInformation("Compaction Queue Size", "Size of queued jobs in bytes.", ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_REJECTED,
        new ColumnInformation("Compaction Jobs Rejected", "Count of rejected jobs.", ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_PRIORITY,
        new ColumnInformation("Compaction Job Lowest Priority", "Lowest priority queued job.", ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_MIN_AGE, new ColumnInformation(
        "Compaction Job Min Age", "Minimum age of currently queued jobs in seconds.", ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_MAX_AGE, new ColumnInformation(
        "Compaction Job Max Age", "Maximum age of currently queued jobs in seconds.", ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_AVG_AGE, new ColumnInformation(
        "Compaction Job Avg Age", "Average age of currently queued jobs in seconds.", ""));
    mappings.put(COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_POLL_TIMER,
        new ColumnInformation("Compaction Job Time in Queue",
            "Tracks time a job spent in the queue before exiting the queue.", ""));
    mappings.put(FATE_TYPE_IN_PROGRESS, new ColumnInformation("Fate Ops In Progress By Type",
        "Number of FATE operations in progress. The op type is designated by the `op.type` tag.",
        ""));
    mappings.put(FATE_OPS, new ColumnInformation("Total Current Fate Ops",
        "Number of all the current FATE ops in any state.", ""));
    mappings.put(FATE_OPS_ACTIVITY, new ColumnInformation("Total Fate Ops",
        "Count of the total number of times fate operations are added, updated, and removed.", ""));
    mappings.put(FATE_ERRORS, new ColumnInformation("Fate Errors",
        "Count of errors that occurred when attempting to gather fate metrics.", ""));
    mappings.put(FATE_TX, new ColumnInformation("Fate Ops By State",
        "Count of FATE operations in a certain state. The state is now in a tag (e.g., state=new, state=in.progress, state=failed, etc.).",
        ""));
    mappings.put(FATE_OPS_THREADS_INACTIVE, new ColumnInformation("Fate Threads Inactive",
        "Keeps track of the number of idle threads (not working on a fate operation) in the thread pool. The pool name can be found in the pool.name tag. The fate instance type can be found in the instanceType tag.",
        ""));
    mappings.put(FATE_OPS_THREADS_TOTAL, new ColumnInformation("Fate Threads Total",
        "Keeps track of the total number of threads in the thread pool. The pool name can be found in the pool.name tag. The fate instance type can be found in the instanceType tag.",
        ""));
    mappings.put(GC_STARTED, new ColumnInformation("GC File Cycle Start",
        "Timestamp GC file collection cycle started.", ""));
    mappings.put(GC_FINISHED, new ColumnInformation("GC File Cycle End",
        "Timestamp GC file collect cycle finished.", ""));
    mappings.put(GC_CANDIDATES, new ColumnInformation("GC File Candidates For Delete",
        "Number of files that are candidates for deletion.", ""));
    mappings.put(GC_IN_USE, new ColumnInformation("GC File Candidates In Use",
        "Number of candidate files still in use.", ""));
    mappings.put(GC_DELETED, new ColumnInformation("GC File Candidates Deleted",
        "Number of candidate files deleted.", ""));
    mappings.put(GC_ERRORS, new ColumnInformation("GC File Candidate Deletion Errors",
        "Number of candidate deletion errors.", ""));
    mappings.put(GC_WAL_STARTED, new ColumnInformation("GC WAL Cycle Start",
        "Timestamp GC WAL collection cycle started.", ""));
    mappings.put(GC_WAL_FINISHED,
        new ColumnInformation("GC WAL Cycle End", "Timestamp GC WAL collect cycle finished.", ""));
    mappings.put(GC_WAL_CANDIDATES, new ColumnInformation("GC WAL Candidates For Delete",
        "Number of files that are candidates for deletion.", ""));
    mappings.put(GC_WAL_IN_USE, new ColumnInformation("GC WAL Candidates In Use",
        "Number of wal file candidates that are still in use.", ""));
    mappings.put(GC_WAL_DELETED, new ColumnInformation("GC WAL Candidates Deleted",
        "Number of candidate wal files deleted.", ""));
    mappings.put(GC_WAL_ERRORS, new ColumnInformation("GC WAL Candidate Deletion Errors",
        "Number candidate wal file deletion errors.", ""));
    mappings.put(GC_POST_OP_DURATION, new ColumnInformation("GC Metadata PostOp Duration",
        "GC metadata table post operation duration in milliseconds.", ""));
    mappings.put(GC_RUN_CYCLE, new ColumnInformation("GC Cycles",
        "Count of gc cycle runs. Value is reset on process start.", ""));
    mappings.put(TSERVER_ENTRIES, new ColumnInformation("Entries Assigned",
        "Number of entries assigned to a TabletServer.", ""));
    mappings.put(TSERVER_MEM_ENTRIES,
        new ColumnInformation("Entries In Memory", "Number of entries in memory.", ""));
    mappings.put(TSERVER_MINC_QUEUED,
        new ColumnInformation("Queued Minc", "Number of queued minor compactions.", ""));
    mappings.put(TSERVER_MINC_RUNNING,
        new ColumnInformation("Running Minc", "Number of active minor compactions.", ""));
    mappings.put(TSERVER_MINC_TOTAL, new ColumnInformation("Minc Completed",
        "Total number of minor compactions performed.", ""));
    mappings.put(TSERVER_TABLETS_ONLINE,
        new ColumnInformation("Tablets Online", "Number of online tablets.", ""));
    mappings.put(TSERVER_TABLETS_LONG_ASSIGNMENTS, new ColumnInformation(
        "Tablet Assignments Overdue",
        "Number of tablet assignments that are taking longer than the configured warning duration.",
        ""));
    mappings.put(TSERVER_TABLETS_OPENING,
        new ColumnInformation("Tablets Opening", "Number of opening tablets.", ""));
    mappings.put(TSERVER_TABLETS_UNOPENED,
        new ColumnInformation("Tablets Unopened", "Number of unopened tablets.", ""));
    mappings.put(TSERVER_TABLETS_FILES,
        new ColumnInformation("Avg Files Per Tablet", "Number of files per tablet.", ""));
    mappings.put(TSERVER_INGEST_ENTRIES, new ColumnInformation("Ingested Entries",
        "Ingest entry (a key/value) count. The rate can be derived from this metric.", ""));
    mappings.put(TSERVER_INGEST_BYTES, new ColumnInformation("Ingested Bytes",
        "Ingest byte count. The rate can be derived from this metric.", ""));
    mappings.put(TSERVER_HOLD, new ColumnInformation("Ingest Commit Hold Time",
        "Duration for which commits have been held in milliseconds.", ""));
    mappings.put(TSERVER_TABLETS_ONLINE_ONDEMAND, new ColumnInformation("Online On-Demand Tablets",
        "Number of online on-demand tablets", ""));
    mappings.put(TSERVER_TABLETS_ONDEMAND_UNLOADED_FOR_MEM,
        new ColumnInformation("On-Demand Tablets Unloaded For Memory",
            "Number of online on-demand tablets unloaded due to low memory", ""));
    mappings.put(SCAN_RESERVATION_TOTAL_TIMER, new ColumnInformation("Scan Reservation Total Time",
        "Time to reserve a tablet's files for scan.", ""));
    mappings.put(SCAN_RESERVATION_WRITEOUT_TIMER,
        new ColumnInformation("Scan Reservation Write Time",
            "Time to write out a tablets file reservations for scan.", ""));
    mappings.put(SCAN_RESERVATION_CONFLICT_COUNTER,
        new ColumnInformation("Scan Reservation Conflicts",
            "Count of instances where file reservation attempts for scans encountered conflicts.",
            ""));
    mappings.put(SCAN_TABLET_METADATA_CACHE, new ColumnInformation("Scan Server Metadata Cache",
        "Scan server tablet cache metrics.", ""));
    mappings.put(SCAN_BUSY_TIMEOUT_COUNT, new ColumnInformation("Scan Busy Count",
        "Count of the scans where a busy timeout happened.", ""));
    mappings.put(SCAN_TIMES, new ColumnInformation("Scan Session Total Time",
        "Scan session lifetime (creation to close).", ""));
    mappings.put(SCAN_OPEN_FILES,
        new ColumnInformation("Scan Files Open", "Number of files open for scans.", ""));
    mappings.put(SCAN_RESULTS, new ColumnInformation("Scan Result Count", "Results per scan.", ""));
    mappings.put(SCAN_YIELDS,
        new ColumnInformation("Scan Yield Count", "Counts scans that have yielded.", ""));
    mappings.put(SCAN_START, new ColumnInformation("Scan Start Count",
        "Number of calls to start a scan or multiscan.", ""));
    mappings.put(SCAN_CONTINUE, new ColumnInformation("Scan Continue Count",
        "Number of calls to continue a scan or multiscan.", ""));
    mappings.put(SCAN_CLOSE, new ColumnInformation("Scan Close Count",
        "Number of calls to close a scan or multiscan.", ""));
    mappings.put(SCAN_QUERIES,
        new ColumnInformation("Tablet Lookup Count", "Number of queries made during scans.", ""));
    mappings.put(SCAN_SCANNED_ENTRIES, new ColumnInformation("Scanned Entry Count",
        "Count of scanned entries. The rate can be derived from this metric.", ""));
    mappings.put(SCAN_QUERY_SCAN_RESULTS, new ColumnInformation("Returned Entry Count",
        "Query count. The rate can be derived from this metric.", ""));
    mappings.put(SCAN_QUERY_SCAN_RESULTS_BYTES, new ColumnInformation("Returned Bytes Count",
        "Query byte count. The rate can be derived from this metric.", ""));
    mappings.put(SCAN_PAUSED_FOR_MEM, new ColumnInformation("Scans Paused For Low Memory",
        "Count of scans paused due to server being low on memory.", ""));
    mappings.put(SCAN_RETURN_FOR_MEM, new ColumnInformation("Scans Returned Early for Low Memory",
        "Count of scans that returned results early due to server being low on memory.", ""));
    mappings.put(SCAN_ZOMBIE_THREADS, new ColumnInformation("Scan Zombie Thread Count",
        "Number of scan threads that have no associated client session.", ""));
    mappings.put(MAJC_PAUSED,
        new ColumnInformation("Majc Paused", "Number of paused major compactions.", ""));
    mappings.put(MINC_QUEUED,
        new ColumnInformation("Minc Queued", "Queued minor compactions time queued.", ""));
    mappings.put(MINC_RUNNING,
        new ColumnInformation("Minc Running", "Minor compactions time active.", ""));
    mappings.put(MINC_PAUSED,
        new ColumnInformation("Minc Paused", "Number of paused minor compactions.", ""));
    mappings.put(UPDATE_ERRORS, new ColumnInformation("Ingest Errors",
        "Count of errors during tablet updates. Type/reason for error is stored in the `type` tag (e.g., type=permission, type=unknown.tablet, type=constraint.violation).",
        ""));
    mappings.put(UPDATE_LOCK, new ColumnInformation("Condititional Mutation Row Lock Wait Time",
        "Average time taken for conditional mutation to get a row lock.", ""));
    mappings.put(UPDATE_CHECK, new ColumnInformation("Conditional Mutation Condition Check Time",
        "Average time taken for conditional mutation to check conditions.", ""));
    mappings.put(UPDATE_COMMIT, new ColumnInformation("Mutation Commit Avg Total Time",
        "Average time taken to commit a mutation.", ""));
    mappings.put(UPDATE_COMMIT_PREP, new ColumnInformation("Mutation Commit Avg Prep Time",
        "Average time taken to prepare to commit a single mutation.", ""));
    mappings.put(UPDATE_WALOG_WRITE, new ColumnInformation("Mutations Write To WAL Time",
        "Time taken to write a batch of mutations to WAL.", ""));
    mappings.put(UPDATE_MUTATION_ARRAY_SIZE,
        new ColumnInformation("Mutations Array Size", "Batch size of mutations from client.", ""));
    mappings.put(BLOCKCACHE_INDEX_HITCOUNT,
        new ColumnInformation("Index Block Cache Hit Count", "Index block cache hit count.", ""));
    mappings.put(BLOCKCACHE_INDEX_REQUESTCOUNT, new ColumnInformation(
        "Index Block Cache Request Count", "Index block cache request count.", ""));
    mappings.put(BLOCKCACHE_INDEX_EVICTIONCOUNT, new ColumnInformation(
        "Index Block Cache Eviction Count", "Index block cache eviction count.", ""));
    mappings.put(BLOCKCACHE_DATA_HITCOUNT,
        new ColumnInformation("Data Block Cache Hit Count", "Data block cache hit count.", ""));
    mappings.put(BLOCKCACHE_DATA_REQUESTCOUNT, new ColumnInformation(
        "Data Block Cache Request Count", "Data block cache request count.", ""));
    mappings.put(BLOCKCACHE_DATA_EVICTIONCOUNT, new ColumnInformation(
        "Data Block Cache Eviction Count", "Data block cache eviction count.", ""));
    mappings.put(BLOCKCACHE_SUMMARY_HITCOUNT, new ColumnInformation("Summary Block Cache Hit Count",
        "Summary block cache hit count.", ""));
    mappings.put(BLOCKCACHE_SUMMARY_REQUESTCOUNT, new ColumnInformation(
        "Summary Block Cache Request Count", "Summary block cache request count.", ""));
    mappings.put(BLOCKCACHE_SUMMARY_EVICTIONCOUNT, new ColumnInformation(
        "Summary Block Cache Eviction Count", "Summary block cache eviction count.", ""));
    mappings.put(MANAGER_BALANCER_MIGRATIONS_NEEDED,
        new ColumnInformation("Balancer Migrations Needed",
            "The number of migrations that need to complete before the system is balanced.", ""));
    mappings.put(MANAGER_ROOT_TGW_ERRORS, new ColumnInformation("Root Tablet Watcher Errors",
        "Error count encountered by the TabletGroupWatcher for the ROOT data level.", ""));
    mappings.put(MANAGER_META_TGW_ERRORS, new ColumnInformation("Meta Tablet Watcher Errors",
        "Error count encountered by the TabletGroupWatcher for the META data level.", ""));
    mappings.put(MANAGER_USER_TGW_ERRORS, new ColumnInformation("User Tablet Watcher Errors",
        "Error count encountered by the TabletGroupWatcher for the USER data level.", ""));
    mappings.put(MANAGER_GOAL_STATE, new ColumnInformation("Manager Goal State",
        "Manager goal state: -1=unknown, 0=CLEAN_STOP, 1=SAFE_MODE, 2=NORMAL.", ""));
    mappings.put(RECOVERIES_IN_PROGRESS, new ColumnInformation("Tablet Recoveries In Progress",
        "The number of recoveries in progress.", ""));
    mappings.put(RECOVERIES_LONGEST_RUNTIME, new ColumnInformation("Tablet Recovery Longest Time",
        "The time (in milliseconds) of the longest running recovery.", ""));
    mappings.put(RECOVERIES_AVG_PROGRESS,
        new ColumnInformation("Tablet Recovery Avg Percent Complete",
            "The average percentage (0.0 - 99.9) of the in progress recoveries.", ""));
  }

  public static ColumnInformation getColumnInformation(Metric m) {
    return mappings.get(m);
  }

}

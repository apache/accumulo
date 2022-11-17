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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * Prior to 2.1.0 Accumulo used the <a href=
 * "https://hadoop.apache.org/docs/current/api/org/apache/hadoop/metrics2/package-summary.html">Hadoop
 * Metrics2</a> framework. In 2.1.0 Accumulo migrated away from the Metrics2 framework to
 * <a href="https://micrometer.io/">Micrometer</a>. Micrometer suggests using a particular
 * <a href="https://micrometer.io/docs/concepts#_naming_meters">naming convention</a> for the
 * metrics. The table below contains a mapping of the old to new metric names.
 *
 * <table border="1">
 * <caption>Summary of Metric Changes</caption> <!-- fate -->
 * <tr>
 * <th>Old Name</th>
 * <th>Hadoop Metrics2 Type</th>
 * <th>New Name</th>
 * <th>Micrometer Type</th>
 * <th>Notes</th>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_COMPACTOR_MAJC_STUCK}</td>
 * <td>LongTaskTimer</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>currentFateOps</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_TOTAL_IN_PROGRESS}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>FateTxOpType_{name}</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_TYPE_IN_PROGRESS}</td>
 * <td>Gauge</td>
 * <td>Previously there was a metric per operation type with the count of in-progress transactions
 * of that type. Now there is one metric and the type is in the tag op.type</td>
 * </tr>
 * <tr>
 * <td>totalFateOps</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_OPS_ACTIVITY}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>totalZkConnErrors</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_ERRORS}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>FateTxState_NEW</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_TX}</td>
 * <td>Gauge</td>
 * <td>The state is now in a tag: state=new</td>
 * </tr>
 * <tr>
 * <td>FateTxState_IN_PROGRESS</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_TX}</td>
 * <td>Gauge</td>
 * <td>The state is now in a tag: state=in.progress</td>
 * </tr>
 * <tr>
 * <td>FateTxState_FAILED_IN_PROGRESS</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_TX}</td>
 * <td>Gauge</td>
 * <td>The state is now in a tag: state=failed.in.progress</td>
 * </tr>
 * <tr>
 * <td>FateTxState_FAILED</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_TX}</td>
 * <td>Gauge</td>
 * <td>The state is now in a tag: state=failed</td>
 * </tr>
 * <tr>
 * <td>FateTxState_SUCCESSFUL</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_TX}</td>
 * <td>Gauge</td>
 * <td>The state is now in a tag: state=successful</td>
 * </tr>
 * <tr>
 * <td>FateTxState_UNKNOWN</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_FATE_TX}</td>
 * <td>Gauge</td>
 * <td>The state is now in a tag: state=unknown</td>
 * </tr>
 * <!-- garbage collection -->
 * <tr>
 * <td>AccGcStarted</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_STARTED}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcFinished</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_FINISHED}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcCandidates</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_CANDIDATES}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcInUse</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_IN_USE}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcDeleted</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_DELETED}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcErrors</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_ERRORS}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcWalStarted</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_WAL_STARTED}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcWalFinished</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_WAL_FINISHED}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcWalCandidates</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_WAL_CANDIDATES}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcWalInUse</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_WAL_IN_USE}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcWalDeleted</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_WAL_DELETED}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcWalErrors</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_WAL_ERRORS}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcPosOpDuration</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_POST_OP_DURATION}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>AccGcRunCycleCount</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_GC_RUN_CYCLE}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <!-- tablet server -->
 * <tr>
 * <td>entries</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_ENTRIES}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>entriesInMem</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_MEM_ENTRIES}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>activeMajCs</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_MAJC_RUNNING}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_TSERVER_MAJC_STUCK}</td>
 * <td>LongTaskTimer</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>queuedMajCs</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_MAJC_QUEUED}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>activeMinCs</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_MINC_RUNNING}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>queuedMinCs</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_MINC_QUEUED}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>totalMinCs</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_MINC_TOTAL}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>onlineTablets</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_TABLETS_ONLINE}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_TSERVER_TABLETS_LONG_ASSIGNMENTS}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>openingTablets</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_TABLETS_OPENING}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>unopenedTablets</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_TABLETS_UNOPENED}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>filesPerTablet</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_TABLETS_FILES}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>queries</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_QUERIES}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>scannedRate</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_SCANNED_ENTRIES}</td>
 * <td>Gauge</td>
 * <td>Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be
 * derived</td>
 * </tr>
 * <tr>
 * <td>queryRate</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_SCAN_RESULTS}</td>
 * <td>Gauge</td>
 * <td>Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be
 * derived</td>
 * </tr>
 * <tr>
 * <td>queryByteRate</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_SCAN_RESULTS_BYTES}</td>
 * <td>Gauge</td>
 * <td>Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be
 * derived</td>
 * </tr>
 * <tr>
 * <td>ingestRate</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_INGEST_MUTATIONS}</td>
 * <td>Gauge</td>
 * <td>Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be
 * derived</td>
 * </tr>
 * <tr>
 * <td>ingestByteRate</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_INGEST_BYTES}</td>
 * <td>Gauge</td>
 * <td>Prior to 2.1.0 this metric was reported as a rate, it is now the count and the rate can be
 * derived</td>
 * </tr>
 * <tr>
 * <td>holdTime</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_TSERVER_HOLD}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <!-- scans -->
 * <tr>
 * <td>scan</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_SCAN}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_SCAN_OPEN_FILES}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>result</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_SCAN_RESULTS}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>yield</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_SCAN_YIELDS}</td>
 * <td>Gauge</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_SCAN_START}</td>
 * <td>Counter</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_SCAN_CONTINUE}</td>
 * <td>Counter</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_SCAN_CLOSE}</td>
 * <td>Counter</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_SCAN_BUSY_TIMEOUT}</td>
 * <td>Counter</td>
 * <td></td>
 * </tr>
 * <!-- major compactions -->
 * <tr>
 * <td>{i|e}_{compactionServiceName}_{executor_name}_queued</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_MAJC_QUEUED}</td>
 * <td>Gauge</td>
 * <td>The compaction service information is in a tag:
 * id={i|e}_{compactionServiceName}_{executor_name}</td>
 * </tr>
 * <tr>
 * <td>{i|e}_{compactionServiceName}_{executor_name}_running</td>
 * <td>Gauge</td>
 * <td>{@link #METRICS_MAJC_RUNNING}</td>
 * <td>Gauge</td>
 * <td>The compaction service information is in a tag:
 * id={i|e}_{compactionServiceName}_{executor_name}</td>
 * </tr>
 * <!-- minor compactions -->
 * <tr>
 * <td>Queue</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_MINC_QUEUED}</td>
 * <td>Timer</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>Minc</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_MINC_RUNNING}</td>
 * <td>Timer</td>
 * <td></td>
 * </tr>
 * <!-- Updates (ingest) -->
 * <tr>
 * <td>permissionErrors</td>
 * <td>Counter</td>
 * <td>{@link #METRICS_UPDATE_ERRORS}</td>
 * <td>Gauge</td>
 * <td>Type is stored in tag: type=permission</td>
 * </tr>
 * <tr>
 * <td>unknownTabletErrors</td>
 * <td>Counter</td>
 * <td>{@link #METRICS_UPDATE_ERRORS}</td>
 * <td>Gauge</td>
 * <td>Type is stored in tag: type=unknown.tablet</td>
 * </tr>
 * <tr>
 * <td>constraintViolations</td>
 * <td>Counter</td>
 * <td>{@link #METRICS_UPDATE_ERRORS}</td>
 * <td>Gauge</td>
 * <td>Type is stored in tag: type=constraint.violation</td>
 * </tr>
 * <tr>
 * <td>commitPrep</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_UPDATE_COMMIT_PREP}</td>
 * <td>Timer</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>commitTime</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_UPDATE_COMMIT}</td>
 * <td>Timer</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>waLogWriteTime</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_UPDATE_WALOG_WRITE}</td>
 * <td>Timer</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>mutationArraysSize</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_UPDATE_MUTATION_ARRAY_SIZE}</td>
 * <td>Distribution Summary</td>
 * <td></td>
 * </tr>
 * <!-- Thrift -->
 * <tr>
 * <td>idle</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_THRIFT_IDLE}</td>
 * <td>Distribution Summary</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>execute</td>
 * <td>Stat</td>
 * <td>{@link #METRICS_THRIFT_EXECUTE}</td>
 * <td>Distribution Summary</td>
 * <td></td>
 * </tr>
 * <!-- ZooKeeper property cache -->
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_PROPSTORE_LOAD_TIMER}</td>
 * <td>Timer</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_PROPSTORE_REFRESH_COUNT}</td>
 * <td>Counter</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_PROPSTORE_REFRESH_LOAD_COUNT}</td>
 * <td>Counter</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_PROPSTORE_EVICTION_COUNT}</td>
 * <td>Counter</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>N/A</td>
 * <td>N/A</td>
 * <td>{@link #METRICS_PROPSTORE_ZK_ERROR_COUNT}</td>
 * <td>Counter</td>
 * <td></td>
 * </tr>
 * </table>
 *
 * @since 2.1.0
 */
public interface MetricsProducer {

  Logger LOG = LoggerFactory.getLogger(MetricsProducer.class);

  String METRICS_COMPACTOR_PREFIX = "accumulo.compactor";
  String METRICS_COMPACTOR_MAJC_STUCK = METRICS_COMPACTOR_PREFIX + "majc.stuck";

  String METRICS_FATE_PREFIX = "accumulo.fate.";
  String METRICS_FATE_TYPE_IN_PROGRESS = METRICS_FATE_PREFIX + "ops.in_progress_by_type";
  String METRICS_FATE_TOTAL_IN_PROGRESS = METRICS_FATE_PREFIX + "ops.in_progress";
  String METRICS_FATE_OPS_ACTIVITY = METRICS_FATE_PREFIX + "ops.activity";
  String METRICS_FATE_ERRORS = METRICS_FATE_PREFIX + "errors";
  String METRICS_FATE_TX = METRICS_FATE_PREFIX + "tx";

  String METRICS_GC_PREFIX = "accumulo.gc.";
  String METRICS_GC_STARTED = METRICS_GC_PREFIX + "started";
  String METRICS_GC_FINISHED = METRICS_GC_PREFIX + "finished";
  String METRICS_GC_CANDIDATES = METRICS_GC_PREFIX + "candidates";
  String METRICS_GC_IN_USE = METRICS_GC_PREFIX + "in.use";
  String METRICS_GC_DELETED = METRICS_GC_PREFIX + "deleted";
  String METRICS_GC_ERRORS = METRICS_GC_PREFIX + "errors";
  String METRICS_GC_WAL_STARTED = METRICS_GC_PREFIX + "wal.started";
  String METRICS_GC_WAL_FINISHED = METRICS_GC_PREFIX + "wal.finished";
  String METRICS_GC_WAL_CANDIDATES = METRICS_GC_PREFIX + "wal.candidates";
  String METRICS_GC_WAL_IN_USE = METRICS_GC_PREFIX + "wal.in.use";
  String METRICS_GC_WAL_DELETED = METRICS_GC_PREFIX + "wal.deleted";
  String METRICS_GC_WAL_ERRORS = METRICS_GC_PREFIX + "wal.errors";
  String METRICS_GC_POST_OP_DURATION = METRICS_GC_PREFIX + "post.op.duration";
  String METRICS_GC_RUN_CYCLE = METRICS_GC_PREFIX + "run.cycle";

  String METRICS_MAJC_PREFIX = "accumulo.tserver.compactions.majc.";
  String METRICS_MAJC_QUEUED = METRICS_MAJC_PREFIX + "queued";
  String METRICS_MAJC_RUNNING = METRICS_MAJC_PREFIX + "running";

  String METRICS_MINC_PREFIX = "accumulo.tserver.compactions.minc.";
  String METRICS_MINC_QUEUED = METRICS_MINC_PREFIX + "queued";
  String METRICS_MINC_RUNNING = METRICS_MINC_PREFIX + "running";

  String METRICS_SCAN = "accumulo.tserver.scans";
  String METRICS_SCAN_OPEN_FILES = METRICS_SCAN + ".files.open";
  String METRICS_SCAN_RESULTS = METRICS_SCAN + ".result";
  String METRICS_SCAN_YIELDS = METRICS_SCAN + ".yields";
  String METRICS_SCAN_START = METRICS_SCAN + ".start";
  String METRICS_SCAN_CONTINUE = METRICS_SCAN + ".continue";
  String METRICS_SCAN_CLOSE = METRICS_SCAN + ".close";
  String METRICS_SCAN_BUSY_TIMEOUT = METRICS_SCAN + ".busy_timeout";

  String METRICS_TSERVER_PREFIX = "accumulo.tserver.";
  String METRICS_TSERVER_ENTRIES = METRICS_TSERVER_PREFIX + "entries";
  String METRICS_TSERVER_MEM_ENTRIES = METRICS_TSERVER_PREFIX + "entries.mem";
  String METRICS_TSERVER_MAJC_QUEUED = METRICS_TSERVER_PREFIX + "majc.queued";
  String METRICS_TSERVER_MAJC_RUNNING = METRICS_TSERVER_PREFIX + "majc.running";
  String METRICS_TSERVER_MAJC_STUCK = METRICS_TSERVER_PREFIX + "majc.stuck";
  String METRICS_TSERVER_MINC_QUEUED = METRICS_TSERVER_PREFIX + "minc.queued";
  String METRICS_TSERVER_MINC_RUNNING = METRICS_TSERVER_PREFIX + "minc.running";
  String METRICS_TSERVER_MINC_TOTAL = METRICS_TSERVER_PREFIX + "minc.total";
  String METRICS_TSERVER_TABLETS_LONG_ASSIGNMENTS =
      METRICS_TSERVER_PREFIX + "tablets.assignments.warning";
  String METRICS_TSERVER_TABLETS_ONLINE = METRICS_TSERVER_PREFIX + "tablets.online";
  String METRICS_TSERVER_TABLETS_OPENING = METRICS_TSERVER_PREFIX + "tablets.opening";
  String METRICS_TSERVER_TABLETS_UNOPENED = METRICS_TSERVER_PREFIX + "tablets.unopened";
  String METRICS_TSERVER_QUERIES = METRICS_TSERVER_PREFIX + "queries";
  String METRICS_TSERVER_TABLETS_FILES = METRICS_TSERVER_PREFIX + "tablets.files";
  String METRICS_TSERVER_HOLD = METRICS_TSERVER_PREFIX + "hold";
  String METRICS_TSERVER_INGEST_MUTATIONS = METRICS_TSERVER_PREFIX + "ingest.mutations";
  String METRICS_TSERVER_INGEST_BYTES = METRICS_TSERVER_PREFIX + "ingest.bytes";
  String METRICS_TSERVER_SCAN_RESULTS = METRICS_TSERVER_PREFIX + "scan.results";
  String METRICS_TSERVER_SCAN_RESULTS_BYTES = METRICS_TSERVER_PREFIX + "scan.results.bytes";
  String METRICS_TSERVER_SCANNED_ENTRIES = METRICS_TSERVER_PREFIX + "scan.scanned.entries";

  String METRICS_THRIFT_PREFIX = "accumulo.thrift.";
  String METRICS_THRIFT_EXECUTE = METRICS_THRIFT_PREFIX + "execute";
  String METRICS_THRIFT_IDLE = METRICS_THRIFT_PREFIX + "idle";

  String METRICS_UPDATE_PREFIX = "accumulo.tserver.updates.";
  String METRICS_UPDATE_ERRORS = METRICS_UPDATE_PREFIX + "error";
  String METRICS_UPDATE_COMMIT = METRICS_UPDATE_PREFIX + "commit";
  String METRICS_UPDATE_COMMIT_PREP = METRICS_UPDATE_COMMIT + ".prep";
  String METRICS_UPDATE_WALOG_WRITE = METRICS_UPDATE_PREFIX + "walog.write";
  String METRICS_UPDATE_MUTATION_ARRAY_SIZE = METRICS_UPDATE_PREFIX + "mutation.arrays.size";

  String METRICS_PROPSTORE_PREFIX = "accumulo.prop.store.";
  String METRICS_PROPSTORE_LOAD_TIMER = METRICS_PROPSTORE_PREFIX + "load";
  String METRICS_PROPSTORE_REFRESH_COUNT = METRICS_PROPSTORE_PREFIX + "refresh";
  String METRICS_PROPSTORE_REFRESH_LOAD_COUNT = METRICS_PROPSTORE_PREFIX + "refresh.load";
  String METRICS_PROPSTORE_EVICTION_COUNT = METRICS_PROPSTORE_PREFIX + "evictions";
  String METRICS_PROPSTORE_ZK_ERROR_COUNT = METRICS_PROPSTORE_PREFIX + "zookeeper.error";

  /**
   * Build Micrometer Meter objects and register them with the registry
   */
  void registerMetrics(MeterRegistry registry);

  /**
   * Returns a new mutable mapping of metric field value to metric field name.
   *
   * @return map of field names to variable names.
   */
  default Map<String,String> getMetricFields() {
    Map<String,String> fields = new HashMap<>();
    for (Field f : MetricsProducer.class.getDeclaredFields()) {
      if (Modifier.isStatic(f.getModifiers()) && f.getType().equals(String.class)
          && !f.getName().contains("PREFIX")) {
        try {

          fields.put((String) f.get(MetricsProducer.class), f.getName());
        } catch (IllegalArgumentException | IllegalAccessException e) {
          // this shouldn't happen, but lets log it anyway
          LOG.error("Error getting metric value for field: " + f.getName());
        }
      }
    }
    return fields;
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

public interface MetricsProducer {

  public static final Logger LOG = LoggerFactory.getLogger(MetricsProducer.class);

  public static final String METRICS_FATE_PREFIX = "accumulo.fate.";
  public static final String METRICS_FATE_CURRENT_OPS = METRICS_FATE_PREFIX + "ops.current";
  public static final String METRICS_FATE_TOTAL_OPS = METRICS_FATE_PREFIX + "ops.total";
  public static final String METRICS_FATE_ERRORS = METRICS_FATE_PREFIX + "errors";
  public static final String METRICS_FATE_TX = METRICS_FATE_PREFIX + "tx";

  public static final String METRICS_GC_PREFIX = "accumulo.gc.";
  public static final String METRICS_GC_STARTED = METRICS_GC_PREFIX + "started";
  public static final String METRICS_GC_FINISHED = METRICS_GC_PREFIX + "finished";
  public static final String METRICS_GC_CANDIDATES = METRICS_GC_PREFIX + "candidates";
  public static final String METRICS_GC_IN_USE = METRICS_GC_PREFIX + "in.use";
  public static final String METRICS_GC_DELETED = METRICS_GC_PREFIX + "deleted";
  public static final String METRICS_GC_ERRORS = METRICS_GC_PREFIX + "errors";
  public static final String METRICS_GC_WAL_STARTED = METRICS_GC_PREFIX + "wal.started";
  public static final String METRICS_GC_WAL_FINISHED = METRICS_GC_PREFIX + "wal.finished";
  public static final String METRICS_GC_WAL_CANDIDATES = METRICS_GC_PREFIX + "wal.candidates";
  public static final String METRICS_GC_WAL_IN_USE = METRICS_GC_PREFIX + "wal.in.use";
  public static final String METRICS_GC_WAL_DELETED = METRICS_GC_PREFIX + "wal.deleted";
  public static final String METRICS_GC_WAL_ERRORS = METRICS_GC_PREFIX + "wal.errors";
  public static final String METRICS_GC_POST_OP_DURATION = METRICS_GC_PREFIX + "post.op.duration";
  public static final String METRICS_GC_RUN_CYCLE = METRICS_GC_PREFIX + "run.cycle";

  public static final String METRICS_MAJC_PREFIX = "accumulo.tserver.compactions.majc.";
  public static final String METRICS_MAJC_QUEUED = METRICS_MAJC_PREFIX + "queued";
  public static final String METRICS_MAJC_RUNNING = METRICS_MAJC_PREFIX + "running";

  public static final String METRICS_MINC_PREFIX = "accumulo.tserver.compactions.minc.";
  public static final String METRICS_MINC_QUEUED = METRICS_MINC_PREFIX + "queued";
  public static final String METRICS_MINC_RUNNING = METRICS_MINC_PREFIX + "running";

  public static final String METRICS_REPLICATION_PREFIX = "accumulo.replication.";
  public static final String METRICS_REPLICATION_QUEUE = METRICS_REPLICATION_PREFIX + "queue";
  public static final String METRICS_REPLICATION_PENDING_FILES =
      METRICS_REPLICATION_PREFIX + "files.pending";
  public static final String METRICS_REPLICATION_PEERS = METRICS_REPLICATION_PREFIX + "peers";
  public static final String METRICS_REPLICATION_THREADS = METRICS_REPLICATION_PREFIX + "threads";

  public static final String METRICS_SCAN = "accumulo.tserver.scans";
  public static final String METRICS_SCAN_RESULTS = METRICS_SCAN + ".result";
  public static final String METRICS_SCAN_YIELDS = METRICS_SCAN + ".yields";

  public static final String METRICS_TSERVER_PREFIX = "accumulo.tserver.";
  public static final String METRICS_TSERVER_ENTRIES = METRICS_TSERVER_PREFIX + "entries";
  public static final String METRICS_TSERVER_MEM_ENTRIES = METRICS_TSERVER_PREFIX + "entries.mem";
  public static final String METRICS_TSERVER_MAJC_QUEUED = METRICS_TSERVER_PREFIX + "majc.queued";
  public static final String METRICS_TSERVER_MAJC_RUNNING = METRICS_TSERVER_PREFIX + "majc.running";
  public static final String METRICS_TSERVER_MINC_QUEUED = METRICS_TSERVER_PREFIX + "minc.queued";
  public static final String METRICS_TSERVER_MINC_RUNNING = METRICS_TSERVER_PREFIX + "minc.running";
  public static final String METRICS_TSERVER_MINC_TOTAL = METRICS_TSERVER_PREFIX + "minc.total";
  public static final String METRICS_TSERVER_TABLETS_ONLINE =
      METRICS_TSERVER_PREFIX + "tablets.online";
  public static final String METRICS_TSERVER_TABLETS_OPENING =
      METRICS_TSERVER_PREFIX + "tablets.opening";
  public static final String METRICS_TSERVER_TABLETS_UNOPENED =
      METRICS_TSERVER_PREFIX + "tablets.unopened";
  public static final String METRICS_TSERVER_QUERIES = METRICS_TSERVER_PREFIX + "queries";
  public static final String METRICS_TSERVER_TABLETS_FILES =
      METRICS_TSERVER_PREFIX + "tablets.files";
  public static final String METRICS_TSERVER_HOLD = METRICS_TSERVER_PREFIX + "hold";
  public static final String METRICS_TSERVER_INGEST_MUTATIONS =
      METRICS_TSERVER_PREFIX + "ingest.mutations";
  public static final String METRICS_TSERVER_INGEST_BYTES = METRICS_TSERVER_PREFIX + "ingest.bytes";
  public static final String METRICS_TSERVER_SCAN_RESULTS = METRICS_TSERVER_PREFIX + "scan.results";
  public static final String METRICS_TSERVER_SCAN_RESULTS_BYTES =
      METRICS_TSERVER_PREFIX + "scan.results.bytes";
  public static final String METRICS_TSERVER_SCANNED_ENTRIES =
      METRICS_TSERVER_PREFIX + "scan.scanned.entries";

  public static final String METRICS_THRIFT_PREFIX = "accumulo.thrift.";
  public static final String METRICS_THRIFT_EXECUTE = METRICS_THRIFT_PREFIX + "execute";
  public static final String METRICS_THRIFT_IDLE = METRICS_THRIFT_PREFIX + "idle";

  public static final String METRICS_UPDATE_PREFIX = "accumulo.tserver.updates.";
  public static final String METRICS_UPDATE_ERRORS = METRICS_UPDATE_PREFIX + "error";
  public static final String METRICS_UPDATE_COMMIT = METRICS_UPDATE_PREFIX + "commit";
  public static final String METRICS_UPDATE_COMMIT_PREP = METRICS_UPDATE_COMMIT + ".prep";
  public static final String METRICS_UPDATE_WALOG_WRITE = METRICS_UPDATE_PREFIX + "walog.write";
  public static final String METRICS_UPDATE_MUTATION_ARRAY_SIZE =
      METRICS_UPDATE_PREFIX + "mutation.arrays.size";

  /**
   * Build Micrometer Meter objects and register them with the registry
   */
  void registerMetrics(MeterRegistry registry);

  /**
   * Returns a mapping of metric field value to metric field name.
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

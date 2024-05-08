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

/**
 * Defines constants used for thread pool metrics names.
 */
public interface MetricsThreadPoolsDef {

  // metered pools and executor service metric names
  String METRICS_POOL_PREFIX = "accumulo.pool.";
  String METRICS_BULK_IMPORT_CLIENT_PREFIX = METRICS_POOL_PREFIX + "bulk.import.client.";
  String METRICS_COORDINATOR_FINALIZER_BACKGROUND_POOL =
      METRICS_POOL_PREFIX + "compaction.finalizer.background.pool";
  String METRICS_COORDINATOR_FINALIZER_NOTIFIER_POOL =
      METRICS_POOL_PREFIX + "compaction.coordinator.compaction.finalizer";
  String METRICS_GC_DELETE_POOL = METRICS_POOL_PREFIX + "gc.threads.delete";
  String METRICS_GENERAL_SERVER_POOL = METRICS_POOL_PREFIX + "general.server";
  String METRICS_GENERAL_SERVER_SIMPLETIMER_POOL =
      METRICS_POOL_PREFIX + "general.server.simpletimer";
  String METRICS_MANAGER_BULK_IMPORT_POOL = METRICS_POOL_PREFIX + "manager.bulk.import";
  String METRICS_MANAGER_FATE_POOL = METRICS_POOL_PREFIX + "manager.fate";
  String METRICS_MANAGER_RENAME_POOL = METRICS_POOL_PREFIX + "manager.rename";
  String METRICS_MANAGER_STATUS_POOL = METRICS_POOL_PREFIX + "manager.status";
  String METRICS_REPLICATION_WORKER_POOL = METRICS_POOL_PREFIX + "replication.worker";
  String METRICS_TSERVER_ASSIGNMENT_POOL = METRICS_POOL_PREFIX + "tserver.assignment";
  String METRICS_TSERVER_COMPACTION_MINOR_POOL = METRICS_POOL_PREFIX + "tserver.compaction.minor";
  String METRICS_TSERVER_MIGRATIONS_POOL = METRICS_POOL_PREFIX + "tserver.migrations";
  String METRICS_TSERVER_MINOR_COMPACTOR_POOL = METRICS_POOL_PREFIX + "tserver.minor.compactor";
  String METRICS_TSERVER_SUMMARY_PARTITION_POOL = METRICS_POOL_PREFIX + "tserver.summary.partition";
  String METRICS_TSERVER_SUMMARY_REMOTE_POOL = METRICS_POOL_PREFIX + "tserver.summary.remote";
  String METRICS_TSERVER_SUMMARY_RETRIEVAL_POOL = METRICS_POOL_PREFIX + "tserver.summary.retrieval";
  String METRICS_TSERVER_TABLET_MIGRATION_POOL = METRICS_POOL_PREFIX + "tserver.tablet.migration";
  String METRICS_TSERVER_WORKQ_POOL = METRICS_POOL_PREFIX + "tserver.workq";
  String METRICS_TSERV_WAL_SORT_CONCURRENT_POOL =
      METRICS_POOL_PREFIX + "tserver.wal.sort.concurrent";
}

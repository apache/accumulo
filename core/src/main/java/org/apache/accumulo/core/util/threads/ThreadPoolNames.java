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
package org.apache.accumulo.core.util.threads;

public enum ThreadPoolNames {

  ACCUMULO_POOL_PREFIX("accumulo.pool"),
  BATCH_WRITER_SEND_POOL("accumulo.pool.batch.writer.send"),
  BATCH_WRITER_BIN_MUTATIONS_POOL("accumulo.pool.batch.writer.bin.mutations"),
  BLOOM_LOADER_POOL("accumulo.pool.bloom.loader"),
  BULK_IMPORT_CLIENT_LOAD_POOL("accumulo.pool.bulk.import.client.bulk.load"),
  BULK_IMPORT_CLIENT_BULK_THREADS_POOL("accumulo.pool.bulk.import.client.bulk.threads"),
  BULK_IMPORT_DIR_MOVE_POOL("accumulo.pool.bulk.dir.move"),
  COMPACTION_SERVICE_COMPACTION_PLANNER_POOL("accumulo.pool.compaction.service.compaction.planner"),
  COMPACTOR_RUNNING_COMPACTIONS_POOL("accumulo.pool.compactor.running.compactions"),
  COMPACTOR_RUNNING_COMPACTION_IDS_POOL("accumulo.pool.compactor.running.compaction.ids"),
  CONDITIONAL_WRITER_POOL("accumulo.pool.conditional.writer"),
  CONDITIONAL_WRITER_CLEANUP_POOL("accumulo.pool.client.context.conditional.writer.cleanup"),
  COORDINATOR_FINALIZER_BACKGROUND_POOL("accumulo.pool.compaction.finalizer.background.pool"),
  COORDINATOR_FINALIZER_NOTIFIER_POOL("accumulo.pool.compaction.coordinator.compaction.finalizer"),
  GC_DELETE_POOL("accumulo.pool.gc.threads.delete"),
  GENERAL_SERVER_POOL("accumulo.pool.general.server"),
  SERVICE_LOCK_POOL("accumulo.pool.service.lock"),
  IMPORT_TABLE_RENAME_POOL("accumulo.pool.import.table.rename"),
  INSTANCE_OPS_COMPACTIONS_FINDER_POOL("accumulo.pool.instance.ops.active.compactions.finder"),
  INSTANCE_OPS_SCANS_FINDER_POOL("accumulo.pool.instance.ops.active.scans.finder"),
  MANAGER_FATE_POOL("accumulo.pool.manager.fate"),
  MANAGER_STATUS_POOL("accumulo.pool.manager.status"),
  MANAGER_UPGRADE_COORDINATOR_METADATA_POOL("accumulo.pool.manager.upgrade.metadata"),
  METADATA_DEFAULT_SPLIT_POOL("accumulo.pool.metadata.tablet.default.splitter"),
  METADATA_TABLET_MIGRATION_POOL("accumulo.pool.metadata.tablet.migration"),
  METADATA_TABLET_ASSIGNMENT_POOL("accumulo.pool.metadata.tablet.assignment"),
  SCAN_SERVER_TABLET_METADATA_CACHE_POOL("accumulo.pool.scan.server.tablet.metadata.cache"),
  SCANNER_READ_AHEAD_POOL("accumulo.pool.client.context.scanner.read.ahead"),
  SCHED_FUTURE_CHECKER_POOL("accumulo.pool.scheduled.future.checker"),
  SPLIT_START_POOL("accumulo.pool.table.ops.add.splits.start"),
  SPLIT_WAIT_POOL("accumulo.pool.table.ops.add.splits.wait"),
  TABLET_ASSIGNMENT_POOL("accumulo.pool.tablet.assignment.pool"),
  TSERVER_ASSIGNMENT_POOL("accumulo.pool.tserver.assignment"),
  TSERVER_MIGRATIONS_POOL("accumulo.pool.tserver.migrations"),
  TSERVER_MINOR_COMPACTOR_POOL("accumulo.pool.tserver.minor.compactor"),
  TSERVER_SUMMARY_FILE_RETRIEVER_POOL("accumulo.pool.tserver.summary.file.retriever.pool"),
  TSERVER_SUMMARY_PARTITION_POOL("accumulo.pool.tserver.summary.partition"),
  TSERVER_SUMMARY_REMOTE_POOL("accumulo.pool.tserver.summary.remote"),
  TSERVER_SUMMARY_RETRIEVAL_POOL("accumulo.pool.tserver.summary.retrieval"),
  TSERVER_TABLET_MIGRATION_POOL("accumulo.pool.tserver.tablet.migration"),
  TSERVER_WAL_CREATOR_POOL("accumulo.pool.tserver.wal.creator"),
  TSERVER_WAL_SORT_CONCURRENT_POOL("accumulo.pool.tserver.wal.sort.concurrent"),
  UTILITY_CHECK_FILE_TASKS("accumulo.pool.util.check.file.tasks"),
  UTILITY_VERIFY_TABLET_ASSIGNMENTS("accumulo.pool.util.check.tablet.servers");

  public final String poolName;

  ThreadPoolNames(String poolName) {
    this.poolName = poolName;
  }
}

package org.apache.accumulo.core.util.threads;

public enum ThreadPoolNames {

  ACCUMULO_POOL_PREFIX("accumulo.pool"),
  BATCH_SCANNER_POOL_NAME("accumulo.pool.client.batch.reader.scanner"),
  BATCH_WRITER_SEND_POOL_NAME("accumulo.pool.batch.writer.send"),
  BATCH_WRITER_BIN_MUTATIONS_POOL_NAME("accumulo.pool.batch.writer.bin.mutations"),
  BULK_IMPORT_CLIENT_LOAD_POOL_NAME("accumulo.pool.bulk.import.client.bulk.load"),
  BULK_IMPORT_DIR_MOVE_POOL_NAME("accumulo.pool.bulk.dir.move"),
  BLOOM_LOADER_POOL_NAME("accumulo.pool.bloom.loader"),
  SCANNER_READ_AHEAD_POOL_NAME("accumulo.pool.client.context.scanner.read.ahead"),
  COMPACTOR_RUNNING_COMPACTIONS_POOL_NAME("accumulo.pool.compactor.running.compactions"),
  COMPACTION_RUNNING_COMPACTION_IDS_POOL_NAME("accumulo.pool.compactor.running.compaction.ids"),
  CONDITIONAL_WRITER_POOL_NAME("accumulo.pool.conditional.writer"),
  CONDITIONAL_WRITER_CLEANUP_POOL_NAME("accumulo.pool.client.context.conditional.writer.cleanup"),
  COORDINATOR_FINALIZER_BACKGROUND_POOL_NAME("accumulo.pool.compaction.finalizer.background.pool"),
  COORDINATOR_FINALIZER_NOTIFIER_POOL_NAME(
      "accumulo.pool.compaction.coordinator.compaction.finalizer"),
  GC_DELETE_POOL_NAME("accumulo.pool.gc.threads.delete"),
  GENERAL_SERVER_POOL_NAME("accumulo.pool.general.server"),
  GENERAL_SERVER_SIMPLETIMER_POOL_NAME("accumulo.pool.general.server.simpletimer"),
  INSTANCE_OPS_COMPACTIONS_FINDER_POOL_NAME("accumulo.pool.instance.ops.active.compactions.finder"),
  IMPORT_TABLE_RENAME_POOL_NAME("accumulo.pool.import.table.rename"),
  MANAGER_BULK_IMPORT_POOL_NAME("accumulo.pool.manager.bulk.import"),
  MANAGER_FATE_POOL_NAME("accumulo.pool.manager.fate"),
  MANAGER_RENAME_POOL_NAME("accumulo.pool.manager.rename"),
  MANAGER_STATUS_POOL_NAME("accumulo.pool.manager.status"),
  METADATA_DEFAULT_SPLIT_POOL_NAME("accumulo.pool.metadata.tablet.default.splitter"),
  METADATA_TABLET_MIGRATION_POOL_NAME("accumulo.pool.metadata.tablet.migration"),
  METADATA_TABLET_ASSIGNMENT_POOL_NAME("accumulo.pool.metadata.tablet.assignment"),
  REPLICATION_WORKER_POOL_NAME("accumulo.pool.replication.worker"),
  SCAN_POOL_NAME("accumulo.pool.scan"),
  SPLIT_POOL_NAME("accumulo.pool.table.ops.add.splits"),
  SCHEDULED_FUTURE_CHECKER_POOL_NAME("accumulo.pool.scheduled.future.checker"),
  TABLET_ASSIGNMENT_POOL_NAME("accumulo.pool.tablet.assignment.pool"),
  TSERVER_SUMMARY_RETRIEVAL_POOL_NAME("accumulo.pool.tserver.summary.retrieval"),
  TSERVER_SUMMARY_FILE_RETRIEVER_POOL_NAME("accumulo.pool.tserver.summary.file.retriever.pool"),
  TSERVER_SUMMARY_REMOTE_POOL_NAME("accumulo.pool.tserver.summary.remote"),
  TSERVER_MIGRATIONS_POOL_NAME("accumulo.pool.tserver.migrations"),
  TSERVER_ASSIGNMENT_POOL_NAME("accumulo.pool.tserver.assignment"),
  TSERVER_COMPACTION_MINOR_POOL_NAME("accumulo.pool.tserver.compaction.minor"),
  TSERVER_MINOR_COMPACTOR_POOL_NAME("accumulo.pool.tserver.minor.compactor"),
  TSERVER_SUMMARY_PARTITION_POOL_NAME("accumulo.pool.tserver.summary.partition"),
  TSERVER_TABLET_MIGRATION_POOL_NAME("accumulo.pool.tserver.tablet.migration"),
  TSERVER_WAL_SORT_CONCURRENT_POOL_NAME("accumulo.pool.tserver.wal.sort.concurrent"),
  TSERVER_WORKQ_POOL_NAME("accumulo.pool.tserver.workq");

  public final String poolName;

  ThreadPoolNames(String poolName) {
    this.poolName = poolName;
  }
}

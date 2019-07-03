/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master.tableOps.bulkVer2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.bulk.BulkSerialize;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.fate.FateTxId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk import makes requests of tablet servers, and those requests can take a long time. Our
 * communications to the tablet server may fail, so we won't know the status of the request. The
 * master will repeat failed requests so now there are multiple requests to the tablet server. The
 * tablet server will not execute the request multiple times, so long as the marker it wrote in the
 * metadata table stays there. The master needs to know when all requests have finished so it can
 * remove the markers. Did it start? Did it finish? We can see that *a* request completed by seeing
 * the flag written into the metadata table, but we won't know if some other rogue thread is still
 * waiting to start a thread and repeat the operation.
 *
 * The master can ask the tablet server if it has any requests still running. Except the tablet
 * server might have some thread about to start a request, but before it has made any bookkeeping
 * about the request. To prevent problems like this, an Arbitrator is used. Before starting any new
 * request, the tablet server checks the Arbitrator to see if the request is still valid.
 */
class BulkImportMove extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(BulkImportMove.class);

  private final BulkInfo bulkInfo;

  public BulkImportMove(BulkInfo bulkInfo) {
    this.bulkInfo = bulkInfo;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    final Path bulkDir = new Path(bulkInfo.bulkDir);
    final Path sourceDir = new Path(bulkInfo.sourceDir);

    String fmtTid = FateTxId.formatTid(tid);

    log.debug("{} sourceDir {}", fmtTid, sourceDir);

    VolumeManager fs = master.getFileSystem();

    if (bulkInfo.tableState == TableState.ONLINE) {
      ZooArbitrator.start(master.getContext(), Constants.BULK_ARBITRATOR_TYPE, tid);
    }

    try {
      Map<String,String> oldToNewNameMap =
          BulkSerialize.readRenameMap(bulkDir.toString(), p -> fs.open(p));
      moveFiles(tid, sourceDir, bulkDir, master, fs, oldToNewNameMap);

      return new LoadFiles(bulkInfo);
    } catch (Exception ex) {
      throw new AcceptableThriftTableOperationException(bulkInfo.tableId.canonical(), null,
          TableOperation.BULK_IMPORT, TableOperationExceptionType.BULK_BAD_INPUT_DIRECTORY,
          bulkInfo.sourceDir + ": " + ex);
    }
  }

  /**
   * For every entry in renames, move the file from the key path to the value path
   */
  private void moveFiles(long tid, Path sourceDir, Path bulkDir, Master master,
      final VolumeManager fs, Map<String,String> renames) throws Exception {
    MetadataTableUtil.addBulkLoadInProgressFlag(master.getContext(),
        "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName(), tid);

    int workerCount = master.getConfiguration().getCount(Property.MASTER_BULK_RENAME_THREADS);
    SimpleThreadPool workers = new SimpleThreadPool(workerCount, "bulkDir move");
    List<Future<Boolean>> results = new ArrayList<>();

    String fmtTid = FateTxId.formatTid(tid);

    for (Map.Entry<String,String> renameEntry : renames.entrySet()) {
      results.add(workers.submit(() -> {
        final Path originalPath = new Path(sourceDir, renameEntry.getKey());
        Path newPath = new Path(bulkDir, renameEntry.getValue());
        boolean success;
        try {
          success = fs.rename(originalPath, newPath);
        } catch (IOException e) {
          // The rename could have failed because this is the second time its running (failures
          // could cause this to run multiple times).
          if (!fs.exists(newPath) || fs.exists(originalPath)) {
            throw e;
          }

          log.debug(
              "Ingoring rename exception because destination already exists. {} orig: {} new: {}",
              fmtTid, originalPath, newPath, e);
          success = true;
        }

        if (!success && fs.exists(newPath) && !fs.exists(originalPath)) {
          log.debug(
              "Ingoring rename failure because destination already exists. {} orig: {} new: {}",
              fmtTid, originalPath, newPath);
          success = true;
        }

        if (success && log.isTraceEnabled())
          log.trace("{} moved {} to {}", fmtTid, originalPath, newPath);
        return success;
      }));
    }
    workers.shutdown();
    while (!workers.awaitTermination(1000L, TimeUnit.MILLISECONDS)) {}

    for (Future<Boolean> future : results) {
      try {
        if (!future.get()) {
          throw new AcceptableThriftTableOperationException(bulkInfo.tableId.canonical(), null,
              TableOperation.BULK_IMPORT, TableOperationExceptionType.OTHER,
              "Failed to move files from " + bulkInfo.sourceDir);
        }
      } catch (ExecutionException ee) {
        throw new AcceptableThriftTableOperationException(bulkInfo.tableId.canonical(), null,
            TableOperation.BULK_IMPORT, TableOperationExceptionType.OTHER,
            ee.getCause().getMessage());
      }
    }
  }
}

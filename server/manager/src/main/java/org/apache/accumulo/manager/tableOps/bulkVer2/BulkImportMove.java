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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.bulk.BulkSerialize;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bulk import makes requests of tablet servers, and those requests can take a long time. Our
 * communications to the tablet server may fail, so we won't know the status of the request. The
 * manager will repeat failed requests so now there are multiple requests to the tablet server. The
 * tablet server will not execute the request multiple times, so long as the marker it wrote in the
 * metadata table stays there. The manager needs to know when all requests have finished so it can
 * remove the markers. Did it start? Did it finish? We can see that *a* request completed by seeing
 * the flag written into the metadata table, but we won't know if some other rogue thread is still
 * waiting to start a thread and repeat the operation.
 *
 * The manager can ask the tablet server if it has any requests still running. Except the tablet
 * server might have some thread about to start a request, but before it has made any bookkeeping
 * about the request. To prevent problems like this, an Arbitrator is used. Before starting any new
 * request, the tablet server checks the Arbitrator to see if the request is still valid.
 */
class BulkImportMove extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(BulkImportMove.class);

  private final BulkInfo bulkInfo;

  public BulkImportMove(BulkInfo bulkInfo) {
    this.bulkInfo = bulkInfo;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    final Path bulkDir = new Path(bulkInfo.bulkDir);
    final Path sourceDir = new Path(bulkInfo.sourceDir);

    String fmtTid = FateTxId.formatTid(tid);

    log.debug("{} sourceDir {}", fmtTid, sourceDir);

    VolumeManager fs = manager.getVolumeManager();

    if (bulkInfo.tableState == TableState.ONLINE) {
      ZooArbitrator.start(manager.getContext(), Constants.BULK_ARBITRATOR_TYPE, tid);
    }

    try {
      manager.updateBulkImportStatus(sourceDir.toString(), BulkImportState.MOVING);
      Map<String,String> oldToNewNameMap =
          BulkSerialize.readRenameMap(bulkDir.toString(), fs::open);
      moveFiles(tid, sourceDir, bulkDir, manager, fs, oldToNewNameMap);

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
  private void moveFiles(long tid, Path sourceDir, Path bulkDir, Manager manager,
      final VolumeManager fs, Map<String,String> renames) throws Exception {
    MetadataTableUtil.addBulkLoadInProgressFlag(manager.getContext(),
        "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName(), tid);

    AccumuloConfiguration aConf = manager.getConfiguration();
    @SuppressWarnings("deprecation")
    int workerCount = aConf.getCount(
        aConf.resolve(Property.MANAGER_RENAME_THREADS, Property.MANAGER_BULK_RENAME_THREADS));
    Map<Path,Path> oldToNewMap = new HashMap<>();
    String fmtTid = FateTxId.formatTid(tid);

    for (Map.Entry<String,String> renameEntry : renames.entrySet()) {
      final Path originalPath = new Path(sourceDir, renameEntry.getKey());
      Path newPath = new Path(bulkDir, renameEntry.getValue());
      oldToNewMap.put(originalPath, newPath);
    }
    try {
      fs.bulkRename(oldToNewMap, workerCount, "bulkDir move", fmtTid);
    } catch (IOException ioe) {
      throw new AcceptableThriftTableOperationException(bulkInfo.tableId.canonical(), null,
          TableOperation.BULK_IMPORT, TableOperationExceptionType.OTHER,
          ioe.getCause().getMessage());
    }
  }
}

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
import java.util.Collections;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanUpBulkImport extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(CleanUpBulkImport.class);

  private BulkInfo info;

  public CleanUpBulkImport(BulkInfo info) {
    this.info = info;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    manager.updateBulkImportStatus(info.sourceDir, BulkImportState.CLEANUP);
    log.debug("{} removing the bulkDir processing flag file in {}", FateTxId.formatTid(tid),
        info.bulkDir);
    Ample ample = manager.getContext().getAmple();
    Path bulkDir = new Path(info.bulkDir);
    ample.removeBulkLoadInProgressFlag(
        "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    ample.putGcFileAndDirCandidates(info.tableId,
        Collections.singleton(ReferenceFile.forFile(info.tableId, bulkDir.toString())));
    if (info.tableState == TableState.ONLINE) {

      Text firstSplit = info.firstSplit == null ? null : new Text(info.firstSplit);
      Text lastSplit = info.lastSplit == null ? null : new Text(info.lastSplit);

      log.debug("{} removing the metadata table markers for loaded files in range {} {}",
          FateTxId.formatTid(tid), firstSplit, lastSplit);

      ample.removeBulkLoadEntries(info.tableId, tid, firstSplit, lastSplit);
    }
    Utils.unreserveHdfsDirectory(manager, info.sourceDir, tid);
    Utils.getReadLock(manager, info.tableId, tid).unlock();
    // delete json renames and mapping files
    Path renamingFile = new Path(bulkDir, Constants.BULK_RENAME_FILE);
    Path mappingFile = new Path(bulkDir, Constants.BULK_LOAD_MAPPING);
    try {
      manager.getVolumeManager().delete(renamingFile);
      manager.getVolumeManager().delete(mappingFile);
    } catch (IOException ioe) {
      log.debug("{} Failed to delete renames and/or loadmap", FateTxId.formatTid(tid), ioe);
    }

    log.debug("completing bulkDir import transaction " + FateTxId.formatTid(tid));
    if (info.tableState == TableState.ONLINE) {
      ZooArbitrator.cleanup(manager.getContext(), Constants.BULK_ARBITRATOR_TYPE, tid);
    }
    manager.removeBulkImportStatus(info.sourceDir);
    return null;
  }
}

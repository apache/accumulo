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
package org.apache.accumulo.master.tableOps.bulkVer2;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.fate.FateTxId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanUpBulkImport extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(CleanUpBulkImport.class);

  private BulkInfo info;

  public CleanUpBulkImport(BulkInfo info) {
    this.info = info;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    log.debug("removing the bulkDir processing flag file in " + info.bulkDir);
    Path bulkDir = new Path(info.bulkDir);
    MetadataTableUtil.removeBulkLoadInProgressFlag(master.getContext(),
        "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    MetadataTableUtil.addDeleteEntry(master.getContext(), info.tableId, bulkDir.toString());
    if (info.tableState == TableState.ONLINE) {
      log.debug("removing the metadata table markers for loaded files");
      AccumuloClient client = master.getContext();
      MetadataTableUtil.removeBulkLoadEntries(client, info.tableId, tid);
    }
    Utils.unreserveHdfsDirectory(master, info.sourceDir, tid);
    Utils.getReadLock(master, info.tableId, tid).unlock();
    // delete json renames and mapping files
    Path renamingFile = new Path(bulkDir, Constants.BULK_RENAME_FILE);
    Path mappingFile = new Path(bulkDir, Constants.BULK_LOAD_MAPPING);
    try {
      master.getVolumeManager().delete(renamingFile);
      master.getVolumeManager().delete(mappingFile);
    } catch (IOException ioe) {
      log.debug("Failed to delete renames and/or loadmap", ioe);
    }

    log.debug("completing bulkDir import transaction " + FateTxId.formatTid(tid));
    if (info.tableState == TableState.ONLINE) {
      ZooArbitrator.cleanup(master.getContext(), Constants.BULK_ARBITRATOR_TYPE, tid);
    }
    return null;
  }
}

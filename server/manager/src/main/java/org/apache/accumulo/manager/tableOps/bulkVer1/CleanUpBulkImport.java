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
package org.apache.accumulo.manager.tableOps.bulkVer1;

import java.util.Collections;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanUpBulkImport extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(CleanUpBulkImport.class);

  private TableId tableId;
  private String source;
  private String bulk;
  private String error;

  CleanUpBulkImport(TableId tableId, String source, String bulk, String error) {
    this.tableId = tableId;
    this.source = source;
    this.bulk = bulk;
    this.error = error;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    manager.updateBulkImportStatus(source, BulkImportState.CLEANUP);
    log.debug("removing the bulkDir processing flag file in " + bulk);
    Path bulkDir = new Path(bulk);
    MetadataTableUtil.removeBulkLoadInProgressFlag(manager.getContext(),
        "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    manager.getContext().getAmple().putGcFileAndDirCandidates(tableId,
        Collections.singleton(new ReferenceFile(tableId, bulkDir.toString())));
    log.debug("removing the metadata table markers for loaded files");
    AccumuloClient client = manager.getContext();
    MetadataTableUtil.removeBulkLoadEntries(client, tableId, tid);
    log.debug("releasing HDFS reservations for " + source + " and " + error);
    Utils.unreserveHdfsDirectory(manager, source, tid);
    Utils.unreserveHdfsDirectory(manager, error, tid);
    Utils.getReadLock(manager, tableId, tid).unlock();
    log.debug("completing bulkDir import transaction " + FateTxId.formatTid(tid));
    ZooArbitrator.cleanup(manager.getContext(), Constants.BULK_ARBITRATOR_TYPE, tid);
    manager.removeBulkImportStatus(source);
    return null;
  }
}

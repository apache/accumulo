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
package org.apache.accumulo.master.tableOps;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.master.thrift.BulkImportState;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.util.MetadataTableUtil;
import org.apache.accumulo.server.zookeeper.TransactionWatcher.ZooArbitrator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CleanUpBulkImport extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(CleanUpBulkImport.class);

  private Table.ID tableId;
  private String source;
  private String bulk;
  private String error;

  public CleanUpBulkImport(Table.ID tableId, String source, String bulk, String error) {
    this.tableId = tableId;
    this.source = source;
    this.bulk = bulk;
    this.error = error;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    master.updateBulkImportStatus(source, BulkImportState.CLEANUP);
    log.debug("removing the bulk processing flag file in " + bulk);
    Path bulkDir = new Path(bulk);
    MetadataTableUtil.removeBulkLoadInProgressFlag(master, "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    MetadataTableUtil.addDeleteEntry(master, tableId, bulkDir.toString());
    log.debug("removing the metadata table markers for loaded files");
    Connector conn = master.getConnector();
    MetadataTableUtil.removeBulkLoadEntries(conn, tableId, tid);
    log.debug("releasing HDFS reservations for " + source + " and " + error);
    Utils.unreserveHdfsDirectory(source, tid);
    Utils.unreserveHdfsDirectory(error, tid);
    Utils.getReadLock(tableId, tid).unlock();
    log.debug("completing bulk import transaction " + tid);
    ZooArbitrator.cleanup(Constants.BULK_ARBITRATOR_TYPE, tid);
    master.removeBulkImportStatus(source);
    return null;
  }
}

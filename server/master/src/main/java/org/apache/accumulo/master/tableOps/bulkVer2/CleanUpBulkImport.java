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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.Table;
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

  private Table.ID tableId;
  private String source;
  private String bulk;

  public CleanUpBulkImport(Table.ID tableId, String source, String bulk) {
    this.tableId = tableId;
    this.source = source;
    this.bulk = bulk;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    log.debug("removing the bulkDir processing flag file in " + bulk);
    Path bulkDir = new Path(bulk);
    MetadataTableUtil.removeBulkLoadInProgressFlag(master,
        "/" + bulkDir.getParent().getName() + "/" + bulkDir.getName());
    MetadataTableUtil.addDeleteEntry(master, tableId, bulkDir.toString());
    log.debug("removing the metadata table markers for loaded files");
    Connector conn = master.getConnector();
    // TODO pass row range so entire metadata table is not scanned
    MetadataTableUtil.removeBulkLoadEntries(conn, tableId, tid);
    Utils.unreserveHdfsDirectory(source, tid);
    Utils.getReadLock(tableId, tid).unlock();
    // delete json renames and mapping files
    Path renamingFile = new Path(bulkDir, Constants.BULK_RENAME_FILE);
    Path mappingFile = new Path(bulkDir, Constants.BULK_LOAD_MAPPING);
    try {
      master.getFileSystem().delete(renamingFile);
      master.getFileSystem().delete(mappingFile);
    } catch (IOException ioe) {
      log.debug("Failed to delete renames and/or loadmap", ioe);
    }

    log.debug("completing bulkDir import transaction " + tid);
    ZooArbitrator.cleanup(Constants.BULK_ARBITRATOR_TYPE, tid);
    return null;
  }
}

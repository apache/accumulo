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
package org.apache.accumulo.master.tableOps.tableImport;

import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CreateImportDir extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(CreateImportDir.class);
  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  CreateImportDir(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {

    Set<String> tableDirs = ServerConstants.getTablesDirs(master.getContext());

    create(tableDirs, master);

    return new MapImportFileNames(tableInfo);
  }

  /**
   * Generate destination directory names under the accumulo table directories imported rfiles.
   * These directories must be on the same volume as each file being imported.
   *
   * @param tableDirs
   *          the set of table directories on HDFS where files will be moved e.g:
   *          hdfs://volume1/accumulo/tables/
   * @param master
   *          the master instance performing the table import.
   * @throws IOException
   *           if any import directory does not reside on a volume configured for accumulo.
   */
  void create(Set<String> tableDirs, Master master) throws IOException {
    UniqueNameAllocator namer = master.getContext().getUniqueNameAllocator();

    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      Path exportDir = new Path(dm.exportDir);

      log.info("Looking for matching filesystem for {} from options {}", exportDir, tableDirs);
      Path base = master.getVolumeManager().matchingFileSystem(exportDir, tableDirs);
      if (base == null) {
        throw new IOException(dm.exportDir + " is not in a volume configured for Accumulo");
      }
      log.info("Chose base table directory of {}", base);
      Path directory = new Path(base, tableInfo.tableId.canonical());

      Path newBulkDir = new Path(directory, Constants.BULK_PREFIX + namer.getNextName());

      dm.importDir = newBulkDir.toString();

      log.info("Using import dir: {}", dm.importDir);
    }
  }
}

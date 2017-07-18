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

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MoveExportedFiles extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(MoveExportedFiles.class);

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  MoveExportedFiles(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    try {
      VolumeManager fs = master.getFileSystem();

      Map<String,String> fileNameMappings = PopulateMetadataTable.readMappingFile(fs, tableInfo);

      for (String oldFileName : fileNameMappings.keySet()) {
        if (!fs.exists(new Path(tableInfo.exportDir, oldFileName))) {
          throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonicalID(), tableInfo.tableName, TableOperation.IMPORT,
              TableOperationExceptionType.OTHER, "File referenced by exported table does not exists " + oldFileName);
        }
      }

      FileStatus[] files = fs.listStatus(new Path(tableInfo.exportDir));

      for (FileStatus fileStatus : files) {
        String newName = fileNameMappings.get(fileStatus.getPath().getName());

        if (newName != null)
          fs.rename(fileStatus.getPath(), new Path(tableInfo.importDir, newName));
      }

      return new FinishImportTable(tableInfo);
    } catch (IOException ioe) {
      log.warn("{}", ioe.getMessage(), ioe);
      throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonicalID(), tableInfo.tableName, TableOperation.IMPORT,
          TableOperationExceptionType.OTHER, "Error renaming files " + ioe.getMessage());
    }
  }
}

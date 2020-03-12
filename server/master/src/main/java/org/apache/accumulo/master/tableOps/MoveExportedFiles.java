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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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

import com.google.common.collect.Sets;

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

      FileStatus[] exportedFiles = fs.listStatus(new Path(tableInfo.exportDir));
      FileStatus[] importedFiles = fs.listStatus(new Path(tableInfo.importDir));

      Function<FileStatus,String> fileStatusName = fstat -> fstat.getPath().getName();

      Set<String> importing = Arrays.stream(exportedFiles).map(fileStatusName)
          .map(fileNameMappings::get).collect(Collectors.toSet());

      Set<String> imported =
          Arrays.stream(importedFiles).map(fileStatusName).collect(Collectors.toSet());

      if (log.isDebugEnabled()) {
        log.debug("Files already present in imported (target) directory: {}",
            imported.stream().collect(Collectors.joining(",")));
      }

      Set<String> missingFiles = Sets.difference(new HashSet<String>(fileNameMappings.values()),
          new HashSet<String>(Sets.union(importing, imported)));

      if (!missingFiles.isEmpty()) {
        throw new AcceptableThriftTableOperationException(tableInfo.tableId, tableInfo.tableName,
            TableOperation.IMPORT, TableOperationExceptionType.OTHER,
            "Missing source files corresponding to files "
                + missingFiles.stream().collect(Collectors.joining(",")));
      }

      for (FileStatus fileStatus : exportedFiles) {
        String newName = fileNameMappings.get(fileStatus.getPath().getName());

        if (newName != null) {
          Path newPath = new Path(tableInfo.importDir, newName);
          log.debug("Renaming file {} to {}", fileStatus.getPath(), newPath);
          fs.rename(fileStatus.getPath(), newPath);
        }
      }

      return new FinishImportTable(tableInfo);
    } catch (IOException ioe) {
      log.warn("{}", ioe.getMessage(), ioe);
      throw new AcceptableThriftTableOperationException(tableInfo.tableId, tableInfo.tableName,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Error renaming files " + ioe.getMessage());
    }
  }
}

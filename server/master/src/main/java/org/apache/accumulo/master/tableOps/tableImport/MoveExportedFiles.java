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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.SimpleThreadPool;
import org.apache.accumulo.fate.FateTxId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
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
    String fmtTid = FateTxId.formatTid(tid);

    int workerCount = master.getConfiguration().getCount(Property.MASTER_RENAME_THREADS);
    SimpleThreadPool workers = new SimpleThreadPool(workerCount, "importtable rename");
    List<Future<Boolean>> results = new ArrayList<>();

    VolumeManager fs = master.getVolumeManager();

    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      Map<String,String> fileNameMappings = new HashMap<>();
      PopulateMetadataTable.readMappingFile(fs, tableInfo, dm.importDir, fileNameMappings);

      FileStatus[] exportedFiles = fs.listStatus(new Path(dm.exportDir));
      FileStatus[] importedFiles = fs.listStatus(new Path(dm.importDir));

      Function<FileStatus,String> fileStatusName = fstat -> fstat.getPath().getName();

      Set<String> importing = Arrays.stream(exportedFiles).map(fileStatusName)
          .map(fileNameMappings::get).collect(Collectors.toSet());

      Set<String> imported =
          Arrays.stream(importedFiles).map(fileStatusName).collect(Collectors.toSet());

      if (log.isDebugEnabled()) {
        log.debug("{} files already present in imported (target) directory: {}", fmtTid,
            String.join(",", imported));
      }

      Set<String> missingFiles = Sets.difference(new HashSet<>(fileNameMappings.values()),
          new HashSet<>(Sets.union(importing, imported)));

      if (!missingFiles.isEmpty()) {
        throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(),
            tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
            "Missing source files corresponding to files " + String.join(",", missingFiles));
      }

      for (FileStatus fileStatus : exportedFiles) {
        Path originalPath = fileStatus.getPath();
        String newName = fileNameMappings.get(originalPath.getName());

        results.add(workers.submit(() -> {
          boolean success = true;

          // Need to exclude any other files which may be present in the exported directory
          if (newName != null) {
            Path newPath = new Path(dm.importDir, newName);

            // No try-catch here, as we do not expect any "benign" exceptions. Prior code already
            // accounts for files which were already moved. So anything returned by the rename
            // operation would be truly unexpected
            success = fs.rename(originalPath, newPath);

            if (!success) {
              log.error("{} rename operation returned false. orig: {} new: {}", fmtTid,
                  originalPath, newPath);
            } else if (log.isTraceEnabled()) {
              log.trace("{} moved {} to {}", fmtTid, originalPath, newPath);
            }
          } else {
            log.debug("{} not moving (unmapped) file {}", fmtTid, originalPath);
          }

          return success;
        }));
      }
    }

    workers.shutdown();
    while (!workers.awaitTermination(1000L, TimeUnit.MILLISECONDS)) {}

    for (Future<Boolean> future : results) {
      try {
        if (!future.get()) {
          throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(), null,
              TableOperation.IMPORT, TableOperationExceptionType.OTHER, "Failed to import files");
        }
      } catch (ExecutionException ee) {
        throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(), null,
            TableOperation.IMPORT, TableOperationExceptionType.OTHER, ee.getCause().getMessage());
      }
    }

    return new FinishImportTable(tableInfo);
  }
}

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
package org.apache.accumulo.manager.tableOps.tableImport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Predicate.not;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.manager.tableOps.tableExport.ExportTable;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Serialization updated for supporting multiple volumes in import table from 1L to 2L.
 */
public class ImportTable extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(ImportTable.class);

  private static final long serialVersionUID = 2L;

  private final ImportedTableInfo tableInfo;

  public ImportTable(String user, String tableName, Set<String> exportDirs, NamespaceId namespaceId,
      boolean keepMappings, boolean onlineTable) {
    tableInfo = new ImportedTableInfo();
    tableInfo.tableName = tableName;
    tableInfo.user = user;
    tableInfo.namespaceId = namespaceId;
    tableInfo.directories = parseExportDir(exportDirs);
    tableInfo.keepMappings = keepMappings;
    tableInfo.onlineTable = onlineTable;
  }

  @Override
  public long isReady(long tid, Manager environment) throws Exception {
    long result = 0;
    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      result += Utils.reserveHdfsDirectory(environment, new Path(dm.exportDir).toString(), tid);
    }
    result += Utils.reserveNamespace(environment, tableInfo.namespaceId, tid, false, true,
        TableOperation.IMPORT);
    return result;
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {
    checkVersions(env);

    // first step is to reserve a table id.. if the machine fails during this step
    // it is ok to retry... the only side effect is that a table id may not be used
    // or skipped

    // assuming only the manager process is creating tables

    Utils.getIdLock().lock();
    try {
      tableInfo.tableId = Utils.getNextId(tableInfo.tableName, env.getContext(), TableId::of);
      return new ImportSetupPermissions(tableInfo);
    } finally {
      Utils.getIdLock().unlock();
    }
  }

  @SuppressFBWarnings(value = "OS_OPEN_STREAM",
      justification = "closing intermediate readers would close the ZipInputStream")
  public void checkVersions(Manager env) throws AcceptableThriftTableOperationException {
    Set<String> exportDirs =
        tableInfo.directories.stream().map(dm -> dm.exportDir).collect(Collectors.toSet());

    log.debug("Searching for export file in {}", exportDirs);

    Integer exportVersion = null;
    Integer dataVersion = null;

    try {
      Path exportFilePath = TableOperationsImpl.findExportFile(env.getContext(), exportDirs);
      tableInfo.exportFile = exportFilePath.toString();
      log.info("Export file is {}", tableInfo.exportFile);

      ZipInputStream zis = new ZipInputStream(env.getVolumeManager().open(exportFilePath));
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().equals(Constants.EXPORT_INFO_FILE)) {
          BufferedReader in = new BufferedReader(new InputStreamReader(zis, UTF_8));
          String line = null;
          while ((line = in.readLine()) != null) {
            String[] sa = line.split(":", 2);
            if (sa[0].equals(ExportTable.EXPORT_VERSION_PROP)) {
              exportVersion = Integer.parseInt(sa[1]);
            } else if (sa[0].equals(ExportTable.DATA_VERSION_PROP)) {
              dataVersion = Integer.parseInt(sa[1]);
            }
          }
          break;
        }
      }
    } catch (IOException | AccumuloException e) {
      log.warn("{}", e.getMessage(), e);
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Failed to read export metadata " + e.getMessage());
    }

    if (exportVersion == null || exportVersion > ExportTable.VERSION) {
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Incompatible export version " + exportVersion);
    }

    if (dataVersion == null || dataVersion > AccumuloDataVersion.get()) {
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Incompatible data version " + dataVersion);
    }
  }

  @Override
  public void undo(long tid, Manager env) throws Exception {
    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      Utils.unreserveHdfsDirectory(env, new Path(dm.exportDir).toString(), tid);
    }

    Utils.unreserveNamespace(env, tableInfo.namespaceId, tid, false);
  }

  static List<ImportedTableInfo.DirectoryMapping> parseExportDir(Set<String> exportDirs) {
    if (exportDirs == null || exportDirs.isEmpty()) {
      return Collections.emptyList();
    }

    return exportDirs.stream().filter(not(String::isEmpty))
        .map(ImportedTableInfo.DirectoryMapping::new).collect(Collectors.toList());
  }
}

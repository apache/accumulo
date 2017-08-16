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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.ServerConstants;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportTable extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(ImportTable.class);

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  public ImportTable(String user, String tableName, String exportDir, Namespace.ID namespaceId) {
    tableInfo = new ImportedTableInfo();
    tableInfo.tableName = tableName;
    tableInfo.user = user;
    tableInfo.exportDir = exportDir;
    tableInfo.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return Utils.reserveHdfsDirectory(new Path(tableInfo.exportDir).toString(), tid)
        + Utils.reserveNamespace(tableInfo.namespaceId, tid, false, true, TableOperation.IMPORT);
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {
    checkVersions(env);

    // first step is to reserve a table id.. if the machine fails during this step
    // it is ok to retry... the only side effect is that a table id may not be used
    // or skipped

    // assuming only the master process is creating tables

    Utils.idLock.lock();
    try {
      Instance instance = env.getInstance();
      tableInfo.tableId = Utils.getNextId(tableInfo.tableName, instance, Table.ID::of);
      return new ImportSetupPermissions(tableInfo);
    } finally {
      Utils.idLock.unlock();
    }
  }

  public void checkVersions(Master env) throws AcceptableThriftTableOperationException {
    Path path = new Path(tableInfo.exportDir, Constants.EXPORT_FILE);
    Integer exportVersion = null;
    Integer dataVersion = null;

    try (ZipInputStream zis = new ZipInputStream(env.getFileSystem().open(path))) {
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().equals(Constants.EXPORT_INFO_FILE)) {
          BufferedReader in = new BufferedReader(new InputStreamReader(zis, UTF_8));
          String line = null;
          while ((line = in.readLine()) != null) {
            String sa[] = line.split(":", 2);
            if (sa[0].equals(ExportTable.EXPORT_VERSION_PROP)) {
              exportVersion = Integer.parseInt(sa[1]);
            } else if (sa[0].equals(ExportTable.DATA_VERSION_PROP)) {
              dataVersion = Integer.parseInt(sa[1]);
            }
          }
          break;
        }
      }
    } catch (IOException ioe) {
      log.warn("{}", ioe.getMessage(), ioe);
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Failed to read export metadata " + ioe.getMessage());
    }

    if (exportVersion == null || exportVersion > ExportTable.VERSION)
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Incompatible export version " + exportVersion);

    if (dataVersion == null || dataVersion > ServerConstants.DATA_VERSION)
      throw new AcceptableThriftTableOperationException(null, tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Incompatible data version " + exportVersion);
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    Utils.unreserveHdfsDirectory(new Path(tableInfo.exportDir).toString(), tid);
    Utils.unreserveNamespace(tableInfo.namespaceId, tid, false);
  }
}

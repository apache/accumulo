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

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.clientImpl.TableOperationsImpl;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.util.PropUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class ImportPopulateZookeeper extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  ImportPopulateZookeeper(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Manager environment) throws Exception {
    return Utils.reserveTable(environment, tableInfo.tableId, tid, true, false,
        TableOperation.IMPORT);
  }

  private Map<String,String> getExportedProps(VolumeManager fs) throws Exception {

    Path path = new Path(tableInfo.exportFile);

    try {
      FileSystem ns = fs.getFileSystemByPath(path);
      return TableOperationsImpl.getExportedProps(ns, path);
    } catch (IOException ioe) {
      throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(),
          tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Error reading table props from " + path + " " + ioe.getMessage());
    }
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {
    // reserve the table name in zookeeper or fail

    Utils.getTableNameLock().lock();
    try {
      // write tableName & tableId to zookeeper
      Utils.checkTableDoesNotExist(env.getContext(), tableInfo.tableName, tableInfo.tableId,
          TableOperation.CREATE);

      String namespace = TableNameUtil.qualify(tableInfo.tableName).getFirst();
      NamespaceId namespaceId = Namespaces.getNamespaceId(env.getContext(), namespace);
      env.getTableManager().addTable(tableInfo.tableId, namespaceId, tableInfo.tableName);

      env.getContext().clearTableListCache();
    } finally {
      Utils.getTableNameLock().unlock();
    }

    VolumeManager volMan = env.getVolumeManager();

    try {
      PropUtil.setProperties(env.getContext(), TablePropKey.of(env.getContext(), tableInfo.tableId),
          getExportedProps(volMan));
    } catch (IllegalStateException ex) {
      throw new AcceptableThriftTableOperationException(tableInfo.tableId.canonical(),
          tableInfo.tableName, TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "failed to set table properties");
    }

    return new CreateImportDir(tableInfo);
  }

  @Override
  public void undo(long tid, Manager env) throws Exception {
    env.getTableManager().removeTable(tableInfo.tableId);
    Utils.unreserveTable(env, tableInfo.tableId, tid, true);
    env.getContext().clearTableListCache();
  }
}

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

import static org.apache.accumulo.core.Constants.IMPORT_MAPPINGS_FILE;

import java.util.EnumSet;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

class FinishImportTable extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private ImportedTableInfo tableInfo;

  public FinishImportTable(ImportedTableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(FateId fateId, Manager environment) {
    return 0;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager env) throws Exception {

    if (!tableInfo.keepMappings) {
      for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
        env.getVolumeManager().deleteRecursively(new Path(dm.importDir, IMPORT_MAPPINGS_FILE));
      }
    }

    final EnumSet<TableState> expectedCurrStates = EnumSet.of(TableState.NEW);
    final TableState newState = tableInfo.keepOffline ? TableState.OFFLINE : TableState.ONLINE;
    env.getTableManager().transitionTableState(tableInfo.tableId, newState, expectedCurrStates);

    Utils.unreserveNamespace(env, tableInfo.namespaceId, fateId, LockType.READ);
    Utils.unreserveTable(env, tableInfo.tableId, fateId, LockType.WRITE);

    for (ImportedTableInfo.DirectoryMapping dm : tableInfo.directories) {
      Utils.unreserveHdfsDirectory(env, new Path(dm.exportDir).toString(), fateId);
    }

    env.getEventCoordinator().event(tableInfo.tableId, "Imported table %s ", tableInfo.tableName);

    LoggerFactory.getLogger(FinishImportTable.class)
        .debug("Imported table " + tableInfo.tableId + " " + tableInfo.tableName);

    return null;
  }

  @Override
  public String getReturn() {
    return tableInfo.tableId.canonical();
  }

  @Override
  public void undo(FateId fateId, Manager env) {}

}

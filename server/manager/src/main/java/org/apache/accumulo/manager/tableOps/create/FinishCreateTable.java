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
package org.apache.accumulo.manager.tableOps.create;

import java.io.IOException;

import org.apache.accumulo.core.client.admin.InitialTableState;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.TableInfo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FinishCreateTable extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(FinishCreateTable.class);

  private final TableInfo tableInfo;

  public FinishCreateTable(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Manager environment) {
    return 0;
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {

    if (tableInfo.getInitialTableState() == InitialTableState.OFFLINE) {
      env.getContext().getTableManager().transitionTableState(tableInfo.getTableId(),
          TableState.OFFLINE);
    } else {
      env.getContext().getTableManager().transitionTableState(tableInfo.getTableId(),
          TableState.ONLINE);
    }

    Utils.unreserveNamespace(env, tableInfo.getNamespaceId(), tid, false);
    Utils.unreserveTable(env, tableInfo.getTableId(), tid, true);

    env.getEventCoordinator().event("Created table %s ", tableInfo.getTableName());

    if (tableInfo.getInitialSplitSize() > 0) {
      cleanupSplitFiles(env);
    }
    return null;
  }

  private void cleanupSplitFiles(Manager env) throws IOException {
    // it is sufficient to delete from the parent, because both files are in the same directory, and
    // we want to delete the directory also
    Path p = null;
    try {
      p = tableInfo.getSplitPath().getParent();
      FileSystem fs = p.getFileSystem(env.getContext().getHadoopConf());
      fs.delete(p, true);
    } catch (IOException e) {
      log.error("Table was created, but failed to clean up temporary splits files at {}", p, e);
    }
  }

  @Override
  public String getReturn() {
    return tableInfo.getTableId().canonical();
  }

  @Override
  public void undo(long tid, Manager env) {}

}

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
package org.apache.accumulo.master.tableOps.create;

import java.io.IOException;

import org.apache.accumulo.core.client.admin.InitialTableState;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.TableInfo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class FinishCreateTable extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private final TableInfo tableInfo;

  public FinishCreateTable(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {

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

  private void cleanupSplitFiles(Master env) throws IOException {
    Volume defaultVolume = env.getVolumeManager().getDefaultVolume();
    FileSystem fs = defaultVolume.getFileSystem();
    fs.delete(new Path(tableInfo.getSplitFile()), true);
    fs.delete(new Path(tableInfo.getSplitDirsFile()), true);
  }

  @Override
  public String getReturn() {
    return tableInfo.getTableId().canonical();
  }

  @Override
  public void undo(long tid, Master env) {}

}

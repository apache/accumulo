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

import org.apache.accumulo.core.client.admin.InitialTableState;
import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.tables.TableManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class FinishCreateTable extends MasterRepo {

  private static final long serialVersionUID = 1L;

  private final TableInfo tableInfo;

  public FinishCreateTable(TableInfo ti) {
    this.tableInfo = ti;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {

    if (tableInfo.initialTableState == InitialTableState.OFFLINE) {
      TableManager.getInstance().transitionTableState(tableInfo.tableId, TableState.OFFLINE);
    } else {
      TableManager.getInstance().transitionTableState(tableInfo.tableId, TableState.ONLINE);
    }

    Utils.unreserveNamespace(env, tableInfo.namespaceId, tid, false);
    Utils.unreserveTable(env, tableInfo.tableId, tid, true);

    env.getEventCoordinator().event("Created table %s ", tableInfo.tableName);

    if (tableInfo.initialSplitSize > 0) {
      cleanupSplitFiles(env);
    }
    return null;
  }

  private void cleanupSplitFiles(Master env) throws IOException {
    Volume defaultVolume = env.getFileSystem().getDefaultVolume();
    FileSystem fs = defaultVolume.getFileSystem();
    fs.delete(new Path(tableInfo.splitFile), true);
    fs.delete(new Path(tableInfo.splitDirsFile), true);
  }

  @Override
  public String getReturn() {
    return tableInfo.tableId.canonicalID();
  }

  @Override
  public void undo(long tid, Master env) throws Exception {}

}

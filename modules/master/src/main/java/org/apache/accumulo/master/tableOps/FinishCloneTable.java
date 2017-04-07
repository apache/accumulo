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

import org.apache.accumulo.core.master.state.tables.TableState;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.tables.TableManager;
import org.slf4j.LoggerFactory;

class FinishCloneTable extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private CloneInfo cloneInfo;

  public FinishCloneTable(CloneInfo cloneInfo) {
    this.cloneInfo = cloneInfo;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master environment) throws Exception {
    // directories are intentionally not created.... this is done because directories should be unique
    // because they occupy a different namespace than normal tablet directories... also some clones
    // may never create files.. therefore there is no need to consume namenode space w/ directories
    // that are not used... tablet will create directories as needed

    TableManager.getInstance().transitionTableState(cloneInfo.tableId, TableState.ONLINE);

    Utils.unreserveNamespace(cloneInfo.srcNamespaceId, tid, false);
    if (!cloneInfo.srcNamespaceId.equals(cloneInfo.namespaceId))
      Utils.unreserveNamespace(cloneInfo.namespaceId, tid, false);
    Utils.unreserveTable(cloneInfo.srcTableId, tid, false);
    Utils.unreserveTable(cloneInfo.tableId, tid, true);

    environment.getEventCoordinator().event("Cloned table %s from %s", cloneInfo.tableName, cloneInfo.srcTableId);

    LoggerFactory.getLogger(FinishCloneTable.class).debug("Cloned table " + cloneInfo.srcTableId + " " + cloneInfo.tableId + " " + cloneInfo.tableName);

    return null;
  }

  @Override
  public void undo(long tid, Master environment) throws Exception {}

}

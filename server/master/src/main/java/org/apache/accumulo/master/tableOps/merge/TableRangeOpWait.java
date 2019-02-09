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
package org.apache.accumulo.master.tableOps.merge;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.master.tableOps.Utils;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merge makes things hard.
 *
 * Typically, a client will read the list of tablets, and begin an operation on that tablet at the
 * location listed in the metadata table. When a tablet splits, the information read from the
 * metadata table doesn't match reality, so the operation fails, and must be retried. But the
 * operation will take place either on the parent, or at a later time on the children. It won't take
 * place on just half of the tablet.
 *
 * However, when a merge occurs, the operation may have succeeded on one section of the merged area,
 * and not on the others, when the merge occurs. There is no way to retry the request at a later
 * time on an unmodified tablet.
 *
 * The code below uses read-write lock to prevent some operations while a merge is taking place.
 * Normal operations, like bulk imports, will grab the read lock and prevent merges (writes) while
 * they run. Merge operations will lock out some operations while they run.
 */
class TableRangeOpWait extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(TableRangeOpWait.class);

  private static final long serialVersionUID = 1L;
  private TableId tableId;
  private NamespaceId namespaceId;

  public TableRangeOpWait(NamespaceId namespaceId, TableId tableId) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long tid, Master env) {
    if (!env.getMergeInfo(tableId).getState().equals(MergeState.NONE)) {
      return 50;
    }
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    MergeInfo mergeInfo = master.getMergeInfo(tableId);
    log.info("removing merge information " + mergeInfo);
    master.clearMergeState(tableId);
    Utils.unreserveTable(master, tableId, tid, true);
    Utils.unreserveNamespace(master, namespaceId, tid, false);
    return null;
  }

}

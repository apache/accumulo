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

import org.apache.accumulo.core.client.impl.thrift.TableOperation;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merge makes things hard.
 *
 * Typically, a client will read the list of tablets, and begin an operation on that tablet at the location listed in the metadata table. When a tablet splits,
 * the information read from the metadata table doesn't match reality, so the operation fails, and must be retried. But the operation will take place either on
 * the parent, or at a later time on the children. It won't take place on just half of the tablet.
 *
 * However, when a merge occurs, the operation may have succeeded on one section of the merged area, and not on the others, when the merge occurs. There is no
 * way to retry the request at a later time on an unmodified tablet.
 *
 * The code below uses read-write lock to prevent some operations while a merge is taking place. Normal operations, like bulk imports, will grab the read lock
 * and prevent merges (writes) while they run. Merge operations will lock out some operations while they run.
 */
class TableRangeOpWait extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(TableRangeOpWait.class);

  private static final long serialVersionUID = 1L;
  private String tableId;
  private String namespaceId;

  public TableRangeOpWait(String namespaceId, String tableId) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(long tid, Master env) throws Exception {
    Text tableIdText = new Text(tableId);
    if (!env.getMergeInfo(tableIdText).getState().equals(MergeState.NONE)) {
      return 50;
    }
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    Text tableIdText = new Text(tableId);
    MergeInfo mergeInfo = master.getMergeInfo(tableIdText);
    log.info("removing merge information " + mergeInfo);
    master.clearMergeState(tableIdText);
    Utils.unreserveTable(tableId, tid, true);
    Utils.unreserveNamespace(Utils.getNamespaceId(master.getInstance(), tableId, TableOperation.MERGE, this.namespaceId), tid, false);
    return null;
  }

}

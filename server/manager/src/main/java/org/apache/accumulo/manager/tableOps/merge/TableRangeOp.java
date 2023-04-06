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
package org.apache.accumulo.manager.tableOps.merge;

import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.manager.state.MergeInfo;
import org.apache.accumulo.server.manager.state.MergeInfo.Operation;
import org.apache.accumulo.server.manager.state.MergeState;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRangeOp extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(TableRangeOp.class);

  private static final long serialVersionUID = 1L;

  private final TableId tableId;
  private final NamespaceId namespaceId;
  private byte[] startRow;
  private byte[] endRow;
  private Operation op;

  @Override
  public long isReady(long tid, Manager env) throws Exception {
    return Utils.reserveNamespace(env, namespaceId, tid, false, true, TableOperation.MERGE)
        + Utils.reserveTable(env, tableId, tid, true, true, TableOperation.MERGE);
  }

  public TableRangeOp(MergeInfo.Operation op, NamespaceId namespaceId, TableId tableId,
      Text startRow, Text endRow) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = TextUtil.getBytes(startRow);
    this.endRow = TextUtil.getBytes(endRow);
    this.op = op;
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {

    if (RootTable.ID.equals(tableId) && Operation.MERGE.equals(op)) {
      log.warn("Attempt to merge tablets for {} does nothing. It is not splittable.",
          RootTable.NAME);
    }

    Text start = startRow.length == 0 ? null : new Text(startRow);
    Text end = endRow.length == 0 ? null : new Text(endRow);

    if (start != null && end != null) {
      if (start.compareTo(end) >= 0) {
        throw new AcceptableThriftTableOperationException(tableId.canonical(), null,
            TableOperation.MERGE, TableOperationExceptionType.BAD_RANGE,
            "start row must be less than end row");
      }
    }

    env.mustBeOnline(tableId);

    MergeInfo info = env.getMergeInfo(tableId);

    if (info.getState() == MergeState.NONE) {
      KeyExtent range = new KeyExtent(tableId, end, start);
      env.setMergeState(new MergeInfo(range, op), MergeState.STARTED);
    }

    return new TableRangeOpWait(namespaceId, tableId);
  }

  @Override
  public void undo(long tid, Manager env) throws Exception {
    // Not sure this is a good thing to do. The Manager state engine should be the one to remove it.
    MergeInfo mergeInfo = env.getMergeInfo(tableId);
    if (mergeInfo.getState() != MergeState.NONE) {
      log.info("removing merge information {}", mergeInfo);
    }
    env.clearMergeState(tableId);
    Utils.unreserveNamespace(env, namespaceId, tid, false);
    Utils.unreserveTable(env, tableId, tid, true);
  }

}

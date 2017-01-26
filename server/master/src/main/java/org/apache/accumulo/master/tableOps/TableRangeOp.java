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
import org.apache.accumulo.core.client.impl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.client.impl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.master.state.MergeInfo;
import org.apache.accumulo.server.master.state.MergeInfo.Operation;
import org.apache.accumulo.server.master.state.MergeState;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRangeOp extends MasterRepo {
  private static final Logger log = LoggerFactory.getLogger(TableRangeOp.class);

  private static final long serialVersionUID = 1L;

  private final String tableId;
  private final String namespaceId;
  private byte[] startRow;
  private byte[] endRow;
  private Operation op;

  private String getNamespaceId(Master env) throws Exception {
    return Utils.getNamespaceId(env.getInstance(), tableId, TableOperation.MERGE, this.namespaceId);
  }

  @Override
  public long isReady(long tid, Master env) throws Exception {
    return Utils.reserveNamespace(getNamespaceId(env), tid, false, true, TableOperation.MERGE)
        + Utils.reserveTable(tableId, tid, true, true, TableOperation.MERGE);
  }

  public TableRangeOp(MergeInfo.Operation op, String namespaceId, String tableId, Text startRow, Text endRow) throws ThriftTableOperationException {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = TextUtil.getBytes(startRow);
    this.endRow = TextUtil.getBytes(endRow);
    this.op = op;
  }

  @Override
  public Repo<Master> call(long tid, Master env) throws Exception {

    if (RootTable.ID.equals(tableId) && Operation.MERGE.equals(op)) {
      log.warn("Attempt to merge tablets for " + RootTable.NAME + " does nothing. It is not splittable.");
    }

    Text start = startRow.length == 0 ? null : new Text(startRow);
    Text end = endRow.length == 0 ? null : new Text(endRow);
    Text tableIdText = new Text(tableId);

    if (start != null && end != null)
      if (start.compareTo(end) >= 0)
        throw new ThriftTableOperationException(tableId, null, TableOperation.MERGE, TableOperationExceptionType.BAD_RANGE,
            "start row must be less than end row");

    env.mustBeOnline(tableId);

    MergeInfo info = env.getMergeInfo(tableIdText);

    if (info.getState() == MergeState.NONE) {
      KeyExtent range = new KeyExtent(tableIdText, end, start);
      env.setMergeState(new MergeInfo(range, op), MergeState.STARTED);
    }

    return new TableRangeOpWait(getNamespaceId(env), tableId);
  }

  @Override
  public void undo(long tid, Master env) throws Exception {
    // Not sure this is a good thing to do. The Master state engine should be the one to remove it.
    Text tableIdText = new Text(tableId);
    MergeInfo mergeInfo = env.getMergeInfo(tableIdText);
    if (mergeInfo.getState() != MergeState.NONE)
      log.info("removing merge information " + mergeInfo);
    env.clearMergeState(tableIdText);
    Utils.unreserveTable(tableId, tid, true);
    Utils.unreserveNamespace(getNamespaceId(env), tid, false);
  }

}

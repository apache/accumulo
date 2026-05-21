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

import static org.apache.accumulo.manager.ManagerClientServiceHandler.mustBeOnline;

import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock.LockType;
import org.apache.accumulo.core.fate.zookeeper.LockRange;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableRangeOp extends AbstractFateOperation {
  private static final Logger log = LoggerFactory.getLogger(TableRangeOp.class);

  private static final long serialVersionUID = 1L;

  private final MergeInfo data;

  @Override
  public long isReady(FateId fateId, FateEnv env) throws Exception {
    return Utils.reserveNamespace(env.getContext(), data.namespaceId, fateId, LockType.READ, true,
        TableOperation.MERGE)
        + Utils.reserveTable(env.getContext(), data.tableId, fateId, LockType.WRITE, true,
            TableOperation.MERGE, LockRange.of(data.getReserveExtent()));
  }

  public TableRangeOp(MergeInfo.Operation op, NamespaceId namespaceId, TableId tableId,
      Text startRow, Text endRow) {
    byte[] start = startRow.getLength() == 0 ? null : TextUtil.getBytes(startRow);
    byte[] end = endRow.getLength() == 0 ? null : TextUtil.getBytes(endRow);
    this.data = new MergeInfo(tableId, namespaceId, start, end, op);
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) throws Exception {

    if (SystemTables.ROOT.tableId().equals(data.tableId) && data.op.isMergeOp()) {
      log.warn("Attempt to merge tablets for {} does nothing. It is not splittable.",
          SystemTables.ROOT.tableName());
    }

    mustBeOnline(env.getContext(), data.tableId);

    data.validate();

    return new ReserveTablets(data);
  }

  @Override
  public void undo(FateId fateId, FateEnv env) throws Exception {
    Utils.unreserveNamespace(env.getContext(), data.namespaceId, fateId, LockType.READ);
    Utils.unreserveTable(env.getContext(), data.tableId, fateId, LockType.WRITE);
  }
}

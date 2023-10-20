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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * ELASTICITY_TODO edit these docs which are pre elasticity changes. Best done after #3763
 *
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
class FinishTableRangeOp extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(FinishTableRangeOp.class);

  private static final long serialVersionUID = 1L;

  private final TableRangeData data;

  public FinishTableRangeOp(TableRangeData data) {
    this.data = data;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    MergeInfo mergeInfo1 = data.getMergeInfo();
    KeyExtent range = mergeInfo1.getExtent();
    var opid = TabletOperationId.from(TabletOperationType.MERGING, tid);

    try (var tablets = manager.getContext().getAmple().readTablets().forTable(data.tableId)
        .overlapping(range.prevEndRow(), range.endRow()).fetch(PREV_ROW, LOCATION, OPID).build();
        var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets();) {
      int opsDeleted = 0;
      int count = 0;

      for (var tabletMeta : tablets) {
        if (opid.equals(tabletMeta.getOperationId())) {
          tabletsMutator.mutateTablet(tabletMeta.getExtent()).requireOperation(opid)
              .deleteOperation().submit(tm -> !opid.equals(tm.getOperationId()));
          opsDeleted++;
        }
        count++;
      }

      Preconditions.checkState(count > 0);

      var results = tabletsMutator.process();
      var deletesAccepted =
          results.values().stream().filter(conditionalResult -> conditionalResult.getStatus()
              == Ample.ConditionalResult.Status.ACCEPTED).count();

      log.debug("{} deleted {}/{} opids out of {} tablets", FateTxId.formatTid(tid),
          deletesAccepted, opsDeleted, count);

      MergeInfo mergeInfo = data.getMergeInfo();
      manager.getEventCoordinator().event(mergeInfo.getExtent(), "Merge or deleterows completed %s",
          FateTxId.formatTid(tid));

      DeleteRows.verifyAccepted(results, FateTxId.formatTid(tid));
      Preconditions.checkState(deletesAccepted == opsDeleted);
    }

    Utils.unreserveTable(manager, data.tableId, tid, true);
    Utils.unreserveNamespace(manager, data.namespaceId, tid, false);
    return null;
  }

}

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

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
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

class FinishTableRangeOp extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(FinishTableRangeOp.class);

  private static final long serialVersionUID = 1L;

  private final MergeInfo data;

  public FinishTableRangeOp(MergeInfo data) {
    this.data = data;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    removeOperationIds(log, data, fateId, manager);

    Utils.unreserveTable(manager, data.tableId, fateId, true);
    Utils.unreserveNamespace(manager, data.namespaceId, fateId, false);
    return null;
  }

  static void removeOperationIds(Logger log, MergeInfo data, FateId fateId, Manager manager) {
    KeyExtent range = data.getReserveExtent();
    var opid = TabletOperationId.from(TabletOperationType.MERGING, fateId);
    log.debug("{} unreserving tablet in range {}", fateId, range);

    AtomicLong acceptedCount = new AtomicLong();
    AtomicLong rejectedCount = new AtomicLong();
    // delete tablets
    Consumer<Ample.ConditionalResult> resultConsumer = result -> {
      if (result.getStatus() == Ample.ConditionalResult.Status.ACCEPTED) {
        acceptedCount.incrementAndGet();
      } else {
        log.error("{} failed to update {}", fateId, result.getExtent());
        rejectedCount.incrementAndGet();
      }
    };

    int submitted = 0;
    int count = 0;

    try (var tablets = manager.getContext().getAmple().readTablets().forTable(data.tableId)
        .overlapping(range.prevEndRow(), range.endRow()).fetch(PREV_ROW, LOCATION, OPID).build();
        var tabletsMutator =
            manager.getContext().getAmple().conditionallyMutateTablets(resultConsumer)) {

      for (var tabletMeta : tablets) {
        if (opid.equals(tabletMeta.getOperationId())) {
          tabletsMutator.mutateTablet(tabletMeta.getExtent()).requireOperation(opid)
              .deleteOperation().submit(tm -> !opid.equals(tm.getOperationId()));
          submitted++;
        }
        count++;
      }

      Preconditions.checkState(count > 0);
    }

    log.debug("{} deleted {}/{} opids out of {} tablets", fateId, acceptedCount.get(), submitted,
        count);

    manager.getEventCoordinator().event(range, "Merge or deleterows completed %s", fateId);

    Preconditions.checkState(acceptedCount.get() == submitted && rejectedCount.get() == 0,
        "Failed to delete tablets accepted:%s != %s rejected:%s", acceptedCount.get(), submitted,
        rejectedCount.get());
  }

}

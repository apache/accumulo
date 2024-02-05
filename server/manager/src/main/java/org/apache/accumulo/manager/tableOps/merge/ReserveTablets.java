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
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ReserveTablets extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(ReserveTablets.class);

  private static final long serialVersionUID = 1L;

  private final MergeInfo data;

  public ReserveTablets(MergeInfo data) {
    this.data = data;
  }

  @Override
  public long isReady(FateId fateId, Manager env) throws Exception {
    var range = data.getReserveExtent();
    log.debug("{} reserving tablets in range {}", fateId, range);
    // ELASTICITY_TODO DEFERRED - ISSUE 4044
    var opid = TabletOperationId.from(TabletOperationType.MERGING, fateId.getTid());

    AtomicLong opsAccepted = new AtomicLong(0);
    Consumer<Ample.ConditionalResult> resultConsumer = result -> {
      if (result.getStatus() == Status.ACCEPTED) {
        opsAccepted.incrementAndGet();
      }
    };

    int count = 0;
    int otherOps = 0;
    int opsSet = 0;
    int locations = 0;
    int wals = 0;

    try (
        var tablets = env.getContext().getAmple().readTablets().forTable(data.tableId)
            .overlapping(range.prevEndRow(), range.endRow()).fetch(PREV_ROW, LOCATION, LOGS, OPID)
            .checkConsistency().build();
        var tabletsMutator =
            env.getContext().getAmple().conditionallyMutateTablets(resultConsumer)) {

      for (var tabletMeta : tablets) {
        if (tabletMeta.getOperationId() == null) {
          tabletsMutator.mutateTablet(tabletMeta.getExtent()).requireAbsentOperation()
              .putOperation(opid).submit(tm -> opid.equals(tm.getOperationId()));
          opsSet++;
        } else if (!tabletMeta.getOperationId().equals(opid)) {
          otherOps++;
        }

        if (tabletMeta.getLocation() != null) {
          locations++;
        }

        wals += tabletMeta.getLogs().size();

        count++;
      }
    }

    log.debug(
        "{} reserve tablets op:{} count:{} other opids:{} opids set:{} locations:{} accepted:{} wals:{}",
        fateId, data.op, count, otherOps, opsSet, locations, opsAccepted, wals);

    // while there are table lock a tablet can be concurrently deleted, so should always see
    // tablets
    Preconditions.checkState(count > 0);

    if (locations > 0 && opsAccepted.get() > 0) {
      // operation ids were set and tablets have locations, so lets send a signal to get them
      // unassigned
      env.getEventCoordinator().event(range, "Tablets %d were reserved for merge %s",
          opsAccepted.get(), fateId);
    }

    if (locations > 0 || otherOps > 0 || wals > 0) {
      // need to wait on these tablets
      return Math.min(Math.max(1000, count), 60000);
    }

    if (opsSet != opsAccepted.get()) {
      // not all operation ids were set
      return Math.min(Math.max(1000, count), 60000);
    }

    // operations ids were set on all tablets and no tablets have locations, so ready
    return 0;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager environment) throws Exception {
    return new CountFiles(data);
  }
}

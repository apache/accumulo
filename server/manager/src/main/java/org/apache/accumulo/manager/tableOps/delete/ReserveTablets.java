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
package org.apache.accumulo.manager.tableOps.delete;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReserveTablets extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(ReserveTablets.class);

  private static final long serialVersionUID = 1L;

  private final TableId tableId;
  private final NamespaceId namespaceId;

  public ReserveTablets(TableId tableId, NamespaceId namespaceId) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
  }

  @Override
  public long isReady(FateId fateId, Manager manager) throws Exception {

    // ELASTICITY_TODO DEFERRED - ISSUE 4044
    var opid = TabletOperationId.from(TabletOperationType.DELETING, fateId.getTid());

    // The consumer may be called in another thread so use an AtomicLong
    AtomicLong accepted = new AtomicLong(0);
    Consumer<Ample.ConditionalResult> resultsConsumer = result -> {
      if (result.getStatus() == Ample.ConditionalResult.Status.ACCEPTED) {
        accepted.incrementAndGet();
      } else {
        log.debug("{} Failed to set operation id {} {}", fateId, opid, result.getExtent());
      }
    };

    long locations = 0;
    long otherOps = 0;
    long submitted = 0;
    long tabletsSeen = 0;

    try (
        var tablets = manager.getContext().getAmple().readTablets().forTable(tableId)
            .fetch(OPID, PREV_ROW, LOCATION).checkConsistency().build();
        var conditionalMutator =
            manager.getContext().getAmple().conditionallyMutateTablets(resultsConsumer)) {

      for (var tabletMeta : tablets) {
        tabletsSeen++;
        if (tabletMeta.getLocation() != null) {
          locations++;
        }

        if (tabletMeta.getOperationId() != null) {
          if (!opid.equals(tabletMeta.getOperationId())) {
            otherOps++;
          }
        } else {
          conditionalMutator.mutateTablet(tabletMeta.getExtent()).requireAbsentOperation()
              .putOperation(opid).submit(tm -> opid.equals(tm.getOperationId()));
          submitted++;
        }
      }
    }

    if (locations > 0 || otherOps > 0 || submitted != accepted.get()) {
      log.debug("{} Waiting to delete table locations:{} operations:{}  submitted:{} accepted:{}",
          fateId, locations, otherOps, submitted, accepted.get());
      return Math.min(Math.max(100, tabletsSeen), 30000);
    }

    return 0;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    return new CleanUp(tableId, namespaceId);
  }
}

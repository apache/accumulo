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

    var opid = TabletOperationId.from(TabletOperationType.DELETING, fateId);

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
          log.debug("Delete table is waiting on tablet unload {} {} {}", tabletMeta.getExtent(),
              tabletMeta.getLocation(), fateId);
        }

        if (tabletMeta.getOperationId() != null) {
          if (!opid.equals(tabletMeta.getOperationId())) {
            otherOps++;
            log.debug("Delete table is waiting on tablet with operation {} {} {}",
                tabletMeta.getExtent(), tabletMeta.getOperationId(), fateId);
          }
        } else {
          // Its ok to set the operation id on a tablet with a location, but after setting it we
          // must wait for the tablet to have no location before proceeding to actually delete. See
          // the documentation about the opid column in the MetadataSchema class for more details.
          conditionalMutator.mutateTablet(tabletMeta.getExtent()).requireAbsentOperation()
              .putOperation(opid).submit(tm -> opid.equals(tm.getOperationId()));
          submitted++;
        }
      }
    }

    if (locations > 0 || otherOps > 0 || submitted != accepted.get()) {
      log.info("{} Waiting to delete table locations:{} operations:{}  submitted:{} accepted:{}",
          fateId, locations, otherOps, submitted, accepted.get());
      return Math.min(Math.max(100, tabletsSeen), 30000);
    }

    // Once all tablets have the delete opid column set AND no tablets have a location set then its
    // safe to proceed with deleting the tablets. These two conditions being true should prevent any
    // concurrent writes to tablet metadata by other threads assuming they are using conditional
    // writes with standard conditional checks.
    return 0;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    return new CleanUp(tableId, namespaceId);
  }
}

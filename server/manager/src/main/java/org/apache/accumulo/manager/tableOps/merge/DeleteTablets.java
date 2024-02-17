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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Delete tablets that were merged into another tablet.
 */
public class DeleteTablets extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private final MergeInfo data;

  private final byte[] lastTabletEndRow;

  private static final Logger log = LoggerFactory.getLogger(DeleteTablets.class);

  DeleteTablets(MergeInfo mergeInfo, Text lastTabletEndRow) {
    this.data = mergeInfo;
    this.lastTabletEndRow = lastTabletEndRow == null ? null : TextUtil.getBytes(lastTabletEndRow);
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {

    KeyExtent range = data.getMergeExtent();
    log.debug("{} Deleting tablets for {}", fateId, range);
    var opid = TabletOperationId.from(TabletOperationType.MERGING, fateId);

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

    long submitted = 0;

    try (
        var tabletsMetadata =
            manager.getContext().getAmple().readTablets().forTable(range.tableId())
                .overlapping(range.prevEndRow(), range.endRow()).saveKeyValues().build();
        var tabletsMutator =
            manager.getContext().getAmple().conditionallyMutateTablets(resultConsumer)) {

      var lastEndRow = lastTabletEndRow == null ? null : new Text(lastTabletEndRow);

      for (var tabletMeta : tabletsMetadata) {
        MergeTablets.validateTablet(tabletMeta, fateId, opid, data.tableId);

        var tabletMutator = tabletsMutator.mutateTablet(tabletMeta.getExtent())
            .requireOperation(opid).requireAbsentLocation();

        // do not delete the last tablet
        if (Objects.equals(tabletMeta.getExtent().endRow(), lastEndRow)) {
          // Clear the merged marker after we are finished on the last tablet
          tabletMutator.deleteMerged();
          tabletMutator.submit((tm) -> !tm.hasMerged());
          submitted++;
          break;
        }

        tabletMeta.getKeyValues().keySet().forEach(key -> {
          log.trace("{} deleting {}", fateId, key);
        });

        tabletMutator.deleteAll(tabletMeta.getKeyValues().keySet());
        // if the tablet no longer exists, then it was successful
        tabletMutator.submit(Ample.RejectionHandler.acceptAbsentTablet());
        submitted++;
      }
    }

    Preconditions.checkState(acceptedCount.get() == submitted && rejectedCount.get() == 0,
        "Failed to delete tablets accepted:%s != %s rejected:%s", acceptedCount.get(), submitted,
        rejectedCount.get());

    log.debug("{} deleted {} tablets", fateId, submitted);

    return new FinishTableRangeOp(data);
  }
}

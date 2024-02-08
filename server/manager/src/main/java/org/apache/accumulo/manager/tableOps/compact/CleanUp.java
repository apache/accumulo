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
package org.apache.accumulo.manager.tableOps.compact;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.COMPACTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.accumulo.server.compaction.CompactionConfigStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanUp extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(CleanUp.class);

  private static final long serialVersionUID = 1L;

  private final TableId tableId;
  private final NamespaceId namespaceId;
  private final byte[] startRow;
  private final byte[] endRow;

  public CleanUp(TableId tableId, NamespaceId namespaceId, byte[] startRow, byte[] endRow) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.startRow = startRow;
    this.endRow = endRow;
  }

  @Override
  public long isReady(FateId fateId, Manager manager) throws Exception {

    var ample = manager.getContext().getAmple();

    AtomicLong rejectedCount = new AtomicLong(0);
    Consumer<Ample.ConditionalResult> resultConsumer = result -> {
      if (result.getStatus() == Status.REJECTED) {
        log.debug("{} update for {} was rejected ", fateId, result.getExtent());
        rejectedCount.incrementAndGet();
      }
    };

    long t1, t2, submitted = 0, total = 0;

    try (
        var tablets = ample.readTablets().forTable(tableId).overlapping(startRow, endRow)
            .fetch(PREV_ROW, COMPACTED).checkConsistency().build();
        var tabletsMutator = ample.conditionallyMutateTablets(resultConsumer)) {

      t1 = System.nanoTime();
      for (TabletMetadata tablet : tablets) {
        total++;
        // ELASTICITY_TODO DEFERRED - ISSUE 4044
        if (tablet.getCompacted().contains(fateId.getTid())) {
          tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
              .requireSame(tablet, COMPACTED).deleteCompacted(fateId.getTid())
              .submit(tabletMetadata -> !tabletMetadata.getCompacted().contains(fateId.getTid()));
          submitted++;
        }
      }

      t2 = System.nanoTime();
    }

    long scanTime = Duration.ofNanos(t2 - t1).toMillis();

    log.debug("{} removed {} of {} compacted markers for {} tablets in {}ms", fateId,
        submitted - rejectedCount.get(), submitted, total, scanTime);

    if (rejectedCount.get() > 0) {
      long sleepTime = scanTime;
      sleepTime = Math.max(100, Math.min(30000, sleepTime * 2));
      return sleepTime;
    }

    return 0;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    // ELASTICITY_TODO DEFERRED - ISSUE 4044
    CompactionConfigStorage.deleteConfig(manager.getContext(), fateId.getTid());
    Utils.getReadLock(manager, tableId, fateId).unlock();
    Utils.getReadLock(manager, namespaceId, fateId).unlock();
    return null;
  }
}

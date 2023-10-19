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

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
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
  public long isReady(long tid, Manager manager) throws Exception {

    var ample = manager.getContext().getAmple();

    try (
        var tablets = ample.readTablets().forTable(tableId).overlapping(startRow, endRow)
            .fetch(PREV_ROW, COMPACTED).checkConsistency().build();
        var tabletsMutator = ample.conditionallyMutateTablets()) {

      long t1 = System.nanoTime();
      for (TabletMetadata tablet : tablets) {
        if (tablet.getCompacted().contains(tid)) {
          tabletsMutator.mutateTablet(tablet.getExtent()).requireAbsentOperation()
              .requireSame(tablet, COMPACTED).deleteCompacted(tid)
              .submit(tabletMetadata -> !tabletMetadata.getCompacted().contains(tid));
        }
      }

      long rejected = tabletsMutator.process().values().stream()
          .filter(result -> result.getStatus() == Status.REJECTED).peek(result -> log
              .debug("{} update for {} was rejected ", FateTxId.formatTid(tid), result.getExtent()))
          .count();

      long t2 = System.nanoTime();

      if (rejected > 0) {
        long sleepTime = Duration.ofNanos(t2 - t1).toMillis();
        sleepTime = Math.max(100, Math.min(30000, sleepTime * 2));
        return sleepTime;
      }
    }

    return 0;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    CompactionConfigStorage.deleteConfig(manager.getContext(), tid);
    Utils.getReadLock(manager, tableId, tid).unlock();
    Utils.getReadLock(manager, namespaceId, tid).unlock();
    return null;
  }
}

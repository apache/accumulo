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
package org.apache.accumulo.manager.tableOps.availability;

import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.RowRange;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.fate.zookeeper.DistributedReadWriteLock;
import org.apache.accumulo.core.fate.zookeeper.LockRange;
import org.apache.accumulo.core.util.RowRangeUtil;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockTable extends AbstractFateOperation {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(LockTable.class);

  private final TableId tableId;
  private final NamespaceId namespaceId;
  private final TRange tRange;
  private final TabletAvailability tabletAvailability;

  public LockTable(TableId tableId, NamespaceId namespaceId, TRange range,
      TabletAvailability tabletAvailability) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.tRange = range;
    this.tabletAvailability = tabletAvailability;
  }

  @Override
  public long isReady(FateId fateId, FateEnv env) throws Exception {
    LockRange lockRange = getLockRange(env);
    return Utils.reserveNamespace(env.getContext(), namespaceId, fateId,
        DistributedReadWriteLock.LockType.READ, true, TableOperation.SET_TABLET_AVAILABILITY)
        + Utils.reserveTable(env.getContext(), tableId, fateId,
            DistributedReadWriteLock.LockType.WRITE, true, TableOperation.SET_TABLET_AVAILABILITY,
            lockRange);
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) throws Exception {
    return new SetTabletAvailability(tableId, namespaceId, tRange, tabletAvailability);
  }

  @Override
  public void undo(FateId fateId, FateEnv env) throws Exception {
    Utils.unreserveNamespace(env.getContext(), namespaceId, fateId,
        DistributedReadWriteLock.LockType.READ);
    Utils.unreserveTable(env.getContext(), tableId, fateId,
        DistributedReadWriteLock.LockType.WRITE);
  }

  /**
   * Get the LockRange for {@code this} object using its tRange field. Converts the key-range to a
   * row-range which is needed for the LockRange. We do a safe conversion meaning potentially
   * locking slightly more than is needed so we have at least what we need. If the key-range can't
   * be converted to a RowRange, an infinite LockRange is returned.
   */
  private LockRange getLockRange(FateEnv env) {
    Range range = new Range(tRange);

    try {
      RowRange rowRange = RowRangeUtil.toRowRange(range);
      Text startRow = rowRange.getLowerBound();
      Text endRow = rowRange.getUpperBound();

      Text lockStartRow = null;
      if (startRow != null) {
        if (rowRange.isLowerBoundInclusive()) {
          lockStartRow =
              Utils.findContaining(env.getContext().getAmple(), tableId, startRow).prevEndRow();
        } else {
          lockStartRow = startRow;
        }
      }

      return LockRange.of(lockStartRow, endRow);
    } catch (IllegalArgumentException | java.util.NoSuchElementException e) {
      LOG.debug("Unable to convert {} to a RowRange, defaulting to infinite lock range", range, e);
      return LockRange.infinite();
    }
  }
}

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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TRange;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample.TabletsMutator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.Utils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetTabletAvailability extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(SetTabletAvailability.class);

  private final TableId tableId;
  private final NamespaceId namespaceId;
  private final TRange tRange;
  private final TabletAvailability tabletAvailability;

  public SetTabletAvailability(TableId tableId, NamespaceId namespaceId, TRange range,
      TabletAvailability tabletAvailability) {
    this.tableId = tableId;
    this.namespaceId = namespaceId;
    this.tRange = range;
    this.tabletAvailability = tabletAvailability;
  }

  @Override
  public long isReady(FateId fateId, Manager manager) throws Exception {
    return Utils.reserveNamespace(manager, namespaceId, fateId, false, true,
        TableOperation.SET_TABLET_AVAILABILITY)
        + Utils.reserveTable(manager, tableId, fateId, true, true,
            TableOperation.SET_TABLET_AVAILABILITY);
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {

    final Range range = new Range(tRange);
    LOG.debug("Finding tablets in Range: {} for table:{}", range, tableId);

    // For all practical purposes the start row is always inclusive, even if the key in the
    // range is exclusive. For example the exclusive key row="a",family="b",qualifier="c" may
    // exclude the column b:c, but it's still falls somewhere in the row "a". The only case where
    // this
    // would not be true is if the start key in a range is the last possible key in a row. The last
    // possible key in a row would contain 2GB column fields of all 0xff, which is why we assume the
    // row is always inclusive.
    final Text scanRangeStart = (range.getStartKey() == null) ? null : range.getStartKey().getRow();

    try (
        TabletsMetadata m = manager.getContext().getAmple().readTablets().forTable(tableId)
            .overlapping(scanRangeStart, true, null).build();
        TabletsMutator mutator = manager.getContext().getAmple().mutateTablets()) {
      for (TabletMetadata tm : m) {
        final KeyExtent tabletExtent = tm.getExtent();
        LOG.trace("Evaluating tablet {} against range {}", tabletExtent, range);
        if (scanRangeStart != null && tm.getEndRow() != null
            && tm.getEndRow().compareTo(scanRangeStart) < 0) {
          // the end row of this tablet is before the start row, skip it
          LOG.trace("tablet {} is before scan start range: {}", tabletExtent, scanRangeStart);
          throw new RuntimeException("Bug in ample or this code.");
        }

        // Obtaining the end row from a range and knowing if the obtained row is inclusive or
        // exclusive is really tricky depending on how the Range was created (using row or key
        // constructors). So avoid trying to obtain an end row from the range and instead use
        // range.afterKey below.
        if (tm.getPrevEndRow() != null
            && range.afterEndKey(new Key(tm.getPrevEndRow()).followingKey(PartialKey.ROW))) {
          // the start row of this tablet is after the scan range, skip it
          LOG.trace("tablet {} is after scan end range: {}", tabletExtent, range);
          break;
        }

        if (tm.getTabletAvailability() == tabletAvailability) {
          LOG.trace("Skipping tablet: {}, tablet availability is already in required state",
              tabletExtent);
          continue;
        }

        LOG.debug("Setting tablet availability to {} requested for: {} ", tabletAvailability,
            tabletExtent);
        mutator.mutateTablet(tabletExtent).putTabletAvailability(tabletAvailability).mutate();
      }
    }
    Utils.unreserveNamespace(manager, namespaceId, fateId, false);
    Utils.unreserveTable(manager, tableId, fateId, true);
    return null;
  }

  @Override
  public void undo(FateId fateId, Manager manager) throws Exception {
    Utils.unreserveNamespace(manager, namespaceId, fateId, false);
    Utils.unreserveTable(manager, tableId, fateId, true);
  }

}

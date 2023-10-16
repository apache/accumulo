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
package org.apache.accumulo.manager.tableOps;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.split.PreSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DestructiveTabletManagerRepo extends ManagerRepo {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(PreSplit.class);

  private final TabletOperationType opType;
  private final KeyExtent extent;

  public DestructiveTabletManagerRepo(TabletOperationType opType, KeyExtent extent) {
    Objects.requireNonNull(opType);
    Objects.requireNonNull(extent);
    this.opType = opType;
    this.extent = extent;
  }

  private TabletMetadata readTabletMetadata(Manager manager) {
    return manager.getContext().getAmple().readTablet(extent, PREV_ROW, LOCATION, OPID);
  }

  /**
   * Destructive tablet operations (e.g. MERGE, SPLIT, DELETE) require that the corresponding tablet
   * is unhosted. This is done by placing the TabltOperationId into the opid column in the tablet
   * metadata. This method returns true when the opid in the tablet metadata is correct and the
   * location is empty (which means the tablet is unhosted. This method should be called from the
   * {@link #isReady(long, Manager)} method of the initial step of destructive tablet FATE
   * operations
   *
   * @param tid transaction id
   * @param manager manager
   * @return true if can start the operation, false otherwise
   */
  public boolean canStart(long tid, Manager manager) {

    final TabletMetadata tm = readTabletMetadata(manager);
    LOG.trace("Attempting tablet {} {} {} {}", opType, FateTxId.formatTid(tid), extent,
        tm == null ? null : tm.getLocation());
    final TabletOperationId opid = TabletOperationId.from(opType, tid);

    if (tm == null || (tm.getOperationId() != null && !opid.equals(tm.getOperationId()))) {
      // tablet no longer exists or is reserved by another operation
      return true;
    } else if (opid.equals(tm.getOperationId())) {
      if (tm.getLocation() == null) {
        // the operation id is set and there is no location, so can proceed
        return true;
      } else {
        // the operation id was set, but a location is also set wait for it be unset
        return false;
      }
    } else {
      try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

        tabletsMutator.mutateTablet(extent).requireAbsentOperation()
            .requireSame(tm, LOCATION, PREV_ROW).putOperation(opid)
            .submit(tmeta -> opid.equals(tmeta.getOperationId()));

        Map<KeyExtent,Ample.ConditionalResult> results = tabletsMutator.process();
        if (results.get(extent).getStatus() == Status.ACCEPTED) {
          LOG.trace("Successfully set operation id for {} {}", opType, FateTxId.formatTid(tid));
          if (tm.getLocation() == null) {
            // the operation id was set and there is no location, so can move on
            return true;
          } else {
            // now that the operation id set, generate an event to unload the tablet
            manager.getEventCoordinator().event(extent, "Set operation id %s on tablet for {}",
                FateTxId.formatTid(tid), opType);
            // the operation id was set, but a location is also set wait for it be unset
            return false;
          }
        } else {
          LOG.trace("Failed to set operation id for {} {}", opType, FateTxId.formatTid(tid));
          // something changed with the tablet, so setting the operation id failed. Try again later
          return false;
        }
      }
    }
  }

  /**
   * This method ensures that the opid in the tablet metadata matches this operation id and type,
   * and ensures that the location is empty. This method should be called from
   * {@link #isReady(long, Manager)} and {@link #call(long, Manager)} methods, except for the
   * {@link #isReady(long, Manager)} method of the initial FATE step.
   *
   * @param tid transaction id
   * @param manager manager
   * @return true if can continue, false otherwise
   */
  public boolean canContinue(long tid, Manager manager) {
    final TabletMetadata tm = readTabletMetadata(manager);
    final TabletOperationId opid = TabletOperationId.from(opType, tid);

    if (tm == null || !opid.equals(tm.getOperationId())) {
      // the tablet no longer exists or we could not set the operation id, maybe another operation
      // was running, lets not proceed with the split.
      var optMeta = Optional.ofNullable(tm);
      LOG.trace("{} Not proceeding with {}. extent:{} location:{} opid:{}", opType,
          FateTxId.formatTid(tid), extent, optMeta.map(TabletMetadata::getLocation).orElse(null),
          optMeta.map(TabletMetadata::getOperationId).orElse(null));
      return false;
    }

    // Its expected that the tablet has no location at this point and if it does its an indication
    // of a bug.
    if (tm.getLocation() == null) {
      LOG.trace("Tablet unexpectedly had location set %s %s %s", FateTxId.formatTid(tid),
          tm.getLocation(), tm.getExtent());
      return false;
    }

    return true;

  }

  /**
   * Removes the operationIds from the tablet metadata. This should be called from the last step in
   * the destructive tablet FATE operation
   *
   * @param tid transaction id
   * @param manager manager
   * @param extents extents from which operationId should be removed
   * @throws IllegalStateException on error
   */
  public void removeOperationIds(long tid, Manager manager, Set<KeyExtent> extents) {

    final TabletOperationId opid = TabletOperationId.from(opType, tid);

    try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

      // As long as the operation is not our operation id, then this step can be considered
      // successful in the case of rejection. If this repo is running for a second time and has
      // already deleted the operation id, then it could be absent or set by another fate operation.
      Ample.RejectionHandler rejectionHandler =
          tabletMetadata -> !opid.equals(tabletMetadata.getOperationId());

      extents.forEach(extent -> {
        tabletsMutator.mutateTablet(extent).requireOperation(opid).requireAbsentLocation()
            .deleteOperation().submit(rejectionHandler);
      });

      var results = tabletsMutator.process();

      boolean allAccepted =
          results.values().stream().allMatch(result -> result.getStatus() == Status.ACCEPTED);

      if (!allAccepted) {
        throw new IllegalStateException("Failed to delete operation ids " + extent + " " + results
            .values().stream().map(Ample.ConditionalResult::getStatus).collect(Collectors.toSet()));
      }
    }
  }

}

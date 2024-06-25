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
package org.apache.accumulo.manager.tableOps.split;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOCATION;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOGS;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class PreSplit extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(PreSplit.class);

  private final SplitInfo splitInfo;

  public PreSplit(KeyExtent expectedExtent, SortedSet<Text> splits) {
    Objects.requireNonNull(expectedExtent);
    Objects.requireNonNull(splits);
    Preconditions.checkArgument(!splits.isEmpty());
    Preconditions.checkArgument(!expectedExtent.isRootTablet());
    this.splitInfo = new SplitInfo(expectedExtent, splits);
  }

  @Override
  public long isReady(FateId fateId, Manager manager) throws Exception {
    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId);

    var tabletMetadata = manager.getContext().getAmple().readTablet(splitInfo.getOriginal(),
        PREV_ROW, LOCATION, OPID, LOGS);

    log.trace("Attempting tablet split {} {} {}", fateId, splitInfo.getOriginal(),
        tabletMetadata == null ? null : tabletMetadata.getLocation());

    if (tabletMetadata == null || (tabletMetadata.getOperationId() != null
        && !opid.equals(tabletMetadata.getOperationId()))) {
      // tablet no longer exists or is reserved by another operation
      return 0;
    } else if (opid.equals(tabletMetadata.getOperationId())) {
      if (tabletMetadata.getLocation() == null && tabletMetadata.getLogs().isEmpty()) {
        // the operation id is set and there is no location or wals, so can proceed to split
        return 0;
      } else {
        // the operation id was set, but a location or wals are also set, so wait for them to be
        // unset
        return 1000;
      }
    } else {
      try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

        tabletsMutator.mutateTablet(splitInfo.getOriginal()).requireAbsentOperation()
            .requireSame(tabletMetadata, LOCATION, LOGS).putOperation(opid)
            .submit(tmeta -> opid.equals(tmeta.getOperationId()));

        Map<KeyExtent,Ample.ConditionalResult> results = tabletsMutator.process();
        if (results.get(splitInfo.getOriginal()).getStatus() == Status.ACCEPTED) {
          log.trace("Successfully set operation id for split {}", fateId);
          if (tabletMetadata.getLocation() == null && tabletMetadata.getLogs().isEmpty()) {
            // the operation id was set and there is no location or wals, so can move on
            return 0;
          } else {
            // now that the operation id set, generate an event to unload the tablet or recover the
            // logs
            manager.getEventCoordinator().event(splitInfo.getOriginal(),
                "Set operation id %s on tablet for split", fateId);
            // the operation id was set, but a location is also set wait for it be unset
            return 1000;
          }
        } else {
          log.trace("Failed to set operation id for split {}", fateId);
          // something changed with the tablet, so setting the operation id failed. Try again later
          return 1000;
        }
      }
    }
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {

    TabletMetadata tabletMetadata = manager.getContext().getAmple()
        .readTablet(splitInfo.getOriginal(), PREV_ROW, LOCATION, OPID, LOGS);

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId);

    if (tabletMetadata == null || !opid.equals(tabletMetadata.getOperationId())) {
      // the tablet no longer exists or we could not set the operation id, maybe another operation
      // was running, lets not proceed with the split.
      var optMeta = Optional.ofNullable(tabletMetadata);
      log.trace("{} Not proceeding with split. extent:{} location:{} opid:{}", fateId,
          splitInfo.getOriginal(), optMeta.map(TabletMetadata::getLocation).orElse(null),
          optMeta.map(TabletMetadata::getOperationId).orElse(null));
      return null;
    }

    // Its expected that the tablet has no location at this point and if it does its an indication
    // of a bug.
    Preconditions.checkState(tabletMetadata.getLocation() == null,
        "Tablet unexpectedly had location set %s %s %s", fateId, tabletMetadata.getLocation(),
        tabletMetadata.getExtent());

    Preconditions.checkState(tabletMetadata.getLogs().isEmpty(),
        "Tablet unexpectedly had walogs %s %s %s", fateId, tabletMetadata.getLogs(),
        tabletMetadata.getExtent());

    return new AllocateDirsAndEnsureOnline(splitInfo);
  }

  @Override
  public void undo(FateId fateId, Manager manager) throws Exception {}
}

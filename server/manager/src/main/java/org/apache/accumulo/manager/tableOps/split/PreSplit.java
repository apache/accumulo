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
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.Ample.ConditionalResult.Status;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
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
  public long isReady(long tid, Manager manager) throws Exception {

    // ELASTICITY_TODO intentionally not getting the table lock because not sure if its needed,
    // revist later when more operations are moved out of tablet server

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);

    // ELASTICITY_TODO write IT that spins up 100 threads that all try to add a diff split to
    // the same tablet.

    // ELASTICITY_TODO does FATE prioritize running Fate txs that have already started? If not would
    // be good to look into this so we can finish things that are started before running new txs
    // that have not completed their first step. Once splits starts running, would like it to move
    // through as quickly as possible.

    var tabletMetadata = manager.getContext().getAmple().readTablet(splitInfo.getOriginal(),
        PREV_ROW, LOCATION, OPID);

    log.trace("Attempting tablet split {} {} {}", FateTxId.formatTid(tid), splitInfo.getOriginal(),
        tabletMetadata == null ? null : tabletMetadata.getLocation());

    if (tabletMetadata == null || (tabletMetadata.getOperationId() != null
        && !opid.equals(tabletMetadata.getOperationId()))) {
      // tablet no longer exists or is reserved by another operation
      return 0;
    } else if (opid.equals(tabletMetadata.getOperationId())) {
      if (tabletMetadata.getLocation() == null) {
        // the operation id is set and there is no location, so can proceed to split
        return 0;
      } else {
        // the operation id was set, but a location is also set wait for it be unset
        return 1000;
      }
    } else {
      try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

        tabletsMutator.mutateTablet(splitInfo.getOriginal()).requireAbsentOperation()
            .requireSame(tabletMetadata, LOCATION, PREV_ROW).putOperation(opid)
            .submit(tmeta -> opid.equals(tmeta.getOperationId()));

        Map<KeyExtent,Ample.ConditionalResult> results = tabletsMutator.process();
        if (results.get(splitInfo.getOriginal()).getStatus() == Status.ACCEPTED) {
          log.trace("Successfully set operation id for split {}", FateTxId.formatTid(tid));
          if (tabletMetadata.getLocation() == null) {
            // the operation id was set and there is no location, so can move on
            return 0;
          } else {
            // now that the operation id set, generate an event to unload the tablet
            manager.getEventCoordinator().event(splitInfo.getOriginal(),
                "Set operation id %s on tablet for split", FateTxId.formatTid(tid));
            // the operation id was set, but a location is also set wait for it be unset
            return 1000;
          }
        } else {
          log.trace("Failed to set operation id for split {}", FateTxId.formatTid(tid));
          // something changed with the tablet, so setting the operation id failed. Try again later
          return 1000;
        }
      }
    }
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {

    manager.getSplitter().removeSplitStarting(splitInfo.getOriginal());

    TabletMetadata tabletMetadata = manager.getContext().getAmple()
        .readTablet(splitInfo.getOriginal(), PREV_ROW, LOCATION, OPID);

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);

    if (tabletMetadata == null || !opid.equals(tabletMetadata.getOperationId())) {
      // the tablet no longer exists or we could not set the operation id, maybe another operation
      // was running, lets not proceed with the split.
      var optMeta = Optional.ofNullable(tabletMetadata);
      log.trace("{} Not proceeding with split. extent:{} location:{} opid:{}",
          FateTxId.formatTid(tid), splitInfo.getOriginal(),
          optMeta.map(TabletMetadata::getLocation).orElse(null),
          optMeta.map(TabletMetadata::getOperationId).orElse(null));
      return null;
    }

    // Its expected that the tablet has no location at this point and if it does its an indication
    // of a bug.
    Preconditions.checkState(tabletMetadata.getLocation() == null,
        "Tablet unexpectedly had location set %s %s %s", FateTxId.formatTid(tid),
        tabletMetadata.getLocation(), tabletMetadata.getExtent());

    // Create the dir name here for the next step. If the next step fails it will always have the
    // same dir name each time it runs again making it idempotent.

    List<String> dirs = new ArrayList<>();

    splitInfo.getSplits().forEach(split -> {
      String dirName = TabletNameGenerator.createTabletDirectoryName(manager.getContext(), split);
      dirs.add(dirName);
      log.trace("{} allocated dir name {}", FateTxId.formatTid(tid), dirName);
    });

    return new UpdateTablets(splitInfo, dirs);
  }

  @Override
  public void undo(long tid, Manager manager) throws Exception {
    // TODO is this called if isReady fails?
    manager.getSplitter().removeSplitStarting(splitInfo.getOriginal());
  }
}

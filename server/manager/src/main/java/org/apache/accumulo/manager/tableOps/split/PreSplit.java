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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.tablets.UniqueNameAllocator;
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

    try (var tabletsMutator = manager.getContext().getAmple().conditionallyMutateTablets()) {

      tabletsMutator.mutateTablet(splitInfo.getOriginal()).requireAbsentOperation()
          .requireAbsentLocation().requirePrevEndRow(splitInfo.getOriginal().prevEndRow())
          .putOperation(opid)
          .submit(tmeta -> tmeta.getOperationId() != null && tmeta.getOperationId().equals(opid));

      Map<KeyExtent,Ample.ConditionalResult> results = tabletsMutator.process();

      if (results.get(splitInfo.getOriginal()).getStatus() == ConditionalWriter.Status.ACCEPTED) {
        log.trace("{} reserved {} for split", FateTxId.formatTid(tid), splitInfo.getOriginal());
        return 0;
      } else {
        var tabletMetadata = results.get(splitInfo.getOriginal()).readMetadata();

        // its possible the tablet no longer exists
        var optMeta = Optional.ofNullable(tabletMetadata);

        log.trace("{} Failed to set operation id. extent:{} location:{} opid:{}",
            FateTxId.formatTid(tid), splitInfo.getOriginal(),
            optMeta.map(TabletMetadata::getLocation).orElse(null),
            optMeta.map(TabletMetadata::getOperationId).orElse(null));

        if (tabletMetadata != null && tabletMetadata.getLocation() != null) {
          // the tablet exists but has a location, lets try again later
          manager.requestUnassignment(tabletMetadata.getExtent(), tid);
          return 2000;
        } else {
          // The tablet may no longer exists, another operation may have it reserved, or maybe we
          // already reserved and a fault happened. In any case lets proceed, the tablet will be
          // checked in the call() function and it will sort everything out.
          return 0;
        }
      }
    }
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    // ELASTICITY_TODO need to make manager ignore tablet with an operation id for assignment
    // purposes
    manager.cancelUnassignmentRequest(splitInfo.getOriginal(), tid);
    manager.getSplitter().removeSplitStarting(splitInfo.getOriginal());

    TabletMetadata tabletMetadata = manager.getContext().getAmple().readTablet(
        splitInfo.getOriginal(), ColumnType.PREV_ROW, ColumnType.LOCATION, ColumnType.OPID);

    var opid = TabletOperationId.from(TabletOperationType.SPLITTING, tid);

    if (tabletMetadata == null || !tabletMetadata.getOperationId().equals(opid)) {
      // the tablet no longer exists or we could not set the operation id, maybe another operation
      // was running, lets not proceed with the split.
      var optMeta = Optional.ofNullable(tabletMetadata);
      log.trace("{} Not proceeding with split. extent:{} location:{} opid:{}",
          FateTxId.formatTid(tid), splitInfo.getOriginal(),
          optMeta.map(TabletMetadata::getLocation).orElse(null),
          optMeta.map(TabletMetadata::getOperationId).orElse(null));
      return null;
    }

    // Create the dir name here for the next step. If the next step fails it will always have the
    // same dir name each time it runs again making it idempotent.

    List<String> dirs = new ArrayList<>();

    splitInfo.getSplits().forEach(split -> {
      String dirName = UniqueNameAllocator.createTabletDirectoryName(manager.getContext(), split);
      dirs.add(dirName);
      log.trace("{} allocated dir name {}", FateTxId.formatTid(tid), dirName);
    });

    return new UpdateTablets(splitInfo, dirs);
  }

  @Override
  public void undo(long tid, Manager manager) throws Exception {
    // TODO is this called if isReady fails?
    // TODO should operation id be cleaned up? maybe not, tablet may be in an odd state
    manager.cancelUnassignmentRequest(splitInfo.getOriginal(), tid);
    manager.getSplitter().removeSplitStarting(splitInfo.getOriginal());
  }
}

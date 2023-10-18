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
import java.util.Objects;
import java.util.SortedSet;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.DestructiveTabletManagerRepo;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class PreSplit extends DestructiveTabletManagerRepo {
  private static final long serialVersionUID = 1L;
  private static final Logger log = LoggerFactory.getLogger(PreSplit.class);

  private final SplitInfo splitInfo;

  public PreSplit(KeyExtent expectedExtent, SortedSet<Text> splits) {
    super(TabletOperationType.SPLITTING, expectedExtent);
    Objects.requireNonNull(splits);
    Preconditions.checkArgument(!splits.isEmpty());
    Preconditions.checkArgument(!expectedExtent.isRootTablet());
    this.splitInfo = new SplitInfo(expectedExtent, splits);
  }

  @Override
  public long isReady(long tid, Manager manager) throws Exception {

    // ELASTICITY_TODO intentionally not getting the table lock because not sure if its needed,
    // revist later when more operations are moved out of tablet server

    // ELASTICITY_TODO write IT that spins up 100 threads that all try to add a diff split to
    // the same tablet.

    // ELASTICITY_TODO does FATE prioritize running Fate txs that have already started? If not would
    // be good to look into this so we can finish things that are started before running new txs
    // that have not completed their first step. Once splits starts running, would like it to move
    // through as quickly as possible.

    if (canStart(tid, manager)) {
      return 0;
    }
    return 1000;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {

    if (!canContinue(tid, manager)) {
      throw new IllegalStateException(
          "Tablet is in an unexpected condition: " + splitInfo.getOriginal());
    }

    manager.getSplitter().removeSplitStarting(splitInfo.getOriginal());

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

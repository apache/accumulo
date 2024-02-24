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

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.UnSplittableMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.split.SplitUtils;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class FindSplits extends ManagerRepo {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(PreSplit.class);
  private final SplitInfo splitInfo;

  public FindSplits(KeyExtent extent) {
    this.splitInfo = new SplitInfo(extent, new TreeSet<>());
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    var extent = splitInfo.getOriginal();
    var ample = manager.getContext().getAmple();
    var tabletMetadata = ample.readTablet(extent);

    if (tabletMetadata == null) {
      log.trace("Table {} no longer exist, so not gonna try to find a split point for it", extent);
      return null;
    }

    if (tabletMetadata.getOperationId() != null) {
      log.debug("Not splitting {} because it has operation id {}", tabletMetadata.getExtent(),
          tabletMetadata.getOperationId());
      return null;
    }

    if (tabletMetadata.getUnSplittable() != null) {
      // The TabletManagementIterator should not be trying to split if the tablet was marked
      // as unsplittable and the metadata hasn't changed
      Preconditions.checkState(
          !tabletMetadata.getUnSplittable()
              .equals(SplitUtils.toUnSplittable(manager.getContext(), tabletMetadata)),
          "Unexpected split attempted on tablet %s that was marked as unsplittable", extent);
    }

    if (!tabletMetadata.getLogs().isEmpty()) {
      // This code is only called by system initiated splits, so if walogs are present it probably
      // makes sense to wait for the data in them to be written to a file before finding splits
      // points.
      log.debug("Not splitting {} because it has walogs {}", tabletMetadata.getExtent(),
          tabletMetadata.getLogs().size());
    }

    SortedSet<Text> splits = SplitUtils.findSplits(manager.getContext(), tabletMetadata);

    if (extent.endRow() != null) {
      splits.remove(extent.endRow());
    }

    if (splits.isEmpty()) {
      log.info("Tablet {} needs to split, but no split points could be found.",
          tabletMetadata.getExtent());

      // TODO: Do we care about conditional mutations here? I don't think it is required because
      // If something changes TabletManagementIterator will be comparing anyways and will detect it
      UnSplittableMetadata unSplittableMeta =
          SplitUtils.toUnSplittable(manager.getContext(), tabletMetadata);
      ample.mutateTablet(extent).setUnSplittable(unSplittableMeta).mutate();

      return null;
    }

    return new PreSplit(extent, splits);
  }

}

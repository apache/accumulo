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
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.split.SplitUtils;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    var tabletMetadata = manager.getContext().getAmple().readTablet(extent);

    if (tabletMetadata == null) {
      log.trace("Table {} no longer exist, so not gonna try to find a split point for it", extent);
      return null;
    }

    if (tabletMetadata.getOperationId() != null) {
      log.debug("Not splitting {} because it has operation id {}", tabletMetadata.getExtent(),
          tabletMetadata.getOperationId());
      return null;
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
      // ELASTICITY_TODO record the fact that tablet is un-splittable in metadata table in a new
      // column. Record the config used to reach this decision and a hash of the file. The tablet
      // mgmt iterator can inspect this column and only try to split the tablet when something has
      // changed.
      return null;
    }

    return new PreSplit(extent, splits);
  }
}

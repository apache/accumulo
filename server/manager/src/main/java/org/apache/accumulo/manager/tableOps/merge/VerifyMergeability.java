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
package org.apache.accumulo.manager.tableOps.merge;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.MERGEABILITY;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.manager.tableOps.merge.MergeInfo.Operation;
import org.apache.accumulo.manager.tableOps.merge.UnreserveAndError.Reason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class VerifyMergeability extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(VerifyMergeability.class);
  private static final long serialVersionUID = 1L;
  private final MergeInfo data;

  public VerifyMergeability(MergeInfo mergeInfo) {
    this.data = mergeInfo;
    Preconditions.checkArgument(data.op == Operation.SYSTEM_MERGE, "Must be a System Merge");
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager env) throws Exception {

    var range = data.getReserveExtent();

    var currentTime = env.getSteadyTime();
    var context = env.getContext();
    var tableConf = context.getTableConfiguration(data.tableId);
    var splitThreshold = tableConf.getAsBytes(Property.TABLE_SPLIT_THRESHOLD);
    var maxMergeabilityThreshold = tableConf.getFraction(Property.TABLE_MAX_MERGEABILITY_THRESHOLD);

    // max percentage of split threshold
    long maxTotalSize = (long) (splitThreshold * maxMergeabilityThreshold);

    long maxFiles = env.getContext().getTableConfiguration(data.getOriginalExtent().tableId())
        .getCount(Property.TABLE_MERGE_FILE_MAX);

    log.debug("Validating system merge for {} with range {}", fateId, range);

    try (var tablets = env.getContext().getAmple().readTablets().forTable(data.tableId)
        .overlapping(range.prevEndRow(), range.endRow()).fetch(FILES, MERGEABILITY)
        .checkConsistency().build()) {

      long totalSize = 0;
      long totalFiles = 0;
      int totalUnMergeable = 0;

      for (var tabletMetadata : tablets) {
        if (!tabletMetadata.getTabletMergeability().isMergeable(currentTime)) {
          totalUnMergeable++;
        }

        totalFiles += tabletMetadata.getFiles().size();
        totalSize += tabletMetadata.getFileSize();
      }

      if (totalFiles > maxFiles) {
        return new UnreserveAndError(data, Reason.MAX_FILES, totalFiles, maxFiles);
      }

      if (totalSize > maxTotalSize) {
        return new UnreserveAndError(data, Reason.MAX_SIZE, totalSize, maxTotalSize);
      }

      if (totalUnMergeable > 0) {
        return new UnreserveAndError(data, totalUnMergeable);
      }
    }

    return new MergeTablets(data);
  }

}

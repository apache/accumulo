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

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountFiles extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(CountFiles.class);
  private static final long serialVersionUID = 1L;
  private final MergeInfo data;

  public CountFiles(MergeInfo mergeInfo) {
    this.data = mergeInfo;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager env) throws Exception {

    var range = data.getReserveExtent();

    long totalFiles = 0;

    try (var tablets = env.getContext().getAmple().readTablets().forTable(data.tableId)
        .overlapping(range.prevEndRow(), range.endRow()).fetch(FILES).checkConsistency().build()) {

      switch (data.op) {
        case MERGE:
          for (var tabletMeta : tablets) {
            totalFiles += tabletMeta.getFiles().size();
          }
          break;
        case DELETE:
          for (var tabletMeta : tablets) {
            // Files in tablets that are completely contained within the merge range will be
            // deleted, so do not count these files .
            if (!data.getOriginalExtent().contains(tabletMeta.getExtent())) {
              totalFiles += tabletMeta.getFiles().size();
            }
          }
          break;
        default:
          throw new IllegalStateException("Unknown op " + data.op);
      }
    }

    long maxFiles = env.getContext().getTableConfiguration(data.getOriginalExtent().tableId())
        .getCount(Property.TABLE_MERGE_FILE_MAX);

    log.debug("{} found {} files in the merge range, maxFiles is {}", fateId, totalFiles, maxFiles);

    if (totalFiles >= maxFiles) {
      return new UnreserveAndError(data, totalFiles, maxFiles);
    } else {
      if (data.op == MergeInfo.Operation.MERGE) {
        return new MergeTablets(data);
      } else {
        return new DeleteRows(data);
      }
    }
  }
}

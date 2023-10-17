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

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateTxId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.manager.state.MergeInfo;
import org.apache.accumulo.server.manager.state.MergeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaitForOffline extends ManagerRepo {

  private static final Logger log = LoggerFactory.getLogger(WaitForOffline.class);

  private static final long serialVersionUID = 1L;

  private final NamespaceId namespaceId;
  private final TableId tableId;

  public WaitForOffline(NamespaceId namespaceId, TableId tableId) {
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  @Override
  public long isReady(long tid, Manager env) throws Exception {
    MergeInfo mergeInfo = env.getMergeInfo(tableId);
    var extent = mergeInfo.getExtent();

    long tabletsWithLocations;

    try (var tabletMeta = env.getContext().getAmple().readTablets().forTable(extent.tableId())
        .overlapping(extent.prevEndRow(), extent.endRow())
        .fetch(TabletMetadata.ColumnType.PREV_ROW, TabletMetadata.ColumnType.LOCATION)
        .checkConsistency().build()) {
      tabletsWithLocations = tabletMeta.stream().filter(tm -> tm.getLocation() != null).count();
    }

    log.info("{} waiting for {} tablets with locations", FateTxId.formatTid(tid),
        tabletsWithLocations);

    if (tabletsWithLocations > 0) {
      return 1000;
    } else {
      return 0;
    }
  }

  @Override
  public Repo<Manager> call(long tid, Manager env) throws Exception {
    MergeInfo mergeInfo = env.getMergeInfo(tableId);
    env.setMergeState(mergeInfo, MergeState.MERGING);
    if (mergeInfo.isDelete()) {
      return new DeleteRows(namespaceId, tableId);
    } else {
      return new MergeTablets(namespaceId, tableId);
    }
  }
}

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
package org.apache.accumulo.manager.compaction.coordinator.commit;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutGcCandidates extends AbstractFateOperation {
  private static final long serialVersionUID = 1L;
  private final CompactionCommitData commitData;
  private final String refreshLocation;
  private final ArrayList<String> filesToDeleteViaGc;
  private static final Logger LOG = LoggerFactory.getLogger(PutGcCandidates.class);

  public PutGcCandidates(CompactionCommitData commitData, String refreshLocation,
      ArrayList<StoredTabletFile> filesToDeleteViaGc) {
    this.commitData = commitData;
    this.refreshLocation = refreshLocation;
    this.filesToDeleteViaGc = new ArrayList<>();
    for (StoredTabletFile file : filesToDeleteViaGc) {
      this.filesToDeleteViaGc.add(file.getMetadataPath());
    }
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv env) throws Exception {
    if (filesToDeleteViaGc != null && !filesToDeleteViaGc.isEmpty()) {
      var extent = KeyExtent.fromThrift(commitData.textent);
      List<ReferenceFile> deleteMarkers = new ArrayList<>();

      for (String filePath : filesToDeleteViaGc) {
        StoredTabletFile file = new StoredTabletFile(filePath);
        deleteMarkers.add(ReferenceFile.forFile(extent.tableId(), file));
        LOG.debug("Creating GC delete marker for file: {}", file.getFileName());
      }

      env.getContext().getAmple().putGcFileAndDirCandidates(extent.tableId(), deleteMarkers);
      LOG.debug("Created {} GC delete markers for compaction", deleteMarkers.size());
    }

    if (refreshLocation == null) {
      env.recordCompactionCompletion(ExternalCompactionId.of(commitData.ecid));
      return null;
    }

    // For user initiated table compactions, the fate operation will refresh tablets. Can also
    // refresh as part of this compaction commit as it may run sooner.
    return new RefreshTablet(commitData.ecid, commitData.textent, refreshLocation);
  }
}

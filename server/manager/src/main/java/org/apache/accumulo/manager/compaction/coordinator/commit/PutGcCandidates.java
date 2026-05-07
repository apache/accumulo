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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.gc.ReferenceFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.manager.tableOps.AbstractFateOperation;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.hadoop.fs.Path;
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
    var extent = KeyExtent.fromThrift(commitData.textent);
    Set<String> nonSharedSet = new HashSet<>(filesToDeleteViaGc);
    List<ReferenceFile> gcCandidates = new ArrayList<>();

    for (StoredTabletFile jobFile : commitData.getJobFiles()) {
      if (nonSharedSet.contains(jobFile.getMetadataPath())) {
        // File is confirmed non-shared, attempt direct filesystem deletion
        try {
          Path filePath = new Path(jobFile.getMetadataPath());
          boolean deleted = env.getContext().getVolumeManager().deleteRecursively(filePath);
          if (deleted) {
            LOG.debug("Directly deleted non-shared compaction input file: {}",
                jobFile.getFileName());
          } else {
            LOG.debug("Non-shared file {} was already absent (likely retry), no GC marker needed",
                jobFile.getFileName());
          }
        } catch (IOException e) {
          // Direct deletion failed, fall back to GC marker to ensure eventual cleanup
          LOG.warn("Failed to directly delete non-shared file {}, falling back to GC marker: {}",
              jobFile.getFileName(), e.getMessage());
          gcCandidates.add(ReferenceFile.forFile(extent.tableId(), jobFile));
        }
      } else {
        LOG.debug("File {} is shared or shared status unknown, will create GC delete marker",
            jobFile.getFileName());
        gcCandidates.add(ReferenceFile.forFile(extent.tableId(), jobFile));
      }
    }

    // Write GC delete markers for any shared files or direct-delete fallbacks
    if (!gcCandidates.isEmpty()) {
      env.getContext().getAmple().putGcFileAndDirCandidates(extent.tableId(), gcCandidates);
      LOG.debug("Created {} GC delete markers for compaction", gcCandidates.size());
    }

    if (refreshLocation == null) {
      return null;
    }

    // For user initiated table compactions, the fate operation will refresh tablets. Can also
    // refresh as part of this compaction commit as it may run sooner.
    return new RefreshTablet(commitData.textent, refreshLocation);
  }
}

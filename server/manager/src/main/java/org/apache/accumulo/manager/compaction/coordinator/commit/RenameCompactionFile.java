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

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RenameCompactionFile extends ManagerRepo {
  private static final Logger log = LoggerFactory.getLogger(RenameCompactionFile.class);
  private static final long serialVersionUID = 1L;
  private final CompactionCommitData commitData;

  public RenameCompactionFile(CompactionCommitData commitData) {
    this.commitData = commitData;
  }

  @Override
  public Repo<Manager> call(FateId fateId, Manager manager) throws Exception {
    ReferencedTabletFile newDatafile = null;
    var ctx = manager.getContext();

    var tmpPath = new Path(commitData.outputTmpPath);

    if (commitData.stats.getEntriesWritten() == 0) {
      // the compaction produced no output so do not need to rename or add a file to the metadata
      // table, only delete the input files.
      try {
        if (!ctx.getVolumeManager().delete(tmpPath)) {
          throw new IOException("delete returned false for " + tmpPath);
        }
      } catch (IOException ioe) {
        // Log something in case there is an exception while doing the check, will have the original
        // exception.
        log.debug("Attempting to see if file exists after delete failure of {}", tmpPath, ioe);
        if (!ctx.getVolumeManager().exists(tmpPath)) {
          log.debug(
              "Failed to delete file {}, but it does not exists.  Assuming this is a 2nd run.",
              tmpPath, ioe);
        } else {
          throw ioe;
        }
      }
    } else {
      newDatafile = TabletNameGenerator.computeCompactionFileDest(ReferencedTabletFile.of(tmpPath));
      try {
        if (!ctx.getVolumeManager().rename(tmpPath, newDatafile.getPath())) {
          throw new IOException("rename returned false for " + tmpPath);
        }
      } catch (IOException ioe) {
        // Log something in case there is an exception while doing the check, will have the original
        // exception.
        log.debug("Attempting to see if file exists after rename failure of {} to {}", tmpPath,
            newDatafile, ioe);
        if (ctx.getVolumeManager().exists(newDatafile.getPath())
            && !ctx.getVolumeManager().exists(tmpPath)) {
          log.debug(
              "Failed to rename {} to {}, but destination exists and source does not.  Assuming this is a 2nd run.",
              tmpPath, newDatafile, ioe);
        } else {
          throw ioe;
        }
      }
    }

    return new CommitCompaction(commitData,
        newDatafile == null ? null : newDatafile.getNormalizedPathStr());
  }
}

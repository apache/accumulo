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

import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.tablets.TabletNameGenerator;
import org.apache.hadoop.fs.Path;

public class RenameCompactionFile extends ManagerRepo {
  private static final long serialVersionUID = 1L;
  private final CompactionCommitData commitData;

  public RenameCompactionFile(CompactionCommitData commitData) {
    this.commitData = commitData;
  }

  @Override
  public Repo<Manager> call(long tid, Manager manager) throws Exception {
    ReferencedTabletFile newDatafile = null;
    var ctx = manager.getContext();

    var tmpPath = commitData.outputTmpPath;

    if (commitData.stats.getEntriesWritten() == 0) {
      // the compaction produced no output so do not need to rename or add a file to the metadata
      // table, only delete the input files.
      if (!ctx.getVolumeManager().delete(new Path(tmpPath))) {
        throw new IOException("delete returned false");
      }
    } else {
      newDatafile =
          TabletNameGenerator.computeCompactionFileDest(ReferencedTabletFile.of(new Path(tmpPath)));
      if (!ctx.getVolumeManager().rename(new Path(tmpPath), newDatafile.getPath())) {
        throw new IOException("rename returned false");
      }
    }

    return new CommitCompaction(commitData,
        newDatafile == null ? null : newDatafile.getNormalizedPathStr());
  }
}

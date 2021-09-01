/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.tserver.tablet.compaction.files;

import static org.apache.accumulo.tserver.tablet.CompactableImpl.FileSelectionStatus;

import java.util.Collections;
import java.util.Set;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.spi.compaction.CompactionKind;

public class SelectedChopCompFiles implements CompactionFiles {
  private final FileSelectionStatus selectStatus;
  private final Set<StoredTabletFile> filesToChop;

  public SelectedChopCompFiles(Set<StoredTabletFile> allCompactingFiles,
      Set<StoredTabletFile> selectedFiles, FileSelectionStatus selectStatus,
      Set<StoredTabletFile> filesToChop) {
    this.selectStatus = selectStatus;
    filesToChop.removeAll(allCompactingFiles);
    if (selectStatus == FileSelectionStatus.SELECTED)
      filesToChop.removeAll(selectedFiles);
    this.filesToChop = Collections.unmodifiableSet(filesToChop);
  }

  @Override
  public Set<StoredTabletFile> getCandidates(Set<StoredTabletFile> currFiles, CompactionKind kind) {
    if (selectStatus == FileSelectionStatus.NEW || selectStatus == FileSelectionStatus.SELECTING)
      return Set.of();
    else
      return filesToChop;
  }
}

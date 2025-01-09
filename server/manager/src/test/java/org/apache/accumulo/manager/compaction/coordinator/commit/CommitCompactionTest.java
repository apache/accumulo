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

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.manager.compaction.coordinator.commit.CommitCompaction.canCommitCompaction;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class CommitCompactionTest {

  @Test
  public void testCanCommit() throws Exception {

    var tableId = TableId.of("4");
    var extent = new KeyExtent(tableId, null, null);

    ExternalCompactionId cid1 = ExternalCompactionId.generate(UUID.randomUUID());
    ExternalCompactionId cid2 = ExternalCompactionId.generate(UUID.randomUUID());
    ReferencedTabletFile tmpFile =
        ReferencedTabletFile.of(new Path("file:///accumulo/tables/t-0/b-0/c1.rf"));
    var file1 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00001.rf"));
    var file2 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00002.rf"));
    var file3 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00003.rf"));

    var dfv1 = new DataFileValue(1000, 100);
    var dfv2 = new DataFileValue(2000, 200);
    var dfv3 = new DataFileValue(3000, 300);

    var fateId1 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    var fateId2 = FateId.from(FateInstanceType.USER, UUID.randomUUID());

    var groupId = CompactorGroupId.of("Q1");

    assertFalse(canCommitCompaction(cid1, null));

    TabletMetadata tm = TabletMetadata.builder(extent).build(OPID, ECOMP);

    // tablet does not have the compaction id so can not commit
    assertFalse(canCommitCompaction(cid1, tm));

    var compactionMetadata = new CompactionMetadata(Set.of(file1, file2), tmpFile, "cid1",
        CompactionKind.SYSTEM, (short) 3, groupId, true, null);

    tm = TabletMetadata.builder(extent)
        .putOperation(TabletOperationId.from(TabletOperationType.SPLITTING, fateId2)).build(ECOMP);
    // tablet has an operation id set so can not commit
    assertFalse(canCommitCompaction(cid1, tm));

    tm = TabletMetadata.builder(extent).putExternalCompaction(cid1, compactionMetadata)
        .putFile(file1, dfv1).putFile(file2, dfv2).putFile(file3, dfv3).build(OPID);

    // tablet has the expected files and compaction id, so can commit
    assertTrue(canCommitCompaction(cid1, tm));
    // tablet has compaction id, but not the one passed in is unknown so can not commit
    assertFalse(canCommitCompaction(cid2, tm));

    // tablet is missing file in the set of compacting files, so can not commit
    tm = TabletMetadata.builder(extent).putExternalCompaction(cid1, compactionMetadata)
        .putFile(file2, dfv2).putFile(file3, dfv3).build(OPID);
    assertFalse(canCommitCompaction(cid1, tm));

    // create user compaction metadata
    compactionMetadata = new CompactionMetadata(Set.of(file1, file2), tmpFile, "cid1",
        CompactionKind.USER, (short) 3, groupId, true, fateId1);

    tm = TabletMetadata.builder(extent).putExternalCompaction(cid1, compactionMetadata)
        .putFile(file1, dfv1).putFile(file2, dfv2).putFile(file3, dfv3).build(OPID, SELECTED);

    // the tablet does not have selected files set which is expected for user compaction so can not
    // commit
    assertFalse(canCommitCompaction(cid1, tm));

    var stime = SteadyTime.from(Duration.ofMinutes(1));
    var selFiles = new SelectedFiles(Set.of(file1, file2, file3), true, fateId1, stime);

    tm = TabletMetadata.builder(extent).putExternalCompaction(cid1, compactionMetadata)
        .putSelectedFiles(selFiles).putFile(file1, dfv1).putFile(file2, dfv2).putFile(file3, dfv3)
        .build(OPID);

    // everything is as expected for user compaction to commit
    assertTrue(canCommitCompaction(cid1, tm));

    selFiles = new SelectedFiles(Set.of(file1, file2, file3), true, fateId2, stime);
    tm = TabletMetadata.builder(extent).putExternalCompaction(cid1, compactionMetadata)
        .putSelectedFiles(selFiles).putFile(file1, dfv1).putFile(file2, dfv2).putFile(file3, dfv3)
        .build(OPID);

    // The fate ids in selected files and compaction metadata do not match so can not commit
    assertFalse(canCommitCompaction(cid1, tm));

    selFiles = new SelectedFiles(Set.of(file1, file3), true, fateId1, stime);
    compactionMetadata = new CompactionMetadata(Set.of(file1, file2), tmpFile, "cid1",
        CompactionKind.USER, (short) 3, groupId, true, fateId1);
    tm = TabletMetadata.builder(extent).putExternalCompaction(cid1, compactionMetadata)
        .putSelectedFiles(selFiles).putFile(file1, dfv1).putFile(file2, dfv2).putFile(file3, dfv3)
        .build(OPID);

    // the files in the compaction metadata are not a subset of the selected files so can not commit
    assertFalse(canCommitCompaction(cid1, tm));
  }
}

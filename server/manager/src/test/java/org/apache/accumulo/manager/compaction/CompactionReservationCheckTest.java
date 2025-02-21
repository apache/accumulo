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
package org.apache.accumulo.manager.compaction;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.ECOMP;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.OPID;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.SELECTED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.USER_COMPACTION_REQUESTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ExternalCompactionColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.UserCompactionRequestedColumnFamily;
import org.apache.accumulo.core.metadata.schema.SelectedFiles;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletOperationId;
import org.apache.accumulo.core.metadata.schema.TabletOperationType;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.manager.compaction.coordinator.CompactionReservationCheck;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class CompactionReservationCheckTest {
  @Test
  public void testCanReserve() throws Exception {
    TableId tableId1 = TableId.of("5");

    var file1 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00001.rf"));
    var file2 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00002.rf"));
    var file3 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00003.rf"));
    var file4 = StoredTabletFile.of(new URI("file:///accumulo/tables/1/default_tablet/F00004.rf"));

    FateId fateId1 = FateId.from(FateInstanceType.USER, UUID.randomUUID());
    FateId fateId2 = FateId.from(FateInstanceType.USER, UUID.randomUUID());

    CompactorGroupId cgid = CompactorGroupId.of("G1");
    ReferencedTabletFile tmp1 =
        ReferencedTabletFile.of(new Path("file:///accumulo/tables/1/default_tablet/C00005.rf_tmp"));
    CompactionMetadata cm1 = new CompactionMetadata(Set.of(file1, file2), tmp1, "localhost:4444",
        CompactionKind.SYSTEM, (short) 5, cgid, false, null);

    ReferencedTabletFile tmp2 =
        ReferencedTabletFile.of(new Path("file:///accumulo/tables/1/default_tablet/C00006.rf_tmp"));
    CompactionMetadata cm2 = new CompactionMetadata(Set.of(file3), tmp2, "localhost:5555",
        CompactionKind.USER, (short) 5, cgid, false, fateId1);

    KeyExtent extent1 = new KeyExtent(tableId1, null, null);

    var dfv = new DataFileValue(1000, 100);

    var cid1 = ExternalCompactionId.generate(UUID.randomUUID());
    var cid2 = ExternalCompactionId.generate(UUID.randomUUID());

    var selectedWithoutComp = new SelectedFiles(Set.of(file1, file2, file3), false, fateId1,
        SteadyTime.from(100, TimeUnit.SECONDS));
    var selectedWithComp = new SelectedFiles(Set.of(file1, file2, file3), false, fateId1, 1,
        SteadyTime.from(100, TimeUnit.SECONDS));

    var time = SteadyTime.from(1000, TimeUnit.SECONDS);

    // nothing should prevent this compaction
    var tablet1 =
        TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv).putFile(file3, dfv)
            .putFile(file4, dfv).build(OPID, ECOMP, USER_COMPACTION_REQUESTED, SELECTED);
    assertTrue(canReserveSystem(tablet1, CompactionKind.SYSTEM, Set.of(file1, file2), false, time));

    // should not be able to do a user compaction unless selected files are present
    assertFalse(canReserveUser(tablet1, CompactionKind.USER, Set.of(file1, file2), fateId1, time));

    // should not be able to compact a tablet with user compaction request in place
    var tablet3 =
        TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv).putFile(file3, dfv)
            .putFile(file4, dfv).putUserCompactionRequested(fateId1).build(OPID, ECOMP, SELECTED);
    assertFalse(
        canReserveSystem(tablet3, CompactionKind.SYSTEM, Set.of(file1, file2), false, time));

    // should not be able to compact a tablet when the job has files not present in the tablet
    var tablet4 = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).build(OPID, ECOMP, USER_COMPACTION_REQUESTED, SELECTED);
    assertFalse(
        canReserveSystem(tablet4, CompactionKind.SYSTEM, Set.of(file1, file2, file4), false, time));

    // should not be able to compact a tablet with an operation id present
    TabletOperationId opid = TabletOperationId.from(TabletOperationType.SPLITTING, fateId1);
    var tablet5 = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).putFile(file4, dfv).putOperation(opid)
        .build(ECOMP, USER_COMPACTION_REQUESTED, SELECTED);
    assertFalse(
        canReserveSystem(tablet5, CompactionKind.SYSTEM, Set.of(file1, file2), false, time));

    // should not be able to compact a tablet if the job files overlaps with running compactions
    var tablet6 = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).putFile(file4, dfv).putExternalCompaction(cid1, cm1)
        .putExternalCompaction(cid2, cm2).build(OPID, USER_COMPACTION_REQUESTED, SELECTED);
    assertFalse(
        canReserveSystem(tablet6, CompactionKind.SYSTEM, Set.of(file1, file2), false, time));
    // should be able to compact the file that is outside of the set of files currently compacting
    assertTrue(canReserveSystem(tablet6, CompactionKind.SYSTEM, Set.of(file4), false, time));

    // create a tablet with a selected set of files
    var selTabletWithComp = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).putFile(file4, dfv).putSelectedFiles(selectedWithComp)
        .build(OPID, USER_COMPACTION_REQUESTED, ECOMP);
    // 0 completed jobs
    var selTabletWithoutComp = TabletMetadata.builder(extent1).putFile(file1, dfv)
        .putFile(file2, dfv).putFile(file3, dfv).putFile(file4, dfv)
        .putSelectedFiles(selectedWithoutComp).build(OPID, USER_COMPACTION_REQUESTED, ECOMP);

    // Should be able to start if no completed and overlap
    assertTrue(canReserveSystem(selTabletWithoutComp, CompactionKind.SYSTEM, Set.of(file1, file2),
        true, time));
    assertTrue(canReserveSystem(selTabletWithoutComp, CompactionKind.SYSTEM, Set.of(file3, file4),
        true, time));
    // Should not be able to start a system compaction that overlaps with selected files that have
    // not expired.
    assertFalse(canReserveSystem(selTabletWithoutComp, CompactionKind.SYSTEM, Set.of(file3, file4),
        true, selectedWithoutComp.getSelectedTime().plus(Duration.ofMillis(5))));
    // When not deleting the selected files column can not overlap with selected set at all. It does
    // not matter if the selected set if eligible for deletion. In this test case the selected set
    // is eligible for deletion.
    assertFalse(canReserveSystem(selTabletWithoutComp, CompactionKind.SYSTEM, Set.of(file3, file4),
        false, time));
    // When deleting the selected files column the files being compacted must overlap with it. In
    // this case there is no overlap and the reservation should fail. This means there was a change
    // since the job was generated and the decision it made to delete the selected set is no longer
    // valid.
    assertFalse(
        canReserveSystem(selTabletWithoutComp, CompactionKind.SYSTEM, Set.of(file4), true, time));

    // should not be able to start a system compaction if the set of files overlaps with the
    // selected files
    assertFalse(canReserveSystem(selTabletWithComp, CompactionKind.SYSTEM, Set.of(file1, file2),
        true, time));
    assertFalse(canReserveSystem(selTabletWithComp, CompactionKind.SYSTEM, Set.of(file3, file4),
        true, time));
    // should be able to start a system compaction on the set of files not in the selected set
    assertTrue(
        canReserveSystem(selTabletWithComp, CompactionKind.SYSTEM, Set.of(file4), false, time));
    // should be able to start user compactions on files that are selected
    assertTrue(canReserveUser(selTabletWithComp, CompactionKind.USER, Set.of(file1, file2), fateId1,
        time));
    assertTrue(canReserveUser(selTabletWithComp, CompactionKind.USER, Set.of(file2, file3), fateId1,
        time));
    assertTrue(canReserveUser(selTabletWithComp, CompactionKind.USER, Set.of(file1, file2, file3),
        fateId1, time));
    // should not be able to start user compactions on files that fall outside of the selected set
    assertFalse(canReserveUser(selTabletWithComp, CompactionKind.USER, Set.of(file1, file4),
        fateId1, time));
    assertFalse(
        canReserveUser(selTabletWithComp, CompactionKind.USER, Set.of(file4), fateId1, time));
    assertFalse(canReserveUser(selTabletWithComp, CompactionKind.USER,
        Set.of(file1, file2, file3, file4), fateId1, time));

    // test selected files and running compaction
    var selRunningTablet = TabletMetadata.builder(extent1).putFile(file1, dfv).putFile(file2, dfv)
        .putFile(file3, dfv).putFile(file4, dfv).putSelectedFiles(selectedWithComp)
        .putExternalCompaction(cid2, cm2).build(OPID, USER_COMPACTION_REQUESTED);
    // should be able to compact files that are in the selected set and not in the running set
    assertTrue(
        canReserveUser(selRunningTablet, CompactionKind.USER, Set.of(file1, file2), fateId1, time));
    // should not be able to compact when the fateId used to generate the compaction job does not
    // match the fate id stored in the selected file set.
    assertFalse(
        canReserveUser(selRunningTablet, CompactionKind.USER, Set.of(file1, file2), fateId2, time));
    // should not be able to compact because files overlap the running set
    assertFalse(
        canReserveUser(selRunningTablet, CompactionKind.USER, Set.of(file2, file3), fateId1, time));
    // should not be able to start a system compaction if the set of files overlaps with the
    // selected files and/or the running set
    assertFalse(canReserveSystem(selRunningTablet, CompactionKind.SYSTEM, Set.of(file1, file2),
        true, time));
    assertFalse(canReserveSystem(selRunningTablet, CompactionKind.SYSTEM, Set.of(file3, file4),
        true, time));
    // should be able to start a system compaction on the set of files not in the selected set
    assertTrue(
        canReserveSystem(selRunningTablet, CompactionKind.SYSTEM, Set.of(file4), false, time));

    // should not be able to compact a tablet that does not exists
    assertThrows(NullPointerException.class,
        () -> canReserveSystem(null, CompactionKind.SYSTEM, Set.of(file1, file2), false, time));
    assertThrows(NullPointerException.class,
        () -> canReserveUser(null, CompactionKind.USER, Set.of(file1, file2), fateId1, time));
  }

  @Test
  public void testResolvedColumns() throws Exception {
    var resolved1 = new CompactionReservationCheck(CompactionKind.SYSTEM, Set.of(), null, false,
        SteadyTime.from(Duration.ZERO), 100L).columnsToRead();
    var resolved2 = new CompactionReservationCheck(CompactionKind.SYSTEM, Set.of(), null, false,
        SteadyTime.from(Duration.ZERO), 100L).columnsToRead();
    var expectedColumnTypes =
        EnumSet.of(PREV_ROW, OPID, SELECTED, FILES, ECOMP, USER_COMPACTION_REQUESTED);
    var expectedFamilies = Set
        .of(ServerColumnFamily.NAME, TabletColumnFamily.NAME, DataFileColumnFamily.NAME,
            ExternalCompactionColumnFamily.NAME, UserCompactionRequestedColumnFamily.NAME)
        .stream().map(family -> new ArrayByteSequence(family.copyBytes()))
        .collect(Collectors.toSet());

    // Verify same object is re-used across instances
    assertSame(resolved1, resolved2);
    assertEquals(expectedColumnTypes, resolved1.getColumns());
    assertEquals(ColumnType.resolveFamilies(expectedColumnTypes), resolved1.getFamilies());
    assertEquals(expectedFamilies, resolved1.getFamilies());
  }

  private boolean canReserveSystem(TabletMetadata tabletMetadata, CompactionKind kind,
      Set<StoredTabletFile> jobFiles, boolean deletingSelected, SteadyTime steadyTime) {
    var check = new CompactionReservationCheck(CompactionKind.SYSTEM, jobFiles, null,
        deletingSelected, steadyTime, 100L);

    return check.canUpdate(tabletMetadata);
  }

  private boolean canReserveUser(TabletMetadata tabletMetadata, CompactionKind kind,
      Set<StoredTabletFile> jobFiles, FateId selectedId, SteadyTime steadyTime) {
    var check = new CompactionReservationCheck(CompactionKind.USER, jobFiles, selectedId, false,
        steadyTime, 100L);

    return check.canUpdate(tabletMetadata);
  }

}

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
package org.apache.accumulo.tserver.tablet;

import static org.apache.accumulo.tserver.tablet.CompactableImplFileManagerTest.newFile;
import static org.apache.accumulo.tserver.tablet.CompactableImplFileManagerTest.newFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.TabletFile;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.util.compaction.CompactionExecutorIdImpl;
import org.junit.jupiter.api.Test;

public class CompactableImplTest {
  private static ExternalCompactionMetadata newECM(Set<StoredTabletFile> jobFiles,
      Set<StoredTabletFile> nextFiles, CompactionKind kind, boolean propagateDeletes,
      boolean initiallySelectedAll) {

    return newECM(jobFiles, nextFiles, kind, propagateDeletes, initiallySelectedAll, 5L);
  }

  private static ExternalCompactionMetadata newECM(Set<StoredTabletFile> jobFiles,
      Set<StoredTabletFile> nextFiles, CompactionKind kind, boolean propagateDeletes,
      boolean initiallySelectedAll, Long compactionId) {

    TabletFile compactTmpName = newFile("C00000A.rf_tmp");
    String compactorId = "cid";
    short priority = 9;
    CompactionExecutorId ceid = CompactionExecutorIdImpl.externalId("ecs1");

    return new ExternalCompactionMetadata(jobFiles, nextFiles, compactTmpName, compactorId, kind,
        priority, ceid, propagateDeletes, initiallySelectedAll, compactionId);
  }

  ExternalCompactionId newEcid() {
    return ExternalCompactionId.generate(UUID.randomUUID());
  }

  @Test
  public void testExternalOverlapping() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, Set.of(), CompactionKind.SYSTEM, true, false);

    var fileSet2 = newFiles("F00002", "F00003");
    var ecm2 = newECM(fileSet2, Set.of(), CompactionKind.USER, true, false);

    var fileSet3 = newFiles("F00004", "F00005");
    var ecm3 = newECM(fileSet3, Set.of(), CompactionKind.SYSTEM, true, false);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    var ecid1 = newEcid();
    var ecid2 = newEcid();
    var ecid3 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1, ecid2, ecm2, ecid3, ecm3);
    var selInfo = CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.empty(),
        newFiles("F00001", "F00002", "F00003", "F00004", "F00005"), toRemove);

    // The external compaction metadata contains compactions with overlapping jobs. The code
    // currently marks everything for remove when this happens. So even though only ecid1 and ecid2
    // overlap, ecid3 is also flagged for removal.
    assertEquals(Set.of(ecid1, ecid2, ecid3), toRemove.keySet());
    assertTrue(toRemove.values().stream()
        .allMatch(v -> v.contains("Some external compaction files overlap")));
    assertTrue(selInfo.isEmpty());
  }

  @Test
  public void testExternalFilesOutsideTablet() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, Set.of(), CompactionKind.SYSTEM, true, false);

    var fileSet2 = newFiles("F00003", "F00004");
    var ecm2 = newECM(fileSet2, Set.of(), CompactionKind.USER, true, false);

    var fileSet3 = newFiles("F00005", "F00006");
    var ecm3 = newECM(fileSet3, Set.of(), CompactionKind.SYSTEM, true, false);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    var ecid1 = newEcid();
    var ecid2 = newEcid();
    var ecid3 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1, ecid2, ecm2, ecid3, ecm3);
    var selInfo = CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.empty(),
        newFiles("F00004", "F00005", "F00006"), toRemove);

    // The external compaction ids ecid1 and ecid2 are related to files that are not in the tablet
    // files, so that external compaction metadata should be removed
    assertEquals(Set.of(ecid1, ecid2), toRemove.keySet());
    assertTrue(
        toRemove.values().stream().allMatch(v -> v.contains("Has files outside of tablet files")));
    assertTrue(selInfo.isEmpty());
  }

  @Test
  public void testExternalUserDifferentSelectedFiles() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.USER, true, false);

    var fileSet2 = newFiles("F00003", "F00004");
    var ecm2 = newECM(fileSet2, newFiles("F00001", "F00002"), CompactionKind.USER, true, false);

    var ecid1 = newEcid();
    var ecid2 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1, ecid2, ecm2);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004"), toRemove);

    // the two external compactions should compute the same set of selected files, so neither should
    // be removed
    assertEquals(Set.of(), toRemove.keySet());

    var fileSet3 = newFiles("F00003", "F00004");
    var ecm3 =
        newECM(fileSet3, newFiles("F00001", "F00002", "F00005"), CompactionKind.USER, true, false);

    extCompactions = Map.of(ecid1, ecm1, ecid2, ecm3);

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004", "F00005"), toRemove);

    // A different set of selected files is computed for the two external compactions, so both
    // should be ignored and removed
    assertEquals(Set.of(ecid1, ecid2), toRemove.keySet());
    assertTrue(
        toRemove.values().stream().allMatch(v -> v.contains("Selected set of files differs")));

    var fileSet4 = newFiles("F00003", "F00004");
    var ecm4 = newECM(fileSet4, newFiles("F00001"), CompactionKind.USER, true, false);

    extCompactions = Map.of(ecid1, ecm1, ecid2, ecm4);
    toRemove.clear();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004", "F00005"), toRemove);

    // A different set of selected files is computed for the two external compactions, so both
    // should be ignored and removed
    assertEquals(Set.of(ecid1, ecid2), toRemove.keySet());
    assertTrue(
        toRemove.values().stream().allMatch(v -> v.contains("Selected set of files differs")));

  }

  @Test
  public void testDifferingCompactionId() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.USER, true, false, 5L);
    var ecid1 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(4L),
        newFiles("F00001", "F00002", "F00003", "F00004"), toRemove);

    assertEquals(Set.of(ecid1), toRemove.keySet());
    assertTrue(toRemove.values().stream()
        .allMatch(v -> v.contains("Compaction id mismatch with zookeeper")));

    ecm1 = newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.USER, true, false, null);

    extCompactions = Map.of(ecid1, ecm1);

    toRemove.clear();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(4L),
        newFiles("F00001", "F00002", "F00003", "F00004"), toRemove);

    assertEquals(Set.of(ecid1), toRemove.keySet());
    assertTrue(
        toRemove.values().stream().allMatch(v -> v.contains("Compaction id mismatch with zookeeper")
            && v.contains("Missing compactionId")));

  }

  @Test
  public void testSelectedAllDisagreement() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.USER, true, true);

    var fileSet2 = newFiles("F00003", "F00004");
    var ecm2 = newECM(fileSet2, newFiles("F00001", "F00002"), CompactionKind.USER, true, false);

    var ecid1 = newEcid();
    var ecid2 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1, ecid2, ecm2);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004"), toRemove);

    assertEquals(Set.of(ecid1, ecid2), toRemove.keySet());
    assertTrue(toRemove.values().stream().allMatch(v -> v.contains("Disagreement on selectedAll")));
  }

  @Test
  public void testConcurrentPropDels() {
    // For concurrent user compactions they should always propagate deletes. Only the last single
    // user compaction can not propogate deletes.
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.USER, false, true);

    var fileSet2 = newFiles("F00003", "F00004");
    var ecm2 = newECM(fileSet2, newFiles("F00001", "F00002"), CompactionKind.USER, false, true);

    var ecid1 = newEcid();
    var ecid2 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1, ecid2, ecm2);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004"), toRemove);

    assertEquals(Set.of(ecid1, ecid2), toRemove.keySet());

    assertTrue(toRemove.values().stream()
        .allMatch(v -> v.contains("Concurrent compactions not propagatingDeletes")));
  }

  @Test
  public void testPropDelDisagreement() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.USER, false, true);

    var fileSet2 = newFiles("F00003", "F00004");
    var ecm2 = newECM(fileSet2, newFiles("F00001", "F00002"), CompactionKind.USER, true, true);

    var ecid1 = newEcid();
    var ecid2 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1, ecid2, ecm2);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004"), toRemove);

    assertEquals(Set.of(ecid1, ecid2), toRemove.keySet());

    assertTrue(
        toRemove.values().stream().allMatch(v -> v.contains("Disagreement on propagateDeletes")));
  }

  @Test
  public void testNoComactionIdInZookeeper() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.USER, true, false, 5L);
    var ecid1 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.empty(),
        newFiles("F00001", "F00002", "F00003", "F00004"), toRemove);

    assertEquals(Set.of(ecid1), toRemove.keySet());

    assertTrue(
        toRemove.values().stream().allMatch(v -> v.contains("No compaction id in zookeeper")));
  }

  @Test
  public void testDifferentKinds() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.USER, true, true);

    var fileSet2 = newFiles("F00003", "F00004");
    var ecm2 =
        newECM(fileSet2, newFiles("F00001", "F00002"), CompactionKind.SELECTOR, true, true, null);

    var ecid1 = newEcid();
    var ecid2 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1, ecid2, ecm2);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004"), toRemove);

    assertEquals(Set.of(ecid1, ecid2), toRemove.keySet());

    assertTrue(toRemove.values().stream().allMatch(v -> v.contains("Saw USER and SELECTOR")));
  }

  @Test
  public void testSelectorWithCompactionId() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 =
        newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.SELECTOR, true, true, 5L);

    var ecid1 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004"), toRemove);

    assertEquals(Set.of(ecid1), toRemove.keySet());

    assertTrue(toRemove.values().stream().allMatch(v -> v.contains("Unexpected compactionId")));
  }

  @Test
  public void testNominalExternalCompactionMetadata() {
    var fileSet1 = newFiles("F00001", "F00002");
    var ecm1 = newECM(fileSet1, newFiles("F00003", "F00004"), CompactionKind.USER, true, true);

    var fileSet2 = newFiles("F00003", "F00004");
    var ecm2 = newECM(fileSet2, newFiles("F00001", "F00002"), CompactionKind.USER, true, true);

    var fileSet3 = newFiles("F00005", "F00006");
    var ecm3 = newECM(fileSet3, Set.of(), CompactionKind.SYSTEM, true, false);

    var ecid1 = newEcid();
    var ecid2 = newEcid();
    var ecid3 = newEcid();

    var extCompactions = Map.of(ecid1, ecm1, ecid2, ecm2, ecid3, ecm3);

    Map<ExternalCompactionId,String> toRemove = new HashMap<>();

    // This test checks the case of when there is nothing unexpected in the ext compaction metadata
    // and then verifies the return values are as expected.

    var selInfoOpt = CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004", "F00005", "F00006"), toRemove);
    assertEquals(Set.of(), toRemove.keySet());
    assertTrue(selInfoOpt.isPresent());
    var selInfo = selInfoOpt.get();

    assertTrue(selInfo.initiallySelectedAll);
    assertEquals(CompactionKind.USER, selInfo.selectKind);
    assertEquals(newFiles("F00001", "F00002", "F00003", "F00004"), selInfo.selectedFiles);

    // Try processing only system compactions.
    extCompactions = Map.of(ecid3, ecm3);
    selInfoOpt = CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004", "F00005", "F00006"), toRemove);
    assertTrue(selInfoOpt.isEmpty());
    assertEquals(Map.of(), toRemove);

    // Try a single user compaction that does not propagate deletes.
    ecm1 = newECM(newFiles("F00001", "F00002", "F00003", "F00004"), Set.of(), CompactionKind.USER,
        false, true);
    selInfoOpt = CompactableImpl.processExternalMetadata(extCompactions, () -> Optional.of(5L),
        newFiles("F00001", "F00002", "F00003", "F00004", "F00005", "F00006"), toRemove);
    assertTrue(selInfo.initiallySelectedAll);
    assertEquals(CompactionKind.USER, selInfo.selectKind);
    assertEquals(newFiles("F00001", "F00002", "F00003", "F00004"), selInfo.selectedFiles);
  }

}

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

import static org.apache.accumulo.core.spi.compaction.CompactionKind.CHOP;
import static org.apache.accumulo.core.spi.compaction.CompactionKind.SELECTOR;
import static org.apache.accumulo.core.spi.compaction.CompactionKind.SYSTEM;
import static org.apache.accumulo.core.spi.compaction.CompactionKind.USER;
import static org.apache.accumulo.tserver.tablet.CompactableImplFileManagerTest.TestFileManager.SELECTION_EXPIRATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.tserver.tablet.CompactableImpl.ChopSelectionStatus;
import org.apache.accumulo.tserver.tablet.CompactableImpl.FileManager.ChopSelector;
import org.apache.accumulo.tserver.tablet.CompactableImpl.FileSelectionStatus;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class CompactableImplFileManagerTest {

  @Test
  public void testSystem() {
    TestFileManager fileMgr = new TestFileManager();

    var tabletFiles = newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf");

    assertEquals(newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf"),
        fileMgr.getCandidates(tabletFiles, SYSTEM, false));
    assertNoCandidates(fileMgr, tabletFiles, CHOP, USER, SELECTOR);

    var job1 = newJob(CompactionKind.SYSTEM, "F00000.rf", "F00001.rf");

    assertTrue(fileMgr.reserveFiles(job1));
    assertEquals(job1.getSTFiles(), fileMgr.getCompactingFiles());

    assertEquals(newFiles("F00002.rf", "F00003.rf"),
        fileMgr.getCandidates(tabletFiles, SYSTEM, false));
    assertNoCandidates(fileMgr, tabletFiles, CHOP, USER, SELECTOR);

    var job2 = newJob(CompactionKind.SYSTEM, "F00002.rf", "F00003.rf");

    assertTrue(fileMgr.reserveFiles(job2));
    assertEquals(Sets.union(job1.getSTFiles(), job2.getSTFiles()), fileMgr.getCompactingFiles());

    // try to reserve files reserved by other compactions, should fail
    assertFalse(fileMgr.reserveFiles(newJob(CompactionKind.SYSTEM, "F00001.rf", "F00002.rf")));

    assertEquals(Set.of(), fileMgr.getCandidates(tabletFiles, CompactionKind.SYSTEM, false));
    assertNoCandidates(fileMgr, tabletFiles, SYSTEM, CHOP, USER, SELECTOR);

    fileMgr.completed(job2, newFile("C00004.rf"));
    fileMgr.completed(job1, newFile("C00005.rf"));

    tabletFiles = newFiles("C00004.rf", "C00005.rf");

    assertEquals(newFiles("C00004.rf", "C00005.rf"),
        fileMgr.getCandidates(tabletFiles, CompactionKind.SYSTEM, false));
    assertNoCandidates(fileMgr, tabletFiles, CHOP, USER, SELECTOR);

    var job3 = newJob(CompactionKind.SYSTEM, "C00004.rf", "C00005.rf");
    assertTrue(fileMgr.reserveFiles(job3));

    assertNoCandidates(fileMgr, tabletFiles, SYSTEM, CHOP, USER, SELECTOR);

    fileMgr.completed(job3, newFile("A00006.rf"));
    assertEquals(Set.of(), fileMgr.getCompactingFiles());

  }

  @Test
  public void testSelection() {
    TestFileManager fileMgr = new TestFileManager();

    var tabletFiles = newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf");

    // This job will be used to attempt to reserve files that are not selected. This could happen if
    // a job was queued for a bit and things changed.
    var staleJob = newJob(USER, "F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf");

    assertFalse(fileMgr.reserveFiles(staleJob));

    assertEquals(newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf"),
        fileMgr.getCandidates(tabletFiles, SYSTEM, false));
    assertNoCandidates(fileMgr, tabletFiles, CHOP, USER, SELECTOR);

    assertTrue(fileMgr.initiateSelection(USER, 1L));

    assertFalse(fileMgr.reserveFiles(staleJob));

    assertNoCandidates(fileMgr, tabletFiles, SYSTEM, CHOP, USER, SELECTOR);

    assertTrue(fileMgr.beginSelection());

    assertFalse(fileMgr.reserveFiles(staleJob));

    assertNoCandidates(fileMgr, tabletFiles, SYSTEM, CHOP, USER, SELECTOR);

    fileMgr.finishSelection(newFiles("F00000.rf", "F00001.rf", "F00002.rf"), false);

    assertFalse(fileMgr.reserveFiles(staleJob));

    assertEquals(newFiles("F00003.rf"), fileMgr.getCandidates(tabletFiles, SYSTEM, false));
    assertEquals(newFiles("F00000.rf", "F00001.rf", "F00002.rf"),
        fileMgr.getCandidates(tabletFiles, USER, false));
    assertNoCandidates(fileMgr, tabletFiles, CHOP, SELECTOR);

    assertFalse(fileMgr.reserveFiles(newJob(SELECTOR, "F00000.rf", "F00001.rf")));
    assertFalse(fileMgr.reserveFiles(newJob(SYSTEM, "F00000.rf", "F00001.rf")));

    var job1 = newJob(USER, "F00000.rf", "F00001.rf");

    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());
    assertTrue(fileMgr.reserveFiles(job1));
    assertEquals(FileSelectionStatus.RESERVED, fileMgr.getSelectionStatus());

    assertFalse(fileMgr.reserveFiles(staleJob));
    assertFalse(fileMgr.reserveFiles(newJob(USER, "F00001.rf", "F00002.rf")));

    // advance time past the expiration timeout, however this should not matter since the first
    // reservation was successfully made
    fileMgr.setNanoTime(2 * SELECTION_EXPIRATION.toNanos());

    assertEquals(newFiles("F00003.rf"), fileMgr.getCandidates(tabletFiles, SYSTEM, false));
    assertEquals(newFiles("F00002.rf"), fileMgr.getCandidates(tabletFiles, USER, false));
    assertNoCandidates(fileMgr, tabletFiles, CHOP, SELECTOR);

    fileMgr.completed(job1, newFile("C00004.rf"));
    assertEquals(FileSelectionStatus.RESERVED, fileMgr.getSelectionStatus());

    tabletFiles = newFiles("C00004.rf", "F00002.rf", "F00003.rf");

    assertEquals(newFiles("F00003.rf"), fileMgr.getCandidates(tabletFiles, SYSTEM, false));
    assertEquals(newFiles("F00002.rf", "C00004.rf"),
        fileMgr.getCandidates(tabletFiles, USER, false));
    assertNoCandidates(fileMgr, tabletFiles, CHOP, SELECTOR);

    var job2 = newJob(USER, "F00002.rf", "C00004.rf");

    assertTrue(fileMgr.reserveFiles(job2));
    assertEquals(FileSelectionStatus.RESERVED, fileMgr.getSelectionStatus());

    fileMgr.completed(job2, newFile("C00005.rf"));
    assertEquals(Set.of(), fileMgr.getCompactingFiles());

    assertEquals(FileSelectionStatus.NOT_ACTIVE, fileMgr.getSelectionStatus());

    tabletFiles = newFiles("C00005.rf", "F00003.rf");

    assertEquals(newFiles("C00005.rf", "F00003.rf"),
        fileMgr.getCandidates(tabletFiles, SYSTEM, false));
    assertNoCandidates(fileMgr, tabletFiles, USER, CHOP, SELECTOR);
  }

  @Test
  public void testSelectionExpiration() {
    TestFileManager fileMgr = new TestFileManager();
    var tabletFiles = newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf");

    assertTrue(fileMgr.initiateSelection(USER, 1L));
    assertTrue(fileMgr.beginSelection());
    fileMgr.finishSelection(newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf"), false);
    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());

    // a system compaction should not be able to reserved files that are selected
    assertFalse(fileMgr.reserveFiles(newJob(SYSTEM, "F00001.rf", "F00002.rf")));

    assertEquals(newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf"),
        fileMgr.getCandidates(tabletFiles, USER, false));
    assertNoCandidates(fileMgr, tabletFiles, SYSTEM, CHOP, SELECTOR);

    // advance time to a point where the selection is eligible to expire
    fileMgr.setNanoTime(2 * SELECTION_EXPIRATION.toNanos());

    // now that the selection is eligible to expire, the selected files should be available as
    // system compaction candidates
    assertEquals(newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf"),
        fileMgr.getCandidates(tabletFiles, USER, false));
    assertEquals(newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf"),
        fileMgr.getCandidates(tabletFiles, SYSTEM, false));
    assertNoCandidates(fileMgr, tabletFiles, CHOP, SELECTOR);

    // a system compaction should be able to reserve selected files after expiration which should
    // deactivate the selection
    var job1 = newJob(SYSTEM, "F00000.rf", "F00001.rf");
    assertTrue(fileMgr.reserveFiles(job1));
    assertEquals(FileSelectionStatus.NOT_ACTIVE, fileMgr.getSelectionStatus());

    // this should fail because the selection was deactivated
    assertFalse(fileMgr.reserveFiles(newJob(USER, "F00002.rf", "F00003.rf")));

    fileMgr.completed(job1, newFile("C00004.rf"));

    assertEquals(Set.of(), fileMgr.getCompactingFiles());

  }

  @Test
  public void testSelectionExpirationDisjoint() {
    TestFileManager fileMgr = new TestFileManager();

    assertTrue(fileMgr.initiateSelection(USER, 1L));
    assertTrue(fileMgr.beginSelection());
    fileMgr.finishSelection(newFiles("F00000.rf", "F00001.rf"), false);
    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());

    // advance time to a point where the selection is eligible to expire
    fileMgr.setNanoTime(2 * SELECTION_EXPIRATION.toNanos());

    // the following reservation for a system compaction should not cancel the selection because its
    // not reserving files that are selected
    var job1 = newJob(SYSTEM, "F00002.rf", "F00003.rf");
    assertTrue(fileMgr.reserveFiles(job1));
    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());
    assertEquals(job1.getSTFiles(), fileMgr.getCompactingFiles());

    var job2 = newJob(USER, "F00000.rf", "F00001.rf");
    assertTrue(fileMgr.reserveFiles(job2));
    assertEquals(FileSelectionStatus.RESERVED, fileMgr.getSelectionStatus());
    assertEquals(Sets.union(job1.getSTFiles(), job2.getSTFiles()), fileMgr.getCompactingFiles());

    fileMgr.completed(job2, newFile("C00004.rf"));
    assertEquals(job1.getSTFiles(), fileMgr.getCompactingFiles());
    assertEquals(FileSelectionStatus.NOT_ACTIVE, fileMgr.getSelectionStatus());

    fileMgr.completed(job1, newFile("C00005.rf"));
    assertEquals(Set.of(), fileMgr.getCompactingFiles());
  }

  @Test
  public void testSelectionWaitsForCompaction() {
    TestFileManager fileMgr = new TestFileManager();

    var job1 = newJob(SYSTEM, "F00000.rf", "F00001.rf");
    assertTrue(fileMgr.reserveFiles(job1));

    assertTrue(fileMgr.initiateSelection(USER, 1L));

    // selection was initiated, so a new system compaction should not be able to start
    assertFalse(fileMgr.reserveFiles(newJob(SYSTEM, "F00002.rf", "F00003.rf")));

    // selection can not begin because there is still a compaction running
    assertFalse(fileMgr.beginSelection());

    fileMgr.completed(job1, null);
    assertEquals(Set.of(), fileMgr.getCompactingFiles());

    // now that no compactions are running, selection can begin
    assertTrue(fileMgr.beginSelection());

    assertFalse(fileMgr.reserveFiles(newJob(SYSTEM, "F00002.rf", "F00003.rf")));

    fileMgr.finishSelection(newFiles("F00000.rf", "F00001.rf"), false);
    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());
  }

  @Test
  public void testUserCompactionPreemptsSelectorCompaction() {
    TestFileManager fileMgr = new TestFileManager();

    assertTrue(fileMgr.initiateSelection(SELECTOR, null));
    assertEquals(SELECTOR, fileMgr.getSelectionKind());
    assertTrue(fileMgr.beginSelection());
    // USER compaction should not be able to preempt while in the middle of selecting files
    assertFalse(fileMgr.initiateSelection(USER, 1L));
    assertEquals(SELECTOR, fileMgr.getSelectionKind());
    fileMgr.finishSelection(newFiles("F00000.rf", "F00001.rf", "F00002.rf"), false);
    // check state is as expected after finishing selection
    assertEquals(SELECTOR, fileMgr.getSelectionKind());
    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());
    assertFalse(fileMgr.getSelectedFiles().isEmpty());

    // USER compaction should not be able to preempt when there are running compactions.
    fileMgr.running.add(SELECTOR);
    assertFalse(fileMgr.initiateSelection(USER, 1L));
    // check state is as expected
    assertEquals(SELECTOR, fileMgr.getSelectionKind());
    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());
    assertFalse(fileMgr.getSelectedFiles().isEmpty());

    // after file selection is complete and there are no running compactions, should be able to
    // preempt
    fileMgr.running.clear();
    assertTrue(fileMgr.initiateSelection(USER, 1L));
    // check that things were properly reset
    assertEquals(USER, fileMgr.getSelectionKind());
    assertEquals(FileSelectionStatus.NEW, fileMgr.getSelectionStatus());
    assertTrue(fileMgr.getSelectedFiles().isEmpty());
  }

  @Test
  public void testUserCompactionCanceled() {
    TestFileManager fileMgr = new TestFileManager();
    var tabletFiles = newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf", "F00004.rf");

    assertTrue(fileMgr.initiateSelection(USER, 1L));
    assertTrue(fileMgr.beginSelection());
    fileMgr.finishSelection(
        newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf", "F00004.rf"), false);
    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());

    // since no compactions are running, this should transition selection to not active
    fileMgr.userCompactionCanceled();
    assertEquals(FileSelectionStatus.NOT_ACTIVE, fileMgr.getSelectionStatus());

    assertTrue(fileMgr.initiateSelection(USER, 1L));
    assertTrue(fileMgr.beginSelection());
    fileMgr.finishSelection(
        newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf", "F00004.rf"), false);
    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());

    var job1 = newJob(USER, "F00000.rf", "F00001.rf");
    fileMgr.running.add(USER);
    assertTrue(fileMgr.reserveFiles(job1));
    assertEquals(job1.getSTFiles(), fileMgr.getCompactingFiles());

    var job2 = newJob(USER, "F00002.rf", "F00003.rf");
    assertTrue(fileMgr.reserveFiles(job2));
    assertEquals(Sets.union(job1.getSTFiles(), job2.getSTFiles()), fileMgr.getCompactingFiles());

    // all tablet files are selected, so there are no candidates for system compaction
    assertEquals(Set.of(), fileMgr.getCandidates(tabletFiles, SYSTEM, false));

    // this time when the user compaction is canceled jobs are running, so transition to canceled
    fileMgr.userCompactionCanceled();
    assertEquals(FileSelectionStatus.CANCELED, fileMgr.getSelectionStatus());

    // files that were selected should now be available as candidates after canceling
    assertEquals(newFiles("F00004.rf"), fileMgr.getCandidates(tabletFiles, SYSTEM, false));

    // when this job completes it should not transition from CANCELED to NOT_ACTIVE because there is
    // still another
    fileMgr.completed(job1, newFile("C00005.rf"));
    assertEquals(FileSelectionStatus.CANCELED, fileMgr.getSelectionStatus());
    assertEquals(job2.getSTFiles(), fileMgr.getCompactingFiles());

    fileMgr.running.remove(USER);
    // when this job completes it should transition from CANCELED to NOT_ACTIVE because its the last
    // job. This transition should happen even though not all selected files were compacted (file
    // F00004.rf was never compacted) because its in the canceled state.
    fileMgr.completed(job2, newFile("C00006.rf"));
    assertEquals(FileSelectionStatus.NOT_ACTIVE, fileMgr.getSelectionStatus());
    assertEquals(Set.of(), fileMgr.getCompactingFiles());

  }

  @Test
  public void testChop() {
    TestFileManager fileMgr = new TestFileManager();

    // simulate a compaction because files that were created by compaction are remembered as not
    // needing a chop
    var job1 = newJob(SYSTEM, "F00000.rf", "F00001.rf");
    assertTrue(fileMgr.reserveFiles(job1));
    fileMgr.completed(job1, newFile("C00005.rf"));

    var tabletFiles = newFiles("C00005.rf", "F00002.rf", "F00003.rf", "F00004.rf");

    ChopSelector chopSel = fileMgr.initiateChop(tabletFiles);
    assertEquals(ChopSelectionStatus.SELECTING, fileMgr.getChopStatus());

    assertEquals(Set.of(), fileMgr.getCandidates(tabletFiles, CHOP, false));

    // this should not include C00005.rf because it was created by a compaction observed by the file
    // manager
    assertEquals(newFiles("F00002.rf", "F00003.rf", "F00004.rf"), chopSel.getFilesToExamine());

    chopSel.selectChopFiles(newFiles("F00002.rf", "F00004.rf"));
    assertEquals(ChopSelectionStatus.SELECTED, fileMgr.getChopStatus());

    assertEquals(newFiles("F00002.rf", "F00004.rf"),
        fileMgr.getCandidates(tabletFiles, CHOP, false));

    // simulate compacting one of the files that needs to be chopped, but this should not finish the
    // chop because more files need to be chopped
    var job2 = newJob(CHOP, "F00002.rf");
    assertTrue(fileMgr.reserveFiles(job2));
    fileMgr.completed(job2, newFile("C00006.rf"));
    tabletFiles = newFiles("C00004.rf", "C00006.rf", "F00003.rf", "F00004.rf");
    assertFalse(fileMgr.finishChop(tabletFiles));
    assertEquals(ChopSelectionStatus.SELECTED, fileMgr.getChopStatus());
    assertThrows(IllegalStateException.class, fileMgr::finishMarkingChop);

    assertEquals(newFiles("F00004.rf"), fileMgr.getCandidates(tabletFiles, CHOP, false));

    // simulate compacting the last file to chop. should cause the chop finish
    var job3 = newJob(CHOP, "F00004.rf");
    assertTrue(fileMgr.reserveFiles(job3));
    fileMgr.completed(job3, newFile("C00007.rf"));
    tabletFiles = newFiles("C00004.rf", "C00006.rf", "F00003.rf", "C00007.rf");
    assertTrue(fileMgr.finishChop(tabletFiles));

    assertEquals(Set.of(), fileMgr.getCandidates(tabletFiles, CHOP, false));
    assertEquals(ChopSelectionStatus.MARKING, fileMgr.getChopStatus());
    assertEquals(Set.of(), fileMgr.getCompactingFiles());

    fileMgr.finishMarkingChop();
    assertEquals(ChopSelectionStatus.NOT_ACTIVE, fileMgr.getChopStatus());

  }

  @Test
  public void testComletedUserCompaction() {
    TestFileManager fileMgr = new TestFileManager();

    fileMgr.lastCompactId.set(2);
    // should fail to initiate because the last compact id is equal
    assertFalse(fileMgr.initiateSelection(USER, 2L));
    assertEquals(FileSelectionStatus.NOT_ACTIVE, fileMgr.selectStatus);

    fileMgr.lastCompactId.set(3);
    // should fail to initiate because the last compact id is greater than
    assertFalse(fileMgr.initiateSelection(USER, 2L));
    assertEquals(FileSelectionStatus.NOT_ACTIVE, fileMgr.selectStatus);

    fileMgr.lastCompactId.set(1);
    assertTrue(fileMgr.initiateSelection(USER, 2L));
    assertEquals(FileSelectionStatus.NEW, fileMgr.selectStatus);

    assertTrue(fileMgr.beginSelection());
    fileMgr.finishSelection(
        newFiles("F00000.rf", "F00001.rf", "F00002.rf", "F00003.rf", "F00004.rf"), false);
    assertEquals(FileSelectionStatus.SELECTED, fileMgr.getSelectionStatus());
  }

  @Test
  public void testIllegalInitiateArgs() {
    TestFileManager fileMgr = new TestFileManager();
    assertThrows(IllegalArgumentException.class, () -> fileMgr.initiateSelection(USER, null));
    assertThrows(IllegalArgumentException.class, () -> fileMgr.initiateSelection(SELECTOR, 2L));
    for (var kind : List.of(SYSTEM, CHOP)) {
      assertThrows(IllegalArgumentException.class, () -> fileMgr.initiateSelection(kind, 2L));
      assertThrows(IllegalArgumentException.class, () -> fileMgr.initiateSelection(kind, null));
    }
  }

  private void assertNoCandidates(TestFileManager fileMgr, Set<StoredTabletFile> tabletFiles,
      CompactionKind... kinds) {
    for (CompactionKind kind : kinds) {
      assertEquals(Set.of(), fileMgr.getCandidates(tabletFiles, kind, false));
    }

  }

  static class TestFileManager extends CompactableImpl.FileManager {

    public static final Duration SELECTION_EXPIRATION = Duration.ofMinutes(2);
    private long time = 0;
    public Set<CompactionKind> running = new HashSet<>();
    public AtomicLong lastCompactId = new AtomicLong(0);

    public TestFileManager() {
      super(new KeyExtent(TableId.of("1"), null, null), Set.of(), Optional.empty(),
          () -> SELECTION_EXPIRATION);
    }

    @Override
    protected boolean noneRunning(CompactionKind kind) {
      return !running.contains(kind);
    }

    boolean reserveFiles(TestCompactionJob job) {
      return super.reserveFiles(job, job.getSTFiles());
    }

    void completed(TestCompactionJob job, StoredTabletFile newFile) {
      super.completed(job, job.getSTFiles(), Optional.ofNullable(newFile), true);
    }

    @Override
    protected long getNanoTime() {
      return time;
    }

    @Override
    protected long getLastCompactId() {
      return lastCompactId.get();
    }

    void setNanoTime(long t) {
      time = t;
    }

    Set<StoredTabletFile> getCompactingFiles() {
      return Set.copyOf(allCompactingFiles);
    }

  }

  static StoredTabletFile newFile(String f) {
    return new StoredTabletFile("hdfs://nn1/accumulo/tables/1/t-0001/" + f);
  }

  static Set<StoredTabletFile> newFiles(String... strings) {
    return Stream.of(strings).map(CompactableImplFileManagerTest::newFile)
        .collect(Collectors.toSet());
  }

  private static class TestCompactionJob implements CompactionJob {

    private final CompactionKind kind;
    private final Set<StoredTabletFile> jobFiles;

    public TestCompactionJob(CompactionKind kind, Set<StoredTabletFile> jobFiles) {
      this.kind = kind;
      this.jobFiles = jobFiles;
    }

    @Override
    public short getPriority() {
      return 0;
    }

    @Override
    public CompactionExecutorId getExecutor() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Set<CompactableFile> getFiles() {
      return jobFiles.stream().map(stf -> new CompactableFileImpl(stf, new DataFileValue(0, 0)))
          .collect(Collectors.toSet());
    }

    public Set<StoredTabletFile> getSTFiles() {
      return jobFiles;
    }

    @Override
    public CompactionKind getKind() {
      return kind;
    }

  }

  private TestCompactionJob newJob(CompactionKind kind, String... files) {
    return new TestCompactionJob(kind, newFiles(files));
  }
}

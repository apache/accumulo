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
package org.apache.accumulo.manager.compaction.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;
import org.apache.accumulo.manager.compaction.queue.CompactionJobPriorityQueue.CompactionJobPriorityQueueStats;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues.MetaJob;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class CompactionJobPriorityQueueTest {

  private static final CompactorGroupId GROUP = CompactorGroupId.of("TEST");

  @Test
  public void testTabletFileReplacement() {

    CompactableFile file1 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file2 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file3 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file4 = EasyMock.createMock(CompactableFileImpl.class);

    KeyExtent extent = new KeyExtent(TableId.of("1"), new Text("z"), new Text("a"));
    TabletMetadata tm = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm.getExtent()).andReturn(extent).anyTimes();

    CompactionJob cj1 = EasyMock.createMock(CompactionJob.class);
    EasyMock.expect(cj1.getGroup()).andReturn(GROUP).anyTimes();
    EasyMock.expect(cj1.getPriority()).andReturn((short) 10).anyTimes();
    EasyMock.expect(cj1.getFiles()).andReturn(Set.of(file1)).anyTimes();

    CompactionJob cj2 = EasyMock.createMock(CompactionJob.class);
    EasyMock.expect(cj2.getGroup()).andReturn(GROUP).anyTimes();
    EasyMock.expect(cj2.getPriority()).andReturn((short) 5).anyTimes();
    EasyMock.expect(cj2.getFiles()).andReturn(Set.of(file2, file3, file4)).anyTimes();

    EasyMock.replay(tm, cj1, cj2);

    CompactionJobPriorityQueue queue = new CompactionJobPriorityQueue(GROUP, 2);
    assertEquals(1, queue.add(tm, List.of(cj1), 1L));

    MetaJob job = queue.peek();
    assertEquals(cj1, job.getJob());
    assertEquals(Set.of(file1), job.getJob().getFiles());

    assertEquals(10L, queue.getLowestPriority());
    assertEquals(2, queue.getMaxSize());
    assertEquals(0, queue.getDequeuedJobs());
    assertEquals(0, queue.getRejectedJobs());
    assertEquals(1, queue.getQueuedJobs());

    // replace the files for the same tablet
    assertEquals(1, queue.add(tm, List.of(cj2), 1L));

    job = queue.peek();
    assertEquals(cj2, job.getJob());
    assertEquals(Set.of(file2, file3, file4), job.getJob().getFiles());
    assertEquals(tm, job.getTabletMetadata());

    assertEquals(5L, queue.getLowestPriority());
    assertEquals(2, queue.getMaxSize());
    assertEquals(0, queue.getDequeuedJobs());
    assertEquals(0, queue.getRejectedJobs());
    assertEquals(1, queue.getQueuedJobs());

    EasyMock.verify(tm, cj1, cj2);

  }

  @Test
  public void testAddEqualToMaxSize() {

    CompactableFile file1 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file2 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file3 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file4 = EasyMock.createMock(CompactableFileImpl.class);

    KeyExtent extent = new KeyExtent(TableId.of("1"), new Text("z"), new Text("a"));
    TabletMetadata tm = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm.getExtent()).andReturn(extent).anyTimes();

    CompactionJob cj1 = EasyMock.createMock(CompactionJob.class);
    EasyMock.expect(cj1.getGroup()).andReturn(GROUP).anyTimes();
    EasyMock.expect(cj1.getPriority()).andReturn((short) 10).anyTimes();
    EasyMock.expect(cj1.getFiles()).andReturn(Set.of(file1)).anyTimes();

    CompactionJob cj2 = EasyMock.createMock(CompactionJob.class);
    EasyMock.expect(cj2.getGroup()).andReturn(GROUP).anyTimes();
    EasyMock.expect(cj2.getPriority()).andReturn((short) 5).anyTimes();
    EasyMock.expect(cj2.getFiles()).andReturn(Set.of(file2, file3, file4)).anyTimes();

    EasyMock.replay(tm, cj1, cj2);

    CompactionJobPriorityQueue queue = new CompactionJobPriorityQueue(GROUP, 2);
    assertEquals(2, queue.add(tm, List.of(cj1, cj2), 1L));

    EasyMock.verify(tm, cj1, cj2);

    assertEquals(5L, queue.getLowestPriority());
    assertEquals(2, queue.getMaxSize());
    assertEquals(0, queue.getDequeuedJobs());
    assertEquals(0, queue.getRejectedJobs());
    assertEquals(2, queue.getQueuedJobs());
    MetaJob job = queue.poll();
    assertEquals(cj1, job.getJob());
    assertEquals(tm, job.getTabletMetadata());
    assertEquals(1, queue.getDequeuedJobs());

    job = queue.poll();
    assertEquals(cj2, job.getJob());
    assertEquals(tm, job.getTabletMetadata());
    assertEquals(2, queue.getDequeuedJobs());

    job = queue.poll();
    assertNull(job);
    assertEquals(2, queue.getDequeuedJobs());

  }

  @Test
  public void testAddMoreThanMax() {

    CompactableFile file1 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file2 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file3 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file4 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file5 = EasyMock.createMock(CompactableFileImpl.class);
    CompactableFile file6 = EasyMock.createMock(CompactableFileImpl.class);

    KeyExtent extent = new KeyExtent(TableId.of("1"), new Text("z"), new Text("a"));
    TabletMetadata tm = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm.getExtent()).andReturn(extent).anyTimes();

    CompactionJob cj1 = EasyMock.createMock(CompactionJob.class);
    EasyMock.expect(cj1.getGroup()).andReturn(GROUP).anyTimes();
    EasyMock.expect(cj1.getPriority()).andReturn((short) 10).anyTimes();
    EasyMock.expect(cj1.getFiles()).andReturn(Set.of(file1)).anyTimes();

    CompactionJob cj2 = EasyMock.createMock(CompactionJob.class);
    EasyMock.expect(cj2.getGroup()).andReturn(GROUP).anyTimes();
    EasyMock.expect(cj2.getPriority()).andReturn((short) 5).anyTimes();
    EasyMock.expect(cj2.getFiles()).andReturn(Set.of(file2, file3, file4)).anyTimes();

    CompactionJob cj3 = EasyMock.createMock(CompactionJob.class);
    EasyMock.expect(cj3.getGroup()).andReturn(GROUP).anyTimes();
    EasyMock.expect(cj3.getPriority()).andReturn((short) 1).anyTimes();
    EasyMock.expect(cj3.getFiles()).andReturn(Set.of(file5, file6)).anyTimes();

    EasyMock.replay(tm, cj1, cj2, cj3);

    CompactionJobPriorityQueue queue = new CompactionJobPriorityQueue(GROUP, 2);
    assertEquals(2, queue.add(tm, List.of(cj1, cj2, cj3), 1L));

    EasyMock.verify(tm, cj1, cj2, cj3);

    assertEquals(5L, queue.getLowestPriority());
    assertEquals(2, queue.getMaxSize());
    assertEquals(0, queue.getDequeuedJobs());
    assertEquals(1, queue.getRejectedJobs());
    assertEquals(2, queue.getQueuedJobs());
    // One tablet was added with jobs
    assertEquals(1, queue.getJobAges().size());

    MetaJob job = queue.poll();
    assertEquals(cj1, job.getJob());
    assertEquals(tm, job.getTabletMetadata());
    assertEquals(1, queue.getDequeuedJobs());
    // still 1 job left so should still have a timer
    assertEquals(1, queue.getJobAges().size());

    job = queue.poll();
    assertEquals(cj2, job.getJob());
    assertEquals(tm, job.getTabletMetadata());
    assertEquals(2, queue.getDequeuedJobs());
    // no more jobs so timer should be gone
    assertTrue(queue.getJobAges().isEmpty());

    job = queue.poll();
    assertNull(job);
    assertEquals(2, queue.getDequeuedJobs());
  }

  private static int counter = 1;

  private Pair<TabletMetadata,CompactionJob> createJob() {

    // Use an ever increasing tableId
    KeyExtent extent = new KeyExtent(TableId.of("" + counter++), new Text("z"), new Text("a"));

    Set<CompactableFile> files = new HashSet<>();
    for (int i = 0; i < counter; i++) {
      files.add(EasyMock.createMock(CompactableFileImpl.class));
    }

    CompactionJob job = EasyMock.createMock(CompactionJob.class);
    TabletMetadata tm = EasyMock.createMock(TabletMetadata.class);
    EasyMock.expect(tm.getExtent()).andReturn(extent).anyTimes();
    EasyMock.expect(job.getGroup()).andReturn(GROUP).anyTimes();
    EasyMock.expect(job.getPriority()).andReturn((short) counter).anyTimes();
    EasyMock.expect(job.getFiles()).andReturn(files).anyTimes();

    EasyMock.replay(tm, job);

    return new Pair<>(tm, job);
  }

  @Test
  public void test() {

    TreeSet<CompactionJob> expected = new TreeSet<>(CompactionJobPrioritizer.JOB_COMPARATOR);

    CompactionJobPriorityQueue queue = new CompactionJobPriorityQueue(GROUP, 100);

    // create and add 1000 jobs
    for (int x = 0; x < 1000; x++) {
      Pair<TabletMetadata,CompactionJob> pair = createJob();
      queue.add(pair.getFirst(), Set.of(pair.getSecond()), 1L);
      expected.add(pair.getSecond());
    }

    assertEquals(100, queue.getMaxSize());
    assertEquals(100, queue.getQueuedJobs());
    assertEquals(900, queue.getRejectedJobs());
    // There should be 1000 total job ages even though 900 were rejected
    // as there were 1000 total tablets added
    assertEquals(1000, queue.getJobAges().size());

    var stats = queue.getJobQueueStats();
    assertTrue(stats.getMinAge().toMillis() > 0);
    assertTrue(stats.getMaxAge().toMillis() > 0);
    assertTrue(stats.getAvgAge().toMillis() > 0);

    // iterate over the expected set and make sure that they next job in the queue
    // matches
    int matchesSeen = 0;
    for (CompactionJob expectedJob : expected) {
      MetaJob queuedJob = queue.poll();
      if (queuedJob == null) {
        break;
      }
      assertEquals(expectedJob.getPriority(), queuedJob.getJob().getPriority());
      assertEquals(expectedJob.getFiles(), queuedJob.getJob().getFiles());
      matchesSeen++;
    }

    assertEquals(100, matchesSeen);
    // Should be 900 left as the 100 that were polled would clear as there are no more
    // jobs for those tablets. These 900 were rejected so their timers remain and will
    // be cleared if there are no computed jobs when jobs are added again or by
    // the call to removeOlderGenerations()
    assertEquals(900, queue.getJobAges().size());

    // Create new stats directly vs using queue.getJobQueueStats() because that method
    // caches the results for a short period
    stats = new CompactionJobPriorityQueueStats(queue.getJobAges());
    assertTrue(stats.getMinAge().toMillis() > 0);
    assertTrue(stats.getMaxAge().toMillis() > 0);
    assertTrue(stats.getAvgAge().toMillis() > 0);

    // Verify jobAges cleared when calling removeOlderGenerations()
    queue.removeOlderGenerations(DataLevel.USER, 2);

    // Stats should be 0 if no jobs
    var jobAges = queue.getJobAges();
    assertTrue(jobAges.isEmpty());
    stats = new CompactionJobPriorityQueueStats(queue.getJobAges());
    assertEquals(0, stats.getMinAge().toMillis());
    assertEquals(0, stats.getMaxAge().toMillis());
    assertEquals(0, stats.getAvgAge().toMillis());
  }

  /**
   * Test to ensure that canceled futures do not build up in memory.
   */
  @Test
  public void testAsyncCancelCleanup() {
    CompactionJobPriorityQueue queue = new CompactionJobPriorityQueue(GROUP, 100);

    List<CompletableFuture<MetaJob>> futures = new ArrayList<>();

    int maxFuturesSize = 0;

    // Add 11 below so that cadence of clearing differs from the internal check cadence
    final int CANCEL_THRESHOLD = CompactionJobPriorityQueue.FUTURE_CHECK_THRESHOLD / 10 + 11;
    final int ITERATIONS = CompactionJobPriorityQueue.FUTURE_CHECK_THRESHOLD * 20;

    for (int x = 0; x < ITERATIONS; x++) {
      futures.add(queue.getAsync());

      maxFuturesSize = Math.max(maxFuturesSize, queue.futuresSize());

      if (futures.size() >= CANCEL_THRESHOLD) {
        futures.forEach(f -> f.cancel(true));
        futures.clear();
      }
    }

    maxFuturesSize = Math.max(maxFuturesSize, queue.futuresSize());

    assertTrue(maxFuturesSize
        < 2 * (CompactionJobPriorityQueue.FUTURE_CHECK_THRESHOLD + CANCEL_THRESHOLD));
    assertTrue(maxFuturesSize > 2 * CompactionJobPriorityQueue.FUTURE_CHECK_THRESHOLD);
  }

  @Test
  public void testChangeMaxSize() {
    CompactionJobPriorityQueue queue = new CompactionJobPriorityQueue(GROUP, 100);
    assertEquals(100, queue.getMaxSize());
    queue.setMaxSize(50);
    assertEquals(50, queue.getMaxSize());
    assertThrows(IllegalArgumentException.class, () -> queue.setMaxSize(0));
    assertThrows(IllegalArgumentException.class, () -> queue.setMaxSize(-1));
    // Make sure previous value was not changed after invalid setting
    assertEquals(50, queue.getMaxSize());
  }

}

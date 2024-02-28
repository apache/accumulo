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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.core.util.compaction.CompactionJobPrioritizer;
import org.apache.accumulo.core.util.compaction.CompactorGroupIdImpl;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues.MetaJob;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

public class CompactionJobPriorityQueueTest {

  private static final CompactorGroupId GROUP = CompactorGroupIdImpl.groupId("TEST");

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
    assertEquals(1, queue.add(tm, List.of(cj1)));

    MetaJob job = queue.peek();
    assertEquals(cj1, job.getJob());
    assertEquals(Set.of(file1), job.getJob().getFiles());

    assertEquals(10L, queue.getLowestPriority());
    assertEquals(2, queue.getMaxSize());
    assertEquals(0, queue.getDequeuedJobs());
    assertEquals(0, queue.getRejectedJobs());
    assertEquals(1, queue.getQueuedJobs());

    // replace the files for the same tablet
    assertEquals(1, queue.add(tm, List.of(cj2)));

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
    assertEquals(2, queue.add(tm, List.of(cj1, cj2)));

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
    assertEquals(2, queue.add(tm, List.of(cj1, cj2, cj3)));

    EasyMock.verify(tm, cj1, cj2, cj3);

    assertEquals(5L, queue.getLowestPriority());
    assertEquals(2, queue.getMaxSize());
    assertEquals(0, queue.getDequeuedJobs());
    assertEquals(1, queue.getRejectedJobs());
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
  public void testAddAfterClose() {

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
    assertEquals(2, queue.add(tm, List.of(cj1, cj2)));

    assertFalse(queue.closeIfEmpty());

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

    MetaJob job2 = queue.poll();
    assertEquals(cj2, job2.getJob());
    assertEquals(tm, job2.getTabletMetadata());
    assertEquals(2, queue.getDequeuedJobs());

    assertTrue(queue.closeIfEmpty());

    assertEquals(-1, queue.add(tm, List.of(cj1, cj2)));

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
      queue.add(pair.getFirst(), Set.of(pair.getSecond()));
      expected.add(pair.getSecond());
    }

    assertEquals(100, queue.getMaxSize());
    assertEquals(100, queue.getQueuedJobs());
    assertEquals(900, queue.getRejectedJobs());

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
  }
}

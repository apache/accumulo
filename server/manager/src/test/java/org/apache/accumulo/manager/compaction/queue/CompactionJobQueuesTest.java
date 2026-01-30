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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class CompactionJobQueuesTest {

  private CompactionJob newJob(short prio, int file, ResourceGroupId cgi)
      throws URISyntaxException {
    Collection<CompactableFile> files = List
        .of(new CompactableFileImpl(new URI("file://accumulo/tables//123/t-0/f" + file), 100, 100));
    return new CompactionJobImpl(prio, cgi, files, CompactionKind.SYSTEM);
  }

  @Test
  public void testFullScanHandling() throws Exception {

    var tid = TableId.of("1");
    var extent1 = new KeyExtent(tid, new Text("z"), new Text("q"));
    var extent2 = new KeyExtent(tid, new Text("q"), new Text("l"));
    var extent3 = new KeyExtent(tid, new Text("l"), new Text("c"));
    var extent4 = new KeyExtent(tid, new Text("c"), new Text("a"));

    var cg1 = ResourceGroupId.of("CG1");
    var cg2 = ResourceGroupId.of("CG2");
    var cg3 = ResourceGroupId.of("CG3");

    CompactionJobQueues jobQueues = new CompactionJobQueues(1000000);

    jobQueues.beginFullScan(DataLevel.USER);

    jobQueues.add(extent1, List.of(newJob((short) 1, 5, cg1)));
    jobQueues.add(extent2, List.of(newJob((short) 2, 6, cg1)));
    jobQueues.add(extent3, List.of(newJob((short) 3, 7, cg1)));
    jobQueues.add(extent4, List.of(newJob((short) 4, 8, cg1)));

    jobQueues.add(extent1, List.of(newJob((short) 4, 1, cg2)));
    jobQueues.add(extent2, List.of(newJob((short) 3, 2, cg2)));
    jobQueues.add(extent3, List.of(newJob((short) 2, 3, cg2)));
    jobQueues.add(extent4, List.of(newJob((short) 1, 4, cg2)));

    jobQueues.endFullScan(DataLevel.USER);

    assertEquals(4, jobQueues.getQueuedJobs(cg1));
    assertEquals(4, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    assertEquals(4, jobQueues.poll(cg1).getPriority());
    assertEquals(4, jobQueues.poll(cg2).getPriority());

    assertEquals(3, jobQueues.getQueuedJobs(cg1));
    assertEquals(3, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    jobQueues.beginFullScan(DataLevel.USER);

    // should still be able to poll and get things added in the last full scan
    assertEquals(3, jobQueues.poll(cg1).getPriority());
    assertEquals(2, jobQueues.getQueuedJobs(cg1));
    assertEquals(3, jobQueues.getQueuedJobs(cg2));

    // add something new during the full scan
    jobQueues.add(extent1, List.of(newJob((short) -7, 9, cg2)));
    assertEquals(2, jobQueues.getQueuedJobs(cg1));
    assertEquals(4, jobQueues.getQueuedJobs(cg2));

    // should still be able to poll and get things added in the last full scan
    assertEquals(3, jobQueues.poll(cg2).getPriority());
    assertEquals(2, jobQueues.getQueuedJobs(cg1));
    assertEquals(3, jobQueues.getQueuedJobs(cg2));

    // this should remove anything that was added before begin full scan was called
    jobQueues.endFullScan(DataLevel.USER);

    assertEquals(0, jobQueues.getQueuedJobs(cg1));
    assertEquals(1, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    assertNull(jobQueues.poll(cg1));
    assertEquals(-7, jobQueues.poll(cg2).getPriority());

    assertEquals(0, jobQueues.getQueuedJobs(cg1));
    assertEquals(0, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // add some things outside of a begin/end full scan calls
    jobQueues.add(extent1, List.of(newJob((short) 1, 5, cg1)));
    jobQueues.add(extent2, List.of(newJob((short) 2, 6, cg1)));

    jobQueues.add(extent1, List.of(newJob((short) 4, 1, cg2)));
    jobQueues.add(extent2, List.of(newJob((short) 3, 2, cg2)));

    jobQueues.beginFullScan(DataLevel.USER);

    assertEquals(2, jobQueues.getQueuedJobs(cg1));
    assertEquals(2, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // add some things inside the begin/end full scan calls
    jobQueues.add(extent3, List.of(newJob((short) 3, 7, cg1)));
    jobQueues.add(extent4, List.of(newJob((short) 4, 8, cg1)));

    jobQueues.add(extent3, List.of(newJob((short) 2, 3, cg2)));
    jobQueues.add(extent4, List.of(newJob((short) 1, 4, cg2)));

    assertEquals(4, jobQueues.getQueuedJobs(cg1));
    assertEquals(4, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // poll inside the full scan calls
    assertEquals(4, jobQueues.poll(cg1).getPriority());
    assertEquals(4, jobQueues.poll(cg2).getPriority());

    assertEquals(3, jobQueues.getQueuedJobs(cg1));
    assertEquals(3, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // should remove any tablets added before the full scan started
    jobQueues.endFullScan(DataLevel.USER);

    assertEquals(1, jobQueues.getQueuedJobs(cg1));
    assertEquals(2, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    assertEquals(3, jobQueues.poll(cg1).getPriority());
    assertEquals(2, jobQueues.poll(cg2).getPriority());
    assertEquals(1, jobQueues.poll(cg2).getPriority());

    assertNull(jobQueues.poll(cg1));
    assertNull(jobQueues.poll(cg2));
    assertNull(jobQueues.poll(cg3));

    assertEquals(0, jobQueues.getQueuedJobs(cg1));
    assertEquals(0, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // add jobs outside of begin/end full scan
    jobQueues.add(extent1, List.of(newJob((short) 1, 5, cg1)));
    jobQueues.add(extent2, List.of(newJob((short) 2, 6, cg1)));
    jobQueues.add(extent3, List.of(newJob((short) 3, 7, cg1)));
    jobQueues.add(extent4, List.of(newJob((short) 4, 8, cg1)));

    jobQueues.add(extent1, List.of(newJob((short) 4, 1, cg2)));
    jobQueues.add(extent2, List.of(newJob((short) 3, 2, cg2)));
    jobQueues.add(extent3, List.of(newJob((short) 2, 3, cg2)));
    jobQueues.add(extent4, List.of(newJob((short) 1, 4, cg2)));

    jobQueues.beginFullScan(DataLevel.USER);

    // readd some of the tablets added before the beginFullScan, this should prevent those tablets
    // from being removed by endFullScan
    jobQueues.add(extent4, List.of(newJob((short) 5, 5, cg2)));
    jobQueues.add(extent1, List.of(newJob((short) -7, 5, cg2)));

    assertEquals(4, jobQueues.getQueuedJobs(cg1));
    assertEquals(4, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // should remove all jobs added before begin full scan
    jobQueues.endFullScan(DataLevel.USER);

    assertEquals(0, jobQueues.getQueuedJobs(cg1));
    assertEquals(2, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // make sure we see what was added last for the tablets
    assertEquals(5, jobQueues.poll(cg2).getPriority());
    assertEquals(-7, jobQueues.poll(cg2).getPriority());

    assertEquals(0, jobQueues.getQueuedJobs(cg1));
    assertEquals(0, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    assertNull(jobQueues.poll(cg1));
    assertNull(jobQueues.poll(cg2));
    assertNull(jobQueues.poll(cg3));
  }

  @Test
  public void testFullScanLevels() throws Exception {
    var tid = TableId.of("1");
    var extent1 = new KeyExtent(tid, new Text("z"), new Text("q"));
    var extent2 = new KeyExtent(tid, new Text("q"), new Text("l"));
    var meta = new KeyExtent(SystemTables.METADATA.tableId(), new Text("l"), new Text("c"));
    var root = RootTable.EXTENT;

    var cg1 = ResourceGroupId.of("CG1");

    CompactionJobQueues jobQueues = new CompactionJobQueues(1000000);

    jobQueues.add(extent1, List.of(newJob((short) 1, 5, cg1)));
    jobQueues.add(extent2, List.of(newJob((short) 2, 6, cg1)));
    jobQueues.add(meta, List.of(newJob((short) 3, 7, cg1)));
    jobQueues.add(root, List.of(newJob((short) 4, 8, cg1)));

    // verify that a begin and end full scan will only drop tablets in its level

    jobQueues.beginFullScan(DataLevel.ROOT);
    assertEquals(4, jobQueues.getQueuedJobs(cg1));
    jobQueues.endFullScan(DataLevel.ROOT);
    assertEquals(3, jobQueues.getQueuedJobs(cg1));

    jobQueues.beginFullScan(DataLevel.USER);
    assertEquals(3, jobQueues.getQueuedJobs(cg1));
    jobQueues.endFullScan(DataLevel.USER);
    assertEquals(1, jobQueues.getQueuedJobs(cg1));

    jobQueues.beginFullScan(DataLevel.METADATA);
    assertEquals(1, jobQueues.getQueuedJobs(cg1));
    jobQueues.endFullScan(DataLevel.METADATA);
    assertEquals(0, jobQueues.getQueuedJobs(cg1));
  }

  /**
   * When a queue goes empty it is removed. This removal should be done safely and should not cause
   * any data being concurrently added to be lost. The way this test adds and removes data in
   * multiple threads it should cause poll() to empty a queue and delete it while another thread is
   * adding, no loss of jobs should happen when this occurs.
   */
  @Test
  public void testAddPollRaceCondition() throws Exception {

    final int numToAdd = 100_000;

    CompactionJobQueues jobQueues = new CompactionJobQueues(10000000);
    ResourceGroupId[] groups =
        Stream.of("G1", "G2", "G3").map(ResourceGroupId::of).toArray(ResourceGroupId[]::new);

    var executor = Executors.newFixedThreadPool(groups.length);

    List<Future<Integer>> futures = new ArrayList<>();

    AtomicBoolean stop = new AtomicBoolean(false);

    // create a background thread per a group that polls jobs for the group
    for (var group : groups) {
      var future = executor.submit(() -> {
        int seen = 0;
        while (!stop.get()) {
          var job = jobQueues.poll(group);
          if (job != null) {
            seen++;
          }
        }

        // After stop was set, nothing should be added to queues anymore. Drain anything that is
        // present and then exit.
        while (jobQueues.poll(group) != null) {
          seen++;
        }

        return seen;
      });
      futures.add(future);
    }

    // Add jobs to queues spread across the groups. While these are being added the background
    // threads should concurrently empty queues causing them to be deleted.
    for (int i = 0; i < numToAdd; i++) {
      // Create unique exents because re-adding the same extent will clobber any jobs already in the
      // queue for that extent which could throw off the counts
      KeyExtent extent = new KeyExtent(TableId.of("1"), new Text(i + "z"), new Text(i + "a"));
      jobQueues.add(extent, List.of(newJob((short) (i % 31), i, groups[i % groups.length])));
    }

    // Cause the background threads to exit after polling all data
    stop.set(true);

    // Count the total jobs seen by background threads
    int totalSeen = 0;
    for (var future : futures) {
      totalSeen += future.get();
    }

    executor.shutdown();

    // The background threads should have seen every job that was added
    assertEquals(numToAdd, totalSeen);
  }

  @Test
  public void testGetAsync() throws Exception {
    CompactionJobQueues jobQueues = new CompactionJobQueues(1000000);

    var tid = TableId.of("1");
    var extent1 = new KeyExtent(tid, new Text("z"), new Text("q"));
    var extent2 = new KeyExtent(tid, new Text("q"), new Text("l"));
    var extent3 = new KeyExtent(tid, new Text("l"), new Text("c"));
    var extent4 = new KeyExtent(tid, new Text("c"), new Text("a"));

    var cg1 = ResourceGroupId.of("CG1");

    var job1 = newJob((short) 1, 5, cg1);
    var job2 = newJob((short) 2, 6, cg1);
    var job3 = newJob((short) 3, 7, cg1);
    var job4 = newJob((short) 4, 8, cg1);

    var future1 = jobQueues.getAsync(cg1);
    var future2 = jobQueues.getAsync(cg1);

    assertFalse(future1.isDone());
    assertFalse(future2.isDone());

    jobQueues.add(extent1, List.of(job1));
    jobQueues.add(extent2, List.of(job2));
    // Futures were immediately completed so nothing should be queued
    assertTrue(jobQueues.getQueue(cg1).getJobAges().isEmpty());

    jobQueues.add(extent3, List.of(job3));
    jobQueues.add(extent4, List.of(job4));
    // No futures available, so jobAges should exist for 2 tablets
    assertEquals(2, jobQueues.getQueue(cg1).getJobAges().size());

    var future3 = jobQueues.getAsync(cg1);
    var future4 = jobQueues.getAsync(cg1);

    // Should be back to 0 size after futures complete
    assertTrue(jobQueues.getQueue(cg1).getJobAges().isEmpty());

    assertTrue(future1.isDone());
    assertTrue(future2.isDone());
    assertTrue(future3.isDone());
    assertTrue(future4.isDone());

    assertEquals(job1, future1.get());
    assertEquals(job2, future2.get());
    assertEquals(job4, future3.get());
    assertEquals(job3, future4.get());

    // test cancelling a future and having a future timeout
    var future5 = jobQueues.getAsync(cg1);
    assertFalse(future5.isDone());
    future5.cancel(false);
    var future6 = jobQueues.getAsync(cg1);
    assertFalse(future6.isDone());
    future6.orTimeout(10, TimeUnit.MILLISECONDS);
    // Wait for future6 to timeout to make sure future7 will
    // receive the job when added to the queue
    var ex = assertThrows(ExecutionException.class, future6::get);
    assertInstanceOf(TimeoutException.class, ex.getCause());
    var future7 = jobQueues.getAsync(cg1);
    assertFalse(future7.isDone());
    // since future5 was canceled and future6 timed out, this addition should go to future7
    var job5 = newJob((short) 1, 5, cg1);
    jobQueues.add(extent1, List.of(job5));
    assertTrue(future7.isDone());
    assertEquals(job5, future7.get());
    assertTrue(future5.isDone());
    assertTrue(future6.isCompletedExceptionally());
    assertTrue(future6.isDone());
  }

  @Test
  public void testResetSize() throws Exception {
    CompactionJobQueues jobQueues = new CompactionJobQueues(1000000);

    var tid = TableId.of("1");
    var extent1 = new KeyExtent(tid, new Text("z"), new Text("q"));

    var cg1 = ResourceGroupId.of("CG1");
    var cg2 = ResourceGroupId.of("CG2");

    jobQueues.add(extent1, List.of(newJob((short) 1, 5, cg1)));

    assertEquals(Set.of(cg1), jobQueues.getQueueIds());
    assertEquals(1000000, jobQueues.getQueueMaxSize(cg1));

    jobQueues.resetMaxSize(500000);

    assertEquals(Set.of(cg1), jobQueues.getQueueIds());
    assertEquals(500000, jobQueues.getQueueMaxSize(cg1));

    // create a new queue and ensure it uses the updated max size
    jobQueues.add(extent1, List.of(newJob((short) 1, 5, cg2)));
    assertEquals(Set.of(cg1, cg2), jobQueues.getQueueIds());
    assertEquals(500000, jobQueues.getQueueMaxSize(cg1));
    assertEquals(500000, jobQueues.getQueueMaxSize(cg2));
  }
}

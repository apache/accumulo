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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class CompactionJobQueuesTest {

  private CompactionJob newJob(short prio, int file, CompactorGroupId cgi)
      throws URISyntaxException {
    Collection<CompactableFile> files = List
        .of(new CompactableFileImpl(new URI("file://accumulo/tables//123/t-0/f" + file), 100, 100));
    return new CompactionJobImpl(prio, cgi, files, CompactionKind.SYSTEM, Optional.empty());
  }

  @Test
  public void testFullScanHandling() throws Exception {

    var tid = TableId.of("1");
    var extent1 = new KeyExtent(tid, new Text("z"), new Text("q"));
    var extent2 = new KeyExtent(tid, new Text("q"), new Text("l"));
    var extent3 = new KeyExtent(tid, new Text("l"), new Text("c"));
    var extent4 = new KeyExtent(tid, new Text("c"), new Text("a"));

    var tm1 = TabletMetadata.builder(extent1).build();
    var tm2 = TabletMetadata.builder(extent2).build();
    var tm3 = TabletMetadata.builder(extent3).build();
    var tm4 = TabletMetadata.builder(extent4).build();

    var cg1 = CompactorGroupId.of("CG1");
    var cg2 = CompactorGroupId.of("CG2");
    var cg3 = CompactorGroupId.of("CG3");

    CompactionJobQueues jobQueues = new CompactionJobQueues(100);

    jobQueues.beginFullScan(DataLevel.USER);

    jobQueues.add(tm1, List.of(newJob((short) 1, 5, cg1)));
    jobQueues.add(tm2, List.of(newJob((short) 2, 6, cg1)));
    jobQueues.add(tm3, List.of(newJob((short) 3, 7, cg1)));
    jobQueues.add(tm4, List.of(newJob((short) 4, 8, cg1)));

    jobQueues.add(tm1, List.of(newJob((short) 4, 1, cg2)));
    jobQueues.add(tm2, List.of(newJob((short) 3, 2, cg2)));
    jobQueues.add(tm3, List.of(newJob((short) 2, 3, cg2)));
    jobQueues.add(tm4, List.of(newJob((short) 1, 4, cg2)));

    jobQueues.endFullScan(DataLevel.USER);

    assertEquals(4, jobQueues.getQueuedJobs(cg1));
    assertEquals(4, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    assertEquals(extent4, jobQueues.poll(cg1).getTabletMetadata().getExtent());
    assertEquals(extent1, jobQueues.poll(cg2).getTabletMetadata().getExtent());

    assertEquals(3, jobQueues.getQueuedJobs(cg1));
    assertEquals(3, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    jobQueues.beginFullScan(DataLevel.USER);

    // should still be able to poll and get things added in the last full scan
    assertEquals(extent3, jobQueues.poll(cg1).getTabletMetadata().getExtent());
    assertEquals(2, jobQueues.getQueuedJobs(cg1));
    assertEquals(3, jobQueues.getQueuedJobs(cg2));

    // add something new during the full scan
    jobQueues.add(tm1, List.of(newJob((short) -7, 9, cg2)));
    assertEquals(2, jobQueues.getQueuedJobs(cg1));
    assertEquals(4, jobQueues.getQueuedJobs(cg2));

    // should still be able to poll and get things added in the last full scan
    assertEquals(extent2, jobQueues.poll(cg2).getTabletMetadata().getExtent());
    assertEquals(2, jobQueues.getQueuedJobs(cg1));
    assertEquals(3, jobQueues.getQueuedJobs(cg2));

    // this should remove anything that was added before begin full scan was called
    jobQueues.endFullScan(DataLevel.USER);

    assertEquals(0, jobQueues.getQueuedJobs(cg1));
    assertEquals(1, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    assertNull(jobQueues.poll(cg1));
    assertEquals(extent1, jobQueues.poll(cg2).getTabletMetadata().getExtent());

    assertEquals(0, jobQueues.getQueuedJobs(cg1));
    assertEquals(0, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // add some things outside of a begin/end full scan calls
    jobQueues.add(tm1, List.of(newJob((short) 1, 5, cg1)));
    jobQueues.add(tm2, List.of(newJob((short) 2, 6, cg1)));

    jobQueues.add(tm1, List.of(newJob((short) 4, 1, cg2)));
    jobQueues.add(tm2, List.of(newJob((short) 3, 2, cg2)));

    jobQueues.beginFullScan(DataLevel.USER);

    assertEquals(2, jobQueues.getQueuedJobs(cg1));
    assertEquals(2, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // add some things inside the begin/end full scan calls
    jobQueues.add(tm3, List.of(newJob((short) 3, 7, cg1)));
    jobQueues.add(tm4, List.of(newJob((short) 4, 8, cg1)));

    jobQueues.add(tm3, List.of(newJob((short) 2, 3, cg2)));
    jobQueues.add(tm4, List.of(newJob((short) 1, 4, cg2)));

    assertEquals(4, jobQueues.getQueuedJobs(cg1));
    assertEquals(4, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // poll inside the full scan calls
    assertEquals(extent4, jobQueues.poll(cg1).getTabletMetadata().getExtent());
    assertEquals(extent1, jobQueues.poll(cg2).getTabletMetadata().getExtent());

    assertEquals(3, jobQueues.getQueuedJobs(cg1));
    assertEquals(3, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // should remove any tablets added before the full scan started
    jobQueues.endFullScan(DataLevel.USER);

    assertEquals(1, jobQueues.getQueuedJobs(cg1));
    assertEquals(2, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    assertEquals(extent3, jobQueues.poll(cg1).getTabletMetadata().getExtent());
    assertEquals(extent3, jobQueues.poll(cg2).getTabletMetadata().getExtent());
    assertEquals(extent4, jobQueues.poll(cg2).getTabletMetadata().getExtent());

    assertNull(jobQueues.poll(cg1));
    assertNull(jobQueues.poll(cg2));
    assertNull(jobQueues.poll(cg3));

    assertEquals(0, jobQueues.getQueuedJobs(cg1));
    assertEquals(0, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // add jobs outside of begin/end full scan
    jobQueues.add(tm1, List.of(newJob((short) 1, 5, cg1)));
    jobQueues.add(tm2, List.of(newJob((short) 2, 6, cg1)));
    jobQueues.add(tm3, List.of(newJob((short) 3, 7, cg1)));
    jobQueues.add(tm4, List.of(newJob((short) 4, 8, cg1)));

    jobQueues.add(tm1, List.of(newJob((short) 4, 1, cg2)));
    jobQueues.add(tm2, List.of(newJob((short) 3, 2, cg2)));
    jobQueues.add(tm3, List.of(newJob((short) 2, 3, cg2)));
    jobQueues.add(tm4, List.of(newJob((short) 1, 4, cg2)));

    jobQueues.beginFullScan(DataLevel.USER);

    // readd some of the tablets added before the beginFullScan, this should prevent those tablets
    // from being removed by endFullScan
    jobQueues.add(tm4, List.of(newJob((short) 5, 5, cg2)));
    jobQueues.add(tm1, List.of(newJob((short) -7, 5, cg2)));

    assertEquals(4, jobQueues.getQueuedJobs(cg1));
    assertEquals(4, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // should remove all jobs added before begin full scan
    jobQueues.endFullScan(DataLevel.USER);

    assertEquals(0, jobQueues.getQueuedJobs(cg1));
    assertEquals(2, jobQueues.getQueuedJobs(cg2));
    assertEquals(0, jobQueues.getQueuedJobs(cg3));

    // make sure we see what was added last for the tablets
    assertEquals(5, jobQueues.poll(cg2).getJob().getPriority());
    assertEquals(-7, jobQueues.poll(cg2).getJob().getPriority());

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
    var meta = new KeyExtent(AccumuloTable.METADATA.tableId(), new Text("l"), new Text("c"));
    var root = RootTable.EXTENT;

    var tm1 = TabletMetadata.builder(extent1).build();
    var tm2 = TabletMetadata.builder(extent2).build();
    var tmm = TabletMetadata.builder(meta).build();
    var tmr = TabletMetadata.builder(root).build();

    var cg1 = CompactorGroupId.of("CG1");

    CompactionJobQueues jobQueues = new CompactionJobQueues(100);

    jobQueues.add(tm1, List.of(newJob((short) 1, 5, cg1)));
    jobQueues.add(tm2, List.of(newJob((short) 2, 6, cg1)));
    jobQueues.add(tmm, List.of(newJob((short) 3, 7, cg1)));
    jobQueues.add(tmr, List.of(newJob((short) 4, 8, cg1)));

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

    CompactionJobQueues jobQueues = new CompactionJobQueues(numToAdd + 1);
    CompactorGroupId[] groups =
        Stream.of("G1", "G2", "G3").map(CompactorGroupId::of).toArray(CompactorGroupId[]::new);

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
      TabletMetadata tm = TabletMetadata.builder(extent).build();
      jobQueues.add(tm, List.of(newJob((short) (i % 31), i, groups[i % groups.length])));
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
    CompactionJobQueues jobQueues = new CompactionJobQueues(100);

    var tid = TableId.of("1");
    var extent1 = new KeyExtent(tid, new Text("z"), new Text("q"));
    var extent2 = new KeyExtent(tid, new Text("q"), new Text("l"));
    var extent3 = new KeyExtent(tid, new Text("l"), new Text("c"));
    var extent4 = new KeyExtent(tid, new Text("c"), new Text("a"));

    var tm1 = TabletMetadata.builder(extent1).build();
    var tm2 = TabletMetadata.builder(extent2).build();
    var tm3 = TabletMetadata.builder(extent3).build();
    var tm4 = TabletMetadata.builder(extent4).build();

    var cg1 = CompactorGroupId.of("CG1");

    var future1 = jobQueues.getAsync(cg1);
    var future2 = jobQueues.getAsync(cg1);

    assertFalse(future1.isDone());
    assertFalse(future2.isDone());

    jobQueues.add(tm1, List.of(newJob((short) 1, 5, cg1)));
    jobQueues.add(tm2, List.of(newJob((short) 2, 6, cg1)));
    jobQueues.add(tm3, List.of(newJob((short) 3, 7, cg1)));
    jobQueues.add(tm4, List.of(newJob((short) 4, 8, cg1)));

    var future3 = jobQueues.getAsync(cg1);
    var future4 = jobQueues.getAsync(cg1);

    assertTrue(future1.isDone());
    assertTrue(future2.isDone());
    assertTrue(future3.isDone());
    assertTrue(future4.isDone());

    assertEquals(extent1, future1.get().getTabletMetadata().getExtent());
    assertEquals(extent2, future2.get().getTabletMetadata().getExtent());
    assertEquals(extent4, future3.get().getTabletMetadata().getExtent());
    assertEquals(extent3, future4.get().getTabletMetadata().getExtent());

    // test cancelling a future and having a future timeout
    var future5 = jobQueues.getAsync(cg1);
    assertFalse(future5.isDone());
    future5.cancel(false);
    var future6 = jobQueues.getAsync(cg1);
    assertFalse(future6.isDone());
    future6.orTimeout(10, TimeUnit.MILLISECONDS);
    // sleep for 20 millis, this should cause future6 to be timed out
    UtilWaitThread.sleep(20);
    var future7 = jobQueues.getAsync(cg1);
    assertFalse(future7.isDone());
    // since future5 was canceled and future6 timed out, this addition should go to future7
    jobQueues.add(tm1, List.of(newJob((short) 1, 5, cg1)));
    assertTrue(future7.isDone());
    assertEquals(extent1, future7.get().getTabletMetadata().getExtent());
    assertTrue(future5.isDone());
    assertTrue(future6.isCompletedExceptionally());
    assertTrue(future6.isDone());
  }
}

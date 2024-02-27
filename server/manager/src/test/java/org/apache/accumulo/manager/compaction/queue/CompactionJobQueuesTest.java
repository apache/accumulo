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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.admin.compaction.CompactableFile;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.CompactableFileImpl;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.CompactorGroupIdImpl;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class CompactionJobQueuesTest {

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
    CompactorGroupId[] groups = Stream.of("G1", "G2", "G3")
        .map(s -> CompactorGroupIdImpl.groupId(s)).toArray(l -> new CompactorGroupId[l]);

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

      Collection<CompactableFile> files = List
          .of(new CompactableFileImpl(new URI("file://accumulo/tables//123/t-0/f" + i), 100, 100));
      Collection<CompactionJob> jobs = List.of(new CompactionJobImpl((short) (i % 31),
          groups[i % groups.length], files, CompactionKind.SYSTEM, Optional.empty()));

      jobQueues.add(tm, jobs);
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
}

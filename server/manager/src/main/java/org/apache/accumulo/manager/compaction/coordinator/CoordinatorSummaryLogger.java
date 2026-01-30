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
package org.apache.accumulo.manager.compaction.coordinator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;
import org.apache.accumulo.server.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;

public class CoordinatorSummaryLogger {
  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorSummaryLogger.class);

  private final ServerContext ctx;
  private final CompactionJobQueues jobQueues;
  private final Map<ExternalCompactionId,RunningCompaction> running;
  private final Cache<ResourceGroupId,Integer> compactorCounts;

  public CoordinatorSummaryLogger(ServerContext ctx, CompactionJobQueues jobQueues,
      Map<ExternalCompactionId,RunningCompaction> running,
      Cache<ResourceGroupId,Integer> compactorCounts) {
    this.ctx = ctx;
    this.jobQueues = jobQueues;
    this.running = running;
    this.compactorCounts = compactorCounts;
  }

  public void logSummary() {

    final Map<ResourceGroupId,AtomicLong> perQueueRunningCount = new HashMap<>();
    final Map<String,AtomicLong> perTableRunningCount = new HashMap<>();

    running.values().forEach(rc -> {
      TableId tid = KeyExtent.fromThrift(rc.getJob().getExtent()).tableId();
      String tableName = null;
      try {
        tableName = ctx.getQualifiedTableName(tid);
      } catch (TableNotFoundException e) {
        tableName = "Unmapped table id: " + tid.canonical();
      }
      perQueueRunningCount.computeIfAbsent(rc.getGroup(), q -> new AtomicLong(0)).incrementAndGet();
      perTableRunningCount.computeIfAbsent(tableName, t -> new AtomicLong(0)).incrementAndGet();
    });

    perQueueRunningCount.forEach((groupId, count) -> {
      LOG.info(
          "Queue {}: compactors: {}, queued majc (minimum, possibly higher): {}, running majc: {}",
          groupId, compactorCounts.asMap().getOrDefault(groupId, 0),
          // This map only contains the highest priority for each tserver. So when tservers have
          // other priorities that need to compact or have more than one compaction for a
          // priority level this count will be lower than the actual number of queued.
          jobQueues.getQueuedJobs(groupId), count.get());

    });
    perTableRunningCount
        .forEach((t, count) -> LOG.info("Running compactions for table {}: {}", t, count));
  }

}

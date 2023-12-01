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

import static org.apache.accumulo.core.metrics.MetricsUtil.formatString;
import static org.apache.accumulo.core.metrics.MetricsUtil.getCommonTags;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

public class QueueMetrics implements MetricsProducer {
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(5);
  private volatile MeterRegistry meterRegistry = null;
  private final CompactionJobQueues compactionJobQueues;

  public QueueMetrics(CompactionJobQueues compactionJobQueues) {
    this.compactionJobQueues = compactionJobQueues;
    ScheduledExecutorService scheduler = ThreadPools.getServerThreadPools()
        .createScheduledExecutorService(1, "queueMetricsPoller", false);
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));
    ThreadPools.watchNonCriticalScheduledTask(scheduler.scheduleAtFixedRate(this::update,
        DEFAULT_MIN_REFRESH_DELAY, DEFAULT_MIN_REFRESH_DELAY, TimeUnit.MILLISECONDS));
  }

  public void update() {

    // read the volatile variable once so the rest of the method has consistent view
    var localRegistry = meterRegistry;

    if (localRegistry == null) {
      return;
    }

    Gauge
        .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUES, compactionJobQueues,
            CompactionJobQueues::getQueueCount)
        .description("Number of current Queues").tags(getCommonTags()).register(localRegistry);

    for (CompactionExecutorId ceid : compactionJobQueues.getQueueIds()) {
      // Normalize the queueId to match metrics tag naming convention.
      String queueId = formatString(ceid.toString());

      // Register queues by ID rather than by object as queues can be deleted.
      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_LENGTH, ceid,
              compactionJobQueues::getQueueMaxSize)
          .description("Length of priority queues")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId)).register(localRegistry);

      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED, ceid,
              compactionJobQueues::getQueuedJobs)
          .description("Count of queued jobs")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId)).register(localRegistry);

      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_DEQUEUED, ceid,
              compactionJobQueues::getDequeuedJobs)
          .description("Count of jobs dequeued")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId)).register(localRegistry);

      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_REJECTED, ceid,
              compactionJobQueues::getRejectedJobs)
          .description("Count of rejected jobs")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId)).register(localRegistry);

      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_PRIORITY, ceid,
              compactionJobQueues::getLowestPriority)
          .description("Lowest priority queued job")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId)).register(localRegistry);
    }
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    this.meterRegistry = registry;
  }
}

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
package org.apache.accumulo.manager.metrics;

import static org.apache.accumulo.core.metrics.MetricsUtil.formatString;
import static org.apache.accumulo.core.metrics.MetricsUtil.getCommonTags;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

public class QueueMetrics implements MetricsProducer {
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(5);
  private MeterRegistry meterRegistry = null;
  private CompactionJobQueues compactionJobQueues;
  private AtomicLong queueCount;

  public QueueMetrics(CompactionJobQueues compactionJobQueues) {
    this.compactionJobQueues = compactionJobQueues;
    ScheduledExecutorService scheduler = ThreadPools.getServerThreadPools()
        .createScheduledExecutorService(1, "queueMetricsPoller", false);
    Runtime.getRuntime().addShutdownHook(new Thread(scheduler::shutdownNow));
    ThreadPools.watchNonCriticalScheduledTask(scheduler.scheduleAtFixedRate(this::update,
        DEFAULT_MIN_REFRESH_DELAY, DEFAULT_MIN_REFRESH_DELAY, TimeUnit.MILLISECONDS));
  }

  public void update() {
    if (meterRegistry != null) {
      queueCount = meterRegistry.gauge(METRICS_COMPACTOR_JOB_PRIORITY_QUEUES, new AtomicLong(0));
    }
    if (queueCount != null) {
      queueCount.set(compactionJobQueues.getQueueCount());
    }

    for (CompactionExecutorId ceid : compactionJobQueues.getQueueIds()) {
      // Normalize the queueId to match metrics tag naming convention.
      String queueId = formatString(ceid.toString());

      // Register queues by ID rather than by object as queues can be deleted.
      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_LENGTH, ceid,
              id -> compactionJobQueues.getQueueSize(id))
          .description("Length of priority queues")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId)).register(meterRegistry);

      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED, ceid,
              id -> compactionJobQueues.getQueueSize(id))
          .description("Count of queued jobs")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId)).register(meterRegistry);

      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_DEQUEUED, ceid,
              id -> compactionJobQueues.getQueueSize(id))
          .description("Count of jobs dequeued")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId)).register(meterRegistry);

      Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_REJECTED, ceid,
              id -> compactionJobQueues.getQueueSize(id))
          .description("Count of rejected jobs")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId)).register(meterRegistry);
    }
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    this.meterRegistry = registry;
  }
}

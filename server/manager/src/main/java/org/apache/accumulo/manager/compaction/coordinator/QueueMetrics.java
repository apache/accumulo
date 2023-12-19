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

import static org.apache.accumulo.core.metrics.MetricsUtil.getCommonTags;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.spi.compaction.CompactionExecutorId;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.manager.compaction.queue.CompactionJobPriorityQueue;
import org.apache.accumulo.manager.compaction.queue.CompactionJobQueues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

public class QueueMetrics implements MetricsProducer {

  private static class QueueMeters {
    private final Gauge length;
    private final Gauge jobsQueued;
    private final Gauge jobsDequeued;
    private final Gauge jobsRejected;
    private final Gauge jobsLowestPriority;

    public QueueMeters(MeterRegistry meterRegistry, CompactionExecutorId queueId,
        CompactionJobPriorityQueue queue) {
      length =
          Gauge.builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_LENGTH, queue, q -> q.getMaxSize())
              .description("Length of priority queues")
              .tags(Tags.concat(getCommonTags(), "queue.id", queueId.canonical()))
              .register(meterRegistry);

      jobsQueued = Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_QUEUED, queue, q -> q.getQueuedJobs())
          .description("Count of queued jobs")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId.canonical()))
          .register(meterRegistry);

      jobsDequeued = Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_DEQUEUED, queue,
              q -> q.getDequeuedJobs())
          .description("Count of jobs dequeued")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId.canonical()))
          .register(meterRegistry);

      jobsRejected = Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_REJECTED, queue,
              q -> q.getRejectedJobs())
          .description("Count of rejected jobs")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId.canonical()))
          .register(meterRegistry);

      jobsLowestPriority = Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUE_JOBS_PRIORITY, queue,
              q -> q.getLowestPriority())
          .description("Lowest priority queued job")
          .tags(Tags.concat(getCommonTags(), "queue.id", queueId.canonical()))
          .register(meterRegistry);
    }

    private void removeMeters(MeterRegistry registry) {
      registry.remove(length);
      registry.remove(jobsQueued);
      registry.remove(jobsDequeued);
      registry.remove(jobsRejected);
      registry.remove(jobsLowestPriority);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(QueueMetrics.class);
  private static final long DEFAULT_MIN_REFRESH_DELAY = TimeUnit.SECONDS.toMillis(5);
  private volatile MeterRegistry meterRegistry = null;
  private final CompactionJobQueues compactionJobQueues;
  private final Map<CompactionExecutorId,QueueMeters> perQueueMetrics = new HashMap<>();
  private Gauge queueCountMeter = null;

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

    if (queueCountMeter == null) {
      queueCountMeter = Gauge
          .builder(METRICS_COMPACTOR_JOB_PRIORITY_QUEUES, compactionJobQueues,
              CompactionJobQueues::getQueueCount)
          .description("Number of current Queues").tags(getCommonTags()).register(localRegistry);
    }
    LOG.debug("update - cjq queues: {}", compactionJobQueues.getQueueIds());

    Set<CompactionExecutorId> definedQueues = compactionJobQueues.getQueueIds();
    LOG.debug("update - defined queues: {}", definedQueues);

    Set<CompactionExecutorId> queuesWithMetrics = perQueueMetrics.keySet();
    LOG.debug("update - queues with metrics: {}", queuesWithMetrics);

    SetView<CompactionExecutorId> queuesWithoutMetrics =
        Sets.difference(definedQueues, queuesWithMetrics);
    queuesWithoutMetrics.forEach(q -> {
      LOG.debug("update - creating meters for queue: {}", q);
      perQueueMetrics.put(q, new QueueMeters(localRegistry, q, compactionJobQueues.getQueue(q)));
    });

    SetView<CompactionExecutorId> metricsWithoutQueues =
        Sets.difference(queuesWithMetrics, definedQueues);
    metricsWithoutQueues.forEach(q -> {
      LOG.debug("update - removing meters for queue: {}", q);
      perQueueMetrics.get(q).removeMeters(localRegistry);
      perQueueMetrics.remove(q);
    });

  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    this.meterRegistry = registry;
  }
}

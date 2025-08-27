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
package org.apache.accumulo.core.fate;

import java.util.Set;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.metrics.Metric;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

public class FateExecutorMetrics<T> implements MetricsProducer {
  private static final Logger log = LoggerFactory.getLogger(FateExecutorMetrics.class);
  private final FateInstanceType type;
  private final String operatesOn;
  private final Set<FateExecutor<T>.TransactionRunner> runningTxRunners;
  private final AtomicInteger idleWorkerCount;
  private final TransferQueue<FateId> workQueue;
  private MeterRegistry registry;
  private State state;
  public static final String INSTANCE_TYPE_TAG_KEY = "instanceType";
  public static final String OPS_ASSIGNED_TAG_KEY = "ops.assigned";

  protected FateExecutorMetrics(FateInstanceType type, String operatesOn,
      Set<FateExecutor<T>.TransactionRunner> runningTxRunners, TransferQueue<FateId> workQueue,
      AtomicInteger idleWorkerCount) {
    this.type = type;
    this.operatesOn = operatesOn;
    this.runningTxRunners = runningTxRunners;
    this.workQueue = workQueue;
    this.state = State.UNREGISTERED;
    this.idleWorkerCount = idleWorkerCount;
  }

  @Override
  public void registerMetrics(MeterRegistry registry) {
    // noop if already registered or cleared
    if (state == State.UNREGISTERED) {
      Gauge.builder(Metric.FATE_OPS_THREADS_TOTAL.getName(), runningTxRunners::size)
          .description(Metric.FATE_OPS_THREADS_TOTAL.getDescription())
          .tag(INSTANCE_TYPE_TAG_KEY, type.name().toLowerCase())
          .tag(OPS_ASSIGNED_TAG_KEY, operatesOn).register(registry);
      Gauge.builder(Metric.FATE_OPS_THREADS_INACTIVE.getName(), idleWorkerCount::get)
          .description(Metric.FATE_OPS_THREADS_INACTIVE.getDescription())
          .tag(INSTANCE_TYPE_TAG_KEY, type.name().toLowerCase())
          .tag(OPS_ASSIGNED_TAG_KEY, operatesOn).register(registry);

      registered(registry);
    }
  }

  public void clearMetrics() {
    // noop if metrics were never registered or have already been cleared
    if (state == State.REGISTERED) {
      var threadsTotalMeter = registry.find(Metric.FATE_OPS_THREADS_TOTAL.getName())
          .tags(INSTANCE_TYPE_TAG_KEY, type.name().toLowerCase(), OPS_ASSIGNED_TAG_KEY, operatesOn)
          .meter();
      // meter will be null if it could not be found, ignore IDE warning if one is seen
      if (threadsTotalMeter == null) {
        log.error(
            "Tried removing meter{name: {} tags: {}={}, {}={}} from the registry, but did "
                + "not find it.",
            Metric.FATE_OPS_THREADS_TOTAL.getName(), INSTANCE_TYPE_TAG_KEY,
            type.name().toLowerCase(), OPS_ASSIGNED_TAG_KEY, operatesOn);
      } else {
        registry.remove(threadsTotalMeter);
      }

      var threadsInactiveMeter = registry.find(Metric.FATE_OPS_THREADS_INACTIVE.getName())
          .tags(INSTANCE_TYPE_TAG_KEY, type.name().toLowerCase(), OPS_ASSIGNED_TAG_KEY, operatesOn)
          .meter();
      // meter will be null if it could not be found, ignore IDE warning if one is seen
      if (threadsInactiveMeter == null) {
        log.error(
            "Tried removing meter{name: {} tags: {}={}, {}={}} from the registry, but did "
                + "not find it.",
            Metric.FATE_OPS_THREADS_TOTAL.getName(), INSTANCE_TYPE_TAG_KEY,
            type.name().toLowerCase(), OPS_ASSIGNED_TAG_KEY, operatesOn);
      } else {
        registry.remove(threadsInactiveMeter);
      }

      cleared();
    }
  }

  private void registered(MeterRegistry registry) {
    this.registry = registry;
    this.state = State.REGISTERED;
  }

  private void cleared() {
    registry = null;
    state = State.CLEARED;
  }

  public boolean isRegistered() {
    return state == State.REGISTERED;
  }

  private enum State {
    UNREGISTERED, REGISTERED, CLEARED
  };
}

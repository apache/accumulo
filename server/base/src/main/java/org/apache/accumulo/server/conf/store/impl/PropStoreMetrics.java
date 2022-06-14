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
package org.apache.accumulo.server.conf.store.impl;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

public class PropStoreMetrics implements MetricsProducer {

  private static final Logger log = LoggerFactory.getLogger(PropStoreMetrics.class);

  private Timer load;
  private Counter refresh;
  private Counter refreshLoad;
  private Counter eviction;
  private Counter zkError;

  @Override
  public void registerMetrics(MeterRegistry registry) {

    load = Timer.builder(METRICS_PROPSTORE_LOAD_TIMER).description("prop store load time")
        .tags(MetricsUtil.getCommonTags()).register(registry);

    refresh =
        Counter.builder(METRICS_PROPSTORE_REFRESH_COUNT).description("prop store refresh count")
            .tags(MetricsUtil.getCommonTags()).register(registry);

    refreshLoad = Counter.builder(METRICS_PROPSTORE_REFRESH_LOAD_COUNT)
        .description("prop store refresh load count").tags(MetricsUtil.getCommonTags())
        .register(registry);

    eviction =
        Counter.builder(METRICS_PROPSTORE_EVICTION_COUNT).description("prop store eviction count")
            .tags(MetricsUtil.getCommonTags()).register(registry);

    zkError = Counter.builder(METRICS_PROPSTORE_ZK_ERROR_COUNT)
        .description("prop store ZooKeeper error count").tags(MetricsUtil.getCommonTags())
        .register(registry);

  }

  public PropStoreMetrics() {
    log.debug("Creating PropStore metrics");
  }

  public void addLoadTime(final long value) {
    log.trace("Load time: {}", value);
    load.record(Duration.ofMillis(value));
    log.trace("Load count: {} time:{}", load.count(), load.totalTime(TimeUnit.MILLISECONDS));
  }

  public void incrRefresh() {
    refresh.increment();
  }

  public void incrRefreshLoad() {
    refreshLoad.increment();
  }

  public void incrEviction() {
    eviction.increment();
  }

  public void incrZkError() {
    zkError.increment();
  }
}

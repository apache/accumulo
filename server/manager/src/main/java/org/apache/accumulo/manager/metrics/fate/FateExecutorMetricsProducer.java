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
package org.apache.accumulo.manager.metrics.fate;

import static org.apache.accumulo.manager.metrics.fate.FateMetrics.DEFAULT_MIN_REFRESH_DELAY;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.fate.FateExecutor;
import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.ServerContext;

import io.micrometer.core.instrument.MeterRegistry;

public class FateExecutorMetricsProducer implements MetricsProducer {
  private final Set<FateExecutor<FateEnv>> fateExecutors;
  private final ServerContext context;
  private final long refreshDelay;
  private MeterRegistry registry;

  public FateExecutorMetricsProducer(ServerContext context,
      Set<FateExecutor<FateEnv>> fateExecutors, long minimumRefreshDelay) {
    this.context = context;
    this.fateExecutors = fateExecutors;
    this.refreshDelay = Math.max(DEFAULT_MIN_REFRESH_DELAY, minimumRefreshDelay);
  }

  protected void update() {
    // there may have been new fate executors added, so these need to be registered.
    // fate executors removed will have their metrics removed from the registry before they are
    // removed from the set.
    if (registry != null) {
      synchronized (fateExecutors) {
        fateExecutors.forEach(fe -> {
          var feMetrics = fe.getFateExecutorMetrics();
          if (!feMetrics.isRegistered()) {
            feMetrics.registerMetrics(registry);
          }
        });
      }
    }
  }

  @Override
  public void registerMetrics(final MeterRegistry registry) {
    this.registry = registry;
    synchronized (fateExecutors) {
      fateExecutors.forEach(fe -> fe.getFateExecutorMetrics().registerMetrics(registry));
    }

    var future = context.getScheduledExecutor().scheduleAtFixedRate(this::update, refreshDelay,
        refreshDelay, TimeUnit.MILLISECONDS);
    ThreadPools.watchCriticalScheduledTask(future);
  }
}

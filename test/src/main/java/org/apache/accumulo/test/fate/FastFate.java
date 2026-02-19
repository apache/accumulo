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
package org.apache.accumulo.test.fate;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.function.Function;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FatePartition;
import org.apache.accumulo.core.fate.FateStore;
import org.apache.accumulo.core.fate.Repo;

/**
 * A FATE which performs the dead reservation cleanup and the check on the pool size with a much
 * shorter delay between. Useful for shortening test times for tests that are waiting for one of
 * these actions to occur.
 */
public class FastFate<T> extends Fate<T> {
  private static final Duration DEAD_RES_CLEANUP_DELAY = Duration.ofSeconds(5);
  private static final Duration POOL_WATCHER_DELAY = Duration.ofSeconds(5);

  public FastFate(T environment, FateStore<T> store, boolean runDeadResCleaner,
      Function<Repo<T>,String> toLogStrFunc, AccumuloConfiguration conf) {
    super(environment, store, runDeadResCleaner, toLogStrFunc, conf,
        new ScheduledThreadPoolExecutor(2));
    setPartitions(Set.of(FatePartition.all(store.type())));
  }

  @Override
  public Duration getDeadResCleanupDelay() {
    return DEAD_RES_CLEANUP_DELAY;
  }

  @Override
  public Duration getPoolWatcherDelay() {
    return POOL_WATCHER_DELAY;
  }
}

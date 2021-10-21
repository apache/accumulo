/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf.store.impl;

import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropStoreMetrics {

  private static final Logger log = LoggerFactory.getLogger(PropStoreMetrics.class);

  private final AtomicLong loadCounter = new AtomicLong(0);
  private final AtomicLong refreshCounter = new AtomicLong(0);
  private final AtomicLong refreshLoadCounter = new AtomicLong(0);
  private final AtomicLong evictionCounter = new AtomicLong(0);
  private final AtomicLong zkCacheErrorCounter = new AtomicLong(0);

  public PropStoreMetrics() {
    log.info("Creating PropStore metrics");
  }

  public void updateLoadCounter() {
    loadCounter.incrementAndGet();
  }

  public void updateRefreshCounter() {
    refreshCounter.incrementAndGet();
  }

  public void updateRefreshLoadCounter() {
    refreshLoadCounter.incrementAndGet();
  }

  public void updateEvictionCounter() {
    evictionCounter.incrementAndGet();
  }

  public void updateZkCacheErrorCounter() {
    zkCacheErrorCounter.incrementAndGet();
  }

  public long getLoadCounter() {
    return loadCounter.get();
  }

  public long getRefreshCounter() {
    return refreshCounter.get();
  }

  public long getRefreshLoadCounter() {
    return refreshLoadCounter.get();
  }

  public long getEvictionCounter() {
    return evictionCounter.get();
  }

  public long getZkCacheErrorCounter() {
    return zkCacheErrorCounter.get();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", PropStoreMetrics.class.getSimpleName() + "[", "]")
        .add("loadCounter=" + loadCounter.get()).add("refreshCounter=" + refreshCounter.get())
        .add("refreshLoadCounter=" + refreshLoadCounter.get())
        .add("evictionCounter=" + evictionCounter.get())
        .add("zkCacheErrorCounter=" + zkCacheErrorCounter.get()).toString();
  }
}

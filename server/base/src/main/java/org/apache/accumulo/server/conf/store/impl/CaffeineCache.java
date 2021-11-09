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

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCache;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;

public class CaffeineCache implements PropCache {

  public static final TimeUnit BASE_TIME_UNITS = TimeUnit.MINUTES;
  public static final int REFRESH_MIN = 15;
  public static final int EXPIRE_MIN = 60;
  private static final Logger log = LoggerFactory.getLogger(CaffeineCache.class);
  private static final Executor executor =
      ThreadPools.createThreadPool(1, 20, 60, TimeUnit.SECONDS, "cache-refresh");

  private final PropStoreMetrics metrics;

  private final LoadingCache<PropCacheId,VersionedProperties> cache;

  private CaffeineCache(final CacheLoader<PropCacheId,VersionedProperties> cacheLoader,
      final PropStoreMetrics metrics, final Ticker ticker) {
    this.metrics = metrics;

    if (ticker != null) { // build test instance with artificial clock.
      cache = Caffeine.newBuilder().refreshAfterWrite(REFRESH_MIN, BASE_TIME_UNITS)
          .expireAfterAccess(EXPIRE_MIN, BASE_TIME_UNITS).evictionListener(this::evictionNotifier)
          .ticker(ticker).executor(executor).build(cacheLoader);
    } else {
      cache = Caffeine.newBuilder().refreshAfterWrite(REFRESH_MIN, BASE_TIME_UNITS)
          .expireAfterAccess(EXPIRE_MIN, BASE_TIME_UNITS).evictionListener(this::evictionNotifier)
          .executor(executor).build(cacheLoader);
    }
  }

  public PropStoreMetrics getMetrics() {
    return metrics;
  }

  void evictionNotifier(PropCacheId cacheId, VersionedProperties value, RemovalCause cause) {
    log.debug("Evicted: ID: {} was evicted from cache. Reason: {}", cacheId, cause);
    metrics.incrEviction();
  }

  @Override
  public @Nullable VersionedProperties get(PropCacheId propCacheId) {
    log.trace("Called get() for {}", propCacheId);
    try {
      return cache.get(propCacheId);
    } catch (Exception ex) {
      log.info("Failed to get properties " + propCacheId, ex);
      metrics.incrZkError();
      return null;
    }
  }

  @Override
  public void remove(PropCacheId propCacheId) {
    log.trace("clear {} from cache", propCacheId);
    cache.invalidate(propCacheId);
  }

  @Override
  public void removeAll() {
    cache.invalidateAll();
  }

  // for testing - force Caffeine housekeeping to run.
  void cleanUp() {
    cache.cleanUp();
  }

  /**
   * Retrieve the version properties if present in the cache, otherwise return null. This prevents
   * caching the properties and should be used when properties will be updated and then committed to
   * the backend store. The process that is updating the values may not need them for additional
   * processing so there is no reason to store them in the cache at this time. If they are used, a
   * normal cache get will load the property into the cache.
   *
   * @param propCacheId
   *          the property id
   * @return the version properties if cached, otherwise return null.
   */
  @Override
  public @Nullable VersionedProperties getWithoutCaching(PropCacheId propCacheId) {
    return cache.getIfPresent(propCacheId);
  }

  public static class Builder {

    private final PropStoreMetrics metrics;
    private final ZooPropLoader zooPropLoader;
    private Ticker ticker = null;

    public Builder(final ZooPropLoader zooPropLoader, final PropStoreMetrics metrics) {
      Objects.requireNonNull(zooPropLoader, "A PropStoreChangeMonitor must be provided");
      this.zooPropLoader = zooPropLoader;
      this.metrics = metrics;
    }

    public CaffeineCache build() {
      return new CaffeineCache(zooPropLoader, metrics, ticker);
    }

    public Builder withTicker(final Ticker ticker) {
      this.ticker = ticker;
      return this;
    }
  }

}

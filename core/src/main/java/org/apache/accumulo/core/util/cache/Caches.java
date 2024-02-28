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
package org.apache.accumulo.core.util.cache;

import static com.google.common.base.Suppliers.memoize;

import java.util.function.Supplier;

import org.apache.accumulo.core.metrics.MetricsProducer;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineStatsCounter;

public class Caches implements MetricsProducer {

  public enum CacheName {
    BULK_IMPORT_FILE_LENGTHS,
    CLASSLOADERS,
    COMBINER_LOGGED_MSGS,
    COMPACTIONS_COMPLETED,
    COMPACTION_CONFIGS,
    COMPACTION_DIR_CACHE,
    COMPACTION_DISPATCHERS,
    COMPACTOR_GROUP_ID,
    COMPRESSION_ALGORITHM,
    CRYPT_PASSWORDS,
    HOST_REGEX_BALANCER_TABLE_REGEX,
    INSTANCE_ID,
    NAMESPACE_ID,
    PROP_CACHE,
    RECOVERY_MANAGER_PATH_CACHE,
    SCAN_SERVER_TABLET_METADATA,
    SERVICE_ENVIRONMENT_TABLE_CONFIGS,
    SPACE_AWARE_VOLUME_CHOICE,
    SPLITTER_FILES,
    SPLITTER_STARTING,
    SPLITTER_UNSPLITTABLE,
    TABLE_ID,
    TABLE_ZOO_HELPER_CACHE,
    TSRM_FILE_LENGTHS,
    TINYLFU_BLOCK_CACHE;
  }

  private static final Logger LOG = LoggerFactory.getLogger(Caches.class);
  private static final Supplier<Caches> CACHES = memoize(() -> new Caches());

  public static Caches getInstance() {
    return CACHES.get();
  }

  private MeterRegistry registry = null;

  private Caches() {}

  @Override
  public void registerMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  private boolean setupMicrometerMetrics(Caffeine<Object,Object> cacheBuilder, String name) {
    if (registry != null) {
      try {
        cacheBuilder.recordStats(
            () -> new CaffeineStatsCounter(registry, name, MetricsUtil.getCommonTags()));
        LOG.trace("Metrics enabled for {} cache.", name);
        return true;
      } catch (IllegalStateException e) {
        // recordStats was already called by the cacheBuilder.
      }
    }
    return false;
  }

  public Caffeine<Object,Object> createNewBuilder(CacheName name, boolean emitMetricsIfEnabled) {
    Caffeine<Object,Object> cacheBuilder = Caffeine.newBuilder();
    boolean metricsConfigured = false;
    if (emitMetricsIfEnabled) {
      metricsConfigured = setupMicrometerMetrics(cacheBuilder, name.name());
    }
    LOG.trace("Caffeine builder created for {}, metrics enabled: {}", name, metricsConfigured);
    return cacheBuilder;
  }

}

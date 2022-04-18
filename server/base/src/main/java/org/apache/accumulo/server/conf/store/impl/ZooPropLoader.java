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

import static java.util.Objects.requireNonNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;

public class ZooPropLoader implements CacheLoader<PropCacheKey,VersionedProperties> {

  private static final Logger log = LoggerFactory.getLogger(ZooPropLoader.class);

  private final ZooReaderWriter zrw;
  private final VersionedPropCodec propCodec;
  // used to set watcher, does not react to events.
  private final PropStoreWatcher propStoreWatcher;
  private final PropStoreMetrics metrics;

  public ZooPropLoader(final ZooReaderWriter zrw, final VersionedPropCodec propCodec,
      final PropStoreWatcher propStoreWatcher, final PropStoreMetrics metrics) {
    this.zrw = zrw;
    this.propCodec = propCodec;
    this.propStoreWatcher = propStoreWatcher;
    this.metrics = metrics;
  }

  @Override
  public @Nullable VersionedProperties load(PropCacheKey propCacheKey) {
    try {
      log.trace("load called for {}", propCacheKey);

      long startNanos = System.nanoTime();

      Stat stat = new Stat();
      byte[] bytes = zrw.getData(propCacheKey.getPath(), propStoreWatcher, stat);
      var vProps = propCodec.fromBytes(stat.getVersion(), bytes);

      metrics.addLoadTime(
          TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS));

      return vProps;
    } catch (KeeperException.NoNodeException ex) {
      metrics.incrZkError();
      log.debug("property node for {} does not exist - it may be being created", propCacheKey);
      propStoreWatcher.signalZkChangeEvent(propCacheKey);
      return null;
    } catch (Exception ex) {
      metrics.incrZkError();
      log.info("Failed to load properties for: {} from ZooKeeper, returning null", propCacheKey,
          ex);
      propStoreWatcher.signalZkChangeEvent(propCacheKey);
      return null;
    }
  }

  @Override
  public CompletableFuture<? extends VersionedProperties> asyncLoad(PropCacheKey propCacheKey,
      Executor executor) {
    log.trace("asyncLoad called for key: {}", propCacheKey);
    return CacheLoader.super.asyncLoad(propCacheKey, executor);
  }

  @Override
  public CompletableFuture<? extends VersionedProperties> asyncReload(PropCacheKey propCacheKey,
      VersionedProperties oldValue, Executor executor) throws Exception {
    log.trace("asyncReload called for key: {}", propCacheKey);
    metrics.incrRefresh();

    return CompletableFuture.supplyAsync(() -> loadIfDifferentVersion(propCacheKey, oldValue),
        executor);
  }

  @Override
  public @Nullable VersionedProperties reload(PropCacheKey propCacheKey,
      VersionedProperties oldValue) throws Exception {
    log.trace("reload called for: {}", propCacheKey);
    metrics.incrRefresh();
    return loadIfDifferentVersion(propCacheKey, oldValue);
  }

  /**
   * First checks that the ZooKeeper data version matches the versioned property's data version,
   * using a lighter weight ZooKeeper call to get the Stat structure. If the data versions match,
   * the original version properties are returned without additional ZooKeeper calls. If the
   * versions do not match, the data is reread from ZooKeeper and returned. If any exceptions occur,
   * a null value is returned and any cached values should be considered invalid.
   *
   * @param propCacheId
   *          the property cache id
   * @param currentValue
   *          the current versioned properties.
   * @return versioned properties that match the values stored in ZooKeeper, or null if the
   *         properties cannot be retrieved.
   */
  private @Nullable VersionedProperties loadIfDifferentVersion(PropCacheKey propCacheId,
      VersionedProperties currentValue) {
    requireNonNull(propCacheId, "propCacheId cannot be null");
    try {
      Stat stat = zrw.getStatus(propCacheId.getPath());

      log.trace("Check stat version on reload. Zk: {}, Cache: {}", stat.getVersion(),
          currentValue.getDataVersion());
      // no change, cached version is valid.
      if (stat.getVersion() == currentValue.getDataVersion()) {
        return currentValue;
      }

      log.trace("different version in cache for {}, calling loader to get update from ZooKeeper",
          propCacheId);

      var updatedValue = load(propCacheId);

      metrics.incrRefreshLoad();

      // The cache will be updated - notify external listeners value changed.
      propStoreWatcher.signalCacheChangeEvent(propCacheId);
      log.trace("Updated value {}", updatedValue == null ? "null" : updatedValue.print(true));
      return updatedValue;
    } catch (Exception ex) {
      log.info("async exception occurred reading properties from ZooKeeper for: {} returning null",
          propCacheId, ex);
      metrics.incrZkError();
      propStoreWatcher.signalZkChangeEvent(propCacheId);
      return null;
    }
  }
}

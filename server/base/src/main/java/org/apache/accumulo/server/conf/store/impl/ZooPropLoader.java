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

import static java.util.Objects.requireNonNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;

public class ZooPropLoader implements CacheLoader<PropStoreKey<?>,VersionedProperties> {

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
  public @Nullable VersionedProperties load(PropStoreKey<?> propStoreKey) {
    try {
      log.trace("load called for {}", propStoreKey);

      long startNanos = System.nanoTime();

      Stat stat = new Stat();
      byte[] bytes = zrw.getData(propStoreKey.getPath(), propStoreWatcher, stat);
      if (stat.getDataLength() == 0) {
        return new VersionedProperties();
      }
      VersionedProperties vProps = propCodec.fromBytes(stat.getVersion(), bytes);

      metrics.addLoadTime(
          TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS));

      return vProps;
    } catch (KeeperException.NoNodeException ex) {
      metrics.incrZkError();
      log.debug("property node for {} does not exist - it may be being created", propStoreKey);
      propStoreWatcher.signalZkChangeEvent(propStoreKey);
      return null;
    } catch (Exception ex) {
      metrics.incrZkError();
      log.info("Failed to load properties for: {} from ZooKeeper, returning null", propStoreKey,
          ex);
      propStoreWatcher.signalZkChangeEvent(propStoreKey);
      return null;
    }
  }

  @Override
  public CompletableFuture<? extends VersionedProperties> asyncLoad(PropStoreKey<?> propStoreKey,
      Executor executor) throws Exception {
    log.trace("asyncLoad called for key: {}", propStoreKey);
    return CacheLoader.super.asyncLoad(propStoreKey, executor);
  }

  @Override
  public CompletableFuture<VersionedProperties> asyncReload(PropStoreKey<?> propStoreKey,
      VersionedProperties oldValue, Executor executor) throws Exception {
    log.trace("asyncReload called for key: {}", propStoreKey);
    metrics.incrRefresh();

    return CompletableFuture.supplyAsync(() -> loadIfDifferentVersion(propStoreKey, oldValue),
        executor);
  }

  @Override
  public @Nullable VersionedProperties reload(PropStoreKey<?> propStoreKey,
      VersionedProperties oldValue) throws Exception {
    log.trace("reload called for: {}", propStoreKey);
    metrics.incrRefresh();
    return loadIfDifferentVersion(propStoreKey, oldValue);
  }

  /**
   * First checks that the ZooKeeper data version matches the versioned property's data version,
   * using a lighter weight ZooKeeper call to get the Stat structure. If the data versions match,
   * the original version properties are returned without additional ZooKeeper calls. If the
   * versions do not match, the data is reread from ZooKeeper and returned. If any exceptions occur,
   * a null value is returned and any cached values should be considered invalid.
   *
   * @param propCacheId the property cache id
   * @param currentValue the current versioned properties.
   * @return versioned properties that match the values stored in ZooKeeper, or null if the
   *         properties cannot be retrieved.
   */
  private @Nullable VersionedProperties loadIfDifferentVersion(PropStoreKey<?> propCacheId,
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
    } catch (RuntimeException | KeeperException | InterruptedException ex) {
      log.warn("async exception occurred reading properties from ZooKeeper for: {} returning null",
          propCacheId, ex);
      metrics.incrZkError();
      propStoreWatcher.signalZkChangeEvent(propCacheId);
      return null;
    }
  }
}

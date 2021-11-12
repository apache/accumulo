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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.CacheLoader;

public class ZooPropLoader implements CacheLoader<PropCacheId,VersionedProperties> {

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
  public @Nullable VersionedProperties load(PropCacheId propCacheId) {
    try {
      log.trace("load called for {}", propCacheId);
      Stat stat = new Stat();
      long startNanos = System.nanoTime();

      var vProps = propCodec.fromBytes(zrw.getData(propCacheId.getPath(), propStoreWatcher, stat));

      metrics.addLoadTime(
          TimeUnit.MILLISECONDS.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS));

      if (stat.getVersion() != vProps.getDataVersion()) {
        log.warn("data versions between decoded value {} and zk value {} do not match",
            vProps.getDataVersion(), stat.getVersion());
        return null;
      }
      return vProps;
    } catch (Exception ex) {
      metrics.incrZkError();
      log.warn("Failed to load: {} from ZooKeeper, returning null", propCacheId, ex);
      propStoreWatcher.signalZkChangeEvent(propCacheId);
      return null;
    }
  }

  @Override
  public CompletableFuture<? extends VersionedProperties> asyncLoad(PropCacheId propCacheId,
      Executor executor) {
    log.trace("asyncLoad called for key: {}", propCacheId);
    return CacheLoader.super.asyncLoad(propCacheId, executor);
  }

  @Override
  public CompletableFuture<? extends VersionedProperties> asyncReload(PropCacheId propCacheId,
      VersionedProperties oldValue, Executor executor) throws Exception {
    log.trace("asyncReload called for key: {}", propCacheId);
    metrics.incrRefresh();

    return CompletableFuture.supplyAsync(() -> loadIfDifferentVersion(propCacheId, oldValue),
        executor);
  }

  @Override
  public @Nullable VersionedProperties reload(PropCacheId propCacheId, VersionedProperties oldValue)
      throws Exception {
    log.trace("reload called for: {}", propCacheId);
    metrics.incrRefresh();
    return loadIfDifferentVersion(propCacheId, oldValue);
  }

  /**
   * First checks that the ZooKeeper data version matches the versioned property's data version,
   * using a lighter weight ZooKeeper call to get the Stat structure. If the data versions match,
   * the original version properties are returned without additional AooKeeper calls. If the
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
  private VersionedProperties loadIfDifferentVersion(PropCacheId propCacheId,
      VersionedProperties currentValue) {
    try {
      Stat stat = getNodeVersion(propCacheId);

      log.trace("Check stat version on reload. Zk: {}, Cache: {}", stat.getVersion(),
          currentValue.getDataVersion());
      // no change, cached version is valid.
      if (stat.getVersion() == currentValue.getDataVersion()) {
        return currentValue;
      }

      log.debug("different version, calling loader to get update from ZooKeeper, id: {}",
          propCacheId);

      var updatedValue = load(propCacheId);
      if (updatedValue == null) {
        return null;
      }

      metrics.incrRefreshLoad();

      // The cache will be updated - notify external listeners value changed.
      propStoreWatcher.signalCacheChangeEvent(propCacheId);
      log.trace("Updated value {}", updatedValue.print(true));
      return updatedValue;
    } catch (Exception ex) {
      log.warn("async exception occurred - returning null", ex);
      metrics.incrZkError();
      propStoreWatcher.signalZkChangeEvent(propCacheId);
      return null;
    }
  }

  /**
   * Return the zooKeeper Stat structure of the node specified.
   *
   * @param propCacheId
   *          the property cache id
   * @return a ZooKeeper Stat structure.
   */
  public @NonNull Stat getNodeVersion(PropCacheId propCacheId) {
    try {
      log.trace("get data version");
      return zrw.getStatus(propCacheId.getPath());
    } catch (InterruptedException | KeeperException ex) {
      throw new IllegalStateException("Failed to get node stat for: " + propCacheId, ex);
    }
  }
}

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

import static java.util.Objects.requireNonNullElseGet;

import java.io.IOException;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCache;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.util.ConfigTransformer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Ticker;

public class ZooPropStore implements PropStore, PropChangeListener {

  private final static Logger log = LoggerFactory.getLogger(ZooPropStore.class);
  private final static VersionedPropCodec codec = VersionedPropCodec.getDefault();

  private final ZooReaderWriter zrw;
  private final PropStoreWatcher propStoreWatcher;
  private final PropCacheCaffeineImpl cache;
  private final PropStoreMetrics cacheMetrics = new PropStoreMetrics();
  private final ReadyMonitor zkReadyMon;

  /**
   * Create instance using ZooPropStore.Builder
   *
   * @param instanceId the instance id
   * @param zrw a wrapper set of utilities for accessing ZooKeeper.
   */
  private ZooPropStore(final InstanceId instanceId, final ZooReaderWriter zrw) {
    this(instanceId, zrw, null, null, null);
  }

  /**
   * For testing create an instance with the optionally pass synthetic clock (Ticker), a
   * ReadyMonitor and a PropStore watcher allowing them to be mocked. If the optional components are
   * passed as null an internal instance is created.
   *
   * @param instanceId the instance id
   * @param zrw a wrapper set of utilities for accessing ZooKeeper.
   * @param monitor a ready monitor. Optional, if null, one is created.
   * @param watcher a watcher. Optional, if null, one is created.
   * @param ticker a synthetic clock used for testing. Optional, if null, one is created.
   */
  ZooPropStore(final InstanceId instanceId, final ZooReaderWriter zrw, final ReadyMonitor monitor,
      final PropStoreWatcher watcher, final Ticker ticker) {

    this.zrw = zrw;

    this.zkReadyMon = requireNonNullElseGet(monitor,
        () -> new ReadyMonitor("prop-store", Math.round(zrw.getSessionTimeout() * 1.75)));

    this.propStoreWatcher = requireNonNullElseGet(watcher, () -> new PropStoreWatcher(zkReadyMon));

    ZooPropLoader propLoader = new ZooPropLoader(zrw, codec, this.propStoreWatcher, cacheMetrics);

    if (ticker == null) {
      this.cache = new PropCacheCaffeineImpl.Builder(propLoader, cacheMetrics).build();
    } else {
      this.cache =
          new PropCacheCaffeineImpl.Builder(propLoader, cacheMetrics).forTests(ticker).build();
    }

    MetricsUtil.initializeProducers(cacheMetrics);

    try {
      var path = ZooUtil.getRoot(instanceId);
      if (zrw.exists(path, propStoreWatcher)) {
        log.debug("Have a ZooKeeper connection and found instance node: {}", instanceId);
        zkReadyMon.setReady();
      } else {
        throw new IllegalStateException("Instance may not have been initialized, root node: " + path
            + " does not exist in ZooKeeper");
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(
          "Interrupted trying to read root node " + instanceId + " from ZooKeeper", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to read root node " + instanceId + " from ZooKeeper",
          ex);
    }
  }

  public static ZooPropStore initialize(@NonNull final InstanceId instanceId,
      @NonNull final ZooReaderWriter zrw) {
    return new ZooPropStore(instanceId, zrw);
  }

  @Override
  public boolean exists(final PropStoreKey<?> propStoreKey) {
    try {
      if (zrw.exists(propStoreKey.getPath())) {
        return true;
      }
    } catch (KeeperException ex) {
      // ignore Keeper exception on check.
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted testing if node exists", ex);
    }
    return false;
  }

  public PropStoreMetrics getMetrics() {
    return cacheMetrics;
  }

  @Override
  public void create(PropStoreKey<?> propStoreKey, Map<String,String> props) {

    try {
      VersionedProperties vProps = new VersionedProperties(props);
      String path = propStoreKey.getPath();
      zrw.putPrivatePersistentData(path, codec.toBytes(vProps), ZooUtil.NodeExistsPolicy.FAIL);
    } catch (IOException | KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to serialize properties for " + propStoreKey, ex);
    }
  }

  /**
   * get or create properties from the store. If the property node does not exist in ZooKeeper,
   * legacy properties exist, they will be converted to the new storage form and naming convention.
   * The legacy properties are deleted once the new node format is written.
   *
   * @param propStoreKey the prop cache key
   * @return The versioned properties.
   * @throws IllegalStateException if the updates fails because of an underlying store exception
   */
  @Override
  public @NonNull VersionedProperties get(final PropStoreKey<?> propStoreKey) {
    checkZkConnection(); // if ZK not connected, block, do not just return a cached value.
    propStoreWatcher.registerListener(propStoreKey, this);

    var props = cache.get(propStoreKey);
    if (props != null) {
      return props;
    }

    if (propStoreKey instanceof SystemPropKey) {
      return new ConfigTransformer(zrw, codec, propStoreWatcher).transform(propStoreKey,
          propStoreKey.getPath(), false);
    }

    throw new IllegalStateException(
        "Invalid request for " + propStoreKey + ", the property node does not exist");
  }

  /**
   * Convenience method for utilities that may not have a PropStore read the encoded properties
   * directly from ZooKeeper. This allows utilities access when there is a ZooKeeper, may there may
   * not be a full instance running. All exception handling is left to the caller.
   *
   * @param propStoreKey the prop cache key
   * @param watcher a prop store watcher that will receive / handle ZooKeeper events.
   * @param zooReader a ZooReader with an authenticated session.
   * @return the versioned properties or null if the node does not exist.
   * @throws IOException if the underlying data from the ZooKeeper node cannot be decoded.
   * @throws KeeperException if a ZooKeeper exception occurs
   * @throws InterruptedException if the ZooKeeper read was interrupted.
   */
  public static @Nullable VersionedProperties readFromZk(final PropStoreKey<?> propStoreKey,
      final PropStoreWatcher watcher, final ZooReader zooReader)
      throws IOException, KeeperException, InterruptedException {
    try {
      Stat stat = new Stat();
      byte[] bytes = zooReader.getData(propStoreKey.getPath(), watcher, stat);
      if (stat.getDataLength() == 0) {
        // node exists - but is empty - no props have been stored on node.
        return null;
      }
      return codec.fromBytes(stat.getVersion(), bytes);
    } catch (KeeperException.NoNodeException ex) {
      // ignore no node - allow other exceptions to propagate
      return null;
    }
  }

  /**
   * Copies all mappings from the specified map and into the existing property values and stores
   * them into the backend store. New keys are added and keys the may have existed in the current
   * properties are overwritten.
   * <p>
   * If multiple threads attempt to update values concurrently, this method will automatically
   * retry. If the threads are setting different keys / values the result will be the sum of the
   * changes. If the concurrent threads are attempting to set a value(s) for the same key(s), the
   * value(s) will be the set to the values provided by the last thread to complete. The order is
   * indeterminate.
   *
   * @param propStoreKey the prop cache id
   * @param props a map of property k,v pairs
   * @throws IllegalStateException if the values cannot be written or if an underlying store
   *         exception occurs.
   */
  @Override
  public void putAll(@NonNull PropStoreKey<?> propStoreKey, @NonNull Map<String,String> props) {
    if (props.isEmpty()) {
      return; // no props - noop
    }
    mutateVersionedProps(propStoreKey, VersionedProperties::addOrUpdate, props);
  }

  @Override
  public void replaceAll(@NonNull PropStoreKey<?> propStoreKey, long version,
      @NonNull Map<String,String> props) {
    mutateVersionedProps(propStoreKey, VersionedProperties::replaceAll, version, props);
  }

  @Override
  public void removeProperties(@NonNull PropStoreKey<?> propStoreKey,
      @NonNull Collection<String> keys) {
    if (keys.isEmpty()) {
      return; // no keys - noop.
    }
    mutateVersionedProps(propStoreKey, VersionedProperties::remove, keys);
  }

  @Override
  public void delete(@NonNull PropStoreKey<?> propStoreKey) {
    Objects.requireNonNull(propStoreKey, "prop store delete() - Must provide propCacheId");
    try {
      log.trace("called delete() for: {}", propStoreKey);
      final String path = propStoreKey.getPath();
      zrw.delete(path);
      cache.remove(propStoreKey);
    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to delete properties for propCacheId " + propStoreKey,
          ex);
    }
  }

  private <T> void mutateVersionedProps(PropStoreKey<?> propStoreKey,
      BiFunction<VersionedProperties,T,VersionedProperties> action, T changes) {

    log.trace("mutateVersionedProps called for: {}", propStoreKey);

    try {

      VersionedProperties vProps = cache.getIfCached(propStoreKey);
      if (vProps == null) {
        vProps = readPropsFromZk(propStoreKey);
      }

      for (int attempts = 3; attempts > 0; --attempts) {

        VersionedProperties updates = action.apply(vProps, changes);

        if (zrw.overwritePersistentData(propStoreKey.getPath(), codec.toBytes(updates),
            (int) updates.getDataVersion())) {
          return;
        }
        Thread.sleep(20); // small pause to get thread to yield.
        // re-read from zookeeper to ensure the latest version.
        vProps = readPropsFromZk(propStoreKey);
      }
      throw new IllegalStateException(
          "failed to remove properties to zooKeeper for " + propStoreKey, null);
    } catch (IllegalArgumentException | IOException ex) {
      throw new IllegalStateException(
          "Codec failed to decode / encode properties for " + propStoreKey, ex);
    } catch (InterruptedException | KeeperException ex) {
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException(
          "failed to remove properties to zooKeeper for " + propStoreKey, ex);
    }
  }

  private <T> void mutateVersionedProps(PropStoreKey<?> propStoreKey,
      BiFunction<VersionedProperties,T,VersionedProperties> action, long existingVersion,
      T changes) {

    log.trace("mutateVersionedProps called for: {}", propStoreKey);

    try {
      // Grab the current properties
      VersionedProperties vProps = cache.getIfCached(propStoreKey);
      if (vProps == null) {
        vProps = readPropsFromZk(propStoreKey);
      }

      // Compare the version of the current properties in the cache/ZK and the passed in version to
      // see if the versions match
      // If the versions do not match then we want to throw an error. This check here allows us to
      // avoid
      // an extra call to zookeeper if the versions don't match
      if (vProps.getDataVersion() != existingVersion) {
        throw new ConcurrentModificationException("Failed to modify properties to zooKeeper for "
            + propStoreKey + ", properties changed since reading.", null);
      }

      final VersionedProperties updates = action.apply(vProps, changes);

      // We checked the version earlier but this could still fail if another update was made
      // and hadn't propagated yet to update the cache during the time that updates were applied
      // after reading
      // Zookeeper will return false if the versions do not match, so we should throw an error to
      // let the caller
      // know the version supplied is no longer the latest
      if (!zrw.overwritePersistentData(propStoreKey.getPath(), codec.toBytes(updates),
          (int) updates.getDataVersion())) {
        throw new ConcurrentModificationException("Failed to modify properties to zooKeeper for "
            + propStoreKey + ", properties changed since reading.", null);
      }
    } catch (IllegalArgumentException | IOException ex) {
      throw new IllegalStateException(
          "Codec failed to decode / encode properties for " + propStoreKey, ex);
    } catch (InterruptedException | KeeperException ex) {
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new IllegalStateException(
          "failed to modify properties to zooKeeper for " + propStoreKey, ex);
    }
  }

  @Override
  public void registerAsListener(PropStoreKey<?> propStoreKey, PropChangeListener listener) {
    propStoreWatcher.registerListener(propStoreKey, listener);
  }

  private void checkZkConnection() {
    if (zkReadyMon.test()) {
      return;
    }

    cache.removeAll();

    // not ready block or propagate error if it times out.
    zkReadyMon.isReady();

  }

  @Override
  public void zkChangeEvent(PropStoreKey<?> propStoreKey) {
    log.trace("Received change event from ZooKeeper for: {} removed from cache", propStoreKey);
    cache.remove(propStoreKey);
  }

  /**
   * NOOP for the prop store - the cache value will reflect the updated value on next read. The
   * change is also sent to external listeners of the need to take action, but for the prop store,
   * no additional action is required.
   *
   * @param propStoreKey the prop cache id.
   */
  @Override
  public void cacheChangeEvent(PropStoreKey<?> propStoreKey) {
    log.trace("zkChangeEvent: {}", propStoreKey);
  }

  @Override
  public void deleteEvent(PropStoreKey<?> propStoreKey) {
    log.trace("deleteEvent: {}", propStoreKey);
    cache.remove(propStoreKey);
  }

  @Override
  public void connectionEvent() {
    log.trace("connectionEvent");
    cache.removeAll();
  }

  /**
   * Read and decode property node from ZooKeeper.
   *
   * @param propStoreKey the propStoreKey
   * @return the decoded properties.
   * @throws KeeperException if a ZooKeeper exception occurs to allow caller to decide on action.
   * @throws IOException if the node data cannot be decoded into properties to allow the caller to
   *         decide on action.
   * @throws IllegalStateException if an interrupt occurs. The interrupt status is reasserted and
   *         usually best to not otherwise try to handle the exception.
   */
  private VersionedProperties readPropsFromZk(PropStoreKey<?> propStoreKey)
      throws KeeperException, IOException {
    try {
      Stat stat = new Stat();
      byte[] bytes = zrw.getData(propStoreKey.getPath(), stat);
      if (stat.getDataLength() == 0) {
        return new VersionedProperties();
      }
      return codec.fromBytes(stat.getVersion(), bytes);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupt received during ZooKeeper read", ex);
    }
  }

  @Override
  public PropCache getCache() {
    return cache;
  }

  @Override
  public @Nullable VersionedProperties getIfCached(PropStoreKey<?> propStoreKey) {
    return cache.getIfCached(propStoreKey);
  }

  @Override
  public boolean validateDataVersion(PropStoreKey<?> storeKey, long expectedVersion) {
    try {
      Stat stat = zrw.getStatus(storeKey.getPath());
      log.trace("data version sync: stat returned: {} for {}", stat, storeKey);
      if (stat == null || expectedVersion != stat.getVersion()) {
        propStoreWatcher.signalZkChangeEvent(storeKey);
        return false;
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(ex);
    } catch (KeeperException.NoNodeException ex) {
      propStoreWatcher.signalZkChangeEvent(storeKey);
      return false;
    } catch (KeeperException ex) {
      log.debug("exception occurred verifying data version for {}", storeKey);
      return false;
    }
    return true;
  }

}

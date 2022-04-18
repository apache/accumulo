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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.metrics.MetricsUtil;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCache;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.apache.accumulo.server.conf.util.ConfigTransformer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;

public class ZooPropStore implements PropStore, PropChangeListener {

  private final static Logger log = LoggerFactory.getLogger(ZooPropStore.class);
  private final static VersionedPropCodec codec = VersionedPropCodec.getDefault();

  private final ZooReaderWriter zrw;
  private final PropStoreWatcher propStoreWatcher;
  private final PropCache cache;
  private final PropStoreMetrics cacheMetrics = new PropStoreMetrics();
  private final ReadyMonitor zkReadyMon;

  /**
   * Create instance using ZooPropStore.Builder
   *
   * @param instanceId
   *          the instance id
   * @param zrw
   *          a wrapper set of utilities for accessing ZooKeeper.
   * @param readyMonitor
   *          coordination utility for ZooKeeper connection status.
   * @param propStoreWatcher
   *          an extended ZooKeeper watcher
   * @param ticker
   *          a synthetic clock used for testing.
   */
  private ZooPropStore(final InstanceId instanceId, final ZooReaderWriter zrw,
      final ReadyMonitor readyMonitor, final PropStoreWatcher propStoreWatcher,
      final Ticker ticker) {

    this.zrw = zrw;
    this.zkReadyMon = readyMonitor;
    this.propStoreWatcher = propStoreWatcher;

    MetricsUtil.initializeProducers(cacheMetrics);

    ZooPropLoader propLoader = new ZooPropLoader(zrw, codec, propStoreWatcher, cacheMetrics);

    if (ticker == null) {
      cache = new PropCacheCaffeineImpl.Builder(propLoader, cacheMetrics).build();
    } else {
      cache =
          new PropCacheCaffeineImpl.Builder(propLoader, cacheMetrics).withTicker(ticker).build();
    }

    try {
      var path = ZooUtil.getRoot(instanceId);
      if (zrw.exists(path, propStoreWatcher)) {
        log.debug("Have a ZooKeeper connection and found instance node: {}", instanceId);
        zkReadyMon.setReady();
      } else {
        throw new IllegalStateException("Instance may not have been initialized, root node: " + path
            + " does not exist in ZooKeeper");
      }
    } catch (InterruptedException | KeeperException ex) {
      throw new IllegalStateException("Failed to read root node " + instanceId + " from ZooKeeper",
          ex);
    }
  }

  public static PropStore initialize(final InstanceId instanceId, final ZooReaderWriter zrw) {
    return new ZooPropStore.Builder(instanceId, zrw, zrw.getSessionTimeout()).build();
  }

  /**
   * Create the system configuration node and initialize with empty props - used when creating a new
   * Accumulo instance.
   * <p>
   * Needs to be called early in the Accumulo ZooKeeper initialization sequence so that correct
   * watchers can be created for the instance when the PropStore is instantiated.
   *
   * @param instanceId
   *          the instance uuid.
   * @param zrw
   *          a ZooReaderWriter
   */
  public static void instancePathInit(final InstanceId instanceId, final ZooReaderWriter zrw)
      throws InterruptedException, KeeperException {
    var sysPropPath = PropCacheKey.forSystem(instanceId).getPath();
    VersionedProperties vProps = new VersionedProperties();
    try {
      var created =
          zrw.putPersistentData(sysPropPath, codec.toBytes(vProps), ZooUtil.NodeExistsPolicy.FAIL);
      if (!created) {
        throw new IllegalStateException(
            "Failed to create default system props during initialization at: {}" + sysPropPath);
      }
    } catch (IOException ex) {
      throw new IllegalStateException(
          "Failed to create default system props during initialization at: {}" + sysPropPath, ex);
    }
  }

  @Override
  public boolean exists(final PropCacheKey propCacheKey) throws PropStoreException {
    try {
      if (zrw.exists(propCacheKey.getPath())) {
        return true;
      }

    } catch (KeeperException ex) {
      // ignore Keeper exception on check.
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new PropStoreException("Interrupted testing if node exists", ex);
    }
    return false;
  }

  /**
   * Create initial system props for the instance. If the node already exists, no action is
   * performed.
   *
   * @param context
   *          the server context.
   * @param initProps
   *          map of k, v pairs of initial properties.
   */
  public synchronized static void initSysProps(final ServerContext context,
      final Map<String,String> initProps) {
    PropCacheKey sysPropKey = PropCacheKey.forSystem(context.getInstanceID());
    createInitialProps(context, sysPropKey, initProps);
  }

  /**
   * Create initial properties if they do not exist. If the node exists, initialization will be
   * skipped.
   *
   * @param context
   *          the system context
   * @param propCacheKey
   *          a prop id
   * @param props
   *          initial properties
   */
  public static void createInitialProps(final ServerContext context,
      final PropCacheKey propCacheKey, Map<String,String> props) {

    try {
      ZooReaderWriter zrw = context.getZooReaderWriter();
      if (zrw.exists(propCacheKey.getPath())) {
        return;
      }
      VersionedProperties vProps = new VersionedProperties(props);
      zrw.putPersistentData(propCacheKey.getPath(), codec.toBytes(vProps),
          ZooUtil.NodeExistsPolicy.FAIL);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new PropStoreException("Interrupted creating node " + propCacheKey, ex);
    } catch (Exception ex) {
      throw new PropStoreException("Failed to create node " + propCacheKey, ex);
    }
  }

  public PropStoreMetrics getMetrics() {
    return cacheMetrics;
  }

  @Override
  public void create(PropCacheKey propCacheKey, Map<String,String> props) {

    try {
      VersionedProperties vProps = new VersionedProperties(props);
      String path = propCacheKey.getPath();
      zrw.putPersistentData(path, codec.toBytes(vProps), ZooUtil.NodeExistsPolicy.FAIL);
    } catch (IOException | KeeperException | InterruptedException ex) {
      throw new PropStoreException("Failed to serialize properties for " + propCacheKey, ex);
    }
  }

  /**
   * get or create properties from the store. If the property node does not exist in ZooKeeper,
   * legacy properties exist, they will be converted to the new storage form and naming convention.
   * The legacy properties are deleted once the new node format is written.
   *
   * @param propCacheKey
   *          the prop cache key
   * @return The versioned properties.
   * @throws PropStoreException
   *           if the updates fails because of an underlying store exception
   */
  @Override
  public @NonNull VersionedProperties get(final PropCacheKey propCacheKey)
      throws PropStoreException {
    checkZkConnection(); // if ZK not connected, block, do not just return a cached value.
    propStoreWatcher.registerListener(propCacheKey, this);

    var props = cache.get(propCacheKey);
    if (props != null) {
      return props;
    }

    if (propCacheKey.getIdType() == PropCacheKey.IdType.SYSTEM) {
      return new ConfigTransformer(zrw, codec, propStoreWatcher).transform(propCacheKey);
    }

    throw new PropStoreException(
        "Invalid request for " + propCacheKey + ", the property node does not exist", null);
  }

  /**
   * Convenience method for utilities that may not have a PropStore read the encoded properties
   * directly from ZooKeeper. This allows utilities access when there is a ZooKeeper, may there may
   * not be a full instance running. All exception handling is left to the caller.
   *
   * @param propCacheKey
   *          the prop cache key
   * @param watcher
   *          a prop store watcher that will receive / handle ZooKeeper events.
   * @param zrw
   *          a ZooReaderWriter
   * @return the versioned properties or null if the node does not exist.
   * @throws IOException
   *           if the underlying data from the ZooKeeper node cannot be decoded.
   * @throws KeeperException
   *           if a ZooKeeper exception occurs
   * @throws InterruptedException
   *           if the ZooKeeper read was interrupted.
   */
  public static @Nullable VersionedProperties readFromZk(final PropCacheKey propCacheKey,
      final PropStoreWatcher watcher, final ZooReaderWriter zrw)
      throws IOException, KeeperException, InterruptedException {
    if (zrw.exists(propCacheKey.getPath())) {
      Stat stat = new Stat();
      byte[] bytes = zrw.getData(propCacheKey.getPath(), watcher, stat);
      return codec.fromBytes(stat.getVersion(), bytes);
    }
    return null;
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
   * @param propCacheKey
   *          the prop cache id
   * @param props
   *          a map of property k,v pairs
   * @throws PropStoreException
   *           if the values cannot be written or if an underlying store exception occurs.
   */
  @Override
  public void putAll(PropCacheKey propCacheKey, Map<String,String> props) {
    mutateVersionedProps(propCacheKey, VersionedProperties::addOrUpdate, props);
  }

  @Override
  public void removeProperties(PropCacheKey propCacheKey, Collection<String> keys) {
    mutateVersionedProps(propCacheKey, VersionedProperties::remove, keys);
  }

  @Override
  public void delete(PropCacheKey propCacheKey) throws PropStoreException {
    Objects.requireNonNull(propCacheKey, "prop store delete() - Must provide propCacheId");
    try {
      log.trace("called delete() for: {}", propCacheKey);
      final String path = propCacheKey.getPath();
      zrw.delete(path);
      cache.remove(propCacheKey);
    } catch (KeeperException | InterruptedException ex) {
      throw new PropStoreException("Failed to delete properties for propCacheId " + propCacheKey,
          ex);
    }
  }

  private <T> void mutateVersionedProps(PropCacheKey propCacheKey,
      BiFunction<VersionedProperties,T,VersionedProperties> action, T changes) {

    log.trace("mutateVersionedProps called for: {}", propCacheKey);

    try {

      VersionedProperties vProps = cache.getWithoutCaching(propCacheKey);
      if (vProps == null) {
        vProps = readPropsFromZk(propCacheKey);
      }

      for (int attempts = 3; attempts > 0; --attempts) {

        VersionedProperties updates = action.apply(vProps, changes);

        if (zrw.overwritePersistentData(propCacheKey.getPath(), codec.toBytes(updates),
            updates.getDataVersion())) {
          return;
        }
        Thread.sleep(20); // small pause to get thread to yield.
        // re-read from zookeeper to ensure the latest version.
        vProps = readPropsFromZk(propCacheKey);
      }
      throw new PropStoreException("failed to remove properties to zooKeeper for " + propCacheKey,
          null);
    } catch (IllegalArgumentException | IOException ex) {
      throw new PropStoreException("Codec failed to decode / encode properties for " + propCacheKey,
          ex);
    } catch (InterruptedException | KeeperException ex) {
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new PropStoreException("failed to remove properties to zooKeeper for " + propCacheKey,
          ex);
    }
  }

  @Override
  public void registerAsListener(PropCacheKey propCacheKey, PropChangeListener listener) {
    propStoreWatcher.registerListener(propCacheKey, listener);
  }

  private void checkZkConnection() throws PropStoreException {
    if (zkReadyMon.test()) {
      return;
    }

    cache.removeAll();

    // not ready block or throw error if it times out.
    try {
      zkReadyMon.isReady();
    } catch (IllegalStateException ex) {
      if (Thread.interrupted()) {
        Thread.currentThread().interrupt();
        throw new PropStoreException("Interrupted waiting for ZooKeeper connection", ex);
      }
      throw new PropStoreException("Failed to get zooKeeper connection", ex);
    }
  }

  @Override
  public void zkChangeEvent(PropCacheKey propCacheKey) {
    log.trace("Received change event from ZooKeeper for: {} removed from cache", propCacheKey);
    cache.remove(propCacheKey);
  }

  /**
   * NOOP for the prop store - the cache value will reflect the updated value on next read. The
   * change is also sent to external listeners of the need to take action, but for the prop store,
   * no additional action is required.
   *
   * @param propCacheKey
   *          the prop cache id.
   */
  @Override
  public void cacheChangeEvent(PropCacheKey propCacheKey) {
    log.trace("zkChangeEvent: {}", propCacheKey);
  }

  @Override
  public void deleteEvent(PropCacheKey propCacheKey) {
    log.trace("deleteEvent: {}", propCacheKey);
    cache.remove(propCacheKey);
  }

  @Override
  public void connectionEvent() {
    log.trace("connectionEvent");
    cache.removeAll();
  }

  /** for testing - force Caffeine housekeeping to run. */
  @VisibleForTesting
  public void cleanUp() {
    ((PropCacheCaffeineImpl) cache).cleanUp();
  }

  /**
   * Read and decode property node from ZooKeeper.
   *
   * @param propCacheKey
   *          the propCacheKey
   * @return the decoded properties.
   * @throws KeeperException
   *           if a ZooKeeper exception occurs to allow caller to decide on action.
   * @throws IOException
   *           if the node data cannot be decoded into properties to allow the caller to decide on
   *           action.
   * @throws PropStoreException
   *           if an interrupt occurs. The interrupt status is reasserted and usually best to not
   *           otherwise try to handle the exception.
   */
  private VersionedProperties readPropsFromZk(PropCacheKey propCacheKey)
      throws KeeperException, IOException {
    try {
      Stat stat = new Stat();
      byte[] bytes = zrw.getData(propCacheKey.getPath(), stat);
      return codec.fromBytes(stat.getVersion(), bytes);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new PropStoreException("Interrupt received during ZooKeeper read", ex);
    }
  }

  /**
   * Construct a ZooPropStore instance. For production only a ServerContext is required to
   * instantiate the prop store. The capability to supply an external watcher is to help with
   * testing so that the tests can mock or have access to the watcher.
   */
  public static class Builder {

    private final InstanceId instanceId;
    private final ZooReaderWriter zrw;
    private final ReadyMonitor readyMonitor;
    private PropStoreWatcher propStoreWatcher = null;
    private Ticker ticker = null;

    /**
     * Create a Builder instance, using provided context to get necessary parameters.
     *
     * @param context
     *          a ServerContext
     */
    public Builder(final ServerContext context) {
      this(context.getInstanceID(), context.getZooReaderWriter(),
          context.getZooKeepersSessionTimeOut());
    }

    /**
     * Alternate builder that makes mocking easier by removing ServerContext dependency and using
     * parameters injected directly.
     *
     * @param instanceId
     *          the instance id.
     * @param zrw
     *          a ZooReaderWriter
     * @param zooSessionTimeout
     *          the zoo session timeout in milliseconds.
     */
    @VisibleForTesting
    public Builder(final InstanceId instanceId, final ZooReaderWriter zrw,
        final long zooSessionTimeout) {
      this.instanceId = instanceId;
      this.zrw = zrw;

      long readyTimeout = Math.round(zooSessionTimeout * 1.75);
      readyMonitor = new ReadyMonitor("prop-store", readyTimeout);
    }

    /**
     * Construct a ZooPropStore instance.
     *
     * @return a ZooPropStore.
     */
    public ZooPropStore build() {
      if (propStoreWatcher == null) { // if watcher not provided, create one.
        propStoreWatcher = new PropStoreWatcher(readyMonitor);
      }
      return new ZooPropStore(instanceId, zrw, readyMonitor, propStoreWatcher, ticker);
    }

    /**
     * Use an external PropStoreWatcher - for testing so that the watcher is accessible to the tests
     * or can be a mock instance.
     *
     * @param watcher
     *          a PropStoreWatcher.
     * @return reference to the builder for fluent style chaining.
     */
    public Builder withWatcher(final PropStoreWatcher watcher) {
      propStoreWatcher = watcher;
      return this;
    }

    /**
     * Allow a fake clock to be used for testing operations in the cache such as expiration without
     * needing to wait the required wall clock time.
     *
     * @param ticker
     *          a synthetic clock used for testing
     * @return reference to the builder for fluent style chaining.
     */
    public Builder withTicker(final Ticker ticker) {
      this.ticker = ticker;
      return this;
    }
  }
}

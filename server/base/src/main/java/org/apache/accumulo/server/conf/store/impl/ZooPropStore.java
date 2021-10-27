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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.codec.VersionedPropCodec;
import org.apache.accumulo.server.conf.codec.VersionedProperties;
import org.apache.accumulo.server.conf.store.PropCache;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropStore implements PropStore, PropChangeListener {

  private final static Logger log = LoggerFactory.getLogger(ZooPropStore.class);

  private final VersionedPropCodec propCodec;

  private final ReadyMonitor zkReadyMon;

  private final ServerContext context;
  private final ZooReaderWriter zrw;
  private final PropStoreWatcher propStoreWatcher;
  private final PropCache cache;
  private final PropStoreMetrics cacheMetrics = new PropStoreMetrics();

  private Map<String,String> fixedProps = null;

  /**
   * Create instance using ZooPropStore.Builder
   *
   * @param context
   *          the server context
   * @param readyMonitor
   *          coordination utility for ZooKeeper connection status.
   * @param propStoreWatcher
   *          an extended ZooKeeper watcher
   */
  private ZooPropStore(final ServerContext context, final ReadyMonitor readyMonitor,
      final PropStoreWatcher propStoreWatcher) {

    this.context = context;
    this.zrw = context.getZooReaderWriter();
    this.zkReadyMon = readyMonitor;
    this.propStoreWatcher = propStoreWatcher;

    this.propCodec = context.getVersionedPropertiesCodec();

    ZooPropLoader propLoader = new ZooPropLoader(zrw, context.getVersionedPropertiesCodec(),
        propStoreWatcher, cacheMetrics);

    cache = new CaffeineCache.Builder(propLoader, cacheMetrics).build();

    try {
      var path = ZooUtil.getRoot(context.getInstanceID());
      if (zrw.exists(path, propStoreWatcher)) {
        log.debug("have a ZooKeeper connection");
        zkReadyMon.setReady();
      } else {
        throw new IllegalStateException(
            "Instance may not have been initialized, root node" + path + " does not exist");
      }
    } catch (InterruptedException | KeeperException ex) {
      throw new IllegalStateException("Failed to read root node from ZooKeeper", ex);
    }
  }

  /**
   * Create initial system props for the instance. If the node already exists, no action is
   * performed.
   *
   * @param context
   *          the server context.
   * @param initProps
   *          map of k, v pairs of initial properties.
   * @return true if props create, false if the node existed and initialization was skipped.
   */
  public synchronized static boolean initSysProps(final ServerContext context,
      final Map<String,String> initProps) {
    PropCacheId sysPropsId = PropCacheId.forSystem(context.getInstanceID());
    return createInitialProps(context, sysPropsId, initProps);
  }

  /**
   * Create initial properties if they do not exist. If the node exists, initialization will be
   * skipped.
   *
   * @param context
   *          the system context
   * @param propCacheId
   *          a prop id
   * @param props
   *          initial properties
   * @return true if props create, false if the node existed and initialization was skipped.
   */
  public static boolean createInitialProps(final ServerContext context,
      final PropCacheId propCacheId, Map<String,String> props) {

    try {
      ZooReaderWriter zrw = context.getZooReaderWriter();
      if (zrw.exists(propCacheId.getPath())) {
        return false;
      }
      VersionedProperties vProps = new VersionedProperties(props);
      return zrw.putPersistentData(propCacheId.getPath(),
          context.getVersionedPropertiesCodec().toBytes(vProps), ZooUtil.NodeExistsPolicy.FAIL);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted creating node " + propCacheId, ex);
    } catch (Exception ex) {
      throw new IllegalStateException("Failed to create node " + propCacheId, ex);
    }
  }

  public PropStoreMetrics getMetrics() {
    return cacheMetrics;
  }

  // TODO - evaluate returning the props instead of boolean.
  @Override
  public boolean create(PropCacheId propCacheId, Map<String,String> props)
      throws PropStoreException {

    VersionedProperties vProps = new VersionedProperties(props);

    try {

      var path = propCacheId.getPath();
      if (!zrw.putPersistentData(path, propCodec.toBytes(vProps), ZooUtil.NodeExistsPolicy.FAIL)) {
        return false;
      }

      Stat stat = zrw.getStatus(path, propStoreWatcher);

      if (stat.getVersion() != vProps.getNextVersion()) {
        throw new PropStoreException(
            "Invalid data version on create, received " + stat.getVersion(),
            new IllegalStateException());
      }

      return true;

    } catch (IOException | KeeperException | InterruptedException ex) {
      throw new PropStoreException("Failed to serialize properties for " + propCacheId, ex);
    }
  }

  @Override
  public VersionedProperties get(final PropCacheId propCacheId) throws PropStoreException {
    try {

      checkZkConnection(); // if ZK not connected, block, do not just return a cached value.
      propStoreWatcher.registerListener(propCacheId, this);
      return cache.get(propCacheId);

    } catch (Exception ex) {
      throw new PropStoreException("read from prop store get() failed for: " + propCacheId, ex);
    }
  }

  @Override
  public boolean putAll(final PropCacheId propCacheId, final Map<String,String> props)
      throws PropStoreException {

    try {

      VersionedProperties vProps = cache.getWithoutCaching(propCacheId);
      if (vProps == null) {
        byte[] bytes = zrw.getData(propCacheId.getPath());
        vProps = propCodec.fromBytes(bytes);
      }

      VersionedProperties updates = vProps.addOrUpdate(props);

      return zrw.overwritePersistentData(propCacheId.getPath(), propCodec.toBytes(updates),
          updates.getDataVersion());

    } catch (IOException ex) {
      throw new PropStoreException("failed to encode properties for " + propCacheId, ex);
    } catch (InterruptedException | KeeperException ex) {
      throw new PropStoreException("failed to write properties to zooKeeper for " + propCacheId,
          ex);
    }
  }

  @Override
  public void delete(PropCacheId propCacheId) throws PropStoreException {
    Objects.requireNonNull(propCacheId, "prop store delete() - Must provide propCacheId");
    try {
      log.trace("called delete() for: {}", propCacheId);
      final String path = propCacheId.getPath();
      zrw.delete(path);
      cache.remove(propCacheId);
      // TODO ? propStoreWatcher.listenerCleanup(propCacheId);
    } catch (KeeperException | InterruptedException ex) {
      throw new PropStoreException("Failed to delete properties for propCacheId " + propCacheId,
          ex);
    }
  }

  @Override
  public boolean removeProperties(PropCacheId propCacheId, Collection<String> keys)
      throws PropStoreException {

    try {

      VersionedProperties vProps = cache.getWithoutCaching(propCacheId);
      if (vProps == null) {
        log.trace("removeProperties - not in cache");
        byte[] bytes = zrw.getData(propCacheId.getPath());
        vProps = propCodec.fromBytes(bytes);
      } else {
        log.trace("removeProperties - read from cache");
      }

      VersionedProperties updates = vProps.remove(keys);

      zrw.getZooKeeper().setData(propCacheId.getPath(), propCodec.toBytes(updates),
          updates.getDataVersion());

      return true;

    } catch (IOException ex) {
      throw new PropStoreException("failed to encode properties for " + propCacheId, ex);
    } catch (InterruptedException | KeeperException ex) {
      throw new PropStoreException("failed to write properties to zooKeeper for " + propCacheId,
          ex);
    }
  }

  @Override
  public synchronized Map<String,String> readFixed() {

    if (fixedProps != null) {
      return fixedProps;
    }

    fixedProps = new HashMap<>();

    PropCacheId systemId = PropCacheId.forSystem(context.getInstanceID());

    try {

      Map<String,String> propsRead;

      byte[] data = zrw.getData(systemId.getPath());
      // TODO - this might not be required - after initSysProps system props should always exist
      if (data != null) {
        try {
          propsRead = propCodec.fromBytes(data).getProperties();
        } catch (IOException ex) {
          throw new IllegalStateException("Failed to decode system properties", ex);
        }
      } else {
        propsRead = new HashMap<>();
      }

      for (Property p : Property.fixedProperties) {
        fixedProps.put(p.getKey(), propsRead.getOrDefault(p.getKey(), p.getDefaultValue()));
      }

      return fixedProps;

    } catch (KeeperException | InterruptedException ex) {
      throw new IllegalStateException("Failed to read system properties", ex);
    }
  }

  @Override
  public void registerAsListener(PropCacheId propCacheId, PropChangeListener listener) {
    propStoreWatcher.registerListener(propCacheId, listener);
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
  public void zkChangeEvent(PropCacheId id) {
    log.trace("zkChangeEvent: {}", id);
    cache.remove(id);
  }

  /**
   * NOOP for the prop store - the cache value will reflect the updated value on next read. The
   * change is also sent to external listeners of the need to take action, but for the prop store,
   * no additional action is required.
   *
   * @param id
   *          the prop cache id.
   */
  @Override
  public void cacheChangeEvent(PropCacheId id) {
    log.trace("zkChangeEvent: {}", id);
    // noop - cache already reflects change
  }

  @Override
  public void deleteEvent(PropCacheId id) {
    log.trace("deleteEvent: {}", id);
    cache.remove(id);
  }

  @Override
  public void connectionEvent() {
    log.trace("connectionEvent");
    cache.removeAll();
  }

  /**
   * Construct a ZooPropStore instance. For production only a ServerContext is required to
   * instantiate the prop store. The capability to supply an external watcher is to help with
   * testing so that the tests can mock or have access to the watcher.
   */
  public static class Builder {

    private final ServerContext context;
    private final ReadyMonitor readyMonitor;
    private PropStoreWatcher propStoreWatcher = null;

    public Builder(final ServerContext context) {
      this.context = context;
      long zooTimeout = Math.round(context.getZooKeepersSessionTimeOut() * 1.75);
      readyMonitor = new ReadyMonitor("prop-store", zooTimeout);
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
      return new ZooPropStore(context, readyMonitor, propStoreWatcher);
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
  }
}

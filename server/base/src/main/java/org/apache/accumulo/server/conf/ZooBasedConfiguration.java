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
package org.apache.accumulo.server.conf;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropCacheId;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreException;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

/**
 * Instances maintain a local cache of the AccumuloConfiguration hierarchy that will be consistent
 * with stored properties.
 * <p>
 * When calling getProperties - the local copy will be updated if ZooKeeper changes have been
 * received.
 * <p>
 * The getUpdateCount() provides an optimization for clients - the count can be used to detect
 * changes without reading the properties. When the update count changes, the next getProperties
 * call will update the local copy and the change count.
 */
public class ZooBasedConfiguration extends AccumuloConfiguration implements PropChangeListener {

  protected final Logger log;
  private final AccumuloConfiguration parent;
  private final PropCacheId propCacheId;
  private final PropStore propStore;

  private final AtomicReference<PropSnapshot> snapshotRef = new AtomicReference<>(null);

  public ZooBasedConfiguration(Logger log, ServerContext context, PropCacheId propCacheId,
      AccumuloConfiguration parent) {
    this.log = requireNonNull(log, "a Logger must be supplied");
    requireNonNull(context, "the context cannot be null");
    this.propCacheId = requireNonNull(propCacheId, "a PropCacheId must be supplied");
    this.parent = requireNonNull(parent, "An AccumuloConfiguration parent must be supplied");

    this.propStore =
        requireNonNull(context.getPropStore(), "The PropStore must be supplied and exist");

    propStore.registerAsListener(propCacheId, this);

    snapshotRef.set(updateSnapshot());

  }

  public long getDataVersion() {
    var snapshot = snapshotRef.get();
    if (snapshot == null) {
      return updateSnapshot().getDataVersion();
    }
    return snapshot.getDataVersion();
  }

  /**
   * The update count is the sum of the change count of this configuration and the change counts of
   * the parents. The count is used to detect if any changes occurred in the configuration hierarchy
   * and if the configuration needs to be recalculated to maintain consistency with values in the
   * backend store.
   * <p>
   * The count is required to be an increasing value.
   */
  @Override
  public long getUpdateCount() {
    long count = 0;
    long dataVersion = 0;
    for (AccumuloConfiguration p = this; p != null; p = p.getParent()) {
      if (p instanceof ZooBasedConfiguration) {
        dataVersion = ((ZooBasedConfiguration) p).getDataVersion();
      } else {
        dataVersion = p.getUpdateCount();
      }
      count += dataVersion;
    }

    log.trace("update count result for: {} - data version: {} update: {}", propCacheId, dataVersion,
        count);
    return count;
  }

  @Override
  public AccumuloConfiguration getParent() {
    return parent;
  }

  public PropCacheId getCacheId() {
    return propCacheId;
  }

  @Override
  public @Nullable String get(final Property property) {
    Map<String,String> props = getSnapshot();
    String value = props.get(property.getKey());
    if (value != null) {
      return value;
    }
    AccumuloConfiguration parent = getParent();
    if (parent != null) {
      return parent.get(property);
    }
    return null;
  }

  @Override
  public void getProperties(final Map<String,String> props, final Predicate<String> filter) {

    parent.getProperties(props, filter);

    Map<String,String> theseProps = getSnapshot();

    log.trace("getProperties() for: {} filter: {}, have: {}, passed: {}", getCacheId(), filter,
        theseProps, props);

    for (Map.Entry<String,String> p : theseProps.entrySet()) {
      if (filter.test(p.getKey()) && p.getValue() != null) {
        log.trace("passed filter - add to map: {} = {}", p.getKey(), p.getValue());
        props.put(p.getKey(), p.getValue());
      }
    }
  }

  @Override
  public boolean isPropertySet(final Property property) {

    Map<String,String> theseProps = getSnapshot();

    if (theseProps.get(property.getKey()) != null) {
      return true;
    }

    return getParent().isPropertySet(property);

  }

  public Map<String,String> getSnapshot() {
    if (snapshotRef.get() == null) {
      return updateSnapshot().getProps();
    }
    return snapshotRef.get().getProps();
  }

  @Override
  public void invalidateCache() {
    snapshotRef.set(null);
  }

  private final Lock updateLock = new ReentrantLock();

  private @NonNull PropSnapshot updateSnapshot() throws PropStoreException {

    PropSnapshot localSnapshot = snapshotRef.get();

    if (localSnapshot != null) {
      // no changes return locally cached config
      return localSnapshot;
    }
    updateLock.lock();
    int retryCount = 5;
    try {
      localSnapshot = snapshotRef.get();
      // check for update while waiting for lock.
      if (localSnapshot != null) {
        return localSnapshot;
      }

      PropSnapshot propsRead;

      long startCount;
      do {
        startCount = propStore.getNodeVersion(propCacheId);
        propsRead = doRead();
        if (propsRead.getDataVersion() == startCount) {
          snapshotRef.set(propsRead);
          return snapshotRef.get();
        }
      } while (--retryCount > 0);

      snapshotRef.set(null);
    } finally {
      updateLock.unlock();
    }
    throw new IllegalStateException(
        "Failed to read property updates for " + propCacheId + " after " + retryCount + " tries");

  }

  private PropSnapshot doRead() throws PropStoreException {

    var vProps = propStore.get(propCacheId);
    log.trace("doRead() - updateSnapshot() for {}, returned: {}", propCacheId, vProps);
    if (vProps == null) {
      // TODO - this could return marker instead?
      // return new PropSnapshot(INVALID_DATA, Map.of());
      throw new IllegalStateException("Properties for " + propCacheId + " do not exist");
    } else {
      return new PropSnapshot(vProps.getDataVersion(), vProps.getProperties());
    }
  }

  @Override
  public void zkChangeEvent(PropCacheId watchedId) {
    if (propCacheId.equals(watchedId)) {
      log.debug("Received zookeeper property change event for {} - current version: {}",
          propCacheId,
          snapshotRef.get() != null ? snapshotRef.get().getDataVersion() : "no data version set");
      // snapshotRef.set(new PropSnapshot(INVALID_DATA_VER, Map.of()));
      snapshotRef.set(null);
    }
  }

  @Override
  public void cacheChangeEvent(PropCacheId watchedId) {
    if (propCacheId.equals(watchedId)) {
      log.debug("Received cache property change event for {} - current version: {}", propCacheId,
          snapshotRef.get() != null ? snapshotRef.get().getDataVersion() : "no data version set");
      // snapshotRef.set(new PropSnapshot(INVALID_DATA_VER, Map.of()));
      snapshotRef.set(null);
    }
  }

  @Override
  public void deleteEvent(PropCacheId watchedId) {
    if (propCacheId.equals(watchedId)) {
      // snapshotRef.set(new PropSnapshot(INVALID_DATA_VER, Map.of()));
      snapshotRef.set(null);
      log.info("Received property delete event for {}", propCacheId);
    }
  }

  @Override
  public void connectionEvent() {
    snapshotRef.set(null);
    log.info("Received connection event - update properties required");
  }

  private static class PropSnapshot {

    private final long dataVersion;
    private final Map<String,String> snapshot;

    public PropSnapshot(final long dataVersion, final Map<String,String> props) {
      requireNonNull(props, "property map must be supplied");
      this.dataVersion = dataVersion;
      this.snapshot = props;
    }

    public long getDataVersion() {
      return dataVersion;
    }

    public @NonNull Map<String,String> getProps() {
      return snapshot;
    }
  }
}

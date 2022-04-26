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
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.util.PropSnapshot;
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
  private final PropCacheKey propCacheKey;

  private final PropSnapshot snapshot;

  public ZooBasedConfiguration(Logger log, ServerContext context, PropCacheKey propCacheKey,
      AccumuloConfiguration parent) {
    this.log = requireNonNull(log, "a Logger must be supplied");
    requireNonNull(context, "the context cannot be null");
    this.propCacheKey = requireNonNull(propCacheKey, "a PropCacheId must be supplied");
    this.parent = requireNonNull(parent, "An AccumuloConfiguration parent must be supplied");

    var propStore = context.getPropStore();
    snapshot = new PropSnapshot(propCacheKey, propStore);
    propStore.registerAsListener(propCacheKey, this);
  }

  public long getDataVersion() {
    return snapshot.get().getDataVersion();
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

    log.trace("update count result for: {} - data version: {} update: {}", propCacheKey,
        dataVersion, count);
    return count;
  }

  @Override
  public AccumuloConfiguration getParent() {
    return parent;
  }

  public PropCacheKey getPropCacheKey() {
    return propCacheKey;
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

    log.trace("getProperties() for: {} filter: {}, have: {}, passed: {}", getPropCacheKey(), filter,
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

  public @NonNull Map<String,String> getSnapshot() {
    return snapshot.get().getProperties();
  }

  @Override
  public void invalidateCache() {
    snapshot.requireUpdate();
  }

  @Override
  public void zkChangeEvent(final PropCacheKey eventPropKey) {
    if (propCacheKey.equals(eventPropKey)) {
      snapshot.requireUpdate();
    }
  }

  @Override
  public void cacheChangeEvent(final PropCacheKey eventPropKey) {
    if (propCacheKey.equals(eventPropKey)) {
      snapshot.requireUpdate();
    }
  }

  @Override
  public void deleteEvent(final PropCacheKey eventPropKey) {
    if (propCacheKey.equals(eventPropKey)) {
      snapshot.requireUpdate();
      log.debug("Received property delete event for {}", propCacheKey);
    }
  }

  @Override
  public void connectionEvent() {
    snapshot.requireUpdate();
    log.debug("Received connection event - update properties required");
  }

}

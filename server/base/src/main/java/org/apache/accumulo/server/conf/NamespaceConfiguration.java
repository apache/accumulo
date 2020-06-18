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

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.ZooCachePropertyAccessor.PropCacheKey;

public class NamespaceConfiguration extends AccumuloConfiguration {

  private static final Map<PropCacheKey,ZooCache> propCaches = new java.util.HashMap<>();

  private final AccumuloConfiguration parent;
  private final AtomicReference<ZooCachePropertyAccessor> propCacheAccessor =
      new AtomicReference<>();
  protected NamespaceId namespaceId = null;
  protected ServerContext context;
  private ZooCacheFactory zcf = new ZooCacheFactory();
  private final String path;

  public NamespaceConfiguration(NamespaceId namespaceId, ServerContext context,
      AccumuloConfiguration parent) {
    this.context = context;
    this.parent = parent;
    this.namespaceId = namespaceId;
    this.path = context.getZooKeeperRoot() + Constants.ZNAMESPACES + "/" + namespaceId
        + Constants.ZNAMESPACE_CONF;
  }

  /**
   * Gets the parent configuration of this configuration.
   *
   * @return parent configuration
   */
  public AccumuloConfiguration getParentConfiguration() {
    return parent;
  }

  void setZooCacheFactory(ZooCacheFactory zcf) {
    this.zcf = zcf;
  }

  private ZooCache getZooCache() {
    synchronized (propCaches) {
      PropCacheKey key = new PropCacheKey(context.getInstanceID(), namespaceId.canonical());
      ZooCache propCache = propCaches.get(key);
      if (propCache == null) {
        propCache = zcf.getZooCache(context.getZooKeepers(), context.getZooKeepersSessionTimeOut());
        propCaches.put(key, propCache);
      }
      return propCache;
    }
  }

  private ZooCachePropertyAccessor getPropCacheAccessor() {
    // updateAndGet below always calls compare and set, so avoid if not null
    ZooCachePropertyAccessor zcpa = propCacheAccessor.get();
    if (zcpa != null)
      return zcpa;

    return propCacheAccessor
        .updateAndGet(pca -> pca == null ? new ZooCachePropertyAccessor(getZooCache()) : pca);
  }

  private String getPath() {
    return path;
  }

  @Override
  public boolean isPropertySet(Property prop, boolean cacheAndWatch) {
    if (!cacheAndWatch)
      throw new UnsupportedOperationException(
          "Namespace configuration only supports checking if a property is set in cache.");

    if (getPropCacheAccessor().isPropertySet(prop, getPath()))
      return true;

    return parent.isPropertySet(prop, cacheAndWatch);
  }

  @Override
  public String get(Property property) {
    String key = property.getKey();
    AccumuloConfiguration getParent;
    if (namespaceId.equals(Namespace.ACCUMULO.id()) && isIteratorOrConstraint(key)) {
      // ignore iterators from parent if system namespace
      getParent = null;
    } else {
      getParent = parent;
    }
    return getPropCacheAccessor().get(property, getPath(), getParent);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    Predicate<String> parentFilter = filter;
    // exclude system iterators/constraints from the system namespace
    // so they don't affect the metadata or root tables.
    if (getNamespaceId().equals(Namespace.ACCUMULO.id()))
      parentFilter = key -> isIteratorOrConstraint(key) ? false : filter.test(key);

    getPropCacheAccessor().getProperties(props, getPath(), filter, parent, parentFilter);
  }

  protected NamespaceId getNamespaceId() {
    return namespaceId;
  }

  static boolean isIteratorOrConstraint(String key) {
    return key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())
        || key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey());
  }

  @Override
  public synchronized void invalidateCache() {
    ZooCachePropertyAccessor pca = propCacheAccessor.get();

    if (pca != null) {
      pca.invalidateCache();
    }
    // Else, if the accessor is null, we could lock and double-check
    // to see if it happened to be created so we could invalidate its cache
    // but I don't see much benefit coming from that extra check.
  }

  @Override
  public long getUpdateCount() {
    return parent.getUpdateCount() + getPropCacheAccessor().getZooCache().getUpdateCount();
  }
}

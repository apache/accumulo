/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.server.conf;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.ObservableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.conf.ZooCachePropertyAccessor.PropCacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableConfiguration extends ObservableConfiguration {
  private static final Logger log = LoggerFactory.getLogger(TableConfiguration.class);

  private static final Map<PropCacheKey,ZooCache> propCaches = new java.util.HashMap<>();

  private ZooCachePropertyAccessor propCacheAccessor = null;
  private final Instance instance;
  private final NamespaceConfiguration parent;
  private ZooCacheFactory zcf = new ZooCacheFactory();

  private final Table.ID tableId;

  public TableConfiguration(Instance instance, Table.ID tableId, NamespaceConfiguration parent) {
    this.instance = requireNonNull(instance);
    this.tableId = requireNonNull(tableId);
    this.parent = requireNonNull(parent);
  }

  void setZooCacheFactory(ZooCacheFactory zcf) {
    this.zcf = zcf;
  }

  private synchronized ZooCachePropertyAccessor getPropCacheAccessor() {
    if (propCacheAccessor == null) {
      synchronized (propCaches) {
        PropCacheKey key = new PropCacheKey(instance.getInstanceID(), tableId.canonicalID());
        ZooCache propCache = propCaches.get(key);
        if (propCache == null) {
          propCache = zcf.getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut(), new TableConfWatcher(instance));
          propCaches.put(key, propCache);
        }
        propCacheAccessor = new ZooCachePropertyAccessor(propCache);
      }
    }
    return propCacheAccessor;
  }

  @Override
  public void addObserver(ConfigurationObserver co) {
    if (tableId == null) {
      String err = "Attempt to add observer for non-table configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    iterator();
    super.addObserver(co);
  }

  @Override
  public void removeObserver(ConfigurationObserver co) {
    if (tableId == null) {
      String err = "Attempt to remove observer for non-table configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    super.removeObserver(co);
  }

  private String getPath() {
    return ZooUtil.getRoot(instance.getInstanceID()) + Constants.ZTABLES + "/" + tableId + Constants.ZTABLE_CONF;
  }

  @Override
  public String get(Property property) {
    return getPropCacheAccessor().get(property, getPath(), parent);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    getPropCacheAccessor().getProperties(props, getPath(), filter, parent, null);
  }

  public Table.ID getTableId() {
    return tableId;
  }

  /**
   * returns the actual NamespaceConfiguration that corresponds to the current parent namespace.
   */
  public NamespaceConfiguration getNamespaceConfiguration() {
    return new ServerConfigurationFactory(parent.inst).getNamespaceConfiguration(parent.namespaceId);
  }

  /**
   * Gets the parent configuration of this configuration. The parent is actually a {@link TableParentConfiguration} that can change which namespace it
   * references.
   *
   * @return parent configuration
   */
  public NamespaceConfiguration getParentConfiguration() {
    return parent;
  }

  @Override
  public synchronized void invalidateCache() {
    if (null != propCacheAccessor) {
      propCacheAccessor.invalidateCache();
    }
    // Else, if the accessor is null, we could lock and double-check
    // to see if it happened to be created so we could invalidate its cache
    // but I don't see much benefit coming from that extra check.
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}

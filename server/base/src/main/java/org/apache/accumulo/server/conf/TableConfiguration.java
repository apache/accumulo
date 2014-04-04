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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.log4j.Logger;

public class TableConfiguration extends AccumuloConfiguration {
  private static final Logger log = Logger.getLogger(TableConfiguration.class);

  // Need volatile keyword to ensure double-checked locking works as intended
  private static volatile ZooCache tablePropCache = null;
  private static final Object initLock = new Object();

  private final String instanceId;
  private final Instance instance;
  private final NamespaceConfiguration parent;

  private String table = null;
  private Set<ConfigurationObserver> observers;

  public TableConfiguration(String instanceId, String table, NamespaceConfiguration parent) {
    this(instanceId, HdfsZooInstance.getInstance(), table, parent);
  }

  public TableConfiguration(String instanceId, Instance instance, String table, NamespaceConfiguration parent) {
    this.instanceId = instanceId;
    this.instance = instance;
    this.table = table;
    this.parent = parent;

    this.observers = Collections.synchronizedSet(new HashSet<ConfigurationObserver>());
  }

  private void initializeZooCache() {
    synchronized (initLock) {
      if (null == tablePropCache) {
        tablePropCache = new ZooCacheFactory().getZooCache(instance.getZooKeepers(), instance.getZooKeepersSessionTimeOut(), new TableConfWatcher(instance));
      }
    }
  }

  private ZooCache getTablePropCache() {
    if (null == tablePropCache) {
      initializeZooCache();
    }
    return tablePropCache;
  }

  public void addObserver(ConfigurationObserver co) {
    if (table == null) {
      String err = "Attempt to add observer for non-table configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    iterator();
    observers.add(co);
  }

  public void removeObserver(ConfigurationObserver configObserver) {
    if (table == null) {
      String err = "Attempt to remove observer for non-table configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    observers.remove(configObserver);
  }

  public void expireAllObservers() {
    Collection<ConfigurationObserver> copy = Collections.unmodifiableCollection(observers);
    for (ConfigurationObserver co : copy)
      co.sessionExpired();
  }

  public void propertyChanged(String key) {
    Collection<ConfigurationObserver> copy = Collections.unmodifiableCollection(observers);
    for (ConfigurationObserver co : copy)
      co.propertyChanged(key);
  }

  public void propertiesChanged(String key) {
    Collection<ConfigurationObserver> copy = Collections.unmodifiableCollection(observers);
    for (ConfigurationObserver co : copy)
      co.propertiesChanged();
  }

  @Override
  public String get(Property property) {
    String key = property.getKey();
    String value = get(getTablePropCache(), key);

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using default value for " + key + " due to improperly formatted " + property.getType() + ": " + value);
      value = parent.get(property);
    }
    return value;
  }

  private String get(ZooCache zc, String key) {
    String zPath = ZooUtil.getRoot(instanceId) + Constants.ZTABLES + "/" + table + Constants.ZTABLE_CONF + "/" + key;
    byte[] v = zc.get(zPath);
    String value = null;
    if (v != null)
      value = new String(v, Constants.UTF8);
    return value;
  }

  @Override
  public void getProperties(Map<String,String> props, PropertyFilter filter) {
    parent.getProperties(props, filter);

    ZooCache zc = getTablePropCache();

    List<String> children = zc.getChildren(ZooUtil.getRoot(instanceId) + Constants.ZTABLES + "/" + table + Constants.ZTABLE_CONF);
    if (children != null) {
      for (String child : children) {
        if (child != null && filter.accept(child)) {
          String value = get(zc, child);
          if (value != null)
            props.put(child, value);
        }
      }
    }
  }

  public String getTableId() {
    return table;
  }

  /**
   * returns the actual NamespaceConfiguration that corresponds to the current parent namespace.
   */
  public NamespaceConfiguration getNamespaceConfiguration() {
    return ServerConfiguration.getNamespaceConfiguration(parent.inst, parent.namespaceId);
  }

  /**
   * returns the parent, which is actually a TableParentConfiguration that can change which namespace it references
   */
  public NamespaceConfiguration getParentConfiguration() {
    return parent;
  }

  @Override
  public void invalidateCache() {
    if (null != tablePropCache) {
      tablePropCache.clear();
    }
    // Else, if the cache is null, we could lock and double-check
    // to see if it happened to be created so we could invalidate it
    // but I don't see much benefit coming from that extra check.
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }
}

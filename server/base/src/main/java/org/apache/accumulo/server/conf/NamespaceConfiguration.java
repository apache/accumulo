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

import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationObserver;
import org.apache.accumulo.core.conf.ObservableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.conf.ZooCachePropertyAccessor.PropCacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceConfiguration extends ObservableConfiguration {
  private static final Logger log = LoggerFactory.getLogger(NamespaceConfiguration.class);

  private static final Map<PropCacheKey,ZooCache> propCaches = new java.util.HashMap<>();

  private final AccumuloConfiguration parent;
  private ZooCachePropertyAccessor propCacheAccessor = null;
  protected String namespaceId = null;
  protected Instance inst = null;
  private ZooCacheFactory zcf = new ZooCacheFactory();

  public NamespaceConfiguration(String namespaceId, Instance inst, AccumuloConfiguration parent) {
    this.inst = inst;
    this.parent = parent;
    this.namespaceId = namespaceId;
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

  private synchronized ZooCachePropertyAccessor getPropCacheAccessor() {
    if (propCacheAccessor == null) {
      synchronized (propCaches) {
        PropCacheKey key = new PropCacheKey(inst.getInstanceID(), namespaceId);
        ZooCache propCache = propCaches.get(key);
        if (propCache == null) {
          propCache = zcf.getZooCache(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), new NamespaceConfWatcher(inst));
          propCaches.put(key, propCache);
        }
        propCacheAccessor = new ZooCachePropertyAccessor(propCache);
      }
    }
    return propCacheAccessor;
  }

  private String getPath() {
    return ZooUtil.getRoot(inst.getInstanceID()) + Constants.ZNAMESPACES + "/" + getNamespaceId() + Constants.ZNAMESPACE_CONF;
  }

  @Override
  public String get(Property property) {
    String key = property.getKey();
    AccumuloConfiguration getParent;
    if (!(namespaceId.equals(Namespaces.ACCUMULO_NAMESPACE_ID) && isIteratorOrConstraint(key))) {
      getParent = parent;
    } else {
      // ignore iterators from parent if system namespace
      getParent = null;
    }
    return getPropCacheAccessor().get(property, getPath(), getParent);
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    Predicate<String> parentFilter = filter;
    // exclude system iterators/constraints from the system namespace
    // so they don't affect the metadata or root tables.
    if (getNamespaceId().equals(Namespaces.ACCUMULO_NAMESPACE_ID))
      parentFilter = key -> isIteratorOrConstraint(key) ? false : filter.test(key);

    getPropCacheAccessor().getProperties(props, getPath(), filter, parent, parentFilter);
  }

  protected String getNamespaceId() {
    return namespaceId;
  }

  @Override
  public void addObserver(ConfigurationObserver co) {
    if (namespaceId == null) {
      String err = "Attempt to add observer for non-namespace configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    iterator();
    super.addObserver(co);
  }

  @Override
  public void removeObserver(ConfigurationObserver co) {
    if (namespaceId == null) {
      String err = "Attempt to remove observer for non-namespace configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    super.removeObserver(co);
  }

  static boolean isIteratorOrConstraint(String key) {
    return key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey()) || key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey());
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
}

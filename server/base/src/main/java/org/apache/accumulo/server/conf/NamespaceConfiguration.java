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

import java.util.List;
import java.util.Map;

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
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.log4j.Logger;

public class NamespaceConfiguration extends ObservableConfiguration {
  private static final Logger log = Logger.getLogger(NamespaceConfiguration.class);

  private final AccumuloConfiguration parent;
  private static volatile ZooCache propCache = null;
  private static final Object lock = new Object();
  protected String namespaceId = null;
  protected Instance inst = null;

  public NamespaceConfiguration(String namespaceId, AccumuloConfiguration parent) {
    this(namespaceId, HdfsZooInstance.getInstance(), parent);
  }

  public NamespaceConfiguration(String namespaceId, Instance inst, AccumuloConfiguration parent) {
    this.inst = inst;
    this.parent = parent;
    this.namespaceId = namespaceId;
  }

  @Override
  public String get(Property property) {
    String key = property.getKey();
    String value = get(getPropCache(), key);

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using default value for " + key + " due to improperly formatted " + property.getType() + ": " + value);
      if (!(namespaceId.equals(Namespaces.ACCUMULO_NAMESPACE_ID) && isIteratorOrConstraint(property.getKey()))) {
        // ignore iterators from parent if system namespace
        value = parent.get(property);
      }
    }
    return value;
  }

  private String get(ZooCache zc, String key) {
    String zPath = ZooUtil.getRoot(inst.getInstanceID()) + Constants.ZNAMESPACES + "/" + getNamespaceId() + Constants.ZNAMESPACE_CONF + "/" + key;
    byte[] v = zc.get(zPath);
    String value = null;
    if (v != null)
      value = new String(v, Constants.UTF8);
    return value;
  }

  private void initializePropCache() {
    synchronized (lock) {
      if (propCache == null)
        propCache = new ZooCacheFactory().getZooCache(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), new NamespaceConfWatcher(inst));
    }
  }

  private ZooCache getPropCache() {
    if (null == propCache) {
      initializePropCache();
    }
    return propCache;
  }

  private class SystemNamespaceFilter implements PropertyFilter {

    private PropertyFilter userFilter;

    SystemNamespaceFilter(PropertyFilter userFilter) {
      this.userFilter = userFilter;
    }

    @Override
    public boolean accept(String key) {
      if (isIteratorOrConstraint(key))
        return false;
      return userFilter.accept(key);
    }

  }

  @Override
  public void getProperties(Map<String,String> props, PropertyFilter filter) {

    PropertyFilter parentFilter = filter;

    // exclude system iterators/constraints from the system namespace
    // so they don't affect the metadata or root tables.
    if (getNamespaceId().equals(Namespaces.ACCUMULO_NAMESPACE_ID))
      parentFilter = new SystemNamespaceFilter(filter);

    parent.getProperties(props, parentFilter);

    ZooCache zc = getPropCache();

    List<String> children = zc.getChildren(ZooUtil.getRoot(inst.getInstanceID()) + Constants.ZNAMESPACES + "/" + getNamespaceId() + Constants.ZNAMESPACE_CONF);
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

  protected boolean isIteratorOrConstraint(String key) {
    return key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey()) || key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey());
  }

  @Override
  public void invalidateCache() {
    if (null != propCache) {
      propCache.clear();
    }
    // Else, if the cache is null, we could lock and double-check
    // to see if it happened to be created so we could invalidate it
    // but I don't see much benefit coming from that extra check.
  }
}

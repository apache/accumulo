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

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.log4j.Logger;

public class TableNamespaceConfiguration extends AccumuloConfiguration {
  private static final Logger log = Logger.getLogger(TableNamespaceConfiguration.class);
  
  private final AccumuloConfiguration parent;
  private static ZooCache propCache = null;
  private String namespaceId = null;
  private Instance inst = null;
  private Set<ConfigurationObserver> observers;

  public TableNamespaceConfiguration(String namespaceId, AccumuloConfiguration parent) {
    inst = HdfsZooInstance.getInstance();
    propCache = new ZooCache(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut());
    this.parent = parent;
    this.namespaceId = namespaceId;
    this.observers = Collections.synchronizedSet(new HashSet<ConfigurationObserver>());
  }
  
  @Override
  public void invalidateCache() {
    if (propCache != null)
      propCache.clear();
  }
  
  @Override
  public String get(Property property) {
    String key = property.getKey();
    String value = get(key);
    
    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using default value for " + key + " due to improperly formatted " + property.getType() + ": " + value);
      value = parent.get(property);
    }
    return value;
  }
  
  private String get(String key) {
    String zPath = ZooUtil.getRoot(inst.getInstanceID()) + Constants.ZNAMESPACES + "/" + getNamespaceId() + Constants.ZNAMESPACE_CONF + "/"
        + key;
    byte[] v = getPropCache().get(zPath);
    String value = null;
    if (v != null)
      value = new String(v, Constants.UTF8);
    return value;
  }
  
  private static ZooCache getPropCache() {
    Instance inst = HdfsZooInstance.getInstance();
    if (propCache == null)
      synchronized (TableNamespaceConfiguration.class) {
        if (propCache == null)
          propCache = new ZooCache(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut(), new TableConfWatcher(inst));
      }
    return propCache;
  }
  
  @Override
  public Iterator<Entry<String,String>> iterator() {
    TreeMap<String,String> entries = new TreeMap<String,String>();
    
    for (Entry<String,String> parentEntry : parent)
      entries.put(parentEntry.getKey(), parentEntry.getValue());
    
    List<String> children = getPropCache().getChildren(
        ZooUtil.getRoot(inst.getInstanceID()) + Constants.ZNAMESPACES + "/" + getNamespaceId() + Constants.ZNAMESPACE_CONF);
    if (children != null) {
      for (String child : children) {
        String value = get(child);
        if (child != null && value != null)
          entries.put(child, value);
      }
    }
    
    return entries.entrySet().iterator();
  }
  
  private String getNamespaceId() {
    return namespaceId;
  }
  
  public void addObserver(ConfigurationObserver co) {
    if (namespaceId == null) {
      String err = "Attempt to add observer for non-table-namespace configuration";
      log.error(err);
      throw new RuntimeException(err);
    }
    iterator();
    observers.add(co);
  }
  
  public void removeObserver(ConfigurationObserver configObserver) {
    if (namespaceId == null) {
      String err = "Attempt to remove observer for non-table-namespace configuration";
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
}

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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooCache;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerConstants;
import org.apache.accumulo.server.client.HdfsZooInstance.AccumuloNotInitializedException;
import org.apache.log4j.Logger;

public class ZooConfiguration extends AccumuloConfiguration {
  private static final Logger log = Logger.getLogger(ZooConfiguration.class);
  
  private final AccumuloConfiguration parent;
  private static ZooConfiguration instance = null;
  private static String instanceId = null;
  private static ZooCache propCache = null;
  private Map<String,String> fixedProps = Collections.synchronizedMap(new HashMap<String,String>());
  
  private ZooConfiguration(AccumuloConfiguration parent) {
    this.parent = parent;
  }
  
  synchronized public static ZooConfiguration getInstance(Instance inst, AccumuloConfiguration parent) {
    if (instance == null) {
      propCache = new ZooCache(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut());
      instance = new ZooConfiguration(parent);
      instanceId = inst.getInstanceID();
    }
    return instance;
  }
  
  @SuppressWarnings("deprecation")
  synchronized public static ZooConfiguration getInstance(AccumuloConfiguration parent) {
    if (instance == null) {
      propCache = new ZooCache(parent.get(Property.INSTANCE_ZK_HOST), (int) parent.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT));
      instance = new ZooConfiguration(parent);
      instanceId = ZooKeeperInstance.getInstanceIDFromHdfs(ServerConstants.getInstanceIdLocation());
    }
    return instance;
  }
  
  private String _get(Property property) {
    String key = property.getKey();
    String value = null;
    
    if (Property.isValidZooPropertyKey(key)) {
      try {
        value = get(key);
      } catch (AccumuloNotInitializedException e) {
        log.warn("failed to lookup property in zookeeper: " + key, e);
      }
    }
    
    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using parent value for " + key + " due to improperly formatted " + property.getType() + ": " + value);
      value = parent.get(property);
    }
    return value;
  }
  
  public String get(Property property) {
    if (Property.isFixedZooPropertyKey(property)) {
      if (fixedProps.containsKey(property.getKey())) {
        return fixedProps.get(property.getKey());
      } else {
        synchronized (fixedProps) {
          String val = _get(property);
          fixedProps.put(property.getKey(), val);
          return val;
        }
        
      }
    } else {
      return _get(property);
    }
  }
  
  private String get(String key) {
    String zPath = ZooUtil.getRoot(instanceId) + Constants.ZCONFIG + "/" + key;
    byte[] v = propCache.get(zPath);
    String value = null;
    if (v != null)
      value = new String(v);
    return value;
  }
  
  @Override
  public Iterator<Entry<String,String>> iterator() {
    TreeMap<String,String> entries = new TreeMap<String,String>();
    
    for (Entry<String,String> parentEntry : parent)
      entries.put(parentEntry.getKey(), parentEntry.getValue());
    
    List<String> children = propCache.getChildren(ZooUtil.getRoot(instanceId) + Constants.ZCONFIG);
    if (children != null) {
      for (String child : children) {
        String value = get(child);
        if (child != null && value != null)
          entries.put(child, value);
      }
    }
    
    /*
     * //this code breaks the shells ability to show updates just made //the code is probably not needed as fixed props are only obtained through get
     * 
     * for(Property prop : Property.getFixedProperties()) get(prop);
     * 
     * for(Entry<String, String> fprop : fixedProps.entrySet()) entries.put(fprop.getKey(), fprop.getValue());
     */
    
    return entries.entrySet().iterator();
  }
}

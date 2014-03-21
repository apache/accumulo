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

import java.security.SecurityPermission;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigSanityCheck;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.KeyExtent;

public class ServerConfiguration {
  
  private static final Map<String,TableConfiguration> tableInstances = new HashMap<String,TableConfiguration>(1);
  private static SecurityPermission CONFIGURATION_PERMISSION = new SecurityPermission("configurationPermission");
  
  public static synchronized SiteConfiguration getSiteConfiguration() {
    checkPermissions();
    return SiteConfiguration.getInstance(getDefaultConfiguration());
  }
  
  private static void checkPermissions() {
    SecurityManager sm = System.getSecurityManager();
    if (sm != null) {
      sm.checkPermission(CONFIGURATION_PERMISSION);
    }
  }
  
  private static synchronized ZooConfiguration getZooConfiguration(Instance instance) {
    checkPermissions();
    return ZooConfiguration.getInstance(instance, getSiteConfiguration());
  }
  
  public static synchronized DefaultConfiguration getDefaultConfiguration() {
    checkPermissions();
    return DefaultConfiguration.getInstance();
  }
  
  public static synchronized AccumuloConfiguration getSystemConfiguration(Instance instance) {
    return getZooConfiguration(instance);
  }

  public static TableConfiguration getTableConfiguration(Instance instance, String tableId) {
    checkPermissions();
    synchronized (tableInstances) {
      TableConfiguration conf = tableInstances.get(tableId);
      if (conf == null) {
        conf = new TableConfiguration(instance.getInstanceID(), tableId, getSystemConfiguration(instance));
        ConfigSanityCheck.validate(conf);
        tableInstances.put(tableId, conf);
      }
      return conf;
    }
  }
  
  static void removeTableIdInstance(String tableId) {
    synchronized (tableInstances) {
      tableInstances.remove(tableId);
    }
  }
  
  static void expireAllTableObservers() {
    synchronized (tableInstances) {
      for (Entry<String,TableConfiguration> entry : tableInstances.entrySet()) {
        entry.getValue().expireAllObservers();
      }
    }
  }
  
  private final Instance instance;
  
  public ServerConfiguration(Instance instance) {
    this.instance = instance;
  }
  
  public TableConfiguration getTableConfiguration(String tableId) {
    return getTableConfiguration(instance, tableId);
  }
  
  public TableConfiguration getTableConfiguration(KeyExtent extent) {
    return getTableConfiguration(extent.getTableId().toString());
  }

  public synchronized AccumuloConfiguration getConfiguration() {
    return getZooConfiguration(instance);
  }
  
  public Instance getInstance() {
    return instance;
  }

}

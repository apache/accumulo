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

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigSanityCheck;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;

/**
 * A factor for configurations used by a server process. Instance of this class are thread-safe.
 */
public class ServerConfigurationFactory {

  private static final Map<String,Map<String,TableConfiguration>> tableConfigs;
  private static final Map<String,Map<String,NamespaceConfiguration>> namespaceConfigs;
  private static final Map<String,Map<String,NamespaceConfiguration>> tableParentConfigs;
  static {
    tableConfigs = new HashMap<String,Map<String,TableConfiguration>>(1);
    namespaceConfigs = new HashMap<String,Map<String,NamespaceConfiguration>>(1);
    tableParentConfigs = new HashMap<String,Map<String,NamespaceConfiguration>>(1);
  }

  private static void addInstanceToCaches(String iid) {
    synchronized (tableConfigs) {
      if (!tableConfigs.containsKey(iid)) {
        tableConfigs.put(iid, new HashMap<String,TableConfiguration>());
      }
    }
    synchronized (namespaceConfigs) {
      if (!namespaceConfigs.containsKey(iid)) {
        namespaceConfigs.put(iid, new HashMap<String,NamespaceConfiguration>());
      }
    }
    synchronized (tableParentConfigs) {
      if (!tableParentConfigs.containsKey(iid)) {
        tableParentConfigs.put(iid, new HashMap<String,NamespaceConfiguration>());
      }
    }
  }

  private static final SecurityPermission CONFIGURATION_PERMISSION = new SecurityPermission("configurationPermission");
  private static final SecurityManager SM = System.getSecurityManager();

  private static void checkPermissions() {
    if (SM != null) {
      SM.checkPermission(CONFIGURATION_PERMISSION);
    }
  }

  static boolean removeCachedTableConfiguration(String instanceId, String tableId) {
    synchronized (tableConfigs) {
      return tableConfigs.get(instanceId).remove(tableId) != null;
    }
  }

  static boolean removeCachedNamespaceConfiguration(String instanceId, String namespaceId) {
    synchronized (namespaceConfigs) {
      return namespaceConfigs.get(instanceId).remove(namespaceId) != null;
    }
  }

  static void clearCachedConfigurations() {
    synchronized (tableConfigs) {
      tableConfigs.clear();
    }
    synchronized (namespaceConfigs) {
      namespaceConfigs.clear();
    }
    synchronized (tableParentConfigs) {
      tableParentConfigs.clear();
    }
  }

  static void expireAllTableObservers() {
    synchronized (tableConfigs) {
      for (Map<String,TableConfiguration> instanceMap : tableConfigs.values()) {
        for (TableConfiguration c : instanceMap.values()) {
          c.expireAllObservers();
        }
      }
    }
  }

  private final Instance instance;
  private final String instanceID;
  private ZooCacheFactory zcf = new ZooCacheFactory();

  public ServerConfigurationFactory(Instance instance) {
    this.instance = instance;
    instanceID = instance.getInstanceID();
    addInstanceToCaches(instanceID);
  }

  void setZooCacheFactory(ZooCacheFactory zcf) {
    this.zcf = zcf;
  }

  private SiteConfiguration siteConfig = null;
  private DefaultConfiguration defaultConfig = null;
  private AccumuloConfiguration systemConfig = null;

  public synchronized SiteConfiguration getSiteConfiguration() {
    if (siteConfig == null) {
      checkPermissions();
      siteConfig = SiteConfiguration.getInstance(getDefaultConfiguration());
    }
    return siteConfig;
  }

  public synchronized DefaultConfiguration getDefaultConfiguration() {
    if (defaultConfig == null) {
      checkPermissions();
      defaultConfig = DefaultConfiguration.getInstance();
    }
    return defaultConfig;
  }

  public synchronized AccumuloConfiguration getConfiguration() {
    if (systemConfig == null) {
      checkPermissions();
      systemConfig = new ZooConfigurationFactory().getInstance(instance, zcf, getSiteConfiguration());
    }
    return systemConfig;
  }

  public TableConfiguration getTableConfiguration(String tableId) {
    checkPermissions();
    TableConfiguration conf;
    synchronized (tableConfigs) {
      conf = tableConfigs.get(instanceID).get(tableId);
    }
    // can't hold the lock during the construction and validation of the config,
    // which may result in creating multiple objects for the same id, but that's ok.
    if (conf == null && Tables.exists(instance, tableId)) {
      conf = new TableConfiguration(instance.getInstanceID(), tableId, getNamespaceConfigurationForTable(tableId));
      ConfigSanityCheck.validate(conf);
      synchronized (tableConfigs) {
        tableConfigs.get(instanceID).put(tableId, conf);
      }
    }
    return conf;
  }

  public TableConfiguration getTableConfiguration(KeyExtent extent) {
    return getTableConfiguration(extent.getTableId().toString());
  }

  public NamespaceConfiguration getNamespaceConfigurationForTable(String tableId) {
    checkPermissions();
    NamespaceConfiguration conf;
    synchronized (tableParentConfigs) {
      conf = tableParentConfigs.get(instanceID).get(tableId);
    }
    // can't hold the lock during the construction and validation of the config,
    // which may result in creating multiple objects for the same id, but that's ok.
    if (conf == null) {
      // changed - include instance in constructor call
      conf = new TableParentConfiguration(tableId, instance, getConfiguration());
      ConfigSanityCheck.validate(conf);
      synchronized (tableParentConfigs) {
        tableParentConfigs.get(instanceID).put(tableId, conf);
      }
    }
    return conf;
  }

  public NamespaceConfiguration getNamespaceConfiguration(String namespaceId) {
    checkPermissions();
    NamespaceConfiguration conf;
    // can't hold the lock during the construction and validation of the config,
    // which may result in creating multiple objects for the same id, but that's ok.
    synchronized (namespaceConfigs) {
      conf = namespaceConfigs.get(instanceID).get(namespaceId);
    }
    if (conf == null) {
      // changed - include instance in constructor call
      conf = new NamespaceConfiguration(namespaceId, instance, getConfiguration());
      conf.setZooCacheFactory(zcf);
      ConfigSanityCheck.validate(conf);
      synchronized (namespaceConfigs) {
        namespaceConfigs.get(instanceID).put(namespaceId, conf);
      }
    }
    return conf;
  }

  public Instance getInstance() {
    return instance;
  }
}

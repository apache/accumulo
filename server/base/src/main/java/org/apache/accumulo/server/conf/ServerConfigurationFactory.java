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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigSanityCheck;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;

/**
 * A factor for configurations used by a server process. Instance of this class are thread-safe.
 */
public class ServerConfigurationFactory extends ServerConfiguration {

  private static final Map<String,Map<Table.ID,TableConfiguration>> tableConfigs = new HashMap<>(1);
  private static final Map<String,Map<Namespace.ID,NamespaceConfiguration>> namespaceConfigs = new HashMap<>(1);
  private static final Map<String,Map<Table.ID,NamespaceConfiguration>> tableParentConfigs = new HashMap<>(1);

  private static void addInstanceToCaches(String iid) {
    synchronized (tableConfigs) {
      tableConfigs.computeIfAbsent(iid, k -> new HashMap<>());
    }
    synchronized (namespaceConfigs) {
      namespaceConfigs.computeIfAbsent(iid, k -> new HashMap<>());
    }
    synchronized (tableParentConfigs) {
      tableParentConfigs.computeIfAbsent(iid, k -> new HashMap<>());
    }
  }

  static boolean removeCachedTableConfiguration(String instanceId, Table.ID tableId) {
    synchronized (tableConfigs) {
      return tableConfigs.get(instanceId).remove(tableId) != null;
    }
  }

  static boolean removeCachedNamespaceConfiguration(String instanceId, Namespace.ID namespaceId) {
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
      for (Map<Table.ID,TableConfiguration> instanceMap : tableConfigs.values()) {
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
      siteConfig = SiteConfiguration.getInstance();
    }
    return siteConfig;
  }

  public synchronized DefaultConfiguration getDefaultConfiguration() {
    if (defaultConfig == null) {
      defaultConfig = DefaultConfiguration.getInstance();
    }
    return defaultConfig;
  }

  @Override
  public synchronized AccumuloConfiguration getSystemConfiguration() {
    if (systemConfig == null) {
      systemConfig = new ZooConfigurationFactory().getInstance(instance, zcf, getSiteConfiguration());
    }
    return systemConfig;
  }

  @Override
  public TableConfiguration getTableConfiguration(Table.ID tableId) {
    TableConfiguration conf;
    synchronized (tableConfigs) {
      conf = tableConfigs.get(instanceID).get(tableId);
    }

    // Can't hold the lock during the construction and validation of the config,
    // which would result in creating multiple objects for the same id.
    //
    // ACCUMULO-3859 We _cannot_ allow multiple instances to be created for a table. If the TableConfiguration
    // instance a Tablet holds is not the same as the one cached here, any ConfigurationObservers that
    // Tablet sets will never see updates from ZooKeeper which means that things like constraints and
    // default visibility labels will never be updated in a Tablet until it is reloaded.
    if (conf == null && Tables.exists(instance, tableId)) {
      conf = new TableConfiguration(instance, tableId, getNamespaceConfigurationForTable(tableId));
      ConfigSanityCheck.validate(conf);
      synchronized (tableConfigs) {
        Map<Table.ID,TableConfiguration> configs = tableConfigs.get(instanceID);
        TableConfiguration existingConf = configs.get(tableId);
        if (null == existingConf) {
          // Configuration doesn't exist yet
          configs.put(tableId, conf);
        } else {
          // Someone beat us to the punch, reuse their instance instead of replacing it
          conf = existingConf;
        }
      }
    }
    return conf;
  }

  public NamespaceConfiguration getNamespaceConfigurationForTable(Table.ID tableId) {
    NamespaceConfiguration conf;
    synchronized (tableParentConfigs) {
      conf = tableParentConfigs.get(instanceID).get(tableId);
    }
    // can't hold the lock during the construction and validation of the config,
    // which may result in creating multiple objects for the same id, but that's ok.
    if (conf == null) {
      // changed - include instance in constructor call
      conf = new TableParentConfiguration(tableId, instance, getSystemConfiguration());
      ConfigSanityCheck.validate(conf);
      synchronized (tableParentConfigs) {
        tableParentConfigs.get(instanceID).put(tableId, conf);
      }
    }
    return conf;
  }

  @Override
  public NamespaceConfiguration getNamespaceConfiguration(Namespace.ID namespaceId) {
    NamespaceConfiguration conf;
    // can't hold the lock during the construction and validation of the config,
    // which may result in creating multiple objects for the same id, but that's ok.
    synchronized (namespaceConfigs) {
      conf = namespaceConfigs.get(instanceID).get(namespaceId);
    }
    if (conf == null) {
      // changed - include instance in constructor call
      conf = new NamespaceConfiguration(namespaceId, instance, getSystemConfiguration());
      conf.setZooCacheFactory(zcf);
      ConfigSanityCheck.validate(conf);
      synchronized (namespaceConfigs) {
        namespaceConfigs.get(instanceID).put(namespaceId, conf);
      }
    }
    return conf;
  }

}

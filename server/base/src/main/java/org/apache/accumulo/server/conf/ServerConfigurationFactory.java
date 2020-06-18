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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigSanityCheck;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.ServerContext;

/**
 * A factor for configurations used by a server process. Instance of this class are thread-safe.
 */
public class ServerConfigurationFactory extends ServerConfiguration {

  private static final Map<String,Map<TableId,TableConfiguration>> tableConfigs = new HashMap<>(1);
  private static final Map<String,Map<NamespaceId,NamespaceConfiguration>> namespaceConfigs =
      new HashMap<>(1);
  private static final Map<String,Map<TableId,NamespaceConfiguration>> tableParentConfigs =
      new HashMap<>(1);

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

  private final ServerContext context;
  private final SiteConfiguration siteConfig;
  private final String instanceID;
  private ZooCacheFactory zcf = new ZooCacheFactory();

  public ServerConfigurationFactory(ServerContext context, SiteConfiguration siteConfig) {
    this.context = context;
    this.siteConfig = siteConfig;
    instanceID = context.getInstanceID();
    addInstanceToCaches(instanceID);
  }

  public ServerContext getServerContext() {
    return context;
  }

  void setZooCacheFactory(ZooCacheFactory zcf) {
    this.zcf = zcf;
  }

  private DefaultConfiguration defaultConfig = null;
  private AccumuloConfiguration systemConfig = null;

  public SiteConfiguration getSiteConfiguration() {
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
      systemConfig =
          new ZooConfigurationFactory().getInstance(context, zcf, getSiteConfiguration());
    }
    return systemConfig;
  }

  @Override
  public TableConfiguration getTableConfiguration(TableId tableId) {
    TableConfiguration conf;
    synchronized (tableConfigs) {
      conf = tableConfigs.get(instanceID).get(tableId);
    }

    // Can't hold the lock during the construction and validation of the config,
    // which would result in creating multiple objects for the same id.
    //
    // ACCUMULO-3859 We _cannot_ allow multiple instances to be created for a table. If the
    // TableConfiguration
    // instance a Tablet holds is not the same as the one cached here, any ConfigurationObservers
    // that
    // Tablet sets will never see updates from ZooKeeper which means that things like constraints
    // and
    // default visibility labels will never be updated in a Tablet until it is reloaded.
    if (conf == null && Tables.exists(context, tableId)) {
      conf = new TableConfiguration(context, tableId, getNamespaceConfigurationForTable(tableId));
      ConfigSanityCheck.validate(conf);
      synchronized (tableConfigs) {
        Map<TableId,TableConfiguration> configs = tableConfigs.get(instanceID);
        TableConfiguration existingConf = configs.get(tableId);
        if (existingConf == null) {
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

  public NamespaceConfiguration getNamespaceConfigurationForTable(TableId tableId) {
    NamespaceConfiguration conf;
    synchronized (tableParentConfigs) {
      conf = tableParentConfigs.get(instanceID).get(tableId);
    }
    // can't hold the lock during the construction and validation of the config,
    // which may result in creating multiple objects for the same id, but that's ok.
    if (conf == null) {
      NamespaceId namespaceId;
      try {
        namespaceId = Tables.getNamespaceId(context, tableId);
      } catch (TableNotFoundException e) {
        throw new RuntimeException(e);
      }
      conf = new NamespaceConfiguration(namespaceId, context, getSystemConfiguration());
      ConfigSanityCheck.validate(conf);
      synchronized (tableParentConfigs) {
        tableParentConfigs.get(instanceID).put(tableId, conf);
      }
    }
    return conf;
  }

  @Override
  public NamespaceConfiguration getNamespaceConfiguration(NamespaceId namespaceId) {
    NamespaceConfiguration conf;
    // can't hold the lock during the construction and validation of the config,
    // which may result in creating multiple objects for the same id, but that's ok.
    synchronized (namespaceConfigs) {
      conf = namespaceConfigs.get(instanceID).get(namespaceId);
    }
    if (conf == null) {
      // changed - include instance in constructor call
      conf = new NamespaceConfiguration(namespaceId, context, getSystemConfiguration());
      conf.setZooCacheFactory(zcf);
      ConfigSanityCheck.validate(conf);
      synchronized (namespaceConfigs) {
        namespaceConfigs.get(instanceID).put(namespaceId, conf);
      }
    }
    return conf;
  }

}

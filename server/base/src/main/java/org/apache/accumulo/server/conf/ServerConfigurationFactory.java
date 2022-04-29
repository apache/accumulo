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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigCheckUtil;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.ServerContext;

import com.google.common.base.Suppliers;

/**
 * A factor for configurations used by a server process. Instance of this class are thread-safe.
 */
public class ServerConfigurationFactory extends ServerConfiguration {

  private final Map<TableId,NamespaceConfiguration> tableParentConfigs = new ConcurrentHashMap<>();
  private final Map<TableId,TableConfiguration> tableConfigs = new ConcurrentHashMap<>();
  private final Map<NamespaceId,NamespaceConfiguration> namespaceConfigs =
      new ConcurrentHashMap<>();

  private final Supplier<ZooConfiguration> systemConfig;

  private final ServerContext context;
  private final SiteConfiguration siteConfig;
  private ZooCacheFactory zcf = new ZooCacheFactory();

  public ServerConfigurationFactory(ServerContext context, SiteConfiguration siteConfig) {
    this.context = context;
    this.siteConfig = siteConfig;
    systemConfig = Suppliers.memoize(() -> {
      // Force the creation of a new ZooCache instead of using a shared one.
      // This is done so that the ZooCache will update less often, causing the
      // configuration update count to increment more slowly.
      ZooCache propCache =
          zcf.getNewZooCache(context.getZooKeepers(), context.getZooKeepersSessionTimeOut());
      return new ZooConfiguration(context, propCache, siteConfig);
    });
  }

  public ServerContext getServerContext() {
    return context;
  }

  void setZooCacheFactory(ZooCacheFactory zcf) {
    this.zcf = zcf;
  }

  public SiteConfiguration getSiteConfiguration() {
    return siteConfig;
  }

  public DefaultConfiguration getDefaultConfiguration() {
    return DefaultConfiguration.getInstance();
  }

  @Override
  public AccumuloConfiguration getSystemConfiguration() {
    return systemConfig.get();
  }

  @Override
  public TableConfiguration getTableConfiguration(TableId tableId) {
    return tableConfigs.computeIfAbsent(tableId, key -> {
      if (context.tableNodeExists(tableId)) {
        var conf =
            new TableConfiguration(context, tableId, getNamespaceConfigurationForTable(tableId));
        ConfigCheckUtil.validate(conf);
        return conf;
      }
      return null;
    });
  }

  public NamespaceConfiguration getNamespaceConfigurationForTable(TableId tableId) {
    NamespaceId namespaceId;
    try {
      namespaceId = context.getNamespaceId(tableId);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    return tableParentConfigs.computeIfAbsent(tableId,
        key -> getNamespaceConfiguration(namespaceId));
  }

  @Override
  public NamespaceConfiguration getNamespaceConfiguration(NamespaceId namespaceId) {
    return namespaceConfigs.computeIfAbsent(namespaceId, key -> {
      var conf = new NamespaceConfiguration(namespaceId, context, getSystemConfiguration());
      conf.setZooCacheFactory(zcf);
      ConfigCheckUtil.validate(conf);
      return conf;
    });
  }

}

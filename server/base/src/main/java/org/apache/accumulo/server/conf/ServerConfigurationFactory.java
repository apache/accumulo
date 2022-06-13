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
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;

/**
 * A factor for configurations used by a server process. Instance of this class are thread-safe.
 */
public class ServerConfigurationFactory extends ServerConfiguration {
  private final static Logger log = LoggerFactory.getLogger(ServerConfigurationFactory.class);

  private final Map<TableId,NamespaceConfiguration> tableParentConfigs = new ConcurrentHashMap<>();
  private final Map<TableId,TableConfiguration> tableConfigs = new ConcurrentHashMap<>();
  private final Map<NamespaceId,NamespaceConfiguration> namespaceConfigs =
      new ConcurrentHashMap<>();

  private final ServerContext context;
  private final SiteConfiguration siteConfig;
  private final Supplier<SystemConfiguration> systemConfig;

  private final DeleteWatcher deleteWatcher = new DeleteWatcher();

  public ServerConfigurationFactory(ServerContext context, SiteConfiguration siteConfig) {
    this.context = context;
    this.siteConfig = siteConfig;
    systemConfig = Suppliers.memoize(
        () -> new SystemConfiguration(context, SystemPropKey.of(context), getSiteConfiguration()));
  }

  public ServerContext getServerContext() {
    return context;
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
        context.getPropStore().registerAsListener(TablePropKey.of(context, tableId), deleteWatcher);
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
      context.getPropStore().registerAsListener(NamespacePropKey.of(context, namespaceId),
          deleteWatcher);
      var conf = new NamespaceConfiguration(context, namespaceId, getSystemConfiguration());
      ConfigCheckUtil.validate(conf);
      return conf;
    });
  }

  private class DeleteWatcher implements PropChangeListener {

    @Override
    public void zkChangeEvent(PropStoreKey<?> propStoreKey) {
      // no-op. changes handled by prop store impl
    }

    @Override
    public void cacheChangeEvent(PropStoreKey<?> propStoreKey) {
      // no-op. changes handled by prop store impl
    }

    @Override
    public void deleteEvent(PropStoreKey<?> propStoreKey) {
      if (propStoreKey instanceof NamespacePropKey) {
        log.trace("configuration snapshot refresh: Handle namespace delete for {}", propStoreKey);
        namespaceConfigs.remove(((NamespacePropKey) propStoreKey).getId());
        return;
      }
      if (propStoreKey instanceof TablePropKey) {
        log.trace("configuration snapshot refresh: Handle table delete for {}", propStoreKey);
        tableConfigs.remove(((TablePropKey) propStoreKey).getId());
        tableParentConfigs.remove(((TablePropKey) propStoreKey).getId());
      }
    }

    @Override
    public void connectionEvent() {
      // no-op. changes handled by prop store impl
    }
  }
}

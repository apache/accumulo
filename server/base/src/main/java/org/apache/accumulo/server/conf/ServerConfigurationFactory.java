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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigCheckUtil;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A factor for configurations used by a server process. Instance of this class are thread-safe.
 */
@SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
    justification = "random number not used in secure context")
public class ServerConfigurationFactory extends ServerConfiguration {
  private final static Logger log = LoggerFactory.getLogger(ServerConfigurationFactory.class);

  public static final int REFRESH_PERIOD_MINUTES = 15;

  private final Map<TableId,NamespaceConfiguration> tableParentConfigs = new ConcurrentHashMap<>();
  private final Map<TableId,TableConfiguration> tableConfigs = new ConcurrentHashMap<>();
  private final Map<NamespaceId,NamespaceConfiguration> namespaceConfigs =
      new ConcurrentHashMap<>();

  private final ServerContext context;
  private final SiteConfiguration siteConfig;
  private final Supplier<SystemConfiguration> systemConfig;

  private final ScheduledFuture<?> refreshTaskFuture;

  private final DeleteWatcher deleteWatcher =
      new DeleteWatcher(tableConfigs, namespaceConfigs, tableParentConfigs);

  public ServerConfigurationFactory(ServerContext context, SiteConfiguration siteConfig) {
    this.context = context;
    this.siteConfig = siteConfig;
    systemConfig = Suppliers.memoize(
        () -> new SystemConfiguration(context, SystemPropKey.of(context), getSiteConfiguration()));

    if (context.threadPools() != null) {
      // simplify testing - only create refresh thread when operating in a context with thread pool
      // initialized
      ScheduledThreadPoolExecutor threadPool = context.threadPools()
          .createScheduledExecutorService(1, this.getClass().getSimpleName(), false);
      Runnable refreshTask = this::verifySnapshotVersions;
      // randomly stagger initial and then subsequent calls across cluster
      int randDelay =
          ThreadLocalRandom.current().nextInt(REFRESH_PERIOD_MINUTES / 4, REFRESH_PERIOD_MINUTES);
      refreshTaskFuture = threadPool.scheduleWithFixedDelay(refreshTask, randDelay,
          REFRESH_PERIOD_MINUTES, MINUTES);
      Runtime.getRuntime().addShutdownHook(new Thread(() -> refreshTaskFuture.cancel(true)));
    } else {
      refreshTaskFuture = null;
    }
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

  /**
   * Check that the stored version in ZooKeeper matches the version held in the local snapshot. When
   * a mismatch is detected, a change event is sent to the prop store which will cause a re-load. If
   * the Zookeeper node has been deleted, the local cache entries are removed.
   * <p>
   * This method is designed to be called as a scheduled task, so it does not propagate exceptions
   * so the scheduled tasks will continue to run.
   */
  private void verifySnapshotVersions() {

    int nsRefreshCount = 0;
    int tblRefreshCount = 0;

    long refreshStart = System.nanoTime();

    ZooReader zooReader = context.getZooReader();

    try {
      refresh(systemConfig.get(), zooReader);
    } catch (Throwable t) {
      log.debug("Exception occurred during system config refresh", t.getCause());
    }

    try {
      for (Map.Entry<NamespaceId,NamespaceConfiguration> entry : namespaceConfigs.entrySet()) {
        if (!refresh(entry.getValue(), zooReader)) {
          namespaceConfigs.remove(entry.getKey());
        }
        nsRefreshCount++;
      }
    } catch (Throwable t) {
      log.debug(
          "Exception occurred during namespace refresh - cycle may not have completed on this pass",
          t.getCause());
    }
    try {
      for (Map.Entry<TableId,TableConfiguration> entry : tableConfigs.entrySet()) {
        if (!refresh(entry.getValue(), zooReader)) {
          tableConfigs.remove(entry.getKey());
          tableParentConfigs.remove(entry.getKey());
        }
        tblRefreshCount++;
      }
    } catch (Throwable t) {
      log.debug(
          "Exception occurred during table refresh - cycle may not have completed on this pass",
          t.getCause());
    }

    log.debug(
        "configuration snapshot refresh completed. Total runtime {} ms for local namespaces: {}, tables: {}",
        MILLISECONDS.convert(System.nanoTime() - refreshStart, NANOSECONDS), nsRefreshCount,
        tblRefreshCount);
  }

  private boolean refresh(ZooBasedConfiguration config, ZooReader zooReader) {
    final PropStoreKey<?> key = config.getPropStoreKey();
    try {
      Stat stat = zooReader.getStatus(key.getPath());
      log.trace("configuration snapshot refresh: stat returned: {} for {}", stat, key);
      if (stat == null) {
        return false;
      }
      if (config.getDataVersion() != stat.getVersion()) {
        log.debug(
            "configuration snapshot refresh - difference found. forcing configuration update for {}}",
            key);
        config.zkChangeEvent(key);
      }
      // add small jitter between calls.
      int randDelay = ThreadLocalRandom.current().nextInt(0, 23);
      Thread.sleep(randDelay);
    } catch (KeeperException.NoNodeException ex) {
      config.zkChangeEvent(key);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading from ZooKeeper during snapshot refresh.",
          ex);
    } catch (KeeperException | IllegalStateException ex) {
      log.debug("configuration snapshot refresh: Exception during refresh - key" + key, ex);
    }
    return true;
  }

  private static class DeleteWatcher implements PropChangeListener {

    final Map<TableId,TableConfiguration> tableConfigs;
    final Map<NamespaceId,NamespaceConfiguration> namespaceConfigs;
    final Map<TableId,NamespaceConfiguration> tableParentConfigs;

    DeleteWatcher(final Map<TableId,TableConfiguration> tableConfigs,
        final Map<NamespaceId,NamespaceConfiguration> namespaceConfigs,
        final Map<TableId,NamespaceConfiguration> tableParentConfigs) {
      this.tableConfigs = tableConfigs;
      this.namespaceConfigs = namespaceConfigs;
      this.tableParentConfigs = tableParentConfigs;
    }

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
        tableConfigs.remove(((TablePropKey) propStoreKey).getId());
      }
    }

    @Override
    public void connectionEvent() {
      // no-op. changes handled by prop store impl
    }
  }
}

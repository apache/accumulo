/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf;

import static com.google.common.base.Suppliers.memoize;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigCheckUtil;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.apache.accumulo.server.conf.store.PropChangeListener;
import org.apache.accumulo.server.conf.store.PropStore;
import org.apache.accumulo.server.conf.store.PropStoreKey;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.apache.accumulo.server.conf.store.TablePropKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A factory for configurations used by a server process. Instance of this class are thread-safe.
 */
public class ServerConfigurationFactory extends ServerConfiguration {
  private final static Logger log = LoggerFactory.getLogger(ServerConfigurationFactory.class);

  // cache expiration is used to remove configurations after deletion, not time sensitive
  private static final int CACHE_EXPIRATION_HRS = 1;
  private final Supplier<SystemConfiguration> systemConfig;
  private final Cache<TableId,NamespaceConfiguration> tableParentConfigs;
  private final Cache<TableId,TableConfiguration> tableConfigs;
  private final Cache<NamespaceId,NamespaceConfiguration> namespaceConfigs;

  private final ServerContext context;
  private final SiteConfiguration siteConfig;
  private final ChangeWatcher changeWatcher = new ChangeWatcher();

  private static final int REFRESH_PERIOD_MINUTES = 15;

  private final ConfigRefreshRunner refresher;

  public ServerConfigurationFactory(ServerContext context, SiteConfiguration siteConfig) {
    this.context = context;
    this.siteConfig = siteConfig;
    this.systemConfig = memoize(() -> new SystemConfiguration(context,
        SystemPropKey.of(context.getInstanceID()), siteConfig));
    tableParentConfigs =
        Caffeine.newBuilder().expireAfterAccess(CACHE_EXPIRATION_HRS, TimeUnit.HOURS).build();
    tableConfigs =
        Caffeine.newBuilder().expireAfterAccess(CACHE_EXPIRATION_HRS, TimeUnit.HOURS).build();
    namespaceConfigs =
        Caffeine.newBuilder().expireAfterAccess(CACHE_EXPIRATION_HRS, TimeUnit.HOURS).build();

    refresher = new ConfigRefreshRunner();
    Runtime.getRuntime()
        .addShutdownHook(Threads.createThread("config-refresh-shutdownHook", refresher::shutdown));
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
    return tableConfigs.get(tableId, key -> {
      if (context.tableNodeExists(tableId)) {
        context.getPropStore().registerAsListener(TablePropKey.of(context, tableId), changeWatcher);
        var conf =
            new TableConfiguration(context, tableId, getNamespaceConfigurationForTable(tableId));
        ConfigCheckUtil.validate(conf, "table id: " + tableId.toString());
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
    return tableParentConfigs.get(tableId, key -> getNamespaceConfiguration(namespaceId));
  }

  @Override
  public NamespaceConfiguration getNamespaceConfiguration(NamespaceId namespaceId) {
    return namespaceConfigs.get(namespaceId, key -> {
      context.getPropStore().registerAsListener(NamespacePropKey.of(context, namespaceId),
          changeWatcher);
      var conf = new NamespaceConfiguration(context, namespaceId, getSystemConfiguration());
      ConfigCheckUtil.validate(conf, "namespace id: " + namespaceId.toString());
      return conf;
    });
  }

  private class ChangeWatcher implements PropChangeListener {

    @Override
    public void zkChangeEvent(PropStoreKey<?> propStoreKey) {
      clearLocalOnEvent(propStoreKey);
    }

    @Override
    public void cacheChangeEvent(PropStoreKey<?> propStoreKey) {
      clearLocalOnEvent(propStoreKey);
    }

    @Override
    public void deleteEvent(PropStoreKey<?> propStoreKey) {
      clearLocalOnEvent(propStoreKey);
    }

    private void clearLocalOnEvent(PropStoreKey<?> propStoreKey) {
      // clearing the local secondary cache stored in this class forces a re-read from the prop
      // store
      // to guarantee that the updated vales(s) are re-read on a ZooKeeper change.
      if (propStoreKey instanceof NamespacePropKey) {
        log.trace("configuration snapshot refresh: Handle namespace change for {}", propStoreKey);
        namespaceConfigs.invalidate(((NamespacePropKey) propStoreKey).getId());
        return;
      }
      if (propStoreKey instanceof TablePropKey) {
        log.trace("configuration snapshot refresh: Handle table change for {}", propStoreKey);
        tableConfigs.invalidate(((TablePropKey) propStoreKey).getId());
        tableParentConfigs.invalidate(((TablePropKey) propStoreKey).getId());
      }
    }

    @Override
    public void connectionEvent() {
      // no-op. changes handled by prop store impl
    }
  }

  private class ConfigRefreshRunner {
    private static final long MIN_JITTER_DELAY = 1;
    private static final long MAX_JITTER_DELAY = 23;
    private final ScheduledFuture<?> refreshTaskFuture;

    ConfigRefreshRunner() {

      Runnable refreshTask = this::verifySnapshotVersions;

      ScheduledThreadPoolExecutor executor =
          ThreadPools.getServerThreadPools().createScheduledExecutorService(1, "config-refresh");

      // scheduleWithFixedDelay - used so only one task will run concurrently.
      // staggering the initial delay prevents synchronization of Accumulo servers communicating
      // with ZooKeeper for the sync process. (Value is 25% -> 100% of the refresh period.)
      long randDelay = jitter(REFRESH_PERIOD_MINUTES / 4, REFRESH_PERIOD_MINUTES);
      refreshTaskFuture =
          executor.scheduleWithFixedDelay(refreshTask, randDelay, REFRESH_PERIOD_MINUTES, MINUTES);
    }

    /**
     * Check that the stored version in ZooKeeper matches the version held in the local snapshot.
     * When a mismatch is detected, a change event is sent to the prop store which will cause a
     * re-load. If the Zookeeper node has been deleted, the local cache entries are removed.
     * <p>
     * This method is designed to be called as a scheduled task, so it does not propagate exceptions
     * other than interrupted Exceptions so the scheduled tasks will continue to run.
     */
    private void verifySnapshotVersions() {

      long refreshStart = System.nanoTime();
      int keyCount = 0;
      int keyChangedCount = 0;

      PropStore propStore = context.getPropStore();
      keyCount++;

      // rely on store to propagate change event if different
      propStore.validateDataVersion(SystemPropKey.of(context),
          ((ZooBasedConfiguration) getSystemConfiguration()).getDataVersion());
      // small yield - spread out ZooKeeper calls
      jitterDelay();

      for (Map.Entry<NamespaceId,NamespaceConfiguration> entry : namespaceConfigs.asMap()
          .entrySet()) {
        keyCount++;
        PropStoreKey<?> propKey = NamespacePropKey.of(context, entry.getKey());
        if (!propStore.validateDataVersion(propKey, entry.getValue().getDataVersion())) {
          keyChangedCount++;
          namespaceConfigs.invalidate(entry.getKey());
        }
        // small yield - spread out ZooKeeper calls between namespace config checks
        jitterDelay();
      }

      for (Map.Entry<TableId,TableConfiguration> entry : tableConfigs.asMap().entrySet()) {
        keyCount++;
        TableId tid = entry.getKey();
        PropStoreKey<?> propKey = TablePropKey.of(context, tid);
        if (!propStore.validateDataVersion(propKey, entry.getValue().getDataVersion())) {
          keyChangedCount++;
          tableConfigs.invalidate(tid);
          tableParentConfigs.invalidate(tid);
          log.debug("data version sync: difference found. forcing configuration update for {}}",
              propKey);
        }
        // small yield - spread out ZooKeeper calls between table config checks
        jitterDelay();
      }

      log.debug("data version sync: Total runtime {} ms for {} entries, changes detected: {}",
          NANOSECONDS.toMillis(System.nanoTime() - refreshStart), keyCount, keyChangedCount);
    }

    /**
     * Generate a small random integer for jitter between [min,max).
     */
    @SuppressFBWarnings(value = "PREDICTABLE_RANDOM",
        justification = "random number not used in secure context")
    private long jitter(final long min, final long max) {
      return ThreadLocalRandom.current().nextLong(min, max);
    }

    /**
     * Sleep for a random jitter interval defined by MIN_JITTER_DELAY and MAX_JITTER_DELAY
     * </p>
     * Used to spread out operations so that Server config sync communications don't overwhelm
     * ZooKeeper and are not synchronized across the cluster.
     *
     */
    private void jitterDelay() {
      try {
        Thread.sleep(jitter(MIN_JITTER_DELAY, MAX_JITTER_DELAY));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(ex);
      }
    }

    public void shutdown() {
      refreshTaskFuture.cancel(true);
    }
  }
}

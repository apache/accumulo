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

import static org.apache.accumulo.core.client.admin.servers.ServerId.Type.COMPACTOR;
import static org.apache.accumulo.core.client.admin.servers.ServerId.Type.GARBAGE_COLLECTOR;
import static org.apache.accumulo.core.client.admin.servers.ServerId.Type.MANAGER;
import static org.apache.accumulo.core.client.admin.servers.ServerId.Type.MONITOR;
import static org.apache.accumulo.core.client.admin.servers.ServerId.Type.SCAN_SERVER;
import static org.apache.accumulo.core.client.admin.servers.ServerId.Type.TABLET_SERVER;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.logging.ConditionalLogger.DeduplicatingLogger;
import org.apache.accumulo.core.util.threads.Threads;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

public class SystemConfiguration extends ZooBasedConfiguration {

  private static final Logger log = LoggerFactory.getLogger(SystemConfiguration.class);

  private static final Logger MONITOR_LOG = LoggerFactory.getLogger(ChangedPropertyMonitor.class);
  private static final Logger DEDUPE_MONITOR_LOG =
      new DeduplicatingLogger(MONITOR_LOG, Duration.ofMinutes(60), 25);
  private static Map<ServerId.Type,List<Property>> SERVER_PROPERTY_PREFIXES = Map.of(COMPACTOR,
      List.of(Property.COMPACTOR_PREFIX, Property.COMPACTION_PREFIX, Property.RPC_PREFIX,
          Property.INSTANCE_PREFIX),
      GARBAGE_COLLECTOR, List.of(Property.GC_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX),
      MANAGER,
      List.of(Property.COMPACTION_PREFIX, Property.MANAGER_PREFIX, Property.RPC_PREFIX,
          Property.INSTANCE_PREFIX),
      MONITOR, List.of(Property.MONITOR_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX),
      SCAN_SERVER, List.of(Property.SSERV_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX),
      TABLET_SERVER, List.of(Property.TSERV_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX));

  /**
   * Tracks properties that have changed, but have not been read, since the start of the process.
   */
  class ChangedPropertyMonitor implements Runnable {

    private final SystemPropKey propStoreKey;
    private final AtomicReference<Set<String>> propsChanged =
        new AtomicReference<>(new HashSet<>());
    private final List<Property> applicableProperties;
    private final Map<String,String> currentProperties = new HashMap<>();
    private volatile long currentVersion;

    public ChangedPropertyMonitor(SystemPropKey propStoreKey, long initialVersion,
        Map<String,String> initialProperties, ServerId.Type serverType) {
      this.propStoreKey = propStoreKey;
      this.applicableProperties = SERVER_PROPERTY_PREFIXES.get(serverType);
      this.currentProperties.putAll(initialProperties);
      this.currentVersion = initialVersion;
    }

    public void update(long version, Map<String,String> properties) {
      if (currentVersion == version) {
        return;
      }
      if (properties == null || properties.isEmpty()) {
        return;
      }
      synchronized (this) {
        MapDifference<String,String> diff = Maps.difference(currentProperties, properties);
        if (diff.areEqual()) {
          return;
        }
        final Set<String> propertyChanges = new HashSet<>();
        propertyChanges.addAll(diff.entriesOnlyOnLeft().keySet()); // property removed in update
        propertyChanges.addAll(diff.entriesDiffering().keySet()); // property changed in update
        propertyChanges.addAll(diff.entriesOnlyOnRight().keySet()); // property added in update
        // There is no retainIf, so the return values here are inverted
        // Want to keep properties that start with the applicable properties
        propertyChanges.removeIf(p -> {
          for (Property ap : applicableProperties) {
            if (p.startsWith(ap.getKey())) {
              return false;
            }
          }
          return true;
        });
        currentProperties.clear();
        currentProperties.putAll(properties);
        currentVersion = version;
        if (!propertyChanges.isEmpty()) {
          propsChanged.accumulateAndGet(propertyChanges, (current, changed) -> {
            current.addAll(changed);
            return current;
          });
        }
      }
    }

    public void propertyRead(Property p) {
      propsChanged.accumulateAndGet(Set.of(p.getKey()), (current, read) -> {
        current.removeAll(read);
        return current;
      });
    }

    // visible for testing
    Set<String> getChangedProperties() {
      return propsChanged.get();
    }

    // visible for testing
    long getVersion() {
      return currentVersion;
    }

    @Override
    public void run() {
      // When a property is set a Watcher fires which marks the PropSnapshot as dirty. The
      // updated properties are retrieved the next time any Property is retrieved. If no property
      // is retrieved, then the PropSnapshot remains stale.
      if (needsUpdate()) {
        MONITOR_LOG.debug(
            "Property store {} has been changed, but internal representation not yet updated.",
            propStoreKey);
      }
      final Set<String> changed = propsChanged.get();
      if (!changed.isEmpty()) {
        DEDUPE_MONITOR_LOG.warn(
            "The following properties relevant to this process have changed, but have not yet been read: {}",
            changed);
      }
    }

  }

  private final Map<String,String> initialProperties;
  private final ChangedPropertyMonitor monitor;

  public SystemConfiguration(ServerContext context, SystemPropKey propStoreKey,
      AccumuloConfiguration parent) {
    this(context, propStoreKey, parent, Optional.empty());
  }

  public SystemConfiguration(ServerContext context, SystemPropKey propStoreKey,
      AccumuloConfiguration parent, Optional<ServerId.Type> serverType) {
    this(context, propStoreKey, parent, serverType, TimeUnit.MINUTES.toMillis(5));
  }

  // visible for testing
  public SystemConfiguration(ServerContext context, SystemPropKey propStoreKey,
      AccumuloConfiguration parent, Optional<ServerId.Type> serverType,
      long monitorUpdateInterval) {
    super(log, context, propStoreKey, parent);
    initialProperties = getSnapshot();
    if (serverType.isPresent()) {
      monitor = new ChangedPropertyMonitor(propStoreKey, getDataVersion(), initialProperties,
          serverType.orElseThrow());
      // Can't use context.getScheduledExecutor() as it creates an endless loop
      Threads.createCriticalThread("System-Configuration-Monitor", () -> {
        while (true) {
          try {
            monitor.run();
            Thread.sleep(TimeUnit.MINUTES.toMillis(5));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("System configuration monitor thread interrupted.", e);
          }
        }
      });
    } else {
      monitor = null;
    }
  }

  @Override
  public String get(Property property) {
    log.trace("system config get() - property request for {}", property);

    String key = property.getKey();
    String value = null;
    if (Property.isValidZooPropertyKey(key)) {
      Map<String,String> snapshot = getSnapshot();
      value = snapshot.get(key);
      if (monitor != null) {
        monitor.update(getDataVersion(), snapshot);
        monitor.propertyRead(property);
      }
    }

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null) {
        log.error("Using parent value for {} due to improperly formatted {}: {}", key,
            property.getType(), value);
      }
      value = getParent().get(property);
    }
    return value;
  }
}

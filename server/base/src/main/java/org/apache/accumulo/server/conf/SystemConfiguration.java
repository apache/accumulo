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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

public class SystemConfiguration extends ZooBasedConfiguration {

  private static final Logger log = LoggerFactory.getLogger(SystemConfiguration.class);

  private static class ChangedPropertyMonitor implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(ChangedPropertyMonitor.class);
    private static Map<ServerId.Type,List<Property>> SERVER_PROPERTY_PREFIXES = Map.of(COMPACTOR,
        List.of(Property.COMPACTOR_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX),
        GARBAGE_COLLECTOR,
        List.of(Property.GC_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX), MANAGER,
        List.of(Property.MANAGER_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX), MONITOR,
        List.of(Property.MONITOR_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX),
        SCAN_SERVER, List.of(Property.SSERV_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX),
        TABLET_SERVER,
        List.of(Property.TSERV_PREFIX, Property.RPC_PREFIX, Property.INSTANCE_PREFIX));

    private final AtomicReference<Set<String>> propsChanged = new AtomicReference<>(Set.of());
    private final List<Property> applicableProperties;
    private volatile Map<String,String> currentProperties;
    private volatile long currentVersion;

    public ChangedPropertyMonitor(long initialVersion, Map<String,String> initialProperties,
        ServerId.Type serverType) {
      this.applicableProperties = SERVER_PROPERTY_PREFIXES.get(serverType);
      this.currentProperties = initialProperties;
      this.currentVersion = initialVersion;
    }

    private void changedProperties(Set<String> props) {
      final Set<String> changed = new HashSet<>();
      props.forEach(p -> {
        applicableProperties.forEach(ap -> {
          if (p.startsWith(ap.getKey())) {
            changed.add(p);
          }
        });
      });
      propsChanged.set(changed);
    }

    public void update(long version, Map<String,String> properties) {
      if (currentVersion == version) {
        return;
      }
      MapDifference<String,String> diff = Maps.difference(currentProperties, properties);
      currentProperties = properties;
      currentVersion = version;
      if (diff.areEqual()) {
        return;
      }
      changedProperties(diff.entriesDiffering().keySet());
    }

    public void propChecked(Property p) {
      final Set<String> changed = propsChanged.get();
      if (changed.isEmpty()) {
        return;
      }
      propsChanged.get().remove(p.getKey());
    }

    @Override
    public void run() {
      final Set<String> changed = propsChanged.get();
      if (!changed.isEmpty()) {
        LOG.warn("The following properties have changed, but have not yet been read: {}", changed);
      }
    }

  }

  private final Map<String,String> initialProperties;
  private final RuntimeFixedProperties runtimeFixedProps;
  private final ChangedPropertyMonitor monitor;

  public SystemConfiguration(ServerContext context, SystemPropKey propStoreKey,
      AccumuloConfiguration parent) {
    this(context, propStoreKey, parent, Optional.empty());
  }

  public SystemConfiguration(ServerContext context, SystemPropKey propStoreKey,
      AccumuloConfiguration parent, Optional<ServerId.Type> serverType) {
    super(log, context, propStoreKey, parent);
    initialProperties = getSnapshot();
    if (serverType.isPresent()) {
      monitor =
          new ChangedPropertyMonitor(getDataVersion(), initialProperties, serverType.orElseThrow());
      // start the monitor as a scheduled task at some interval.
    } else {
      monitor = null;
    }
    runtimeFixedProps =
        new RuntimeFixedProperties(initialProperties, context.getSiteConfiguration());
  }

  @Override
  public String get(Property property) {
    log.trace("system config get() - property request for {}", property);

    if (Property.isFixedZooPropertyKey(property)) {
      return runtimeFixedProps.get(property);
    }

    String key = property.getKey();
    String value = null;
    if (Property.isValidZooPropertyKey(key)) {
      Map<String,String> snapshot = getSnapshot();
      value = snapshot.get(key);
      if (monitor != null) {
        monitor.update(getDataVersion(), snapshot);
        monitor.propChecked(property);
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

  @Override
  public boolean isPropertySet(Property prop) {
    if (Property.isFixedZooPropertyKey(prop)) {
      return runtimeFixedProps.wasPropertySet(prop);
    }
    return super.isPropertySet(prop);
  }
}

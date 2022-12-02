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

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Utility class to a manage a fixed set of defined properties (designated in Properties as fixed).
 * Certain properties are stored for persistence across restarts, they are read during start-up and
 * remain unchanged for the life of the instance. Any updates to the properties will only be
 * reflected with a restart.
 * <p>
 * Note that there are no guarantees that all services will always have the same values. If a fixed
 * property value is changed and if all services are not restarted, they would be operating with
 * different values.
 */
public class RuntimeFixedProperties {

  private static final Logger log = LoggerFactory.getLogger(RuntimeFixedProperties.class);

  // combined site, stored or default props.
  private final Map<String,String> fixed = new HashMap<>();

  // original, stored fixed props.
  private final Map<String,String> origStored = new HashMap<>();

  public RuntimeFixedProperties(final Map<String,String> storedProps,
      final SiteConfiguration siteConfig) {
    requireNonNull(siteConfig, "a site configuration must be provided");
    requireNonNull(storedProps, "stored property map must be provided");
    Property.fixedProperties.forEach(prop -> {
      String key = prop.getKey();
      String value = storedProps.get(key);
      // use value in ZooKeeper
      if (value != null) {
        origStored.put(key, value);
      } else {
        // Not in ZK, use config or default.
        value = siteConfig.get(prop);
      }
      fixed.put(key, value);
      log.trace("fixed property name: {} = {}", key, value);
    });
  }

  @Nullable
  public String get(final Property property) {
    return fixed.get(property.getKey());
  }

  @VisibleForTesting
  Map<String,String> getAll() {
    return Collections.unmodifiableMap(fixed);
  }

  /**
   * Test if a property has changed since this instance was created. If a property has changed, it
   * will take effect on next restart.
   *
   * @param current a map of the current stored properties.
   * @return true if changed and restart required, false otherwise
   */
  @VisibleForTesting
  boolean hasChanged(final Map<String,String> current) {
    for (Property prop : Property.fixedProperties) {
      var key = prop.getKey();
      if (current.containsKey(key)) {
        if (!current.get(key).equals(fixed.get(key))) {
          return true;
        }
      } else if (origStored.containsKey(key)) {
        return true;
      }
    }
    return false;
  }
}

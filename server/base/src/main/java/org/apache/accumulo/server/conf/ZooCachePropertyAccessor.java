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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper object for accessing properties in a {@link ZooCache}.
 */
public class ZooCachePropertyAccessor {
  private static final Logger log = LoggerFactory.getLogger(ZooCachePropertyAccessor.class);

  static class PropCacheKey {
    final String instanceId;
    final String scope;

    PropCacheKey(String iid, String s) {
      instanceId = iid;
      scope = s;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof PropCacheKey)) {
        return false;
      }
      PropCacheKey o = (PropCacheKey) other;
      return (instanceId.equals(o.instanceId) && scope.equals(o.scope));
    }

    @Override
    public int hashCode() {
      int c = 17;
      c = (37 * c) + instanceId.hashCode();
      c = (37 * c) + scope.hashCode();
      return c;
    }
  }

  private final ZooCache propCache;

  /**
   * Creates a new accessor.
   *
   * @param propCache
   *          property cache
   */
  ZooCachePropertyAccessor(ZooCache propCache) {
    this.propCache = propCache;
  }

  /**
   * Gets the property cache accessed by this object.
   *
   * @return property cache
   */
  ZooCache getZooCache() {
    return propCache;
  }

  /**
   * Gets a property. If the property is not in ZooKeeper or is present but an invalid format for the property type, the parent configuration is consulted (if
   * provided).
   *
   * @param property
   *          property to get
   * @param path
   *          ZooKeeper path where properties lie
   * @param parent
   *          parent configuration (optional)
   * @return property value, or null if not found
   */
  String get(Property property, String path, AccumuloConfiguration parent) {
    String key = property.getKey();
    String value = get(path + "/" + key);

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null) {
        log.error("Using default value for {} due to improperly formatted {}: {}", key, property.getType(), value);
      }
      if (parent != null) {
        value = parent.get(property);
      }
    }
    return value;
  }

  private String get(String path) {
    byte[] v = propCache.get(path);
    if (v != null) {
      return new String(v, UTF_8);
    } else {
      return null;
    }
  }

  /**
   * Gets all properties into the given map. Properties are filtered using the given filter. Properties from a parent configuration are also added to the map
   * and filtered, either using a separate filter or, if not specified, the other filter.
   *
   * @param props
   *          map to populate with properties
   * @param path
   *          ZooKeeper path where properties lie
   * @param filter
   *          property filter (required)
   * @param parent
   *          parent configuration (required)
   * @param parentFilter
   *          separate filter for parent properties (optional)
   */
  void getProperties(Map<String,String> props, String path, Predicate<String> filter, AccumuloConfiguration parent, Predicate<String> parentFilter) {
    parent.getProperties(props, parentFilter != null ? parentFilter : filter);

    List<String> children = propCache.getChildren(path);
    if (children != null) {
      for (String child : children) {
        if (child != null && filter.test(child)) {
          String value = get(path + "/" + child);
          if (value != null) {
            props.put(child, value);
          }
        }
      }
    }
  }

  /**
   * Clears the internal {@link ZooCache}.
   */
  void invalidateCache() {
    propCache.clear();
  }

}

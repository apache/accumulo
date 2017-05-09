/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.file.blockfile.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum CacheType {

  DATA, INDEX, SUMMARY;

  public static final String ENABLED_SUFFIX = "enabled";

  private static final Logger LOG = LoggerFactory.getLogger(CacheType.class);

  public String getPropertyPrefix(String impl) {
    return BlockCacheFactory.CACHE_PROPERTY_BASE + impl + "." + name().toLowerCase() + ".";
  }

  /**
   * Return configuration properties for this cache type, if enabled
   * 
   * @param conf
   *          accumulo configuration object
   * @param type
   *          type of cache
   * @param typePrefix
   *          property prefix for this cache type
   * @return configuration for this type of cache, or null
   */
  public ConfigurationCopy getCacheProperties(AccumuloConfiguration conf, String implName) {
    Map<String,String> props = conf.getAllPropertiesWithPrefix(Property.GENERAL_ARBITRARY_PROP_PREFIX);
    String prefix = getPropertyPrefix(implName);
    String enabled = props.get(prefix + ENABLED_SUFFIX);
    if (enabled != null && Boolean.parseBoolean(enabled)) {
      LOG.info("{} cache is enabled.", this);
      Map<String,String> results = new HashMap<>();
      for (Entry<String,String> prop : props.entrySet()) {
        if (prop.getKey().startsWith(prefix)) {
          results.put(prop.getKey(), prop.getValue());
        }
      }
      return new ConfigurationCopy(results);
    } else {
      LOG.info("{} cache is disabled.", this);
      return null;
    }
  }

}

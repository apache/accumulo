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
package org.apache.accumulo.core.conf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;

/**
 * An {@link AccumuloConfiguration} which holds a flat copy of properties defined in another configuration
 */
public class ConfigurationCopy extends AccumuloConfiguration {
  final Map<String,String> copy = Collections.synchronizedMap(new HashMap<String,String>());

  /**
   * Creates a new configuration.
   *
   * @param config
   *          configuration property key/value pairs to copy
   */
  public ConfigurationCopy(Map<String,String> config) {
    this(config.entrySet());
  }

  /**
   * Creates a new configuration.
   *
   * @param config
   *          configuration property iterable to use for copying
   */
  public ConfigurationCopy(Iterable<Entry<String,String>> config) {
    for (Entry<String,String> entry : config) {
      copy.put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Creates a new empty configuration.
   */
  public ConfigurationCopy() {
    this(new HashMap<String,String>());
  }

  @Override
  public String get(Property property) {
    return copy.get(property.getKey());
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    for (Entry<String,String> entry : copy.entrySet()) {
      if (filter.test(entry.getKey())) {
        props.put(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * Sets a property in this configuration.
   *
   * @param prop
   *          property to set
   * @param value
   *          property value
   */
  public void set(Property prop, String value) {
    copy.put(prop.getKey(), value);
  }

  /**
   * Sets a property in this configuration.
   *
   * @param key
   *          key of property to set
   * @param value
   *          property value
   */
  public void set(String key, String value) {
    copy.put(key, value);
  }

}

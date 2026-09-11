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
package org.apache.accumulo.core.conf;

import static com.google.common.base.Suppliers.memoize;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AccumuloConfiguration} that contains only default values for properties. This class is
 * a singleton.
 */
public class DefaultConfiguration extends AccumuloConfiguration {

  private static final Supplier<DefaultConfiguration> instance = memoize(DefaultConfiguration::new);
  private static final Logger LOG = LoggerFactory.getLogger(DefaultConfiguration.class);

  private final Map<String,String> resolvedProps;
  private boolean keyDuplication = false;

  private DefaultConfiguration() {
    Map<String,String> tmp = new HashMap<>();

    tmp.putAll(Arrays.stream(Property.values()).filter(p -> p.getType() != PropertyType.PREFIX)
        .collect(Collectors.toMap(Property::getKey, Property::getDefaultValue)));

    Map<String,
        String> clientDefaults = (Arrays.stream(ClientProperty.values())
            .filter(p -> p.getType() != PropertyType.PREFIX)
            .collect(Collectors.toMap(ClientProperty::getKey, ClientProperty::getDefaultValue)));

    for (Entry<String,String> e : clientDefaults.entrySet()) {
      if (tmp.containsKey(e.getKey())) {
        keyDuplication = true;
        if (keyDuplication) {
          LOG.warn("Name collision between client and server properties: {}", e.getKey());
        }

      } else {
        tmp.put(e.getKey(), e.getValue());
      }
    }

    resolvedProps = Map.copyOf(tmp);
  }

  /**
   * Gets a default configuration.
   *
   * @return default configuration
   */
  public static DefaultConfiguration getInstance() {
    return instance.get();
  }

  @Override
  public String get(Property property) {
    if (keyDuplication) {
      throw new IllegalStateException(
          "Name collision between client and server properties, check the log");
    }
    return resolvedProps.get(property.getKey());
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    if (keyDuplication) {
      throw new IllegalStateException(
          "Name collision between client and server properties, check the log");
    }
    resolvedProps.entrySet().stream().filter(p -> filter.test(p.getKey()))
        .forEach(e -> props.put(e.getKey(), e.getValue()));
  }

  @Override
  public boolean isPropertySet(Property prop) {
    return false;
  }
}

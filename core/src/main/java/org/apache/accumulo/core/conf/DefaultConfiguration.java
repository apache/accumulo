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
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * An {@link AccumuloConfiguration} that contains only default values for properties. This class is
 * a singleton.
 */
public class DefaultConfiguration extends AccumuloConfiguration {

  private static final Supplier<DefaultConfiguration> instance = memoize(DefaultConfiguration::new);

  private final Map<String,String> resolvedProps =
      Arrays.stream(Property.values()).filter(p -> p.getType() != PropertyType.PREFIX)
          .collect(Collectors.toMap(Property::getKey, Property::getDefaultValue));

  private DefaultConfiguration() {}

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
    return resolvedProps.get(property.getKey());
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    resolvedProps.entrySet().stream().filter(p -> filter.test(p.getKey()))
        .forEach(e -> props.put(e.getKey(), e.getValue()));
  }

  @Override
  public boolean isPropertySet(Property prop) {
    return false;
  }
}

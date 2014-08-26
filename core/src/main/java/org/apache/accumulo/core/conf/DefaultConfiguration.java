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

public class DefaultConfiguration extends AccumuloConfiguration {
  private final static Map<String,String> resolvedProps;
  static {
    Map<String,String> m = new HashMap<String,String>();
    for (Property prop : Property.values()) {
      if (!prop.getType().equals(PropertyType.PREFIX)) {
        m.put(prop.getKey(), prop.getDefaultValue());
      }
    }
    ConfigSanityCheck.validate(m.entrySet());
    resolvedProps = Collections.unmodifiableMap(m);
  }

  /**
   * Gets a default configuration.
   *
   * @return default configuration
   */
  public static DefaultConfiguration getInstance() {
    return new DefaultConfiguration();
  }

  @Override
  public String get(Property property) {
    return resolvedProps.get(property.getKey());
  }

  @Override
  public void getProperties(Map<String,String> props, PropertyFilter filter) {
    for (Entry<String,String> entry : resolvedProps.entrySet())
      if (filter.accept(entry.getKey()))
        props.put(entry.getKey(), entry.getValue());
  }

}

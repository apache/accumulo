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

public class ConfigurationCopy extends AccumuloConfiguration {
  final Map<String,String> copy = Collections.synchronizedMap(new HashMap<String,String>());

  public ConfigurationCopy(Map<String,String> config) {
    this(config.entrySet());
  }

  public ConfigurationCopy(Iterable<Entry<String,String>> config) {
    for (Entry<String,String> entry : config) {
      copy.put(entry.getKey(), entry.getValue());
    }
  }

  public ConfigurationCopy() {
    this(new HashMap<String,String>());
  }

  @Override
  public String get(Property property) {
    return copy.get(property.getKey());
  }

  @Override
  public void getProperties(Map<String,String> props, PropertyFilter filter) {
    for (Entry<String,String> entry : copy.entrySet()) {
      if (filter.accept(entry.getKey())) {
        props.put(entry.getKey(), entry.getValue());
      }
    }
  }

  public void set(Property prop, String value) {
    copy.put(prop.getKey(), value);
  }

  public void set(String key, String value) {
    copy.put(key, value);
  }

}

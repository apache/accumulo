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

import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.client.PluginEnvironment;

public class AccumuloConfigurationAdapter extends AccumuloConfiguration {
  private final PluginEnvironment.Configuration conf;

  public AccumuloConfigurationAdapter(PluginEnvironment.Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String get(Property property) {
    return conf.get(property.getKey());
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    Iterator<Map.Entry<String,String>> it = conf.iterator();
    while (it.hasNext()) {
      Map.Entry<String,String> entry = it.next();
      if (filter.test(entry.getKey())) {
        props.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public boolean isPropertySet(Property prop) {
    return conf.isSet(prop.getKey());
  }
}

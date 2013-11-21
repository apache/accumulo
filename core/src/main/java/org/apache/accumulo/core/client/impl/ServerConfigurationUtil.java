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
package org.apache.accumulo.core.client.impl;

import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.configuration.Configuration;

/**
 * All client side code that needs a server side configuration object should obtain it from here.
 */
public class ServerConfigurationUtil {
  @SuppressWarnings("deprecation")
  public static AccumuloConfiguration getConfiguration(Instance instance) {
    return instance.getConfiguration();
  }
  
  public static AccumuloConfiguration convertClientConfig(final AccumuloConfiguration base, final Configuration config) {

    return new AccumuloConfiguration() {
      @Override
      public String get(Property property) {
        if (config.containsKey(property.getKey()))
          return config.getString(property.getKey());
        else
          return base.get(property);
      }

      @Override
      public void getProperties(Map<String,String> props, PropertyFilter filter) {

        base.getProperties(props, filter);

        @SuppressWarnings("unchecked")
        Iterator<String> keyIter = config.getKeys();
        while (keyIter.hasNext()) {
          String key = keyIter.next();
          if (filter.accept(key))
            props.put(key, config.getString(key));
        }
      }
    };

  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.conf;

import java.util.Map;
import java.util.Objects;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemConfiguration extends ZooBasedConfiguration {

  private static final Logger log = LoggerFactory.getLogger(SystemConfiguration.class);

  private final RuntimeFixedProperties fixedProperties;

  public SystemConfiguration(ServerContext context, PropCacheKey propCacheKey,
      AccumuloConfiguration parent) {
    super(log, context, propCacheKey, parent);
    var sysProps = getSnapshot();
    fixedProperties = new RuntimeFixedProperties(context.getSiteConfiguration(),
        Objects.requireNonNullElseGet(sysProps, Map::of));
  }

  // TODO - this needs tested - logic seems to ignore child and return parent.
  @Override
  public String get(Property property) {
    log.trace("system config get() - property request for {}", property);

    if (Property.isFixedZooPropertyKey(property)) {
      String val = fixedProperties.get(property);
      if (val != null) {
        return val;
      }
    }

    return _get(property);
  }

  private String _get(Property property) {
    String key = property.getKey();
    String value = null;
    if (Property.isValidZooPropertyKey(key)) {
      Map<String,String> m = getSnapshot();
      value = m == null ? null : m.get(key);
    }

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null) {
        log.error("Using parent value for {} due to improperly formatted {}: {}", key,
            property.getType(), value);
      }
      value = super.getParent().get(property);
    }
    return value;
  }

  @Override
  public boolean isPropertySet(Property prop) {
    return fixedProperties.get(prop) != null || super.isPropertySet(prop);
  }
}

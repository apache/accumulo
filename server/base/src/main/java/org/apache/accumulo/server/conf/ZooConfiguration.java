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

import static com.google.common.base.Charsets.UTF_8;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.log4j.Logger;

public class ZooConfiguration extends AccumuloConfiguration {
  private static final Logger log = Logger.getLogger(ZooConfiguration.class);

  private final String instanceId;
  private final ZooCache propCache;
  private final AccumuloConfiguration parent;
  private final Map<String,String> fixedProps = Collections.synchronizedMap(new HashMap<String,String>());

  protected ZooConfiguration(String instanceId, ZooCache propCache, AccumuloConfiguration parent) {
    this.instanceId = instanceId;
    this.propCache = propCache;
    this.parent = parent;
  }

  @Override
  public void invalidateCache() {
    if (propCache != null)
      propCache.clear();
  }

  /**
   * Gets the parent configuration of this configuration.
   *
   * @return parent configuration
   */
  public AccumuloConfiguration getParentConfiguration() {
    return parent;
  }

  private String _get(Property property) {
    String key = property.getKey();
    String value = null;

    if (Property.isValidZooPropertyKey(key)) {
      value = get(key);
    }

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using parent value for " + key + " due to improperly formatted " + property.getType() + ": " + value);
      value = parent.get(property);
    }
    return value;
  }

  @Override
  public String get(Property property) {
    if (Property.isFixedZooPropertyKey(property)) {
      if (fixedProps.containsKey(property.getKey())) {
        return fixedProps.get(property.getKey());
      } else {
        synchronized (fixedProps) {
          String val = _get(property);
          fixedProps.put(property.getKey(), val);
          return val;
        }

      }
    } else {
      return _get(property);
    }
  }

  private String get(String key) {
    String zPath = ZooUtil.getRoot(instanceId) + Constants.ZCONFIG + "/" + key;
    byte[] v = propCache.get(zPath);
    String value = null;
    if (v != null)
      value = new String(v, UTF_8);
    return value;
  }

  @Override
  public void getProperties(Map<String,String> props, PropertyFilter filter) {
    parent.getProperties(props, filter);

    List<String> children = propCache.getChildren(ZooUtil.getRoot(instanceId) + Constants.ZCONFIG);
    if (children != null) {
      for (String child : children) {
        if (child != null && filter.accept(child)) {
          String value = get(child);
          if (value != null)
            props.put(child, value);
        }
      }
    }
  }
}

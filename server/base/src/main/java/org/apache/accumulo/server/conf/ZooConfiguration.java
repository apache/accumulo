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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooConfiguration extends AccumuloConfiguration {

  private static final Logger log = LoggerFactory.getLogger(ZooConfiguration.class);

  private final ServerContext context;
  private final ZooCache propCache;
  private final AccumuloConfiguration parent;
  private final Map<String,String> fixedProps = Collections.synchronizedMap(new HashMap<>());
  private final String propPathPrefix;

  public ZooConfiguration(ServerContext context, ZooCache propCache, AccumuloConfiguration parent) {
    this.context = context;
    this.propCache = propCache;
    this.parent = parent;
    this.propPathPrefix = context.getZooKeeperRoot() + Constants.ZCONFIG;
  }

  @Override
  public void invalidateCache() {
    if (propCache != null)
      propCache.clear();
  }

  private String _get(Property property) {
    String key = property.getKey();
    String value = null;

    if (Property.isValidZooPropertyKey(key)) {
      value = getRaw(key);
    }

    if (value == null || !property.getType().isValidFormat(value)) {
      if (value != null)
        log.error("Using parent value for {} due to improperly formatted {}: {}", key,
            property.getType(), value);
      value = parent.get(property);
    }
    return value;
  }

  @Override
  public String get(Property property) {
    if (Property.isFixedZooPropertyKey(property)) {
      String val = fixedProps.get(property.getKey());
      if (val != null) {
        return val;
      } else {
        synchronized (fixedProps) {
          val = _get(property);
          fixedProps.put(property.getKey(), val);
          return val;
        }

      }
    } else {
      return _get(property);
    }
  }

  @Override
  public boolean isPropertySet(Property prop, boolean cacheAndWatch) {
    if (fixedProps.containsKey(prop.getKey())) {
      return true;
    }
    if (cacheAndWatch) {
      if (getRaw(prop.getKey()) != null) {
        return true;
      }
    } else {
      ZooReader zr = context.getZooReaderWriter();
      String zPath = propPathPrefix + "/" + prop.getKey();
      try {
        if (zr.exists(zPath)) {
          return true;
        }
      } catch (KeeperException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    return parent.isPropertySet(prop, cacheAndWatch);
  }

  private String getRaw(String key) {
    String zPath = propPathPrefix + "/" + key;
    byte[] v = propCache.get(zPath);
    String value = null;
    if (v != null)
      value = new String(v, UTF_8);
    return value;
  }

  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    parent.getProperties(props, filter);

    List<String> children = propCache.getChildren(propPathPrefix);
    if (children != null) {
      for (String child : children) {
        if (child != null && filter.test(child)) {
          String value = getRaw(child);
          if (value != null)
            props.put(child, value);
        }
      }
    }
  }

  @Override
  public long getUpdateCount() {
    return parent.getUpdateCount() + propCache.getUpdateCount();
  }
}

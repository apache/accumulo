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
package org.apache.accumulo.server.conf.codec;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeMap;

public class VersionedPropertiesImpl implements VersionedProperties {

  private final Map<String,String> props;
  private final VersionInfo versionInfo;

  /**
   * Instantiate an initial instance with default version info and empty map.
   */
  public VersionedPropertiesImpl() {
    this(new VersionInfo.Builder().build(), new HashMap<>());
  }

  /**
   * Instantiate an initial instance with default version info and provided property map.
   *
   * @param props
   *          optional map of initial property key, value pairs. The properties are assumed to have
   *          been previously validated (if required)
   */
  public VersionedPropertiesImpl(final Map<String,String> props) {
    this(new VersionInfo.Builder().build(), props);
  }

  /**
   * Instantiate an instance with empty properties and provided version info
   *
   * @param versionInfo
   *          version info with data version and timestamp.
   */
  public VersionedPropertiesImpl(final VersionInfo versionInfo) {
    this(versionInfo, new HashMap<>());
  }

  /**
   * Instantiate an instance and set the initial properties to the provided values.
   *
   * @param versionInfo
   *          version info with data version and timestamp.
   * @param props
   *          optional map of initial property key, value pairs. The properties are assumed to have
   *          been previously validated (if required)
   */
  public VersionedPropertiesImpl(final VersionInfo versionInfo, final Map<String,String> props) {

    Objects.requireNonNull(props, "Initial properties map cannot be null");
    this.versionInfo = versionInfo;
    this.props = new HashMap<>(props);

  }

  @Override
  public void addProperty(String k, String v) {
    props.put(k, v);
  }

  @Override
  public void addProperties(Map<String,String> properties) {
    if (Objects.nonNull(properties)) {
      props.putAll(properties);
    }
  }

  @Override
  public String getProperty(final String key) {
    return props.get(key);
  }

  @Override
  public Map<String,String> getAllProperties() {
    return Collections.unmodifiableMap(props);
  }

  @Override
  public String removeProperty(final String key) {
    return props.remove(key);
  }

  @Override
  public int removeProperties(final Collection<String> keys) {
    int count = 0;
    for (String k : keys) {
      if (props.remove(k) != null) {
        count++;
      }
    }
    return count;
  }

  @Override
  public VersionInfo getVersionInfo() {
    return versionInfo;
  }

  @Override
  public String print(boolean prettyPrint) {
    StringBuilder sb = new StringBuilder();

    sb.append(versionInfo.print(prettyPrint));

    Map<String,String> sorted = new TreeMap<>(props);
    sorted.forEach((k, v) -> {
      if (prettyPrint) {
        sb.append("  ");
      }
      sb.append(k).append("=").append(v);
      sb.append(prettyPrint ? "\n" : ", ");
    });
    return sb.toString();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", VersionedPropertiesImpl.class.getSimpleName() + "[", "]")
        .add("versionInfo=" + versionInfo).add("props=" + props).toString();
  }
}

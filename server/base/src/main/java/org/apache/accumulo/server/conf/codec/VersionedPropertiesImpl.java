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

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeMap;

/**
 * Version properties maintain a {@code Map<String,String>}; of property k,v pairs along with
 * versioning information metadata.
 * <p>
 * The metadata used to verify cached values match stored values. Storing the metadata with the
 * properties allows for comparison of properties and can be used to ensure that vales being written
 * to the backend store have not changed. This metadata should be written / appear early in the
 * encoded bytes and be uncompressed so that decisions can be made that may make deserialization
 * unnecessary.
 * <p>
 * Note: Avoid using -1 because that has significance in ZooKeeper - writing a ZooKeeper node with a
 * version of -1 disables the ZooKeeper expected version checking and just overwrites the node.
 * <p>
 * Instances of this class are immutable.
 */
public class VersionedPropertiesImpl implements VersionedProperties {

  private final int dataVersion;
  private final Instant timestamp;
  private final Map<String,String> props;

  /**
   * Instantiate an initial instance with default version info and empty map.
   */
  public VersionedPropertiesImpl() {
    this(NO_VERSION, Instant.now(), new HashMap<>());
  }

  /**
   * Instantiate an initial instance with default version info and provided property map.
   *
   * @param props
   *          optional map of initial property key, value pairs. The properties are assumed to have
   *          been previously validated (if required)
   */
  public VersionedPropertiesImpl(final Map<String,String> props) {
    this(NO_VERSION, Instant.now(), props);
  }

  /**
   * Instantiate an instance with versioning info and default, empty property mao.
   *
   * @param dataVersion
   *          the data version
   * @param timestamp
   *          the timestamp
   */
  public VersionedPropertiesImpl(final int dataVersion, final Instant timestamp) {
    this(dataVersion, timestamp, new HashMap<>());
  }

  /**
   * Instantiate an instance and set the initial properties to the provided values.
   *
   * @param dataVersion
   *          version info with data version and timestamp.
   * @param timestamp
   *          timestamp of this version.
   * @param props
   *          optional map of initial property key, value pairs. The properties are assumed to have
   *          been previously validated (if required)
   */
  public VersionedPropertiesImpl(final int dataVersion, final Instant timestamp,
      final Map<String,String> props) {

    Objects.requireNonNull(props, "Initial properties map cannot be null");
    this.dataVersion = dataVersion;
    this.timestamp = timestamp;
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

  /**
   * Get the current data version. The version should match the node version of the stored data.
   * This value returned should be used on data writes as the expected version. If the data write
   * fails do to an unexpected version, it signals that the node version has changed.
   *
   * @return 0 for initial version, otherwise the data version when the properties were serialized.
   */
  @Override
  public int getDataVersion() {
    return Math.max(dataVersion, 0);
  }

  /**
   * Calculates the version that should be stored when serialized. The serialized version, when
   * stored, should match the version that will be assigned. This way, data reading the serialized
   * version can compare the stored version with the node version at any time to detect if the node
   * version has been updated.
   * <p>
   * The initialization of the data version to a negative value allows this value to be calculated
   * correctly for the first serialization. On the first store, the expected version will be 0.
   *
   * @return the next version number that should be serialized, or 0 if this is the initial version.
   */
  @Override
  public int getNextVersion() {
    return Math.max(dataVersion + 1, 0);
  }

  @Override
  public Instant getTimestamp() {
    return Instant.from(timestamp);
  }

  @Override
  public String getTimestampISO() {
    return tsFormatter.format(timestamp);
  }

  @Override
  public String print(boolean prettyPrint) {
    StringBuilder sb = new StringBuilder();

    sb.append("dataVersion=").append(dataVersion).append(prettyPrint ? "\n" : ", ");

    sb.append("timeStamp=").append(tsFormatter.format(timestamp)).append(prettyPrint ? "\n" : ", ");

    Map<String,String> sorted = new TreeMap<>(props);
    sorted.forEach((k, v) -> {
      if (prettyPrint) {
        // indent if pretty
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
        .add("dataVersion=" + dataVersion).add("timestamp=" + timestamp).add("props=" + props)
        .toString();
  }
}

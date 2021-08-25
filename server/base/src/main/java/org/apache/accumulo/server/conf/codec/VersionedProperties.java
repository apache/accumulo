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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.google.common.collect.ImmutableMap;

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
public class VersionedProperties {

  public static final DateTimeFormatter tsFormatter =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));
  // flag value for initialization - on store both the version and next version should be 0.
  private static final int NO_VERSION = -2;
  private final int dataVersion;
  private final Instant timestamp;
  private final ImmutableMap<String,String> props;

  /**
   * Instantiate an initial instance with default version info and empty map.
   */
  public VersionedProperties() {
    this(NO_VERSION, Instant.now(), Collections.emptyMap());
  }

  /**
   * Instantiate an initial instance with default version info and provided property map.
   *
   * @param props
   *          optional map of initial property key, value pairs. The properties are assumed to have
   *          been previously validated (if required)
   */
  public VersionedProperties(Map<String,String> props) {
    this(NO_VERSION, Instant.now(), props);
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
  public VersionedProperties(final int dataVersion, final Instant timestamp,
      final Map<String,String> props) {
    this.dataVersion = dataVersion;
    this.timestamp = timestamp;
    if (Objects.nonNull(props)) {
      this.props = new ImmutableMap.Builder<String,String>().putAll(props).build();
    } else {
      this.props = new ImmutableMap.Builder<String,String>().build();
    }
  }

  /**
   * Get an unmodifiable map with all property key,value pairs.
   *
   * @return An unmodifiable view of the property key, value pairs.
   */
  public Map<String,String> getProperties() {
    return props;
  }

  /**
   * Get the current data version. The version should match the node version of the stored data. The
   * value should be used on data writes as the expected version. If the data write fails do to an
   * unexpected version, it signals that the node version has changed since the instance was
   * instantiated and encoded.
   *
   * @return 0 for initial version, otherwise the data version when the properties were serialized.
   */
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
  public int getNextVersion() {
    return Math.max(dataVersion + 1, 0);
  }

  /**
   * The timestamp of the instance when created or last modified.
   *
   * @return the timestamp of the instance.
   */
  public Instant getTimestamp() {
    return Instant.from(timestamp);
  }

  /**
   * The timestamp formatted as an ISO 8601 string with format of
   * {@code YYYY-MM-DDTHH:mm:ss.SSSSSSZ}
   *
   * @return a formatted timestamp string.
   */
  public String getTimestampISO() {
    return tsFormatter.format(timestamp);
  }

  /**
   * Update a single property. If a property already exists it is overwritten.
   * <p>
   * It is much more efficient to add multiple properties at a time rather than one by one. Because
   * instances of this class are immutable, the creates a new copy of the properties. Other
   * processes will continue to see original values retrieved from the data store. Other processes
   * will be updated when the instance is encoded and stored in the data store.
   *
   * @param key
   *          the property name.
   * @param value
   *          the property value.
   * @return A new instance of this class with the property added or updated.
   */
  public VersionedProperties update(final String key, final String value) {
    ImmutableMap<String,String> updated =
        ImmutableMap.<String,String>builder().putAll(new HashMap<>() {
          {
            putAll(props);
            put(key, value);
          }
        }).build();
    return new VersionedProperties(dataVersion, Instant.now(), updated);
  }

  /**
   * Add or update multiple properties. If a property already exists it is overwritten.
   * <p>
   * Because instances of this class are immutable, the creates a new copy of the properties. Other
   * processes will continue to see original values retrieved from the data store. Other processes
   * will be updated when the instance is encoded and stored in the data store.
   *
   * @param updates
   *          A map of key, values pairs.
   * @return A new instance of this class with the properties added or updated.
   */
  public VersionedProperties update(final Map<String,String> updates) {
    ImmutableMap<String,String> updated =
        ImmutableMap.<String,String>builder().putAll(new HashMap<>() {
          {
            putAll(props);
            putAll(updates);
          }
        }).build();
    return new VersionedProperties(dataVersion, Instant.now(), updated);
  }

  /**
   * Delete multiple properties provided as a collection of keys.
   * <p>
   * Because instances of this class are immutable, the creates a new copy of the properties. Other
   * processes will continue to see original values retrieved from the data store. Other processes
   * will be updated when the instance is encoded and stored in the data store.
   *
   * @param keys
   *          a collection of the keys that if they exist, will be removed.
   * @return A new instance of this class.
   */
  public VersionedProperties remove(Collection<String> keys) {

    HashMap<String,String> orig = new HashMap<>(props);
    keys.forEach(orig::remove);

    ImmutableMap<String,String> updated =
        ImmutableMap.<String,String>builder().putAll(orig).build();

    return new VersionedProperties(dataVersion, Instant.now(), updated);
  }

  /**
   * Generate a formatted string for debugging, either as a single line or human-friendly,
   * multi-line format.
   *
   * @param prettyPrint
   *          if true, generate human-friendly string
   * @return a formatted string
   */
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
    return print(false);
  }
}

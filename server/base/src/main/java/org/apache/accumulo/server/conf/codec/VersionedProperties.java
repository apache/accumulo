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

import static java.util.Objects.requireNonNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Version properties maintain a {@code Map<String,String>}; of property k,v pairs along with
 * versioning information metadata.
 * <p>
 * The metadata used to verify cached values match stored values. Storing the metadata with the
 * properties allows for comparison of properties and can be used to ensure that values being
 * written to the backend store have not changed. This metadata should be written / appear early in
 * the encoded bytes and be uncompressed so that decisions can be made that may make deserialization
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
  private static final int INIT_VERSION = 0;
  private final int dataVersion;
  private final Instant timestamp;
  private final Map<String,String> props;

  /**
   * Instantiate an initial instance with default version info and empty map.
   */
  public VersionedProperties() {
    this(Map.of());
  }

  /**
   * Instantiate an initial instance with default version info and provided property map.
   *
   * @param props
   *          optional map of initial property key, value pairs. The properties are assumed to have
   *          been previously validated (if required)
   */
  public VersionedProperties(Map<String,String> props) {
    this(INIT_VERSION, Instant.now(), props);
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
    this.timestamp = requireNonNull(timestamp, "A timestamp must be supplied");
    this.props = props == null ? Map.of() : Map.copyOf(props);
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
    return dataVersion;
  }

  /**
   * The timestamp of the instance when created or last modified.
   *
   * @return the timestamp of the instance.
   */
  public Instant getTimestamp() {
    return timestamp;
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
   * It is much more efficient to add multiple properties at a time rather than one by one.
   * <p>
   * Because instances of this class are immutable, this method creates a new copy of the
   * properties. Other processes will continue to see original values retrieved from the data store.
   * Other processes will receive an update when the instance is encoded and stored in the data
   * store and then retrieved with the normal store update mechanisms.
   *
   * @param key
   *          the property name.
   * @param value
   *          the property value.
   * @return A new instance of this class with the property added or updated.
   */
  public VersionedProperties addOrUpdate(final String key, final String value) {
    var updated = new HashMap<>(props);
    updated.put(key, value);
    return new VersionedProperties(dataVersion, Instant.now(), updated);
  }

  /**
   * Add or update multiple properties. If a property already exists it is overwritten.
   * <p>
   * Because instances of this class are immutable, this method creates a new copy of the
   * properties. Other processes will continue to see original values retrieved from the data store.
   * Other processes will receive an update when the instance is encoded and stored in the data
   * store and then retrieved with the normal store update mechanisms.
   *
   * @param updates
   *          A map of key, values pairs.
   * @return A new instance of this class with the properties added or updated.
   */
  public VersionedProperties addOrUpdate(final Map<String,String> updates) {
    var updated = new HashMap<>(props);
    updated.putAll(updates);
    return new VersionedProperties(dataVersion, Instant.now(), updated);
  }

  /**
   * Delete multiple properties provided as a collection of keys.
   * <p>
   * Because instances of this class are immutable, this method creates a new copy of the
   * properties. Other processes will continue to see original values retrieved from the data store.
   * Other processes will receive an update when the instance is encoded and stored in the data
   * store and then retrieved with the normal store update mechanisms.
   *
   * @param keys
   *          a collection of the keys that if they exist, will be removed.
   * @return A new instance of this class.
   */
  public VersionedProperties remove(Collection<String> keys) {
    var updated = new HashMap<>(props);
    updated.keySet().removeAll(keys);
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

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
import java.util.Map;

public interface VersionedProperties {

  // flag value for initialization - on store both the version and next version should be 0.
  int NO_VERSION = -2;

  DateTimeFormatter tsFormatter =
      DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC));

  /**
   * Add a property. If the property already exists it is overwritten.
   *
   * @param key
   *          the name of the property
   * @param value
   *          the value of the property.
   */
  void addProperty(String key, String value);

  /**
   * Add multiple properties. If a property already exists it is overwritten.
   *
   * @param properties
   *          A map of key, value pairs.
   */
  void addProperties(Map<String,String> properties);

  /**
   * Get a stored property or null if it does not exist.
   *
   * @param key
   *          the name of the property.
   * @return the property value.
   */
  String getProperty(String key);

  /**
   * Get an unmodifiable map with all property, values.
   *
   * @return An unmodifiable view of the property key, values.
   */
  Map<String,String> getAllProperties();

  /**
   * Delete a property.
   *
   * @param key
   *          the name of the property.
   * @return the previous value if the property was present.
   */
  String removeProperty(String key);

  /**
   * Delete multiple properties provided as a collection of keys.
   *
   * @param keys
   *          a collection of keys
   * @return the number of properties actually removed.
   */
  int removeProperties(Collection<String> keys);

  /**
   * Get the current data version. The version should always match the node version of the stored
   * data, and used as the expected version on write. If this version and the data version do not
   * match, it signals that the node version has changed and the properties have changed. When
   * serializing, use {@link #getNextVersion()} so that when the node is written, it will match the
   * node version (the node version will increment on the store)
   *
   * @return 0 for initial version, otherwise the data version when the properties were serialized.
   */
  int getDataVersion();

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
  int getNextVersion();

  /**
   * Properties are timestamped when the properties are serialized for storage. This is to allow
   * easy comparison of properties that could have been retrieved at different times.
   *
   * @return the timestamp when the properties were serialized.
   */
  Instant getTimestamp();

  /**
   * Get a String formatted version of the timestamp.
   *
   * @return the timestamp formatted as an ISO time.
   */
  String getTimestampISO();

  /**
   * Provide user-friend display string of the version information and the property key / value
   * pairs.
   *
   * @param prettyPrint
   *          if true, insert new lines to improve readability.
   * @return a formatted string, with optional new lines.
   */
  String print(boolean prettyPrint);

}

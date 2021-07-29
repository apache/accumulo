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
import java.util.Map;

public interface VersionedProperties {

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
   * Get an unmodifiable map with all of the property, values.
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

  VersionInfo getVersionInfo();

  /**
   * Provide user-friend display string.
   *
   * @param prettyPrint
   *          if true, insert new lines to improve readability.
   * @return a formatted string, with optional new lines.
   */
  String print(boolean prettyPrint);

}

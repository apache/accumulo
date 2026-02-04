/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.clientImpl;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;

final class IteratorSettingsUtil {

  private static final String ITERATOR_PREFIX = Property.TABLE_ITERATOR_PREFIX.getKey();
  private static final String OPT_PREFIX = ".opt.";

  private IteratorSettingsUtil() {}

  /**
   * Validates iterator property keys for known scopes and expected formatting.
   */
  static void validateIteratorScopes(Map<String,String> properties) throws AccumuloException {
    requireNonNull(properties, "properties is null");
    for (String key : properties.keySet()) {
      if (!key.startsWith(ITERATOR_PREFIX)) {
        continue;
      }
      String suffix = key.substring(ITERATOR_PREFIX.length());
      int dotIdx = suffix.indexOf('.');
      if (dotIdx <= 0) {
        throw new AccumuloException("Invalid iterator property: " + key);
      }
      String scopeStr = suffix.substring(0, dotIdx);
      try {
        IteratorScope.valueOf(scopeStr);
      } catch (IllegalArgumentException e) {
        throw new AccumuloException("Invalid iterator scope in property: " + key, e);
      }
    }
  }

  /**
   * Parses iterator settings for a scope and validates scopes.
   *
   * @param properties properties to parse
   * @param scope scope to validate for
   */
  static List<IteratorSetting> parseIteratorSettings(Map<String,String> properties,
      IteratorScope scope) throws AccumuloException {
    return parseIteratorSettings(properties, scope, true);
  }

  /**
   * Parses iterator settings for a scope.
   *
   * @param properties properties to parse
   * @param scope scope to validate for
   * @param validateScopes whether to validate scopes or not
   */
  static List<IteratorSetting> parseIteratorSettings(Map<String,String> properties,
      IteratorScope scope, boolean validateScopes) throws AccumuloException {
    requireNonNull(properties, "properties is null");
    requireNonNull(scope, "scope is null");
    if (validateScopes) {
      validateIteratorScopes(properties);
    }

    String scopePrefix = ITERATOR_PREFIX + scope.name().toLowerCase() + ".";
    Map<String,ParsedIterator> parsed = new HashMap<>();

    for (Entry<String,String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (!key.startsWith(scopePrefix)) {
        continue;
      }

      String suffix = key.substring(scopePrefix.length());
      if (suffix.isEmpty()) {
        throw new AccumuloException("Invalid iterator format: " + key);
      }

      int optIndex = suffix.indexOf(OPT_PREFIX);
      if (optIndex >= 0) {
        String iterName = suffix.substring(0, optIndex);
        String optName = suffix.substring(optIndex + OPT_PREFIX.length());
        if (iterName.isEmpty() || optName.isEmpty() || iterName.indexOf('.') >= 0) {
          throw new AccumuloException("Invalid iterator format: " + key);
        }
        parsed.computeIfAbsent(iterName, k -> new ParsedIterator()).options.put(optName,
            entry.getValue());
      } else {
        if (suffix.indexOf('.') >= 0) {
          throw new AccumuloException("Invalid iterator format: " + key);
        }
        ParsedIterator iter = parsed.computeIfAbsent(suffix, k -> new ParsedIterator());
        parseIteratorValue(iter, key, entry.getValue());
      }
    }

    List<IteratorSetting> settings = new ArrayList<>(parsed.size());
    for (Entry<String,ParsedIterator> entry : parsed.entrySet()) {
      ParsedIterator iter = entry.getValue();
      if (iter.className == null) {
        continue;
      }
      settings
          .add(new IteratorSetting(iter.priority, entry.getKey(), iter.className, iter.options));
    }
    return settings;
  }

  private static void parseIteratorValue(ParsedIterator iter, String key, String value)
      throws AccumuloException {
    int firstComma = value.indexOf(',');
    if (firstComma < 0 || firstComma != value.lastIndexOf(',')) {
      throw new AccumuloException("Bad value for iterator setting: " + key + "=" + value);
    }
    String priorityString = value.substring(0, firstComma);
    String className = value.substring(firstComma + 1);
    if (className.isEmpty()) {
      throw new AccumuloException("Bad value for iterator setting: " + key + "=" + value);
    }
    int priority;
    try {
      priority = Integer.parseInt(priorityString);
    } catch (NumberFormatException e) {
      throw new AccumuloException("Bad value for iterator setting: " + key + "=" + value, e);
    }
    if (priority <= 0) {
      throw new AccumuloException("Bad value for iterator setting: " + key + "=" + value);
    }
    iter.priority = priority;
    iter.className = className;
  }

  private static final class ParsedIterator {
    int priority;
    String className;
    final Map<String,String> options = new HashMap<>();
  }
}

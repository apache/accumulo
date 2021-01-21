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
package org.apache.accumulo.core.conf;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeprecatedPropertyUtil {

  public static class PropertyRenamer {
    public final Predicate<String> keyFilter;
    final UnaryOperator<String> keyMapper;

    public PropertyRenamer(Predicate<String> keyFilter, UnaryOperator<String> keyMapper) {
      this.keyFilter = requireNonNull(keyFilter);
      this.keyMapper = requireNonNull(keyMapper);
    }
  }

  private static final Logger log = LoggerFactory.getLogger(DeprecatedPropertyUtil.class);
  private static final HashSet<String> propertyDeprecationWarnings = new HashSet<>();

  public static final PropertyRenamer MASTER_MANAGER_RENAMER = new PropertyRenamer(
      s -> s.startsWith("master."), s -> Property.MANAGER_PREFIX + s.substring(7));
  protected static final List<PropertyRenamer> renamers =
      new ArrayList<>(List.of(MASTER_MANAGER_RENAMER));

  /**
   * Checks if {@code propertyName} is a deprecated property name and return its replacement name,
   * if one is available, or the original name if no replacement is available. This method will log
   * a warning about any deprecated properties found with a replacement, logging at most once per
   * property name (per classloader).
   *
   * @param propertyName
   *          the name of the potentially deprecated property to check for a replacement name
   * @return either the replacement for {@code propertyName}, or {@code propertyName} if the
   *         property is not deprecated
   * @see #renameDeprecatedProperty(String) if no warning is desired
   */
  public static String renameDeprecatedPropertyAndWarn(String propertyName) {
    String renamed = renameDeprecatedProperty(propertyName);
    // Warn about the deprecation if we renamed the property.
    if (!renamed.equals(propertyName) && !propertyDeprecationWarnings.contains(propertyName)) {
      log.warn("{} has been deprecated and will be removed in a future release. Use {} instead.",
          propertyName, renamed);
      propertyDeprecationWarnings.add(propertyName);
    }
    return renamed;
  }

  /**
   * Checks if {@code propertyName} is a deprecated property name and return its replacement name,
   * if one is available, or the original name if no replacement is available.
   *
   * @param propertyName
   *          the name of the potentially deprecated property to check for a replacement name
   * @return either the replacement for {@code propertyName}, or {@code propertyName} if the
   *         property is not deprecated
   * @see #renameDeprecatedPropertyAndWarn(String) if a warning is desired
   */
  public static String renameDeprecatedProperty(String propertyName) {
    // Find first renamer that is able to rename this property, if available, and use it to rename
    return renamers.stream().filter(r -> r.keyFilter.test(propertyName)).findFirst()
        .map(r -> r.keyMapper.apply(propertyName)).orElse(propertyName);
  }

  /**
   * Ensures that for any deprecated properties, both the deprecated and replacement property name
   * are not both used in {@code config}.
   *
   * @param config
   *          the configuration to check for invalid use of deprecated and replacement properties
   */
  static void sanityCheck(CompositeConfiguration config) {
    config.getKeys().forEachRemaining(currentProp -> {
      String renamedProp = renameDeprecatedProperty(currentProp);
      if (!renamedProp.equals(currentProp) && config.containsKey(renamedProp)) {
        log.error("Cannot set both {} and deprecated {} in the configuration.", renamedProp,
            currentProp);
        throw new IllegalStateException(renamedProp + " and deprecated " + currentProp
            + " cannot both be set in the configuration.");
      }
    });
  }
}

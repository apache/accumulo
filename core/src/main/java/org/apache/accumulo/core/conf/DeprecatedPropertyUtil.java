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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.configuration2.CompositeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings("DeprecatedIsStillUsed")
public class DeprecatedPropertyUtil {
  private static final Logger log = LoggerFactory.getLogger(DeprecatedPropertyUtil.class);
  private static final HashSet<String> propertyDeprecationWarnings = new HashSet<>();
  @VisibleForTesting
  protected static final List<PropertyRenamer> renamers =
      new ArrayList<>(List.of(new MasterPropertyRenamer()));

  public interface PropertyRenamer {
    boolean matches(String property);

    String rename(String property);
  }

  /**
   * Renames any properties starting with "master." to the corresponding property name starting with
   * "manager.".
   *
   * @deprecated since 2.1.0, "manager.*" properties should be used instead
   */
  @Deprecated(since = "2.1.0", forRemoval = true)
  public static class MasterPropertyRenamer implements PropertyRenamer {
    public static final String MASTER_PREFIX = "master.";

    @Override
    public boolean matches(String property) {
      return property.startsWith(MASTER_PREFIX);
    }

    @Override
    public String rename(String property) {
      return Property.MANAGER_PREFIX + property.substring(MASTER_PREFIX.length());
    }
  }

  /**
   * @see #renameDeprecatedProperty(String, boolean)
   */
  public static String renameDeprecatedProperty(String propertyName) {
    return renameDeprecatedProperty(propertyName, true);
  }

  /**
   * Checks if {@code propertyName} is a deprecated property name that has a replacement name. The
   * replacement name, if any, is returned. Otherwise, {@code propertyName} is returned. If a
   * deprecated property is used and {@code warnIfRenamed} is {@code true}, then a warning log
   * message is emitted once per property per classloader when the property is renamed.
   *
   * @param propertyName
   *          the name of the potentially deprecated property to check for a replacement name
   * @param warnIfRenamed
   *          indicates whether or not the warning log message should be emitted when a deprecated
   *          property is renamed
   * @return either the replacement for {@code propertyName}, or {@code propertyName} if the
   *         property is not deprecated
   */
  public static String renameDeprecatedProperty(String propertyName, boolean warnIfRenamed) {
    String renamed = renamers.stream().filter(r -> r.matches(propertyName)).findFirst()
        .map(r -> r.rename(propertyName)).orElse(propertyName);

    // Warn about the deprecation if we renamed the property.
    if (warnIfRenamed && !renamed.equals(propertyName)
        && !propertyDeprecationWarnings.contains(propertyName)) {
      log.warn("{} has been deprecated and will be removed in a future release. Use {} instead.",
          propertyName, renamed);
      propertyDeprecationWarnings.add(propertyName);
    }

    return renamed;
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
      String renamedProp = renameDeprecatedProperty(currentProp, false);
      if (!renamedProp.equals(currentProp) && config.containsKey(renamedProp)) {
        log.error("Cannot set both {} and deprecated {} in the configuration.", renamedProp,
            currentProp);
        throw new IllegalStateException(renamedProp + " and deprecated " + currentProp
            + " cannot both be set in the configuration.");
      }
    });
  }
}

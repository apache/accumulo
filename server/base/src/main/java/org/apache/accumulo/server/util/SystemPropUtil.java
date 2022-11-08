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
package org.apache.accumulo.server.util;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.SystemPropKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemPropUtil {

  private static final Logger log = LoggerFactory.getLogger(SystemPropUtil.class);

  public static void setSystemProperty(ServerContext context, String property, String value) {
    context.getPropStore().putAll(SystemPropKey.of(context),
        Map.of(validateSystemProperty(property, value), value));
  }

  public static void modifyProperties(ServerContext context, long version,
      Map<String,String> properties) {
    final Map<String,
        String> checkedProperties = properties.entrySet().stream().collect(
            Collectors.toMap(entry -> validateSystemProperty(entry.getKey(), entry.getValue()),
                Map.Entry::getValue));
    context.getPropStore().replaceAll(SystemPropKey.of(context), version, checkedProperties);
  }

  public static void removeSystemProperty(ServerContext context, String property) {
    String resolved =
        DeprecatedPropertyUtil
            .getReplacementName(property,
                (log, replacement) -> log.warn(
                    "{} was deprecated and will be removed in a future release; assuming user meant"
                        + " its replacement {} and will remove that instead",
                    property, replacement));
    removePropWithoutDeprecationWarning(context, resolved);
  }

  public static void removePropWithoutDeprecationWarning(ServerContext context, String property) {
    context.getPropStore().removeProperties(SystemPropKey.of(context), List.of(property));
  }

  private static String validateSystemProperty(String property, final String value) {
    // Retrieve the replacement name for this property, if there is one.
    // Do this before we check if the name is a valid zookeeper name.
    final var original = property;
    property = DeprecatedPropertyUtil.getReplacementName(property,
        (log, replacement) -> log.warn("{} was deprecated and will be removed in a future release;"
            + " setting its replacement {} instead", original, replacement));

    if (!Property.isValidZooPropertyKey(property)) {
      IllegalArgumentException iae =
          new IllegalArgumentException("Zookeeper property is not mutable: " + property);
      log.debug("Attempted to set zookeeper property.  It is not mutable", iae);
      throw iae;
    }

    // Find the property taking prefix into account
    Property foundProp = null;
    for (Property prop : Property.values()) {
      if (prop.getType() == PropertyType.PREFIX && property.startsWith(prop.getKey())
          || prop.getKey().equals(property)) {
        foundProp = prop;
        break;
      }
    }

    if ((foundProp == null || (foundProp.getType() != PropertyType.PREFIX
        && !foundProp.getType().isValidFormat(value)))) {
      IllegalArgumentException iae = new IllegalArgumentException(
          "Ignoring property " + property + " it is either null or in an invalid format");
      log.debug("Attempted to set zookeeper property.  Value is either null or invalid", iae);
      throw iae;
    }

    return property;
  }
}

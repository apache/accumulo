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

import static org.apache.accumulo.core.conf.Property.COMPACTOR_PREFIX;
import static org.apache.accumulo.core.conf.Property.SSERV_PREFIX;
import static org.apache.accumulo.core.conf.Property.TSERV_PREFIX;

import org.apache.accumulo.core.conf.DeprecatedPropertyUtil;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.PropertyType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceGroupPropUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceGroupPropUtil.class);

  public static String validateResourceGroupProperty(String property, String value) {
    // Retrieve the replacement name for this property, if there is one.
    // Do this before we check if the name is a valid zookeeper name.
    final var original = property;
    property = DeprecatedPropertyUtil.getReplacementName(property,
        (log, replacement) -> log.warn("{} was deprecated and will be removed in a future release;"
            + " setting its replacement {} instead", original, replacement));

    if (property.startsWith(COMPACTOR_PREFIX.getKey()) || property.startsWith(SSERV_PREFIX.getKey())
        || property.startsWith(TSERV_PREFIX.getKey())) {

      if (!Property.isValidProperty(property, value)) {
        IllegalArgumentException iae = new IllegalArgumentException(
            "Property " + property + " with value: " + value + " is not valid");
        LOG.trace("Encountered error setting zookeeper property", iae);
        throw iae;
      }

      // Find the property taking prefix into account
      Property foundProp = null;
      for (Property prop : Property.values()) {
        if ((prop.getType() == PropertyType.PREFIX && property.startsWith(prop.getKey()))
            || prop.getKey().equals(property)) {
          foundProp = prop;
          break;
        }
      }

      if (foundProp == null || (foundProp.getType() != PropertyType.PREFIX
          && !foundProp.getType().isValidFormat(value))) {
        IllegalArgumentException iae = new IllegalArgumentException(
            "Ignoring property " + property + " it is either null or in an invalid format");
        LOG.trace("Attempted to set zookeeper property.  Value is either null or invalid", iae);
        throw iae;
      }

      SystemPropUtil.logIfFixed(Property.getPropertyByKey(property), value);

      return property;

    } else {
      IllegalArgumentException iae = new IllegalArgumentException(
          "Property is not valid resource group override: " + property);
      LOG.trace("Encountered error setting zookeeper property", iae);
      throw iae;
    }

  }

}

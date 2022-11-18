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

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropStoreKey;

public final class PropUtil {

  private PropUtil() {}

  /**
   * Method to set provided properties for the provided AbstractId.
   *
   * @throws IllegalStateException if an underlying exception (KeeperException, InterruptException)
   *         or other failure to read properties from the cache / backend store
   * @throws IllegalArgumentException if a provided property is not valid
   */
  public static void setProperties(final ServerContext context, final PropStoreKey<?> propStoreKey,
      final Map<String,String> properties) {
    PropUtil.validateProperties(context, propStoreKey, properties);
    context.getPropStore().putAll(propStoreKey, properties);
  }

  public static void removeProperties(final ServerContext context,
      final PropStoreKey<?> propStoreKey, final Collection<String> propertyNames) {
    context.getPropStore().removeProperties(propStoreKey, propertyNames);
  }

  public static void replaceProperties(final ServerContext context,
      final PropStoreKey<?> propStoreKey, final long version, final Map<String,String> properties) {
    PropUtil.validateProperties(context, propStoreKey, properties);
    context.getPropStore().replaceAll(propStoreKey, version, properties);
  }

  protected static void validateProperties(final ServerContext context,
      final PropStoreKey<?> propStoreKey, final Map<String,String> properties) {
    for (Map.Entry<String,String> prop : properties.entrySet()) {
      if (!Property.isTablePropertyValid(prop.getKey(), prop.getValue())) {
        throw new IllegalArgumentException("Invalid property for : " + propStoreKey + " name: "
            + prop.getKey() + ", value: " + prop.getValue());
      }
    }
  }

}

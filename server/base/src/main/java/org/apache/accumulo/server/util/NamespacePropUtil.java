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
package org.apache.accumulo.server.util;

import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropCacheKey;

public class NamespacePropUtil implements PropUtil {

  private NamespaceId namespaceId;

  private NamespacePropUtil() {}

  public static NamespacePropUtil factory() {
    return new NamespacePropUtil();
  }

  @Override
  public void setProperties(ServerContext context, AbstractId<?> namespaceId,
      Map<String,String> properties) {
    for (Map.Entry<String,String> prop : properties.entrySet()) {
      // TODO reconcile with TablePropUtil on invalid, this throws exception, table ignores
      if (!Property.isTablePropertyValid(prop.getKey(), prop.getValue())) {
        throw new IllegalArgumentException("Invalid table property for namespace: " + namespaceId
            + " name: " + prop.getKey() + ", value: " + prop.getValue());
      }
    }
    context.getPropStore().putAll(PropCacheKey.forNamespace(context, (NamespaceId) namespaceId),
        properties);
  }

  @Override
  public void removeProperties(ServerContext context, AbstractId<?> namespaceId,
      Collection<String> propertyNames) {
    context.getPropStore().removeProperties(
        PropCacheKey.forNamespace(context, (NamespaceId) namespaceId), propertyNames);
  }
}

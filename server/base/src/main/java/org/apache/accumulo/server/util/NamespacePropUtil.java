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

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.PropCacheKey;
import org.apache.zookeeper.KeeperException;
import org.slf4j.LoggerFactory;

public class NamespacePropUtil {
  public static boolean setNamespaceProperty(ServerContext context, NamespaceId namespaceId,
      String property, String value) throws KeeperException, InterruptedException {
    if (!Property.isTablePropertyValid(property, value))
      return false;

    context.getPropStore().putAll(PropCacheKey.forNamespace(context, namespaceId),
        Map.of(property, value));

    return true;
  }

  public static void removeNamespaceProperty(ServerContext context, NamespaceId namespaceId,
      String property) {

    LoggerFactory.getLogger(NamespacePropUtil.class).warn("Request to remove property: {}",
        property);

    context.getPropStore().removeProperties(PropCacheKey.forNamespace(context, namespaceId),
        List.of(property));
  }
}

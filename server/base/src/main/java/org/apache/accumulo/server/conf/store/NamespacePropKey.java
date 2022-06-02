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
package org.apache.accumulo.server.conf.store;

import static org.apache.accumulo.core.Constants.ZNAMESPACES;
import static org.apache.accumulo.core.Constants.ZNAMESPACE_CONF;
import static org.apache.accumulo.core.Constants.ZTABLE_CONF;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
import org.checkerframework.checker.nullness.qual.NonNull;

public class NamespacePropKey extends PropStoreKey<NamespaceId> {

  private NamespacePropKey(final InstanceId instanceId, final String path, final NamespaceId id) {
    super(instanceId, path, id);
  }

  private static String getNodeName(final InstanceId instanceId, final NamespaceId id) {
    return ZooUtil.getRoot(instanceId) + ZNAMESPACES + "/" + id.canonical() + ZTABLE_CONF + "/"
        + PROP_NODE_NAME;
  }

  public static NamespacePropKey of(final ServerContext context, final NamespaceId id) {
    return of(context.getInstanceID(), id);
  }

  public static NamespacePropKey of(final InstanceId instanceId, final NamespaceId id) {
    return new NamespacePropKey(instanceId, getNodePath(instanceId, id), id);
  }

  private static String getNodePath(final InstanceId instanceId, final NamespaceId id) {
    return ZooUtil.getRoot(instanceId) + ZNAMESPACES + "/" + id.canonical() + ZNAMESPACE_CONF + "/"
        + PROP_NODE_NAME;
  }

  @Override
  public @NonNull String getNodePath() {
    return getNodeName(instanceId, id);
  }

  @Override
  @NonNull
  public String getBasePath() {
    return ZooUtil.getRoot(instanceId) + ZNAMESPACES + "/" + id.canonical() + ZNAMESPACE_CONF;
  }
}

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
package org.apache.accumulo.server.conf;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceConfiguration extends ZooBasedConfiguration {

  private static final Logger log = LoggerFactory.getLogger(NamespaceConfiguration.class);

  public NamespaceConfiguration(ServerContext context, NamespaceId namespaceId,
      AccumuloConfiguration parent) {
    super(log, context, NamespacePropKey.of(namespaceId), parent);
  }

  protected NamespaceId getNamespaceId() {
    var id = ((NamespacePropKey) getPropStoreKey()).getId();
    if (id == null) {
      throw new IllegalArgumentException(
          "Invalid request for namespace id on " + getPropStoreKey());
    }
    return id;
  }

}

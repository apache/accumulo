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

import java.util.Map;
import java.util.function.Predicate;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.store.NamespacePropKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceConfiguration extends ZooBasedConfiguration {

  private static final Logger log = LoggerFactory.getLogger(NamespaceConfiguration.class);
  protected ServerContext context;

  public NamespaceConfiguration(ServerContext context, NamespaceId namespaceId,
      AccumuloConfiguration parent) {
    super(log, context, NamespacePropKey.of(context, namespaceId), parent);
  }

  @Override
  public String get(Property property) {

    String key = property.getKey();

    var namespaceId = getPropStoreKey().getId();
    if (namespaceId != null && namespaceId.equals(Namespace.ACCUMULO.id())
        && isIteratorOrConstraint(key)) {
      // ignore iterators from parent if system namespace
      return null;
    }

    Map<String,String> theseProps = getSnapshot();
    String value = theseProps.get(key);

    if (value != null) {
      return value;
    }

    return getParent().get(property);
  }

  /**
   * exclude system iterators/constraints from the system namespace so that they don't affect the
   * metadata or root tables.
   */
  @Override
  public void getProperties(Map<String,String> props, Predicate<String> filter) {
    Predicate<String> parentFilter = filter;
    // exclude system iterators/constraints from the system namespace
    // so they don't affect the metadata or root tables.
    if (getNamespaceId().equals(Namespace.ACCUMULO.id())) {
      parentFilter = key -> !isIteratorOrConstraint(key) && filter.test(key);
    }

    getParent().getProperties(props, parentFilter);

    getSnapshot().entrySet().stream().filter(e -> filter.test(e.getKey()) && e.getValue() != null)
        .forEach(e -> props.put(e.getKey(), e.getValue()));
  }

  protected NamespaceId getNamespaceId() {
    NamespaceId id = (NamespaceId) getPropStoreKey().getId();
    if (id == null) {
      throw new IllegalArgumentException(
          "Invalid request for namespace id on " + getPropStoreKey());
    }
    return id;
  }

  static boolean isIteratorOrConstraint(String key) {
    return key.startsWith(Property.TABLE_ITERATOR_PREFIX.getKey())
        || key.startsWith(Property.TABLE_CONSTRAINT_PREFIX.getKey());
  }
}

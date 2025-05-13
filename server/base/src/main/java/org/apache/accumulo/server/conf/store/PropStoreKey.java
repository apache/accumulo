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
package org.apache.accumulo.server.conf.store;

import static org.apache.accumulo.core.Constants.ZCONFIG;

import java.util.Comparator;
import java.util.Objects;

import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a strongly-typed id for storing properties in ZooKeeper. The provided path is the
 * canonical String version of this key, because the path is the location in ZooKeeper where this
 * prop store is stored, so the path is the essential characteristic of the key.
 *
 * <p>
 * Provides utility methods from constructing different id based on type and methods to parse a
 * ZooKeeper path and return a prop cache id.
 */
public abstract class PropStoreKey implements Comparable<PropStoreKey> {

  private static final Logger log = LoggerFactory.getLogger(PropStoreKey.class);

  private final String path;

  protected PropStoreKey(final String path) {
    this.path = Objects.requireNonNull(path);
  }

  public @NonNull String getPath() {
    return path;
  }

  /**
   * Determine the prop cache id from a ZooKeeper path
   *
   * @param path the path
   * @return the prop cache id
   */
  public static @Nullable PropStoreKey fromPath(final String path) {

    if (path.equals(ZCONFIG)) {
      return SystemPropKey.of();
    }

    String[] parts = path.split("/");

    // Expected Path: /namespaces/<namespaceId>/config
    if (parts.length == 4 && parts[0].isEmpty() && "namespaces".equals(parts[1])
        && "config".equals(parts[3])) {
      String namespaceId = parts[2];
      return NamespacePropKey.of(NamespaceId.of(namespaceId));
    }

    // Expected Path: /namespaces/<namespaceId>/tables/<tableId>/config
    if (parts.length == 6 && parts[0].isEmpty() && "namespaces".equals(parts[1])
        && "tables".equals(parts[3]) && "config".equals(parts[5])) {
      String namespaceId = parts[2];
      String tableId = parts[4];
      return TablePropKey.of(TableId.of(tableId), NamespaceId.of(namespaceId));
    }
    // without tokens or it does not end with PROP_NAME_NAME
    log.warn("Path '{}' is an invalid path for a property cache key", path);
    return null;
  }

  @Override
  public int compareTo(@NonNull PropStoreKey other) {
    return Comparator.comparing(PropStoreKey::getPath).compare(this, other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return path.equals(((PropStoreKey) o).path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{path=" + getPath() + "}";
  }
}

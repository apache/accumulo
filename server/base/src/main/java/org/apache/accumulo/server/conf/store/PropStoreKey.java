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
import static org.apache.accumulo.core.Constants.ZNAMESPACES;
import static org.apache.accumulo.core.Constants.ZTABLES;

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

  // indices for path.split() on config node paths;
  public static final int TYPE_TOKEN_POSITION = 1;
  public static final int ID_TOKEN_POSITION = 2;

  // remove starting slash from constant.
  public static final String TABLES_NODE_NAME = ZTABLES.substring(1);
  public static final String NAMESPACE_NODE_NAME = ZNAMESPACES.substring(1);
  // expected token length for table and namespace config
  public static final int EXPECTED_CONFIG_LEN = 4;
  // expected token length for sys config
  public static final int EXPECTED_SYS_CONFIG_LEN = 2;

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
    String[] tokens = path.split("/");

    if (tokens.length != EXPECTED_CONFIG_LEN && tokens.length != EXPECTED_SYS_CONFIG_LEN) {
      log.warn("Path '{}' is an invalid path for a property cache key - bad length", path);
      return null;
    }

    String nodeName = "/" + tokens[tokens.length - 1];
    if (tokens.length == EXPECTED_CONFIG_LEN && tokens[TYPE_TOKEN_POSITION].equals(TABLES_NODE_NAME)
        && nodeName.equals(ZCONFIG)) {
      return TablePropKey.of(TableId.of(tokens[ID_TOKEN_POSITION]));
    }

    if (tokens.length == EXPECTED_CONFIG_LEN
        && tokens[TYPE_TOKEN_POSITION].equals(NAMESPACE_NODE_NAME) && nodeName.equals(ZCONFIG)) {
      return NamespacePropKey.of(NamespaceId.of(tokens[ID_TOKEN_POSITION]));
    }

    if (tokens.length == EXPECTED_SYS_CONFIG_LEN && nodeName.equals(ZCONFIG)) {
      return SystemPropKey.of();
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

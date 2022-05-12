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
import static org.apache.accumulo.core.Constants.ZTABLES;

import java.util.Comparator;
import java.util.Objects;

import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a strongly-typed id for storing properties in ZooKeeper. The path in ZooKeeper is
 * determined by the instance id and the type (system, namespace and table), with different root
 * paths.
 * <p>
 * Provides utility methods from constructing different id based on type and methods to parse a
 * ZooKeeper path and return a prop cache id.
 */
public abstract class PropCacheKey<ID_TYPE extends AbstractId<ID_TYPE>>
    implements Comparable<PropCacheKey<ID_TYPE>> {

  public static final String PROP_NODE_NAME = "encoded_props";

  private static final Logger log = LoggerFactory.getLogger(PropCacheKey.class);

  // indices for path.split();
  public static final int TYPE_TOKEN_POSITION = 3;
  public static final int IID_TOKEN_POSITION = 2;
  public static final int ID_TOKEN_POSITION = 4;

  // remove starting slash from constant.
  public static final String TABLES_NODE_NAME = ZTABLES.substring(1);
  public static final String NAMESPACE_NODE_NAME = ZNAMESPACES.substring(1);

  protected final InstanceId instanceId;
  protected final ID_TYPE id;

  private final String path;

  protected PropCacheKey(final InstanceId instanceId, final String path, final ID_TYPE id) {
    this.instanceId = instanceId;
    this.path = path;
    this.id = id;
  }

  public @NonNull String getPath() {
    return path;
  }

  public @NonNull abstract String getBasePath();

  public @NonNull abstract String getNodePath();

  public @NonNull ID_TYPE getId() {
    return id;
  }

  /**
   * Determine the prop cache id from a ZooKeeper path
   *
   * @param path
   *          the path
   * @return the prop cache id
   */
  public static @Nullable PropCacheKey<?> fromPath(final String path) {
    String[] tokens = path.split("/");

    InstanceId instanceId;
    try {
      instanceId = InstanceId.of(tokens[IID_TOKEN_POSITION]);
    } catch (ArrayIndexOutOfBoundsException ex) {
      log.warn("Path '{}' is an invalid path for a property cache key", path);
      return null;
    }
    if (tokens.length < 1 || !tokens[tokens.length - 1].equals(PROP_NODE_NAME)) {
      // without tokens or it does not end with PROP_NAME_NAME
      return null;
    }
    if (tokens[TYPE_TOKEN_POSITION].equals(TABLES_NODE_NAME)) {
      return TablePropKey.of(instanceId, TableId.of(tokens[ID_TOKEN_POSITION]));
    }
    if (tokens[TYPE_TOKEN_POSITION].equals(NAMESPACE_NODE_NAME)) {
      return NamespacePropKey.of(instanceId, NamespaceId.of(tokens[ID_TOKEN_POSITION]));
    }
    return SystemPropKey.of(instanceId);
  }

  @Override
  public int compareTo(@NonNull PropCacheKey<ID_TYPE> other) {
    return Comparator.comparing(PropCacheKey<ID_TYPE>::getPath).compare(this, other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    PropCacheKey<?> that = (PropCacheKey<?>) o;
    if (getId().getClass() != that.getId().getClass()) {
      return false;
    }
    return path.equals(that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "{" + getId().getClass().getSimpleName() + "="
        + getId().canonical() + "'}";
  }
}

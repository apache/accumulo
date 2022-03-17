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

import static org.apache.accumulo.core.Constants.ZCONFIG;
import static org.apache.accumulo.core.Constants.ZNAMESPACES;
import static org.apache.accumulo.core.Constants.ZNAMESPACE_CONF;
import static org.apache.accumulo.core.Constants.ZTABLES;
import static org.apache.accumulo.core.Constants.ZTABLE_CONF;

import java.util.Comparator;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.fate.zookeeper.ZooUtil;
import org.apache.accumulo.server.ServerContext;
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
public class PropCacheKey implements Comparable<PropCacheKey> {

  public static final String PROP_NODE_NAME = "encoded_props";

  private static final Logger log = LoggerFactory.getLogger(PropCacheKey.class);

  // indices for path.split();
  public static final int TYPE_TOKEN_POSITION = 3;
  public static final int IID_TOKEN_POSITION = 2;
  public static final int ID_TOKEN_POSITION = 4;

  // remove starting slash from constant.
  public static final String TABLES_NODE_NAME = ZTABLES.substring(1);
  public static final String NAMESPACE_NODE_NAME = ZNAMESPACES.substring(1);

  private final String path;
  private final IdType idType;
  private final NamespaceId namespaceId;
  private final TableId tableId;

  private PropCacheKey(final String path, final IdType idType, final NamespaceId namespaceId,
      final TableId tableId) {
    this.path = path;
    this.idType = idType;
    this.namespaceId = namespaceId;
    this.tableId = tableId;
  }

  /**
   * Instantiate a system prop cache id using the instance id from the context.
   *
   * @param context
   *          the system context specifying the instance id
   * @return a prop cache id for system properties,
   */
  public static PropCacheKey forSystem(final ServerContext context) {
    return forSystem(context.getInstanceID());
  }

  /**
   * Instantiate a system prop cache id.
   *
   * @param instanceId
   *          the instance id.
   * @return a prop cache id for system properties,
   */
  public static PropCacheKey forSystem(final InstanceId instanceId) {
    return new PropCacheKey(ZooUtil.getRoot(instanceId) + ZCONFIG + "/" + PROP_NODE_NAME,
        IdType.SYSTEM, null, null);
  }

  /**
   * Instantiate a namespace prop cache id using the instance id from the context.
   *
   * @param context
   *          the system context specifying the instance id
   * @param namespaceId
   *          the namespace id
   * @return a prop cache id a namespaces properties,
   */
  public static PropCacheKey forNamespace(final ServerContext context,
      final NamespaceId namespaceId) {
    return forNamespace(context.getInstanceID(), namespaceId);
  }

  /**
   * Instantiate a namespace prop cache id using the instance id from the context.
   *
   * @param instanceId
   *          the instance id
   * @param namespaceId
   *          the namespace id
   * @return a prop cache id a namespaces properties,
   */
  public static PropCacheKey forNamespace(final InstanceId instanceId,
      final NamespaceId namespaceId) {
    return new PropCacheKey(ZooUtil.getRoot(instanceId) + ZNAMESPACES + "/"
        + namespaceId.canonical() + ZNAMESPACE_CONF + "/" + PROP_NODE_NAME, IdType.NAMESPACE,
        namespaceId, null);
  }

  /**
   * Instantiate a namespace prop cache id using the instance id from the context.
   *
   * @param context
   *          the system context specifying the instance id
   * @param tableId
   *          the table id
   * @return a prop cache id a namespaces properties,
   */
  public static PropCacheKey forTable(final ServerContext context, final TableId tableId) {
    return forTable(context.getInstanceID(), tableId);
  }

  /**
   * Instantiate a namespace prop cache id using the instance id from the context.
   *
   * @param instanceId
   *          the instance id
   * @param tableId
   *          the table id
   * @return a prop cache id a namespaces properties,
   */
  public static PropCacheKey forTable(final InstanceId instanceId, final TableId tableId) {
    return new PropCacheKey(ZooUtil.getRoot(instanceId) + ZTABLES + "/" + tableId.canonical()
        + ZTABLE_CONF + "/" + PROP_NODE_NAME, IdType.TABLE, null, tableId);
  }

  /**
   * Determine the prop cache id from a ZooKeeper path
   *
   * @param path
   *          the path
   * @return the prop cache id
   */
  public static @Nullable PropCacheKey fromPath(final String path) {
    String[] tokens = path.split("/");

    InstanceId instanceId;
    try {
      instanceId = InstanceId.of(tokens[IID_TOKEN_POSITION]);
    } catch (ArrayIndexOutOfBoundsException ex) {
      log.warn("Path '{}' is an invalid path for a property cache key", path);
      return null;
    }

    IdType type = extractType(tokens);

    switch (type) {
      case SYSTEM:
        return PropCacheKey.forSystem(instanceId);
      case NAMESPACE:
        return PropCacheKey.forNamespace(instanceId, NamespaceId.of(tokens[ID_TOKEN_POSITION]));
      case TABLE:
        return PropCacheKey.forTable(instanceId, TableId.of(tokens[ID_TOKEN_POSITION]));
      case UNKNOWN:
      default:
        return null;
    }
  }

  /**
   * Determine if the IdType is system, namespace or table from a tokenized path. To be a valid id,
   * the final token is PROP_NODE_NAME and then the type is defined if the path has table or
   * namespace in the path, otherwise it is assumed to be system.
   *
   * @param tokens
   *          a path split into String[] of tokens
   * @return the id type.
   */
  public static IdType extractType(final String[] tokens) {
    if (tokens.length == 0 || !tokens[tokens.length - 1].equals(PROP_NODE_NAME)) {
      // without tokens or it does not end with PROP_NAME_NAME
      return IdType.UNKNOWN;
    }
    if (tokens[TYPE_TOKEN_POSITION].equals(TABLES_NODE_NAME)) {
      return IdType.TABLE;
    }
    if (tokens[TYPE_TOKEN_POSITION].equals(NAMESPACE_NODE_NAME)) {
      return IdType.NAMESPACE;
    }
    return IdType.SYSTEM;
  }

  public String getPath() {
    return path;
  }

  public IdType getIdType() {
    return idType;
  }

  @Override
  public int compareTo(@NonNull PropCacheKey other) {
    return Comparator.comparing(PropCacheKey::getIdType).thenComparing(PropCacheKey::getPath)
        .compare(this, other);
  }

  /**
   * If the prop cache is for a namespace, return the namespace id.
   *
   * @return the namespace id.
   */
  public @Nullable NamespaceId getNamespaceId() {
    return namespaceId;
  }

  /**
   * if the prop cache is for a table, return the table id.
   *
   * @return the table id.
   */
  public @Nullable TableId getTableId() {
    return tableId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    PropCacheKey that = (PropCacheKey) o;
    return path.equals(that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path);
  }

  @Override
  public String toString() {
    switch (idType) {
      case SYSTEM:
        return new StringJoiner(", ", PropCacheKey.class.getSimpleName() + "[", "]")
            .add("idType=System").toString();
      case NAMESPACE:
        return new StringJoiner(", ", PropCacheKey.class.getSimpleName() + "[", "]")
            .add("idType=Namespace").add("namespaceId=" + namespaceId).toString();
      case TABLE:
        return new StringJoiner(", ", PropCacheKey.class.getSimpleName() + "[", "]")
            .add("idType=Table").add("tableId=" + tableId).toString();
      default:
        return new StringJoiner(", ", PropCacheKey.class.getSimpleName() + "[", "]")
            .add("idType=" + idType).add("namespaceId=" + namespaceId).add("tableId=" + tableId)
            .add("path='" + path + "'").toString();
    }

  }

  /**
   * Define types of properties stored in zookeeper. Note: default properties are not in zookeeper
   * but come from code.
   */
  public enum IdType {
    UNKNOWN, SYSTEM, NAMESPACE, TABLE
  }
}

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
package org.apache.accumulo.core.fate.zookeeper;

import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * Immutable data class used by zoo cache to hold what it is caching for single zookeeper node. Data
 * and children are obtained from zookeeper at different times. This class is structured so that
 * data can be obtained first and then children added later or visa veras.
 *
 * <p>
 * Four distinct states can be cached for a zookeeper node.
 * <ul>
 * <li>Can cache that a node does not exist in zookeeper. This state is represented by data, state,
 * and children all being null.</li>
 * <li>Can cache only the data for a zookeeper node. For this state data and stat are non-null while
 * children is null. Calling getChildren on node in this state will throw an exception.</li>
 * <li>Can cache only the children for a zookeeper node. For this state children is non-null while
 * data and stat are null. Calling getData or getStat on node in this state will throw an
 * exception.</li>
 * <li>Can cache the children and data for a zookeeper node. For this state data,stat, and children
 * are non-null.</li>
 * </ul>
 * <p>
 *
 */
class ZcNode {

  private final byte[] data;
  private final ZooCache.ZcStat stat;
  private final List<String> children;

  static final ZcNode NON_EXISTENT = new ZcNode();

  private ZcNode() {
    this.data = null;
    this.stat = null;
    this.children = null;
  }

  /**
   * Creates a new ZcNode that combines the data and stat from an existing ZcNode and sets the
   * children.
   */
  ZcNode(List<String> children, ZcNode existing) {
    Objects.requireNonNull(children);
    if (existing == null) {
      this.data = null;
      this.stat = null;
    } else {
      this.data = existing.data;
      this.stat = existing.stat;
    }

    this.children = List.copyOf(children);
  }

  /**
   * Creates a new ZcNode that combines the children from an existing ZcNode and sets the data and
   * stat.
   */
  ZcNode(byte[] data, ZooCache.ZcStat zstat, ZcNode existing) {
    this.data = Objects.requireNonNull(data);
    this.stat = Objects.requireNonNull(zstat);
    if (existing == null) {
      this.children = null;
    } else {
      this.children = existing.children;
    }
  }

  /**
   * @return the data if the node exists and the data was set OR return null when the node does not
   *         exist
   * @throws IllegalStateException in the case where the node exists and the data was never set
   */
  byte[] getData() {
    Preconditions.checkState(cachedData());
    return data;
  }

  /**
   * @return the stat if the node exists and the stat was set OR return null when the node does not
   *         exist
   * @throws IllegalStateException in the case where the node exists and the data was never set
   */
  ZooCache.ZcStat getStat() {
    Preconditions.checkState(cachedData());
    return stat;
  }

  /**
   * @return the children if the node exists and the children were set OR return null when the node
   *         does not exist exists
   * @throws IllegalStateException in the case where the node exists and the children were never set
   */
  List<String> getChildren() {
    Preconditions.checkState(cachedChildren());
    return children;
  }

  /**
   * @return true if the node does not exists or it exists and children are cached.
   */
  boolean cachedChildren() {
    return children != null || notExists();
  }

  /**
   * @return true if the node does not exists or it exists and data and stat cached.
   */
  boolean cachedData() {
    return data != null || notExists();
  }

  /**
   * @return true if the node does not exists in zookeeper
   */
  boolean notExists() {
    return stat == null && data == null && children == null;
  }
}

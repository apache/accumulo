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
package org.apache.accumulo.core.zookeeper;

import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

/**
 * Immutable data class used by zoo cache to hold what it is caching for single zookeeper node. Data
 * and children are obtained from zookeeper at different times. This class is structured so that
 * data can be obtained first and then children added later or visa veras.
 *
 */
class ZcNode {

  /**
   * This enum represents what ZooCache has discovered about a given node in zookeeper so far.
   */
  enum Discovered {
    /**
     * An attempt was made to fetch data or children and ZooKeeper threw a NoNodeException. In this
     * case ZooCache knows the node did not exist.
     */
    NON_EXISTENT,
    /**
     * ZooCache knows the node existed and what its children were because a successful call was made
     * to ZooKeeper to get children. However zoocache has never requested the nodes data and does
     * not know its data.
     */
    CHILDREN_ONLY,
    /**
     * ZooCache knows the node exists and what its data was because a successful call was made to
     * ZooKeeper to get data. However zoocache has never requested the nodes children and does not
     * know its children.
     */
    DATA_ONLY,
    /**
     * ZooCache knows the node existed and has made successful calls to ZooKeeper to get the nodes
     * data and children.
     */
    CHILDREN_AND_DATA;

  }

  private final byte[] data;
  private final ZcStat stat;
  private final List<String> children;
  private final Discovered discovered;

  static final ZcNode NON_EXISTENT = new ZcNode();

  private ZcNode() {
    this.data = null;
    this.stat = null;
    this.children = null;
    this.discovered = Discovered.NON_EXISTENT;
  }

  /**
   * Creates a new ZcNode that combines the data and stat from an existing ZcNode and sets the
   * children.
   */
  ZcNode(List<String> children, ZcNode existing) {
    Objects.requireNonNull(children);
    if (existing == null) {
      this.discovered = Discovered.CHILDREN_ONLY;
      this.data = null;
      this.stat = null;
    } else {
      switch (existing.discovered) {
        case NON_EXISTENT:
        case CHILDREN_ONLY:
          Preconditions.checkArgument(existing.data == null && existing.stat == null);
          this.discovered = Discovered.CHILDREN_ONLY;
          this.data = null;
          this.stat = null;
          break;
        case DATA_ONLY:
        case CHILDREN_AND_DATA:
          this.discovered = Discovered.CHILDREN_AND_DATA;
          this.data = Objects.requireNonNull(existing.data);
          this.stat = Objects.requireNonNull(existing.stat);
          break;
        default:
          throw new IllegalStateException("Unknown enum " + existing.discovered);
      }

    }

    this.children = List.copyOf(children);
  }

  /**
   * Creates a new ZcNode that combines the children from an existing ZcNode and sets the data and
   * stat.
   */
  ZcNode(byte[] data, ZcStat zstat, ZcNode existing) {

    this.data = Objects.requireNonNull(data);
    this.stat = Objects.requireNonNull(zstat);

    if (existing == null) {
      this.discovered = Discovered.DATA_ONLY;
      this.children = null;
    } else {
      switch (existing.discovered) {
        case NON_EXISTENT:
        case DATA_ONLY:
          Preconditions.checkArgument(existing.children == null);
          this.discovered = Discovered.DATA_ONLY;
          this.children = null;
          break;
        case CHILDREN_ONLY:
        case CHILDREN_AND_DATA:
          this.discovered = Discovered.CHILDREN_AND_DATA;
          this.children = Objects.requireNonNull(existing.children);
          break;
        default:
          throw new IllegalStateException("Unknown enum " + existing.discovered);
      }
    }
  }

  /**
   * @return the data if the node exists and the data was set OR return null when the node does not
   *         exist
   * @throws IllegalStateException in the case where the node exists and the data was never set.
   *         This is thrown when {@link #cachedData()} returns false.
   */
  byte[] getData() {
    Preconditions.checkState(cachedData());
    return data;
  }

  /**
   * @return the stat if the node exists and the stat was set OR return null when the node does not
   *         exist
   * @throws IllegalStateException in the case where the node exists and the data was never set.
   *         This is thrown when {@link #cachedData()} returns false.
   */
  ZcStat getStat() {
    Preconditions.checkState(cachedData());
    return stat;
  }

  /**
   * @return the children if the node exists and the children were set OR return null when the node
   *         does not exist exists
   * @throws IllegalStateException in the case where the node exists and the children were never
   *         set. This is thrown when {@link #cachedChildren()} returns false.
   */
  List<String> getChildren() {
    Preconditions.checkState(cachedChildren());
    return children;
  }

  /**
   * @return true if the node does not exists or it exists and children are cached.
   */
  boolean cachedChildren() {
    return discovered != Discovered.DATA_ONLY;
  }

  /**
   * @return true if the node does not exists or it exists and data and stat cached.
   */
  boolean cachedData() {
    return discovered != Discovered.CHILDREN_ONLY;
  }
}

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
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.accumulo.core.lock.ServiceLockData;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.zookeeper.WatchedEvent;

public interface ZooCache {

  interface ZooCacheWatcher extends Consumer<WatchedEvent> {}

  /**
   * Add a ZooCacheWatcher object to this ZooCache
   *
   * @param watcher
   */
  void addZooCacheWatcher(ZooCacheWatcher watcher);

  /**
   * Gets the children of the given node. A watch is established by this call.
   *
   * @param zPath path of node
   * @return children list, or null if node has no children or does not exist
   */
  List<String> getChildren(String zPath);

  /**
   * Gets data at the given path. Status information is not returned. A watch is established by this
   * call.
   *
   * @param zPath path to get
   * @return path data, or null if non-existent
   */
  byte[] get(String zPath);

  /**
   * Gets data at the given path, filling status information into the given <code>Stat</code>
   * object. A watch is established by this call.
   *
   * @param zPath path to get
   * @param status status object to populate
   * @return path data, or null if non-existent
   */
  byte[] get(String zPath, ZcStat status);

  /**
   * Returns a monotonically increasing count of the number of time the cache was updated. If the
   * count is the same, then it means cache did not change.
   */
  long getUpdateCount();

  /**
   * Checks if a data value (or lack of one) is cached.
   *
   * @param zPath path of node
   * @return true if data value is cached
   */
  boolean dataCached(String zPath);

  /**
   * Checks if children of a node (or lack of them) are cached.
   *
   * @param zPath path of node
   * @return true if children are cached
   */
  boolean childrenCached(String zPath);

  /**
   * Removes all paths in the cache match the predicate.
   */
  void clear(Predicate<String> pathPredicate);

  /**
   * Clears this cache of all information about nodes rooted at the given path.
   *
   * @param zPath path of top node
   */
  void clear(String zPath);

  /**
   * Gets the lock data from the node in the cache at the specified path
   */
  Optional<ServiceLockData> getLockData(ServiceLockPath path);

}

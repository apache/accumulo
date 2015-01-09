/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.fate.zookeeper;

import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.Watcher;

/**
 * A factory for {@link ZooCache} instances.
 */
public class ZooCacheFactory {
  // TODO: make this better - LRU, soft references, ...
  private static Map<String,ZooCache> instances = new HashMap<String,ZooCache>();

  /**
   * Gets a {@link ZooCache}. The same object may be returned for multiple calls with the same arguments.
   *
   * @param zooKeepers
   *          comma-seprated list of ZooKeeper host[:port]s
   * @param sessionTimeout
   *          session timeout
   * @return cache object
   */
  public ZooCache getZooCache(String zooKeepers, int sessionTimeout) {
    String key = zooKeepers + ":" + sessionTimeout;
    synchronized (instances) {
      ZooCache zc = instances.get(key);
      if (zc == null) {
        zc = new ZooCache(zooKeepers, sessionTimeout);
        instances.put(key, zc);
      }
      return zc;
    }
  }

  /**
   * Gets a watched {@link ZooCache}. If the watcher is null, then the same (unwatched) object may be returned for multiple calls with the same remaining
   * arguments.
   *
   * @param zooKeepers
   *          comma-seprated list of ZooKeeper host[:port]s
   * @param sessionTimeout
   *          session timeout
   * @param watcher
   *          watcher (optional)
   * @return cache object
   */
  public ZooCache getZooCache(String zooKeepers, int sessionTimeout, Watcher watcher) {
    if (watcher == null) {
      // reuse
      return getZooCache(zooKeepers, sessionTimeout);
    }
    return new ZooCache(zooKeepers, sessionTimeout, watcher);
  }

  /**
   * Resets the factory. All cached objects are flushed.
   */
  void reset() {
    synchronized (instances) {
      instances.clear();
    }
  }

}

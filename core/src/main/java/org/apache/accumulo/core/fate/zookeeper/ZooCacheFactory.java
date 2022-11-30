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

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.singletons.SingletonManager;
import org.apache.accumulo.core.singletons.SingletonService;

/**
 * A factory for {@link ZooCache} instances.
 * <p>
 * Implementation note: We were using the instances map to track all the instances that have been
 * created, so we could explicitly close them when the SingletonManager detected that the last
 * legacy client (using Connector/ZooKeeperInstance) has gone away. This class may no longer be
 * needed, since the legacy client code has been removed, so long as the ZooCache instances it is
 * tracking are managed as resources within ClientContext or ServerContext, and explicitly closed
 * when those are closed.
 */
public class ZooCacheFactory {

  private static Map<String,ZooCache> instances = new HashMap<>();
  private static boolean enabled = true;

  public ZooCacheFactory() {}

  private static boolean isEnabled() {
    synchronized (instances) {
      return enabled;
    }
  }

  private static void enable() {
    synchronized (instances) {
      enabled = true;
    }
  }

  private static void disable() {
    synchronized (instances) {
      try {
        instances.values().forEach(ZooCache::close);
      } finally {
        instances.clear();
        enabled = false;
      }
    }
  }

  static {
    // important because of ZOOKEEPER-2368.. when zookeeper client is closed it does not generate an
    // event!
    SingletonManager.register(new SingletonService() {

      @Override
      public synchronized boolean isEnabled() {
        return ZooCacheFactory.isEnabled();
      }

      @Override
      public synchronized void enable() {
        ZooCacheFactory.enable();
      }

      @Override
      public synchronized void disable() {
        ZooCacheFactory.disable();
      }
    });

  }

  /**
   * Gets a {@link ZooCache}. The same object may be returned for multiple calls with the same
   * arguments.
   *
   * @param zooKeepers comma-separated list of ZooKeeper host[:port]s
   * @param sessionTimeout session timeout
   * @return cache object
   */
  public ZooCache getZooCache(String zooKeepers, int sessionTimeout) {
    String key = zooKeepers + ":" + sessionTimeout;
    synchronized (instances) {
      if (!isEnabled()) {
        throw new IllegalStateException("The Accumulo singleton for zookeeper caching is "
            + "disabled. This is likely caused by all AccumuloClients being closed");
      }
      return instances.computeIfAbsent(key, k -> getNewZooCache(zooKeepers, sessionTimeout));
    }
  }

  /**
   * Always return a new {@link ZooCache}.
   *
   * @param zooKeepers comma-separated list of ZooKeeper host[:port]s
   * @param sessionTimeout session timeout
   * @return a new instance
   */
  public ZooCache getNewZooCache(String zooKeepers, int sessionTimeout) {
    return new ZooCache(new ZooReader(zooKeepers, sessionTimeout), null);
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

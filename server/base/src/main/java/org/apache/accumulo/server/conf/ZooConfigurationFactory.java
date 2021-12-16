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
package org.apache.accumulo.server.conf;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * A factory for {@link ZooConfiguration} objects.
 */
class ZooConfigurationFactory {
  private static final Map<String,ZooConfiguration> instances = new HashMap<>();

  /**
   * Gets a configuration object for the given instance with the given parent. Repeated calls will
   * return the same object.
   *
   * @param context
   *          ServerContext; if null, instance is determined from HDFS
   * @param zcf
   *          {@link ZooCacheFactory} for building {@link ZooCache} to contact ZooKeeper (required)
   * @param parent
   *          parent configuration (required)
   * @return configuration
   */
  ZooConfiguration getInstance(ServerContext context, ZooCacheFactory zcf,
      AccumuloConfiguration parent) {
    ZooConfiguration config;
    synchronized (instances) {
      config = instances.get(context.getInstanceID());
      if (config == null) {
        ZooCache propCache;

        // The purpose of this watcher is a hack. It forces the creation on a new zoocache instead
        // of using a shared one. This was done so that the zoocache
        // would update less, causing the configuration update count to changes less.
        Watcher watcher = new Watcher() {
          @Override
          public void process(WatchedEvent arg0) {}
        };
        propCache = zcf.getZooCache(context.getZooKeepers(), context.getZooKeepersSessionTimeOut(),
            watcher);
        config = new ZooConfiguration(context, propCache, parent);
        instances.put(context.getInstanceID(), config);
      }
    }
    return config;
  }

}

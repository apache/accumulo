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
package org.apache.accumulo.server.conf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooCache;
import org.apache.accumulo.fate.zookeeper.ZooCacheFactory;
import org.apache.accumulo.server.Accumulo;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.fs.Path;

/**
 * A factory for {@link ZooConfiguration} objects.
 */
class ZooConfigurationFactory {
  private static final Map<String,ZooConfiguration> instances = new HashMap<>();

  /**
   * Gets a configuration object for the given instance with the given parent. Repeated calls will return the same object.
   *
   * @param inst
   *          instance; if null, instance is determined from HDFS
   * @param zcf
   *          {@link ZooCacheFactory} for building {@link ZooCache} to contact ZooKeeper (required)
   * @param parent
   *          parent configuration (required)
   * @return configuration
   */
  ZooConfiguration getInstance(Instance inst, ZooCacheFactory zcf, AccumuloConfiguration parent) {
    String instanceId;
    if (inst == null) {
      // InstanceID should be the same across all volumes, so just choose one
      VolumeManager fs;
      try {
        fs = VolumeManagerImpl.get();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      Path instanceIdPath = Accumulo.getAccumuloInstanceIdPath(fs);
      instanceId = ZooUtil.getInstanceIDFromHdfs(instanceIdPath, parent);
    } else {
      instanceId = inst.getInstanceID();
    }

    ZooConfiguration config;
    synchronized (instances) {
      config = instances.get(instanceId);
      if (config == null) {
        ZooCache propCache;
        if (inst == null) {
          propCache = zcf.getZooCache(parent.get(Property.INSTANCE_ZK_HOST), (int) parent.getTimeInMillis(Property.INSTANCE_ZK_TIMEOUT));
        } else {
          propCache = zcf.getZooCache(inst.getZooKeepers(), inst.getZooKeepersSessionTimeOut());
        }
        config = new ZooConfiguration(instanceId, propCache, parent);
        instances.put(instanceId, config);
      }
    }
    return config;
  }

  /**
   * Gets a configuration object for the given instance with the given parent. Repeated calls will return the same object.
   *
   * @param inst
   *          instance; if null, instance is determined from HDFS
   * @param parent
   *          parent configuration (required)
   * @return configuration
   */
  public ZooConfiguration getInstance(Instance inst, AccumuloConfiguration parent) {
    return getInstance(inst, new ZooCacheFactory(), parent);
  }
}

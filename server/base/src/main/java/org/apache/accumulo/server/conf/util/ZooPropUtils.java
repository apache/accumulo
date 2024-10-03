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
package org.apache.accumulo.server.conf.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.ZINSTANCES;
import static org.apache.accumulo.core.Constants.ZROOT;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPropUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ZooPropUtils.class);

  private ZooPropUtils() {}

  /**
   * Read the instance names and instance ids from ZooKeeper. The storage structure in ZooKeeper is:
   *
   * <pre>
   *   /accumulo/instances/instance_name  - with the instance id stored as data.
   * </pre>
   *
   * @return a map of (instance name, instance id) entries
   */
  public static Map<String,InstanceId> readInstancesFromZk(final ZooReader zooReader) {
    String instanceRoot = ZROOT + ZINSTANCES;
    Map<String,InstanceId> idMap = new TreeMap<>();
    try {
      List<String> names = zooReader.getChildren(instanceRoot);
      names.forEach(name -> {
        InstanceId iid = getInstanceIdForName(zooReader, name);
        idMap.put(name, iid);
      });
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading instance name info from ZooKeeper", ex);
    } catch (KeeperException ex) {
      throw new IllegalStateException("Failed to read instance name info from ZooKeeper", ex);
    }
    return idMap;
  }

  private static InstanceId getInstanceIdForName(ZooReader zooReader, String name) {
    String instanceRoot = ZROOT + ZINSTANCES;
    String path = "";
    try {
      path = instanceRoot + "/" + name;
      byte[] uuid = zooReader.getData(path);
      return InstanceId.of(UUID.fromString(new String(uuid, UTF_8)));
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading instance id from ZooKeeper", ex);
    } catch (KeeperException ex) {
      LOG.warn("Failed to read instance id for " + path);
      return null;
    }
  }

}

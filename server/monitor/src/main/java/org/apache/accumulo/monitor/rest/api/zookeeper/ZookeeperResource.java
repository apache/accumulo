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
package org.apache.accumulo.monitor.rest.api.zookeeper;

import javax.ws.rs.GET;

import org.apache.accumulo.monitor.ZooKeeperStatus;
import org.apache.accumulo.monitor.ZooKeeperStatus.ZooKeeperState;
import org.apache.accumulo.monitor.rest.api.BasicResource;

/**
 *
 * Generates a new ZooKeeper information as a JSON object
 *
 * @since 2.0.0
 *
 */
public class ZookeeperResource extends BasicResource {

  /**
   * Generates a list of zookeeper information
   *
   * @return Zookeeper information
   */
  @GET
  public ZKInformation getZKInformation() {

    ZKInformation zk = new ZKInformation();

    // Adds new zk to the list
    for (ZooKeeperState k : ZooKeeperStatus.getZooKeeperStatus()) {
      if (k.clients >= 0) {
        zk.addZK(new ZooKeeper(k.keeper, k.mode, k.clients));
      }
    }
    return zk;
  }
}

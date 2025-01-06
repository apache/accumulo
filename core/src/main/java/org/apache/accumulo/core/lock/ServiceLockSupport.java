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
package org.apache.accumulo.core.lock;

import org.apache.accumulo.core.client.admin.servers.ServerId.Type;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceLockSupport {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceLockSupport.class);

  /**
   * Ensures that the resource group node in ZooKeeper is created for this server
   */
  public static void createNonHaServiceLockPath(Type server, ZooReaderWriter zrw,
      ServiceLockPath slp) throws KeeperException, InterruptedException {
    // The ServiceLockPath contains a resource group in the path which is not created
    // at initialization time. If it does not exist, then create it.
    String rgPath = slp.toString().substring(0, slp.toString().lastIndexOf("/" + slp.getServer()));
    LOG.debug("Creating {} resource group path in zookeeper: {}", server, rgPath);
    try {
      zrw.mkdirs(rgPath);
      zrw.putPersistentData(slp.toString(), new byte[] {}, NodeExistsPolicy.SKIP);
    } catch (KeeperException e) {
      if (e.code() == KeeperException.Code.NOAUTH) {
        LOG.error("Failed to write to ZooKeeper. Ensure that"
            + " accumulo.properties, specifically instance.secret, is consistent.");
      }
      throw e;
    }

  }
}

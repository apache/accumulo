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

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.LoggerFactory;

public class ZooReservation {

  private static final String DELIMITER = "-";

  public static boolean attempt(ZooReaderWriter zk, String path, FateId fateId, String debugInfo)
      throws KeeperException, InterruptedException {

    while (true) {
      try {
        zk.putPersistentData(path, (fateId.canonical() + DELIMITER + debugInfo).getBytes(UTF_8),
            NodeExistsPolicy.FAIL);
        return true;
      } catch (NodeExistsException nee) {
        byte[] zooData;
        try {
          zooData = zk.getData(path);
        } catch (NoNodeException nne) {
          continue;
        }

        FateId idInZoo = FateId.from(new String(zooData, UTF_8).split(DELIMITER)[0]);

        return idInZoo.equals(fateId);
      }
    }

  }

  public static void release(ZooReaderWriter zk, String path, FateId fateId)
      throws KeeperException, InterruptedException {
    byte[] zooData;

    try {
      zooData = zk.getData(path);
    } catch (NoNodeException e) {
      // Just logging a warning, if data is gone then our work here is done.
      LoggerFactory.getLogger(ZooReservation.class).debug("Node does not exist {}", path);
      return;
    }

    String zooDataStr = new String(zooData, UTF_8);
    FateId idInZoo = FateId.from(zooDataStr.split(DELIMITER)[0]);

    if (!idInZoo.equals(fateId)) {
      throw new IllegalStateException("Tried to release reservation " + path
          + " with data mismatch " + fateId + " " + zooDataStr);
    }

    zk.recursiveDelete(path, NodeMissingPolicy.SKIP);
  }

}

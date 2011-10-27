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
package org.apache.accumulo.server.zookeeper;

import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;

public class ZooReservation {
  
  public static boolean attempt(IZooReaderWriter zk, String path, String reservationID, String debugInfo) throws KeeperException, InterruptedException {
    if (reservationID.contains(":"))
      throw new IllegalArgumentException();
    
    while (true) {
      try {
        zk.putPersistentData(path, (reservationID + ":" + debugInfo).getBytes(), NodeExistsPolicy.FAIL);
        return true;
      } catch (NodeExistsException nee) {
        Stat stat = new Stat();
        byte[] zooData;
        try {
          zooData = zk.getData(path, stat);
        } catch (NoNodeException nne) {
          continue;
        }
        
        String idInZoo = new String(zooData).split(":")[0];
        
        return idInZoo.equals(new String(reservationID));
      }
    }
    
  }
  
  public static void release(IZooReaderWriter zk, String path, String reservationID) throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    byte[] zooData;
    
    try {
      zooData = zk.getData(path, stat);
    } catch (NoNodeException e) {
      // TODO log warning? this may happen as a normal course of business.... could return a boolean...
      Logger.getLogger(ZooReservation.class).debug("Node does not exist " + path);
      return;
    }
    
    String idInZoo = new String(zooData).split(":")[0];
    
    if (!idInZoo.equals(new String(reservationID))) {
      throw new IllegalStateException("Tried to release reservation " + path + " with data mismatch " + new String(reservationID) + " " + new String(zooData));
    }
    
    zk.recursiveDelete(path, stat.getVersion(), NodeMissingPolicy.SKIP);
  }
  
}

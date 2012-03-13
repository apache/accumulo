/**
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
package org.apache.accumulo.server.fate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.fate.TStore.TStatus;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;

/**
 * Prints info about the FATE operations that are currently running.
 */
public class Print {
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub

    Instance instance = HdfsZooInstance.getInstance();
    String path = ZooUtil.getRoot(instance) + Constants.ZFATE;
    IZooReaderWriter zk = ZooReaderWriter.getRetryingInstance();
    
    Map<Long,List<String>> heldLocks = new HashMap<Long,List<String>>();
    Map<Long,List<String>> waitingLocks = new HashMap<Long,List<String>>();

    List<String> lockedTables = zk.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS);

    for (String tableId : lockedTables) {
      try {
        List<String> lockNodes = zk.getChildren(ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS + "/" + tableId);
        lockNodes = new ArrayList<String>(lockNodes);
        Collections.sort(lockNodes);

        int pos = 0;
        boolean sawWriteLock = false;

        for (String node : lockNodes) {
          try {
            byte[] data = zk.getData(ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS + "/" + tableId + "/" + node, null);
            String lda[] = new String(data).split(":");
            
            if (lda[0].charAt(0) == 'W')
              sawWriteLock = true;
            
            Map<Long,List<String>> locks;
            
            if (pos == 0) {
              locks = heldLocks;
            } else {
              if (lda[0].charAt(0) == 'R' && !sawWriteLock) {
                locks = heldLocks;
              } else {
                locks = waitingLocks;
              }
            }

            List<String> tables = locks.get(Long.parseLong(lda[1], 16));
            if (tables == null) {
              tables = new ArrayList<String>();
              locks.put(Long.parseLong(lda[1], 16), tables);
            }
            
            tables.add(lda[0].charAt(0) + ":" + tableId);
            
          } catch (Exception e) {
            e.printStackTrace();
          }
          pos++;
        }
        
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println("Failed to locks for " + tableId + " continuing");
      }
    }

    ZooStore<Master> zs = new ZooStore<Master>(path, zk);
    List<Long> transactions = zs.list();
    
    for (Long tid : transactions) {
      
      zs.reserve(tid);

      String debug = (String) zs.getProperty(tid, "debug");
      
      List<String> hlocks = heldLocks.get(tid);
      if (hlocks == null)
        hlocks = Collections.emptyList();
      
      List<String> wlocks = waitingLocks.get(tid);
      if (wlocks == null)
        wlocks = Collections.emptyList();
      
      String top = null;
      Repo<Master> repo = zs.top(tid);
        if (repo != null)
          top = repo.getDescription();
      
      TStatus status = null;
      status = zs.getStatus(tid);

      zs.unreserve(tid, 0);

      System.out.printf("txid: %016x  status: %-15s  op: %-15s  locked: %-15s locking: %-15s top: %s\n", tid, status, debug, hlocks, wlocks, top);
    }


  }
  
}

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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.AdminUtil;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.accumulo.server.master.Master;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;

/**
 * A utility to administer FATE operations
 */
public class Admin {
  public static void main(String[] args) throws Exception {
    AdminUtil<Master> admin = new AdminUtil<Master>();
    boolean valid = (args.length == 2 && args[0].matches("fail|delete")) || (args.length == 1 && args[0].equals("print"));
    
    if (!valid) {
      System.err.println("Usage : " + Admin.class.getSimpleName() + " fail <txid> | delete <txid> | print");
      System.exit(-1);
    }
    
    Instance instance = HdfsZooInstance.getInstance();
    String path = ZooUtil.getRoot(instance) + Constants.ZFATE;
    String masterPath = ZooUtil.getRoot(instance) + Constants.ZMASTER_LOCK;
    IZooReaderWriter zk = ZooReaderWriter.getRetryingInstance();
    ZooStore<Master> zs = new ZooStore<Master>(path, zk);
    
    if (args[0].equals("fail")) {
      admin.prepFail(zs, masterPath, args[1]);
    } else if (args[0].equals("delete")) {
      admin.prepDelete(zs, masterPath, args[1]);
      admin.deleteLocks(zs, zk, ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS, args[1]);
    } else if (args[0].equals("print")) {
      admin.print(zs, zk, ZooUtil.getRoot(instance) + Constants.ZTABLE_LOCKS);
    }
  }
}

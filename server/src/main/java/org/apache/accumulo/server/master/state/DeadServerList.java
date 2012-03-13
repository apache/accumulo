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
package org.apache.accumulo.server.master.state;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.master.thrift.DeadServer;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.server.zookeeper.IZooReaderWriter;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

public class DeadServerList {
  private static final Logger log = Logger.getLogger(DeadServerList.class);
  private final String path;
  
  public DeadServerList(String path) {
    this.path = path;
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    try {
      zoo.mkdirs(path);
    } catch (Exception ex) {
      log.error("Unable to make parent directories of " + path, ex);
    }
  }
  
  public List<DeadServer> getList() {
    List<DeadServer> result = new ArrayList<DeadServer>();
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    try {
      List<String> children = zoo.getChildren(path);
      if (children != null) {
        for (String child : children) {
          Stat stat = new Stat();
          byte[] data = zoo.getData(path + "/" + child, stat);
          DeadServer server = new DeadServer(child, stat.getMtime(), new String(data));
          result.add(server);
        }
      }
    } catch (Exception ex) {
      log.error(ex, ex);
    }
    return result;
  }
  
  public void delete(String server) {
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    try {
      zoo.recursiveDelete(path + "/" + server, NodeMissingPolicy.SKIP);
    } catch (Exception ex) {
      log.error(ex, ex);
    }
  }
  
  public void post(String server, String cause) {
    IZooReaderWriter zoo = ZooReaderWriter.getInstance();
    try {
      zoo.putPersistentData(path + "/" + server, cause.getBytes(), NodeExistsPolicy.SKIP);
    } catch (Exception ex) {
      log.error(ex, ex);
    }
  }
}

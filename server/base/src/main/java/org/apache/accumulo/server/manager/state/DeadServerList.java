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
package org.apache.accumulo.server.manager.state;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.manager.thrift.DeadServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeadServerList {

  private static final Logger log = LoggerFactory.getLogger(DeadServerList.class);

  private final String path;
  private final ZooReaderWriter zoo;

  public DeadServerList(ServerContext context) {
    this.path = context.getZooKeeperRoot() + Constants.ZDEADTSERVERS;
    zoo = context.getZooReaderWriter();
    try {
      context.getZooReaderWriter().mkdirs(path);
    } catch (Exception ex) {
      log.error("Unable to make parent directories of " + path, ex);
    }
  }

  public List<DeadServer> getList() {
    List<DeadServer> result = new ArrayList<>();
    try {
      List<String> children = zoo.getChildren(path);
      if (children != null) {
        for (String child : children) {
          Stat stat = new Stat();
          byte[] data;
          try {
            data = zoo.getData(path + "/" + child, stat);
          } catch (NoNodeException nne) {
            // Another thread or process can delete child while this loop is running.
            // We ignore this error since it's harmless if we miss the deleted server
            // in the dead server list.
            continue;
          }
          DeadServer server = new DeadServer(child, stat.getMtime(), new String(data, UTF_8));
          result.add(server);
        }
      }
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
    }
    return result;
  }

  public void delete(String server) {
    try {
      zoo.recursiveDelete(path + "/" + server, NodeMissingPolicy.SKIP);
    } catch (Exception ex) {
      log.error("delete failed with exception", ex);
    }
  }

  public void post(String server, String cause) {
    try {
      zoo.putPersistentData(path + "/" + server, cause.getBytes(UTF_8), NodeExistsPolicy.SKIP);
    } catch (Exception ex) {
      log.error("post failed with exception", ex);
    }
  }
}

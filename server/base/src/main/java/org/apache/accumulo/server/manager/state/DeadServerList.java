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
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.manager.thrift.DeadServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

public class DeadServerList {

  private static final Logger log = LoggerFactory.getLogger(DeadServerList.class);

  // ELASTICITY_TODO See if we can get the ResourceGroup from the Monitor
  // and replace the "UNKNOWN" value with the ResourceGroup
  private static final String RESOURCE_GROUP = "UNKNOWN";
  private final ServerContext ctx;
  private final String root;
  private final ZooReaderWriter zoo;
  private final String path;

  public DeadServerList(ServerContext context) {
    this.ctx = context;
    zoo = this.ctx.getZooReaderWriter();
    root = this.ctx.getZooKeeperRoot();

    this.path = root + Constants.ZDEADTSERVERS + "/" + RESOURCE_GROUP;
    try {
      ctx.getZooReaderWriter().mkdirs(path);
    } catch (Exception ex) {
      log.error("Unable to make parent directories of " + path, ex);
    }

  }

  public List<DeadServer> getList() {
    List<DeadServer> result = new ArrayList<>();
    try {
      Set<ServiceLockPath> deadServers =
          ctx.getServerPaths().getDeadTabletServer(Optional.empty(), Optional.empty(), false);
      for (ServiceLockPath path : deadServers) {
        Stat stat = new Stat();
        byte[] data;
        try {
          data = zoo.getData(path.toString(), stat);
        } catch (NoNodeException nne) {
          // Another thread or process can delete child while this loop is running.
          // We ignore this error since it's harmless if we miss the deleted server
          // in the dead server list.
          continue;
        }
        DeadServer server = new DeadServer(path.getServer(), stat.getMtime(),
            new String(data, UTF_8), path.getResourceGroup());
        result.add(server);
      }
    } catch (Exception ex) {
      log.error("{}", ex.getMessage(), ex);
    }
    return result;
  }

  public void delete(String server) {
    try {
      zoo.recursiveDelete(ctx.getServerPaths()
          .createDeadTabletServerPath(RESOURCE_GROUP, HostAndPort.fromString(server)).toString(),
          NodeMissingPolicy.SKIP);
    } catch (Exception ex) {
      log.error("delete failed with exception", ex);
    }
  }

  public void post(String server, String cause) {
    try {
      zoo.putPersistentData(ctx.getServerPaths()
          .createDeadTabletServerPath(RESOURCE_GROUP, HostAndPort.fromString(server)).toString(),
          cause.getBytes(UTF_8), NodeExistsPolicy.SKIP);
    } catch (Exception ex) {
      log.error("post failed with exception", ex);
    }
  }
}

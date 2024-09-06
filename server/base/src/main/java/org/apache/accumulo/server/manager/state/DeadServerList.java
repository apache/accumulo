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
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.manager.thrift.DeadServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

public class DeadServerList {

  private static final Logger log = LoggerFactory.getLogger(DeadServerList.class);

  private final ServerContext ctx;
  private final String path;
  private final ZooReaderWriter zoo;

  public DeadServerList(ServerContext context) {
    this.ctx = context;
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
      Set<ServiceLockPath> deadServers =
          ServiceLockPaths.getDeadTabletServer(ctx, Optional.empty(), Optional.empty());
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
      final HostAndPort hp = HostAndPort.fromString(server);
      final Set<ServiceLockPath> paths =
          ServiceLockPaths.getTabletServer(ctx, Optional.empty(), Optional.of(hp));
      Preconditions.checkArgument(paths.size() == 1,
          "Did not find a unique ZooKeeper path for server address: " + server);
      zoo.recursiveDelete(ServiceLockPaths
          .createDeadTabletServerPath(ctx, paths.iterator().next().getResourceGroup(), hp)
          .toString(), NodeMissingPolicy.SKIP);
    } catch (Exception ex) {
      log.error("delete failed with exception", ex);
    }
  }

  public void post(String server, String cause) {
    try {
      final HostAndPort hp = HostAndPort.fromString(server);
      final Set<ServiceLockPath> paths =
          ServiceLockPaths.getTabletServer(ctx, Optional.empty(), Optional.of(hp));
      Preconditions.checkArgument(paths.size() == 1,
          "Did not find a unique ZooKeeper path for server address: " + server);
      zoo.putPersistentData(ServiceLockPaths.createDeadTabletServerPath(ctx,
          paths.iterator().next().getResourceGroup(), HostAndPort.fromString(server)).toString(),
          cause.getBytes(UTF_8), NodeExistsPolicy.SKIP);
    } catch (Exception ex) {
      log.error("post failed with exception", ex);
    }
  }
}

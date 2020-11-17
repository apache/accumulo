/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.master.tserverOps;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.ZooLock;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownTServer extends MasterRepo {

  private static final long serialVersionUID = 2L;
  private static final Logger log = LoggerFactory.getLogger(ShutdownTServer.class);
  private final HostAndPort hostAndPort;
  private final String serverSession;
  private final boolean force;

  public ShutdownTServer(TServerInstance server, boolean force) {
    this.hostAndPort = server.getHostAndPort();
    this.serverSession = server.getSession();
    this.force = force;
  }

  @Override
  public long isReady(long tid, Master master) {
    TServerInstance server = new TServerInstance(hostAndPort, serverSession);
    // suppress assignment of tablets to the server
    if (force) {
      return 0;
    }

    // Inform the master that we want this server to shutdown
    master.shutdownTServer(server);

    if (master.onlineTabletServers().contains(server)) {
      TServerConnection connection = master.getConnection(server);
      if (connection != null) {
        try {
          TabletServerStatus status = connection.getTableMap(false);
          if (status.tableMap != null && status.tableMap.isEmpty()) {
            log.info("tablet server hosts no tablets {}", server);
            connection.halt(master.getMasterLock());
            log.info("tablet server asked to halt {}", server);
            return 0;
          }
        } catch (TTransportException ex) {
          // expected
        } catch (Exception ex) {
          log.error("Error talking to tablet server {}: ", server, ex);
        }

        // If the connection was non-null and we could communicate with it
        // give the master some more time to tell it to stop and for the
        // tserver to ack the request and stop itself.
        return 1000;
      }
    }

    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // suppress assignment of tablets to the server
    if (force) {
      ZooReaderWriter zoo = master.getContext().getZooReaderWriter();
      String path = master.getZooKeeperRoot() + Constants.ZTSERVERS + "/" + hostAndPort;
      ZooLock.deleteLock(zoo, path);
      path = master.getZooKeeperRoot() + Constants.ZDEADTSERVERS + "/" + hostAndPort;
      zoo.putPersistentData(path, "forced down".getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
    }

    return null;
  }

  @Override
  public void undo(long tid, Master m) {}
}

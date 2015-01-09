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
package org.apache.accumulo.master.tserverOps;

import static com.google.common.base.Charsets.UTF_8;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.master.thrift.TabletServerStatus;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.zookeeper.IZooReaderWriter;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.master.EventCoordinator.Listener;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.master.tableOps.MasterRepo;
import org.apache.accumulo.server.master.LiveTServerSet.TServerConnection;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.zookeeper.ZooLock;
import org.apache.accumulo.server.zookeeper.ZooReaderWriter;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

public class ShutdownTServer extends MasterRepo {

  private static final long serialVersionUID = 1L;
  private static final Logger log = Logger.getLogger(ShutdownTServer.class);
  private TServerInstance server;
  private boolean force;

  public ShutdownTServer(TServerInstance server, boolean force) {
    this.server = server;
    this.force = force;
  }

  @Override
  public long isReady(long tid, Master environment) throws Exception {
    return 0;
  }

  @Override
  public Repo<Master> call(long tid, Master master) throws Exception {
    // suppress assignment of tablets to the server
    if (force) {
      String path = ZooUtil.getRoot(master.getInstance()) + Constants.ZTSERVERS + "/" + server.getLocation();
      ZooLock.deleteLock(path);
      path = ZooUtil.getRoot(master.getInstance()) + Constants.ZDEADTSERVERS + "/" + server.getLocation();
      IZooReaderWriter zoo = ZooReaderWriter.getInstance();
      zoo.putPersistentData(path, "forced down".getBytes(UTF_8), NodeExistsPolicy.OVERWRITE);
      return null;
    }

    // TODO move this to isReady() and drop while loop? - ACCUMULO-1259
    Listener listener = master.getEventCoordinator().getListener();
    master.shutdownTServer(server);
    while (master.onlineTabletServers().contains(server)) {
      TServerConnection connection = master.getConnection(server);
      if (connection != null) {
        try {
          TabletServerStatus status = connection.getTableMap(false);
          if (status.tableMap != null && status.tableMap.isEmpty()) {
            log.info("tablet server hosts no tablets " + server);
            connection.halt(master.getMasterLock());
            log.info("tablet server asked to halt " + server);
            break;
          }
        } catch (TTransportException ex) {
          // expected
        } catch (Exception ex) {
          log.error("Error talking to tablet server " + server + ": " + ex);
        }
      }
      listener.waitForEvents(1000);
    }

    return null;
  }

  @Override
  public void undo(long tid, Master m) throws Exception {}
}

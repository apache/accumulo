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
package org.apache.accumulo.test.upgrade;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;

public class UpgradeIT extends AccumuloClusterHarness {

  private class ServerThatWontStart extends AbstractServer {

    protected ServerThatWontStart(String[] args) {
      super(ServerId.Type.TABLET_SERVER, new ConfigOpts(), (conf) -> getServerContext(), args);
    }

    @Override
    public void run() {}

    @Override
    public ServiceLock getLock() {
      return null;
    }

  }

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Test
  public void testServersWontStart() throws Exception {
    // Constants.ZPREPARE_FOR_UPGRADE is created by 'ZooZap -prepare-for-upgrade'
    // which is run when the user wants to shutdown an instance in preparation
    // to upgrade it. When this node exists, no servers should start. There is
    // no ability to create this node in ZooKeeper before MAC starts for this
    // test, so we will create the node after MAC starts, then try to restart
    // MAC.

    final ZooSession zs = getServerContext().getZooSession();
    final String zkRoot = getServerContext().getZooKeeperRoot();
    final ZooReaderWriter zrw = zs.asReaderWriter();
    final String upgradePath = zkRoot + Constants.ZPREPARE_FOR_UPGRADE;
    zrw.putPersistentData(upgradePath, new byte[0], NodeExistsPolicy.SKIP);

    getCluster().stop();

    getCluster().getClusterControl().startAllServers(ServerType.ZOOKEEPER);

    final ZooReader zr = zs.asReader();
    Wait.waitFor(() -> zr.getChildren(zkRoot + Constants.ZCOMPACTORS).isEmpty());
    Wait.waitFor(() -> zr.getChildren(zkRoot + Constants.ZGC_LOCK).isEmpty());
    Wait.waitFor(() -> zr.getChildren(zkRoot + Constants.ZMANAGER_LOCK).isEmpty());
    Wait.waitFor(() -> zr.getChildren(zkRoot + Constants.ZSSERVERS).isEmpty());
    Wait.waitFor(() -> zr.getChildren(zkRoot + Constants.ZTSERVERS).isEmpty());

    assertThrows(IllegalStateException.class,
        () -> assertTimeoutPreemptively(Duration.ofMinutes(2), () -> getCluster().start()));

    assertTrue(zr.getChildren(zkRoot + Constants.ZCOMPACTORS).isEmpty());
    assertTrue(zr.getChildren(zkRoot + Constants.ZGC_LOCK).isEmpty());
    assertTrue(zr.getChildren(zkRoot + Constants.ZMANAGER_LOCK).isEmpty());
    assertTrue(zr.getChildren(zkRoot + Constants.ZSSERVERS).isEmpty());
    assertTrue(zr.getChildren(zkRoot + Constants.ZTSERVERS).isEmpty());

    // Validate the exception from the servers
    List<String> args = new ArrayList<>();
    args.add("--props");
    args.add(getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new ServerThatWontStart(args.toArray(new String[0])));
    assertTrue(ise.getMessage()
        .startsWith("Instance has been prepared for upgrade, no servers can be started."));

  }

}

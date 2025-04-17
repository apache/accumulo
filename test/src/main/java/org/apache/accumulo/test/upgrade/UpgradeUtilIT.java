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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.UpgradeUtil;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;

public class UpgradeUtilIT extends AccumuloClusterHarness {

  @Test
  public void testPrepareFailsDueToManagerUp() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    ctx.getZooReaderWriter().putPersistentData(
        ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE, new byte[0],
        NodeExistsPolicy.SKIP);
    assertTrue(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--prepare"}));
    assertEquals("Manager is running, shut it down and retry this operation", ise.getMessage());
    assertFalse(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

  }

  @Test
  public void testPrepareFailsDueToFateTransactions() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    ctx.getZooReaderWriter().putPersistentData(
        ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE, new byte[0],
        NodeExistsPolicy.SKIP);
    assertTrue(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

    assertTrue(ctx.getZooReader().getChildren(ctx.getZooKeeperRoot() + Constants.ZFATE).isEmpty());
    ctx.getZooReaderWriter().putEphemeralData(
        ctx.getZooKeeperRoot() + Constants.ZFATE + "/" + UUID.randomUUID(), new byte[0]);
    assertFalse(ctx.getZooReader().getChildren(ctx.getZooKeeperRoot() + Constants.ZFATE).isEmpty());

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Wait.waitFor(() -> ctx.getZooReader()
        .getChildren(ctx.getZooKeeperRoot() + Constants.ZMANAGER_LOCK).isEmpty());

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--prepare"}));
    assertTrue(ise.getMessage()
        .startsWith("Cannot complete upgrade preparation because FATE transactions exist."));
    assertFalse(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

  }

  @Test
  public void testPrepareSucceeds() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    ctx.getZooReaderWriter().putPersistentData(
        ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE, new byte[0],
        NodeExistsPolicy.SKIP);
    assertTrue(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Wait.waitFor(() -> ctx.getZooReader()
        .getChildren(ctx.getZooKeeperRoot() + Constants.ZMANAGER_LOCK).isEmpty());

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    new UpgradeUtil().execute(new String[] {"--prepare"});
    assertTrue(ctx.getZooReader().exists(ctx.getZooKeeperRoot() + Constants.ZPREPARE_FOR_UPGRADE));

  }

}

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

import java.io.IOException;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerDirs;
import org.apache.accumulo.server.util.UpgradeUtil;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class UpgradeUtilIT extends AccumuloClusterHarness {

  private void downgradePersistentVersion(ServerContext context) throws IOException {
    ServerDirs serverDirs = context.getServerDirs();
    for (Volume volume : context.getVolumeManager().getVolumes()) {
      final FileSystem fs = volume.getFileSystem();
      final Path dataVersionLocation = serverDirs.getDataVersionLocation(volume);
      if (!fs.delete(dataVersionLocation, true)) {
        throw new RuntimeException(
            "Could not delete data version location (" + dataVersionLocation + ") for " + volume);
      }
      fs.create(new Path(dataVersionLocation, Integer.toString(AccumuloDataVersion.get() - 1)))
          .close();
    }
  }

  @Test
  public void testPrepareFailsDueToManagerUp() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    ZooReader zr = ctx.getZooSession().asReader();
    ZooReaderWriter zrw = ctx.getZooSession().asReaderWriter();

    zrw.putPersistentData(Constants.ZPREPARE_FOR_UPGRADE, new byte[0], NodeExistsPolicy.SKIP);
    assertTrue(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--prepare"}));
    assertEquals("Manager is running, shut it down and retry this operation", ise.getMessage());
    assertFalse(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));
  }

  @Test
  public void testPrepareFailsDueToFateTransactions() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    ZooReader zr = ctx.getZooSession().asReader();
    ZooReaderWriter zrw = ctx.getZooSession().asReaderWriter();

    zrw.putPersistentData(Constants.ZPREPARE_FOR_UPGRADE, new byte[0], NodeExistsPolicy.SKIP);
    assertTrue(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));

    assertTrue(zr.getChildren(Constants.ZFATE).isEmpty());
    zrw.putEphemeralData(Constants.ZFATE + "/" + UUID.randomUUID(), new byte[0]);
    assertFalse(zr.getChildren(Constants.ZFATE).isEmpty());

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Wait.waitFor(() -> zr.getChildren(Constants.ZMANAGER_LOCK).isEmpty());

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--prepare"}));
    assertTrue(ise.getMessage()
        .startsWith("Cannot complete upgrade preparation because FATE transactions exist."));
    assertFalse(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));
  }

  @Test
  public void testPrepareSucceeds() throws Exception {
    ServerContext ctx = getCluster().getServerContext();
    ZooReader zr = ctx.getZooSession().asReader();
    ZooReaderWriter zrw = ctx.getZooSession().asReaderWriter();

    zrw.putPersistentData(Constants.ZPREPARE_FOR_UPGRADE, new byte[0], NodeExistsPolicy.SKIP);
    assertTrue(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));

    getCluster().getClusterControl().stopAllServers(ServerType.MANAGER);
    Wait.waitFor(() -> zr.getChildren(Constants.ZMANAGER_LOCK).isEmpty());

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    new UpgradeUtil().execute(new String[] {"--prepare"});
    assertTrue(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));
  }

  @Test
  public void testExclusiveOptionsFail() {
    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalArgumentException ise = assertThrows(IllegalArgumentException.class,
        () -> new UpgradeUtil().execute(new String[] {"--prepare", "--check"}));
    assertTrue(ise.getMessage().equals("prepare and check options are mutually exclusive"));
  }

  @Test
  public void testCheckFailsNotNeeded() throws Exception {
    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--check"}));
    assertTrue(ise.getMessage().startsWith("Running this utility is unnecessary"));
  }

  @Test
  public void testCheckFailsPrepareMissing() throws Exception {

    downgradePersistentVersion(getServerContext());

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--check"}));
    assertTrue(ise.getMessage()
        .startsWith(Constants.ZPREPARE_FOR_UPGRADE + " node not found in ZooKeeper"));
  }

  @Test
  public void testCheckFailsDueToFateTransactions() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    downgradePersistentVersion(ctx);

    ZooReader zr = ctx.getZooSession().asReader();
    ZooReaderWriter zrw = ctx.getZooSession().asReaderWriter();

    assertFalse(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));

    assertTrue(zr.getChildren(Constants.ZFATE).isEmpty());
    zrw.putEphemeralData(Constants.ZFATE + "/" + UUID.randomUUID(), new byte[0]);
    assertFalse(zr.getChildren(Constants.ZFATE).isEmpty());

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    IllegalStateException ise = assertThrows(IllegalStateException.class,
        () -> new UpgradeUtil().execute(new String[] {"--check", "--force"}));
    assertTrue(ise.getMessage()
        .startsWith("Cannot continue pre-upgrade checks because FATE transactions exist."));
    assertFalse(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));
  }

  @Test
  public void testCheckSucceedsWithPrepare() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    downgradePersistentVersion(ctx);

    ZooReader zr = ctx.getZooSession().asReader();
    ZooReaderWriter zrw = ctx.getZooSession().asReaderWriter();

    zrw.putPersistentData(Constants.ZPREPARE_FOR_UPGRADE, new byte[0], NodeExistsPolicy.SKIP);
    assertTrue(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));

    // The prepare node exists in ZooKeeper, so checks for Fate transactions
    // won't be done and server locks won't be deleted.

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    new UpgradeUtil().execute(new String[] {"--check"});
    assertFalse(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));
    assertTrue(zr.exists(Constants.ZUPGRADE_PROGRESS));
  }

  @Test
  public void testCheckSucceedsWithoutPrepare() throws Exception {
    ServerContext ctx = getCluster().getServerContext();

    downgradePersistentVersion(ctx);

    ZooReader zr = ctx.getZooSession().asReader();

    assertFalse(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));

    // The prepare node exists in ZooKeeper, checks for Fate transactions
    // will be done and server locks will be deleted.

    System.setProperty("accumulo.properties", "file://" + getCluster().getAccumuloPropertiesPath());
    new UpgradeUtil().execute(new String[] {"--check", "--force"});
    assertFalse(zr.exists(Constants.ZPREPARE_FOR_UPGRADE));
    assertTrue(zr.exists(Constants.ZUPGRADE_PROGRESS));
  }

}

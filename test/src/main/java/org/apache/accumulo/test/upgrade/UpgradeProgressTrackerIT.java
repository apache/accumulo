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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.util.LazySingletons.GSON;
import static org.apache.accumulo.harness.AccumuloITBase.ZOOKEEPER_TESTING_SERVER;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.core.volume.VolumeImpl;
import org.apache.accumulo.core.zookeeper.ZooSession;
import org.apache.accumulo.manager.upgrade.UpgradeProgressTracker;
import org.apache.accumulo.manager.upgrade.UpgradeProgressTracker.UpgradeProgress;
import org.apache.accumulo.server.AccumuloDataVersion;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.ServerDirs;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(ZOOKEEPER_TESTING_SERVER)
public class UpgradeProgressTrackerIT {

  @TempDir
  private static File tempDir;

  private static final String zRoot = "/accumulo/" + UUID.randomUUID().toString();

  private static ZooKeeperTestingServer testZk = null;
  private static ZooSession zk = null;
  private static Volume volume;

  @BeforeAll
  public static void setup() throws Exception {
    testZk = new ZooKeeperTestingServer(tempDir);
    zk = testZk.newClient();
    zk.asReaderWriter().mkdirs(zRoot);
    volume = new VolumeImpl(new Path(tempDir.toURI()), new Configuration());
  }

  @AfterAll
  public static void teardown() throws Exception {
    try {
      zk.close();
    } finally {
      testZk.close();
    }
  }

  private ServerContext ctx = createMock(ServerContext.class);
  private ServerDirs sd = createMock(ServerDirs.class);
  private VolumeManager vm = createMock(VolumeManagerImpl.class);

  @BeforeEach
  public void beforeTest() {
    reset(ctx, sd, vm);
    expect(ctx.getZooKeeperRoot()).andReturn(zRoot).anyTimes();
    expect(ctx.getZooSession()).andReturn(zk).anyTimes();
    expect(ctx.getServerDirs()).andReturn(sd).anyTimes();
    expect(ctx.getVolumeManager()).andReturn(vm).anyTimes();
    expect(vm.getFirst()).andReturn(volume).anyTimes();
  }

  @AfterEach
  public void afterTest() throws KeeperException, InterruptedException {
    verify(ctx, sd, vm);
    zk.asReaderWriter().recursiveDelete(zRoot + Constants.ZUPGRADE_PROGRESS,
        NodeMissingPolicy.SKIP);
  }

  @Test
  public void testUpgradeAlreadyStarted() throws KeeperException, InterruptedException {
    expect(sd.getAccumuloPersistentVersion(volume)).andReturn(AccumuloDataVersion.get()).anyTimes();
    replay(ctx, sd, vm);
    assertFalse(UpgradeProgressTracker.upgradeInProgress(ctx));
    UpgradeProgress progress =
        new UpgradeProgress(AccumuloDataVersion.get() - 2, AccumuloDataVersion.get() - 1);
    zk.create(zRoot + Constants.ZUPGRADE_PROGRESS, GSON.get().toJson(progress).getBytes(UTF_8),
        ZooUtil.PUBLIC, CreateMode.PERSISTENT);
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    IllegalStateException ise =
        assertThrows(IllegalStateException.class, () -> UpgradeProgressTracker.get(ctx));
    assertTrue(ise.getMessage()
        .startsWith("Upgrade was already started with a different version of software"));
  }

  @Test
  public void testGetInitial() throws KeeperException, InterruptedException {
    expect(sd.getAccumuloPersistentVersion(volume)).andReturn(AccumuloDataVersion.get()).anyTimes();
    replay(ctx, sd, vm);
    assertFalse(UpgradeProgressTracker.upgradeInProgress(ctx));
    UpgradeProgress cv = UpgradeProgressTracker.get(ctx);
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    assertNotNull(cv);
    assertEquals(AccumuloDataVersion.get(), cv.getZooKeeperVersion());
    assertEquals(AccumuloDataVersion.get(), cv.getRootVersion());
    assertEquals(AccumuloDataVersion.get(), cv.getMetadataVersion());
    byte[] serialized = zk.asReader().getData(zRoot + Constants.ZUPGRADE_PROGRESS);
    assertArrayEquals(GSON.get().toJson(cv).getBytes(UTF_8), serialized);
  }

  @Test
  public void testUpdates() throws KeeperException, InterruptedException {
    expect(sd.getAccumuloPersistentVersion(volume)).andReturn(AccumuloDataVersion.get() - 1)
        .anyTimes();
    replay(ctx, sd, vm);
    assertFalse(UpgradeProgressTracker.upgradeInProgress(ctx));
    final UpgradeProgress cv = UpgradeProgressTracker.get(ctx);
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    assertNotNull(cv);
    assertEquals(AccumuloDataVersion.get() - 1, cv.getZooKeeperVersion());
    assertEquals(AccumuloDataVersion.get() - 1, cv.getRootVersion());
    assertEquals(AccumuloDataVersion.get() - 1, cv.getMetadataVersion());
    byte[] serialized = zk.asReader().getData(zRoot + Constants.ZUPGRADE_PROGRESS);
    assertArrayEquals(GSON.get().toJson(cv).getBytes(UTF_8), serialized);

    // Test updating out of order
    assertThrows(IllegalArgumentException.class,
        () -> cv.updateMetadataVersion(ctx, AccumuloDataVersion.get()));
    assertThrows(IllegalArgumentException.class,
        () -> cv.updateRootVersion(ctx, AccumuloDataVersion.get()));
    serialized = zk.asReader().getData(zRoot + Constants.ZUPGRADE_PROGRESS);
    assertArrayEquals(GSON.get().toJson(cv).getBytes(UTF_8), serialized);

    cv.updateZooKeeperVersion(ctx, AccumuloDataVersion.get());
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    UpgradeProgress cv2 = UpgradeProgressTracker.get(ctx);
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    assertNotNull(cv2);
    assertEquals(AccumuloDataVersion.get(), cv2.getZooKeeperVersion());
    assertEquals(AccumuloDataVersion.get() - 1, cv2.getRootVersion());
    assertEquals(AccumuloDataVersion.get() - 1, cv2.getMetadataVersion());
    serialized = zk.asReader().getData(zRoot + Constants.ZUPGRADE_PROGRESS);
    assertArrayEquals(GSON.get().toJson(cv2).getBytes(UTF_8), serialized);

    cv2.updateRootVersion(ctx, AccumuloDataVersion.get());
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    cv2 = UpgradeProgressTracker.get(ctx);
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    assertNotNull(cv2);
    assertEquals(AccumuloDataVersion.get(), cv2.getZooKeeperVersion());
    assertEquals(AccumuloDataVersion.get(), cv2.getRootVersion());
    assertEquals(AccumuloDataVersion.get() - 1, cv2.getMetadataVersion());
    serialized = zk.asReader().getData(zRoot + Constants.ZUPGRADE_PROGRESS);
    assertArrayEquals(GSON.get().toJson(cv2).getBytes(UTF_8), serialized);

    cv2.updateMetadataVersion(ctx, AccumuloDataVersion.get());
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    cv2 = UpgradeProgressTracker.get(ctx);
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    assertNotNull(cv2);
    assertEquals(AccumuloDataVersion.get(), cv2.getZooKeeperVersion());
    assertEquals(AccumuloDataVersion.get(), cv2.getRootVersion());
    assertEquals(AccumuloDataVersion.get(), cv2.getMetadataVersion());
    serialized = zk.asReader().getData(zRoot + Constants.ZUPGRADE_PROGRESS);
    assertArrayEquals(GSON.get().toJson(cv2).getBytes(UTF_8), serialized);
  }

  @Test
  public void testCompleteUpgrade() throws KeeperException, InterruptedException {
    expect(sd.getAccumuloPersistentVersion(volume)).andReturn(AccumuloDataVersion.get()).anyTimes();
    replay(ctx, sd, vm);
    assertFalse(UpgradeProgressTracker.upgradeInProgress(ctx));
    final UpgradeProgress cv = UpgradeProgressTracker.get(ctx);
    assertTrue(UpgradeProgressTracker.upgradeInProgress(ctx));
    assertNotNull(cv);
    assertEquals(AccumuloDataVersion.get(), cv.getZooKeeperVersion());
    assertEquals(AccumuloDataVersion.get(), cv.getRootVersion());
    assertEquals(AccumuloDataVersion.get(), cv.getMetadataVersion());
    byte[] serialized = zk.asReader().getData(zRoot + Constants.ZUPGRADE_PROGRESS);
    assertArrayEquals(GSON.get().toJson(cv).getBytes(UTF_8), serialized);

    UpgradeProgressTracker.upgradeComplete(ctx);
    assertFalse(zk.asReader().exists(zRoot + Constants.ZUPGRADE_PROGRESS));
    assertFalse(UpgradeProgressTracker.upgradeInProgress(ctx));
  }

}

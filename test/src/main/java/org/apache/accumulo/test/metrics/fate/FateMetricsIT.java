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
package org.apache.accumulo.test.metrics.fate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.List;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.metrics.MicrometerMetricsFactory;
import org.apache.accumulo.fate.ReadOnlyTStore;
import org.apache.accumulo.fate.Repo;
import org.apache.accumulo.fate.ZooStore;
import org.apache.accumulo.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.metrics.fate.FateMetrics;
import org.apache.accumulo.manager.tableOps.ManagerRepo;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.categories.ZooKeeperTestingServerTests;
import org.apache.accumulo.test.zookeeper.ZooKeeperTestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Test FATE metrics using stubs and in-memory version of supporting infrastructure - a test
 * zookeeper server is used an the FATE repos are stubs, but this should represent the metrics
 * collection execution without needed to stand up a mini cluster to exercise these execution paths.
 */
@Category({ZooKeeperTestingServerTests.class})
public class FateMetricsIT {

  public static final String INSTANCE_ID = UUID.randomUUID().toString();
  public static final String MOCK_ZK_ROOT = "/accumulo/" + INSTANCE_ID;
  public static final String A_FAKE_SECRET = "aPasswd";
  private static final Logger log = LoggerFactory.getLogger(FateMetricsIT.class);
  private static ZooKeeperTestingServer szk = null;
  private ZooStore<Manager> zooStore = null;
  private ZooKeeper zookeeper = null;
  private ServerContext context = null;
  private Manager manager;
  private final SimpleMeterRegistry testRegistry = new SimpleMeterRegistry();

  @BeforeClass
  public static void setupZk() {
    // using default zookeeper port - we don't have a full configuration
    szk = new ZooKeeperTestingServer();
    szk.initPaths(MOCK_ZK_ROOT);
  }

  @AfterClass
  public static void shutdownZK() throws Exception {
    szk.close();
  }

  /**
   * Instantiate a test zookeeper and setup mocks for Manager and Context. The test zookeeper is
   * used create a ZooReaderWriter. The zookeeper used in tests needs to be the one from the
   * zooReaderWriter, not the test server, because the zooReaderWriter sets up ACLs.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Before
  public void init() throws Exception {

    ZooReaderWriter zooReaderWriter = new ZooReaderWriter(szk.getConn(), 10_0000, A_FAKE_SECRET);
    zookeeper = zooReaderWriter.getZooKeeper();

    clear(MOCK_ZK_ROOT);

    zooStore = new ZooStore<>(MOCK_ZK_ROOT + Constants.ZFATE, zooReaderWriter);

    manager = EasyMock.createMock(Manager.class);
    context = EasyMock.createMock(ServerContext.class);

    MicrometerMetricsFactory mmFactory = EasyMock.createMock(MicrometerMetricsFactory.class);
    EasyMock.expect(context.getZooReaderWriter()).andReturn(zooReaderWriter).anyTimes();
    EasyMock.expect(context.getZooKeeperRoot()).andReturn(MOCK_ZK_ROOT).anyTimes();

    AccumuloConfiguration conf = EasyMock.mock(AccumuloConfiguration.class);

    EasyMock.expect(context.getConfiguration()).andReturn(conf);
    EasyMock.replay(manager, context, mmFactory);
  }

  @After
  public void cleanup() throws Exception {
    clear(MOCK_ZK_ROOT);
  }

  /**
   * Validate that the expected metrics values are present in the metrics collector output
   */
  @Test
  public void micrometerNoFates() {

    FateMetrics metrics = new FateMetrics(context, 10);
    log.trace("fate metrics: {}", metrics);

    Gauge currentFateOps = testRegistry.find("fate.status").tag("status", "currentFateOps").gauge();
    assertNotNull(currentFateOps);
    assertNotNull(testRegistry.find("fate.status").tag("status", "zkChildFateOpsTotal").gauge());
    assertNotNull(
        testRegistry.find("fate.status").tag("status", "zkConnectionErrorsTotal").gauge());

    // Transaction STATES - defined by TStatus.
    assertNotNull(testRegistry.find("fate.status").tag("status", "FateTxState_NEW").gauge());
    Gauge inProgress =
        testRegistry.find("fate.status").tag("status", "FateTxState_IN_PROGRESS").gauge();
    assertNotNull(inProgress);
    assertNotNull(
        testRegistry.find("fate.status").tag("status", "FateTxState_FAILED_IN_PROGRESS").gauge());
    assertNotNull(testRegistry.find("fate.status").tag("status", "FateTxState_FAILED").gauge());
    assertNotNull(testRegistry.find("fate.status").tag("status", "FateTxState_SUCCESSFUL").gauge());
    assertNotNull(testRegistry.find("fate.status").tag("status", "FateTxState_UNKNOWN").gauge());

    // metrics derived from operation types when see - none should have been seen.
    assertNull(testRegistry.find("fate.status").tag("status", "FateTxOpType_FakeOp").gauge());

    assertEquals(0, inProgress.value(), 0.0);
    assertEquals(0, currentFateOps.value(), 0.0);

    EasyMock.verify(manager);
  }

  /**
   * Seed a fake FAKE fate that has a reserved transaction id. This sets a "new" status, but the
   * repo and debug props are not yet set. Verify the the metric collection handles partial
   * transaction states.
   */
  @Test
  public void micrometerFateNewStatus() {

    long tx1Id = zooStore.create();
    log.debug("ZooStore tx1 id {}", tx1Id);

    FateMetrics metrics = new FateMetrics(context, 10);
    log.trace("fate metrics: {}", metrics);

    Gauge fateTxStateNew =
        testRegistry.find("fate.status").tag("status", "FateTxState_NEW").gauge();
    assertNotNull(fateTxStateNew);
    assertEquals(1, fateTxStateNew.value(), 0.0);

    Gauge currentFateOps = testRegistry.find("fate.status").tag("status", "currentFateOps").gauge();
    assertNotNull(currentFateOps);
    assertEquals(1, currentFateOps.value(), 0.0);

    Gauge fateTxStateInProgress =
        testRegistry.find("fate.status").tag("status", "FateTxState_IN_PROGRESS").gauge();
    assertNotNull(fateTxStateInProgress);
    assertEquals(0, fateTxStateInProgress.value(), 0.0);

    EasyMock.verify(manager);
  }

  /**
   * Seeds the zoo store with a "fake" repo operation with a step, and sets the prop_debug field.
   * This emulates the actions performed with {@link org.apache.accumulo.fate.Fate} for what is
   * expected in zookeeper / the zoo store for an IN_PROGRESS transaction.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void micrometerOneInProgress() throws Exception {

    long tx1Id = seedTransaction();

    log.debug("FATE tx: {}", prettyStat(
        zookeeper.exists(MOCK_ZK_ROOT + "/fate/" + String.format("tx_%016x", tx1Id), false)));

    FateMetrics metrics = new FateMetrics(context, 10);
    log.trace("fate metrics: {}", metrics);

    Gauge inProgress =
        testRegistry.find("fate.status").tag("status", "FateTxState_IN_PROGRESS").gauge();
    assertNotNull(inProgress);
    assertEquals(1, inProgress.value(), 0.0);

    Gauge fakeOp = testRegistry.find("fate.status").tag("status", "FateTxOpType_FakeOp").gauge();
    assertNotNull(fakeOp);
    assertEquals(1, fakeOp.value(), 0.0);

    EasyMock.verify(manager);
  }

  private long seedTransaction() throws Exception {

    long txId = zooStore.create();
    zooStore.reserve(txId);

    zooStore.setStatus(txId, ReadOnlyTStore.TStatus.IN_PROGRESS);

    Repo<Manager> repo = new FakeOp();
    zooStore.push(txId, repo);

    Repo<Manager> step = new FakeOpStep1();
    zooStore.push(txId, step);

    zooStore.setProperty(txId, "debug", repo.getDescription());

    zooStore.unreserve(txId, 50);

    return txId;
  }

  /**
   * builds on the "in progress" transaction - when a transaction completes, the op type metric
   * should not reflect the previous operation that was "in progress".
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void micrometerTypeClears() throws Exception {
    initTypeClears();

    FateMetrics metrics = new FateMetrics(context, 10);
    log.trace("fate metrics: {}", metrics);

    Gauge inProgress =
        testRegistry.find("fate.status").tag("status", "FateTxState_IN_PROGRESS").gauge();
    assertNotNull(inProgress);
    assertEquals(0, inProgress.value(), 0.0);

    Gauge successful =
        testRegistry.find("fate.status").tag("status", "FateTxState_SUCCESSFUL").gauge();
    assertNotNull(successful);
    assertEquals(1, successful.value(), 0.0);

    assertNull(testRegistry.find("fate.status").tag("status", "FateTxOpType_FakeOp").gauge());
  }

  private void initTypeClears() throws Exception {
    long txId = seedTransaction();

    zooStore.reserve(txId);
    zooStore.setStatus(txId, ReadOnlyTStore.TStatus.SUCCESSFUL);
    zooStore.unreserve(txId, 50);
  }

  String prettyStat(final Stat stat) {

    if (stat == null) {
      return "{Stat:[null]}";
    }

    return "{Stat:[" + "czxid:" + stat.getCzxid() + ", mzxid:" + stat.getMzxid() + ", ctime: "
        + stat.getCtime() + ", mtime: " + stat.getMtime() + ", version: " + stat.getVersion()
        + ", cversion: " + stat.getCversion() + ", aversion: " + stat.getAversion()
        + ", eph owner: " + stat.getEphemeralOwner() + ", dataLength: " + stat.getDataLength()
        + ", numChildren: " + stat.getNumChildren() + ", pzxid: " + stat.getPzxid() + "}";

  }

  private void clear(String path) throws Exception {

    log.debug("clean up - search: {}", path);

    List<String> children = zookeeper.getChildren(path, false);

    if (children.isEmpty()) {
      log.debug("clean up - delete path: {}", path);

      if (!path.equals(MOCK_ZK_ROOT)) {
        zookeeper.delete(path, -1);
      }
      return;
    }

    for (String cp : children) {
      clear(path + "/" + cp);
    }
  }

  private static class FakeOp extends ManagerRepo {
    private static final long serialVersionUID = -1L;

    @Override
    public Repo<Manager> call(long tid, Manager environment) {
      return null;
    }
  }

  private static class FakeOpStep1 extends ManagerRepo {
    private static final long serialVersionUID = -1L;

    @Override
    public Repo<Manager> call(long tid, Manager environment) {
      return null;
    }
  }
}

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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.accumulo.core.Constants;
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
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Test FATE metrics using stubs and in-memory version of supporting infrastructure - a test
 * zookeeper server is used an the FATE repos are stubs, but this should represent the metrics
 * collection execution without needed to stand up a mini cluster to exercise these execution paths.
 */
@Category({ZooKeeperTestingServerTests.class})
public class FateMetricsIT {

  public static final String INSTANCE_ID = "1234";
  public static final String MOCK_ZK_ROOT = "/accumulo/" + INSTANCE_ID;
  public static final String A_FAKE_SECRET = "aPasswd";
  private static final Logger log = LoggerFactory.getLogger(FateMetricsIT.class);
  private static ZooKeeperTestingServer szk = null;
  private ZooStore<Manager> zooStore = null;
  private ZooKeeper zookeeper = null;
  private ServerContext context = null;
  private Manager manager;

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

    EasyMock.expect(context.getZooReaderWriter()).andReturn(zooReaderWriter).anyTimes();
    EasyMock.expect(context.getZooKeeperRoot()).andReturn(MOCK_ZK_ROOT).anyTimes();

    EasyMock.replay(manager, context);
  }

  @After
  public void cleanup() throws Exception {
    clear(MOCK_ZK_ROOT);
  }

  /**
   * Validate that the expected metrics values are present in the metrics collector output
   */
  @Test
  public void noFates() {

    FateMetrics metrics = new FateMetrics(context, 10);
    metrics.overrideRefresh(0);
    /*
     * InMemTestCollector collector = new InMemTestCollector(); metrics.getMetrics(collector, true);
     */
    MeterRegistry registry = metrics.getRegistry1();

    // might be better to make a helper function somewhere such as '.containsMeter(String name)'
    // instead of doing it like this.
    // MetricsRegistry.find() return null if non-existent where .get() errors out

    /*
     * assertTrue(collector.contains("currentFateOps"));
     * assertTrue(collector.contains("totalFateOps"));
     * assertTrue(collector.contains("totalZkConnErrors"));
     */
    assertNotNull(registry.find("current.fate.ops").gauge());
    assertNotNull(registry.find("total.fate.ops").gauge());
    assertNotNull(registry.find("total.zk.conn.errors").gauge());

    // Transaction STATES - defined by TStatus.
    /*
     * assertTrue(collector.contains("FateTxState_NEW"));
     * assertTrue(collector.contains("FateTxState_IN_PROGRESS"));
     * assertTrue(collector.contains("FateTxState_FAILED_IN_PROGRESS"));
     * assertTrue(collector.contains("FateTxState_FAILED"));
     * assertTrue(collector.contains("FateTxState_SUCCESSFUL"));
     * assertTrue(collector.contains("FateTxState_UNKNOWN"));
     */
    assertNotNull(registry.find("fate.tx.state.NEW").gauge());
    assertNotNull(registry.find("fate.tx.state.IN_PROGRESS").gauge());
    assertNotNull(registry.find("fate.tx.state.FAILED_IN_PROGRESS").gauge());
    assertNotNull(registry.find("fate.tx.state.FAILED").gauge());
    assertNotNull(registry.find("fate.tx.state.SUCCESSFUL").gauge());
    assertNotNull(registry.find("fate.tx.state.UNKNOWN").gauge());

    // metrics derived from operation types when see - none should have been seen.
    // assertFalse(collector.contains("FateTxOpType_FakeOp"));
    assertNull(registry.find("fate.tx.op.type.FakeOp").gauge());
    /*
     * assertEquals(0L, collector.getValue("FateTxState_IN_PROGRESS")); assertEquals(0L,
     * collector.getValue("currentFateOps"));
     */
    assertTrue(0L == registry.get("fate.tx.state.IN_PROGRESS").gauge().value());
    assertTrue(0L == registry.get("current.fate.ops").gauge().value());

    EasyMock.verify(manager);

  }

  /**
   * Seed a fake FAKE fate that has a reserved transaction id. This sets a "new" status, but the
   * repo and debug props are not yet set. Verify the the metric collection handles partial
   * transaction states.
   */
  @Test
  public void fateNewStatus() {

    long tx1Id = zooStore.create();
    log.debug("ZooStore tx1 id {}", tx1Id);

    FateMetrics metrics = new FateMetrics(context, 10);
    metrics.overrideRefresh(0);

    // InMemTestCollector collector = new InMemTestCollector();
    MeterRegistry registry = metrics.getRegistry1();

    // metrics.getMetrics(collector, true);

    // log.debug("Collector: {}", collector);
    log.debug("Registry: {}", registry.toString());
    /*
     * assertTrue(collector.contains("FateTxState_NEW")); assertEquals(1L,
     * collector.getValue("FateTxState_NEW")); assertEquals(1L,
     * collector.getValue("currentFateOps"));
     */
    Gauge fateTxStateNewGauge = registry.find("fate.tx.state.NEW").gauge();
    assertNotNull(fateTxStateNewGauge);
    assertTrue(1L == fateTxStateNewGauge.value());

    Gauge currentFateOpsGauge = registry.find("fate.tx.state.NEW").gauge();
    assertNotNull(currentFateOpsGauge);
    assertTrue(1L == currentFateOpsGauge.value());

    /*
     * assertTrue(collector.contains("FateTxState_IN_PROGRESS")); assertEquals(0L,
     * collector.getValue("FateTxState_IN_PROGRESS"));
     */
    Gauge fateStateInProgressGauge = registry.find("fate.tx.state.IN_PROGRESS").gauge();
    assertNotNull(fateStateInProgressGauge);
    assertTrue(0L == fateStateInProgressGauge.value());

    EasyMock.verify(manager);
  }
  /*
   * @Test public void testingChamber() { //Set<MeterRegistry> registries1 =null; FateMetrics fate =
   * new FateMetrics(context, 10); Set<MeterRegistry> registries =
   * Metrics.globalRegistry.getRegistries();
   * 
   * for(MeterRegistry m : registries) { Gauge g = m.find("current.fate.ops").gauge();
   * log.debug("GAUGE: {}", g.getId()); log.debug("Registry: {}", m.toString()); for(Meter n :
   * m.getMeters()) { Meter.Id temp = n.getId();
   * log.debug("Name: {} Tags: {} Description: {} ID: {}",temp.getName(),
   * temp.getTags(),temp.getDescription(),temp); for(Measurement o : n.measure()) {
   * log.debug(o.toString()); } } } }
   * 
   * @Test public void testingChamber2() { FateMetrics fate = new FateMetrics(context, 10);
   * MeterRegistry registry = fate.getRegistry1(); log.debug("Cirrent: {}",
   * registry.get("current.fate.ops").gauge().getId().getName()); }
   */

  /**
   * Seeds the zoo store with a "fake" repo operation with a step, and sets the prop_debug field.
   * This emulates the actions performed with {@link org.apache.accumulo.fate.Fate} for what is
   * expected in zookeeper / the zoo store for an IN_PROGRESS transaction.
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void oneInProgress() throws Exception {

    long tx1Id = seedTransaction();

    log.debug("FATE tx: {}", prettyStat(
        zookeeper.exists(MOCK_ZK_ROOT + "/fate/" + String.format("tx_%016x", tx1Id), false)));

    FateMetrics metrics = new FateMetrics(context, 10);
    metrics.overrideRefresh(0);

    // InMemTestCollector collector = new InMemTestCollector();
    // metrics.getMetrics(collector, true);
    // log.debug("Collector: {}", collector);

    MeterRegistry registry = metrics.getRegistry1();
    /*
     * assertTrue(collector.contains("FateTxState_IN_PROGRESS")); assertEquals(1L,
     * collector.getValue("FateTxState_IN_PROGRESS")); assertEquals(1L,
     * collector.getValue("FateTxOpType_FakeOp"));
     */

    Gauge inProgressGauge = registry.get("fate.tx.state.IN_PROGRESS").gauge();
    assertNotNull(inProgressGauge);
    assertTrue(1L == inProgressGauge.value());

    Gauge fakeOpGauge = registry.get("fate.tx.op.type.FAKEOP").gauge();
    assertNotNull(fakeOpGauge);
    assertTrue(1L == fakeOpGauge.value());

    EasyMock.verify(manager);
  }

  /**
   * builds on the "in progress" transaction - when a transaction completes, the op type metric
   * should not reflect the previous operation that was "in progress".
   *
   * @throws Exception
   *           any exception is a test failure.
   */
  @Test
  public void typeClears() throws Exception {
    long txId = seedTransaction();

    zooStore.reserve(txId);
    zooStore.setStatus(txId, ReadOnlyTStore.TStatus.SUCCESSFUL);
    zooStore.unreserve(txId, 50);

    FateMetrics metrics = new FateMetrics(context, 10);
    metrics.overrideRefresh(0);

    // InMemTestCollector collector = new InMemTestCollector();
    // metrics.getMetrics(collector, true);

    MeterRegistry registry = metrics.getRegistry1();
    /*
     * assertEquals(0L, collector.getValue("FateTxState_IN_PROGRESS")); assertEquals(1L,
     * collector.getValue("FateTxState_SUCCESSFUL"));
     * assertNull(collector.getValue("FateTxOpType_FakeOp"));
     */

    assertTrue(0L == registry.find("fate.tx.state.IN_PROGRESS").gauge().value());
    assertTrue(1L == registry.find("fate.tx.state.SUCCESSFUL").gauge().value());
    assertNull(registry.find("fate.tx.op.type.FAKEOP").gauge());

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

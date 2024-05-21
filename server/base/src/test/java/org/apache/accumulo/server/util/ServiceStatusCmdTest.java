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
package org.apache.accumulo.server.util;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.Constants.ZGC_LOCK;
import static org.apache.accumulo.server.util.ServiceStatusCmd.NO_GROUP_TAG;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.fate.zookeeper.ZooReader;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.util.serviceStatus.ServiceStatusReport;
import org.apache.accumulo.server.util.serviceStatus.StatusSummary;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceStatusCmdTest {

  private static final Logger LOG = LoggerFactory.getLogger(ServiceStatusCmdTest.class);

  private ServerContext context;
  private String zRoot;
  private ZooReader zooReader;

  @BeforeEach
  public void populateContext() {
    InstanceId iid = InstanceId.of(UUID.randomUUID());
    zRoot = "/accumulo/" + iid.canonical();
    context = createMock(ServerContext.class);
    expect(context.getInstanceID()).andReturn(iid).anyTimes();
    expect(context.getZooKeeperRoot()).andReturn(zRoot).anyTimes();

    zooReader = createMock(ZooReader.class);

    expect(context.getZooReader()).andReturn(zooReader).anyTimes();

    replay(context);
  }

  @AfterEach
  public void validateMocks() {
    verify(context, zooReader);
  }

  @Test
  void testManagerHosts() throws Exception {
    String lock1Name = "zlock#" + UUID.randomUUID() + "#0000000001";
    String lock2Name = "zlock#" + UUID.randomUUID() + "#0000000002";
    String lock3Name = "zlock#" + UUID.randomUUID() + "#0000000003";

    String host1 = "hostA:8080";
    String host2 = "hostB:9090";
    String host3 = "host1:9091";

    String lockPath = zRoot + Constants.ZMANAGER_LOCK;
    expect(zooReader.getChildren(eq(lockPath))).andReturn(List.of(lock1Name, lock2Name, lock3Name))
        .anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock1Name))).andReturn(host1.getBytes(UTF_8))
        .anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock2Name))).andReturn(host2.getBytes(UTF_8))
        .anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock3Name))).andReturn(host3.getBytes(UTF_8))
        .anyTimes();

    replay(zooReader);

    ServiceStatusCmd cmd = new ServiceStatusCmd();
    StatusSummary status = cmd.getManagerStatus(zooReader, zRoot);
    LOG.info("manager status data: {}", status);

    assertEquals(3, status.getServiceCount());

    // expect sorted by name
    Set<String> hosts = new TreeSet<>(List.of(host1, host2, host3));
    Map<String,Set<String>> hostByGroup = new TreeMap<>();
    hostByGroup.put(NO_GROUP_TAG, hosts);

    StatusSummary expected =
        new StatusSummary(ServiceStatusReport.ReportKey.MANAGER, Set.of(), hostByGroup, 0);

    assertEquals(expected.hashCode(), status.hashCode());
    assertEquals(expected.getDisplayName(), status.getDisplayName());
    assertEquals(expected.getResourceGroups(), status.getResourceGroups());
    assertEquals(expected.getServiceByGroups(), status.getServiceByGroups());
    assertEquals(expected.getServiceCount(), status.getServiceCount());
    assertEquals(expected.getErrorCount(), status.getErrorCount());
    assertEquals(expected, status);
  }

  @Test
  void testMonitorHosts() throws Exception {
    String lock1Name = "zlock#" + UUID.randomUUID() + "#0000000001";
    String lock2Name = "zlock#" + UUID.randomUUID() + "#0000000002";

    String host1 = "hostA:8080";
    String host2 = "host1:9091";

    String lockPath = zRoot + Constants.ZMONITOR_LOCK;
    expect(zooReader.getChildren(eq(lockPath))).andReturn(List.of(lock1Name, lock2Name)).anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock1Name))).andReturn(host1.getBytes(UTF_8))
        .anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock2Name))).andReturn(host2.getBytes(UTF_8))
        .anyTimes();

    replay(zooReader);

    ServiceStatusCmd cmd = new ServiceStatusCmd();
    StatusSummary status = cmd.getMonitorStatus(zooReader, zRoot);
    LOG.info("monitor status data: {}", status);

    assertEquals(2, status.getServiceCount());

    // expect sorted by name
    Map<String,Set<String>> hostByGroup = new TreeMap<>();
    hostByGroup.put(NO_GROUP_TAG, new TreeSet<>(List.of(host1, host2)));

    StatusSummary expected =
        new StatusSummary(ServiceStatusReport.ReportKey.MONITOR, Set.of(), hostByGroup, 0);

    assertEquals(expected.hashCode(), status.hashCode());
    assertEquals(expected.getDisplayName(), status.getDisplayName());
    assertEquals(expected.getResourceGroups(), status.getResourceGroups());
    assertEquals(expected.getServiceByGroups(), status.getServiceByGroups());
    assertEquals(expected.getServiceCount(), status.getServiceCount());
    assertEquals(expected.getErrorCount(), status.getErrorCount());
    assertEquals(expected, status);
  }

  @Test
  void testTServerHosts() throws Exception {
    String lock1Name = "zlock#" + UUID.randomUUID() + "#0000000001";
    String lock2Name = "zlock#" + UUID.randomUUID() + "#0000000002";
    String lock3Name = "zlock#" + UUID.randomUUID() + "#0000000003";

    String host1 = "hostA:8080";
    String host2 = "hostB:9090";
    String host3 = "host1:9091";

    String basePath = zRoot + Constants.ZTSERVERS;
    expect(zooReader.getChildren(eq(basePath))).andReturn(List.of(host1, host2, host3)).anyTimes();

    expect(zooReader.getChildren(eq(basePath + "/" + host1))).andReturn(List.of(lock1Name)).once();
    expect(zooReader.getData(eq(basePath + "/" + host1 + "/" + lock1Name)))
        .andReturn(("TSERV_CLIENT=" + host1).getBytes(UTF_8)).anyTimes();

    expect(zooReader.getChildren(eq(basePath + "/" + host2))).andReturn(List.of(lock2Name)).once();
    expect(zooReader.getData(eq(basePath + "/" + host2 + "/" + lock2Name)))
        .andReturn(("TSERV_CLIENT=" + host2).getBytes(UTF_8)).anyTimes();

    expect(zooReader.getChildren(eq(basePath + "/" + host3))).andReturn(List.of(lock3Name)).once();
    expect(zooReader.getData(eq(basePath + "/" + host3 + "/" + lock3Name)))
        .andReturn(("TSERV_CLIENT=" + host3).getBytes(UTF_8)).anyTimes();

    replay(zooReader);

    ServiceStatusCmd cmd = new ServiceStatusCmd();
    StatusSummary status = cmd.getTServerStatus(zooReader, zRoot);
    LOG.info("tserver status data: {}", status);

    assertEquals(3, status.getServiceCount());

    // expect sorted by name
    Map<String,Set<String>> hostByGroup = new TreeMap<>();
    hostByGroup.put(NO_GROUP_TAG, new TreeSet<>(List.of(host1, host2, host3)));

    StatusSummary expected =
        new StatusSummary(ServiceStatusReport.ReportKey.T_SERVER, Set.of(), hostByGroup, 0);

    assertEquals(expected.hashCode(), status.hashCode());
    assertEquals(expected.getDisplayName(), status.getDisplayName());
    assertEquals(expected.getResourceGroups(), status.getResourceGroups());
    assertEquals(expected.getServiceByGroups(), status.getServiceByGroups());
    assertEquals(expected.getServiceCount(), status.getServiceCount());
    assertEquals(expected.getErrorCount(), status.getErrorCount());
    assertEquals(expected, status);
  }

  @Test
  void testScanServerHosts() throws Exception {
    UUID uuid1 = UUID.randomUUID();
    String lock1Name = "zlock#" + uuid1 + "#0000000001";
    UUID uuid2 = UUID.randomUUID();
    String lock2Name = "zlock#" + uuid2 + "#0000000022";
    UUID uuid3 = UUID.randomUUID();
    String lock3Name = "zlock#" + uuid3 + "#0000000033";
    String lock4Name = "zlock#" + uuid3 + "#0000000044";

    // UUID uuidLock = UUID.randomUUID();

    String host1 = "host1:8080";
    String host2 = "host2:9090";
    String host3 = "host3:9091";
    String host4 = "host4:9091";

    String lockPath = zRoot + Constants.ZSSERVERS;
    expect(zooReader.getChildren(eq(lockPath))).andReturn(List.of(host1, host2, host3, host4))
        .anyTimes();

    expect(zooReader.getChildren(eq(lockPath + "/" + host1))).andReturn(List.of(lock1Name)).once();
    expect(zooReader.getData(eq(lockPath + "/" + host1 + "/" + lock1Name)))
        .andReturn((UUID.randomUUID() + ",rg1").getBytes(UTF_8)).once();

    expect(zooReader.getChildren(eq(lockPath + "/" + host2))).andReturn(List.of(lock2Name)).once();
    expect(zooReader.getData(eq(lockPath + "/" + host2 + "/" + lock2Name)))
        .andReturn((UUID.randomUUID() + ",default").getBytes(UTF_8)).once();

    expect(zooReader.getChildren(eq(lockPath + "/" + host3))).andReturn(List.of(lock3Name)).once();
    expect(zooReader.getData(eq(lockPath + "/" + host3 + "/" + lock3Name)))
        .andReturn((UUID.randomUUID() + ",rg1").getBytes(UTF_8)).once();

    expect(zooReader.getChildren(eq(lockPath + "/" + host4))).andReturn(List.of(lock4Name)).once();
    expect(zooReader.getData(eq(lockPath + "/" + host4 + "/" + lock4Name)))
        .andReturn((UUID.randomUUID() + ",default").getBytes(UTF_8)).once();

    replay(zooReader);

    ServiceStatusCmd cmd = new ServiceStatusCmd();
    StatusSummary status = cmd.getScanServerStatus(zooReader, zRoot);
    assertEquals(4, status.getServiceCount());

    Map<String,Set<String>> hostByGroup = new TreeMap<>();
    hostByGroup.put("default", new TreeSet<>(List.of("host2:9090", "host4:9091")));
    hostByGroup.put("rg1", new TreeSet<>(List.of("host1:8080", "host3:9091")));

    StatusSummary expected = new StatusSummary(ServiceStatusReport.ReportKey.S_SERVER,
        Set.of("default", "rg1"), hostByGroup, 0);

    assertEquals(expected, status);

  }

  @Test
  void testCoordinatorHosts() throws Exception {
    String lock1Name = "zlock#" + UUID.randomUUID() + "#0000000001";
    String lock2Name = "zlock#" + UUID.randomUUID() + "#0000000002";
    String lock3Name = "zlock#" + UUID.randomUUID() + "#0000000003";

    String host1 = "hostA:8080";
    String host2 = "hostB:9090";
    String host3 = "host1:9091";

    String lockPath = zRoot + Constants.ZCOORDINATOR_LOCK;
    expect(zooReader.getChildren(eq(lockPath))).andReturn(List.of(lock1Name, lock2Name, lock3Name))
        .anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock1Name))).andReturn(host1.getBytes(UTF_8))
        .anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock2Name))).andReturn(host2.getBytes(UTF_8))
        .anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock3Name))).andReturn(host3.getBytes(UTF_8))
        .anyTimes();

    replay(zooReader);

    ServiceStatusCmd cmd = new ServiceStatusCmd();
    StatusSummary status = cmd.getCoordinatorStatus(zooReader, zRoot);
    LOG.info("tserver status data: {}", status);

    assertEquals(3, status.getServiceCount());

    // expect sorted by name
    Set<String> hosts = new TreeSet<>(List.of(host1, host2, host3));
    Map<String,Set<String>> hostByGroup = new TreeMap<>();
    hostByGroup.put(NO_GROUP_TAG, hosts);

    StatusSummary expected =
        new StatusSummary(ServiceStatusReport.ReportKey.COORDINATOR, Set.of(), hostByGroup, 0);

    assertEquals(expected.hashCode(), status.hashCode());
    assertEquals(expected.getDisplayName(), status.getDisplayName());
    assertEquals(expected.getResourceGroups(), status.getResourceGroups());
    assertEquals(expected.getServiceByGroups(), status.getServiceByGroups());
    assertEquals(expected.getServiceCount(), status.getServiceCount());
    assertEquals(expected.getErrorCount(), status.getErrorCount());
    assertEquals(expected, status);
  }

  @Test
  public void testCompactorStatus() throws Exception {
    String lockPath = zRoot + Constants.ZCOMPACTORS;
    expect(zooReader.getChildren(eq(lockPath))).andReturn(List.of("q1", "q2")).once();

    expect(zooReader.getChildren(eq(lockPath + "/q1")))
        .andReturn(List.of("hostA:8080", "hostC:8081")).once();
    expect(zooReader.getChildren(eq(lockPath + "/q2")))
        .andReturn(List.of("hostB:9090", "hostD:9091")).once();

    replay(zooReader);

    ServiceStatusCmd cmd = new ServiceStatusCmd();
    StatusSummary status = cmd.getCompactorStatus(zooReader, zRoot);
    LOG.info("compactor group counts: {}", status);
    assertEquals(2, status.getResourceGroups().size());
  }

  @Test
  public void testGcHosts() throws Exception {

    String lockPath = zRoot + ZGC_LOCK;
    UUID uuid1 = UUID.randomUUID();
    String lock1Name = "zlock#" + uuid1 + "#0000000001";
    UUID uuid2 = UUID.randomUUID();
    String lock2Name = "zlock#" + uuid2 + "#0000000022";

    String host1 = "host1:8080";
    String host2 = "host2:9090";

    expect(zooReader.getChildren(eq(lockPath))).andReturn(List.of(lock1Name, lock2Name)).once();
    expect(zooReader.getData(eq(lockPath + "/" + lock1Name)))
        .andReturn(("GC_CLIENT=" + host1).getBytes(UTF_8)).once();
    expect(zooReader.getData(eq(lockPath + "/" + lock2Name)))
        .andReturn(("GC_CLIENT=" + host2).getBytes(UTF_8)).once();

    replay(zooReader);

    ServiceStatusCmd cmd = new ServiceStatusCmd();
    StatusSummary status = cmd.getGcStatus(zooReader, zRoot);
    LOG.info("gc server counts: {}", status);
    assertEquals(0, status.getResourceGroups().size());
    assertEquals(2, status.getServiceCount());
    assertEquals(0, status.getErrorCount());
    assertEquals(1, status.getServiceByGroups().size());
    assertEquals(2, status.getServiceByGroups().get(NO_GROUP_TAG).size());
    assertEquals(new TreeSet<>(List.of(host1, host2)),
        status.getServiceByGroups().get(NO_GROUP_TAG));
  }

  /**
   * Simulates node being deleted after lock list is read from ZooKeeper. Expect that the no node
   * error is skipped and available hosts are returned.
   */
  @Test
  void zkNodeDeletedTest() throws Exception {
    String lock1Name = "zlock#" + UUID.randomUUID() + "#0000000001";
    String lock2Name = "zlock#" + UUID.randomUUID() + "#0000000022";
    String lock3Name = "zlock#" + UUID.randomUUID() + "#0000000099";
    String host2 = "hostZ:8080";
    String host3 = "hostA:8080";

    String lockPath = zRoot + Constants.ZMANAGER_LOCK;
    expect(zooReader.getChildren(eq(lockPath))).andReturn(List.of(lock1Name, lock2Name, lock3Name))
        .anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock1Name)))
        .andThrow(new KeeperException.NoNodeException("no node forced exception")).anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock2Name))).andReturn(host2.getBytes(UTF_8))
        .anyTimes();
    expect(zooReader.getData(eq(lockPath + "/" + lock3Name))).andReturn(host3.getBytes(UTF_8))
        .anyTimes();
    replay(zooReader);

    ServiceStatusCmd cmd = new ServiceStatusCmd();
    StatusSummary status = cmd.getManagerStatus(zooReader, zRoot);
    LOG.info("manager status data: {}", status);

    assertEquals(1, status.getServiceByGroups().size());
    assertEquals(2, status.getServiceByGroups().get(NO_GROUP_TAG).size());
    assertEquals(1, status.getErrorCount());

    // host 1 missing - no node exception
    Set<String> sortedHosts = new TreeSet<>(List.of(host3, host2));
    assertEquals(sortedHosts, status.getServiceByGroups().get(NO_GROUP_TAG));
  }

  @Test
  public void testServiceStatusCommandOpts() {
    replay(zooReader); // needed for @AfterAll verify
    ServiceStatusCmd.Opts opts = new ServiceStatusCmd.Opts();
    assertFalse(opts.json);
    assertFalse(opts.noHosts);
  }

}

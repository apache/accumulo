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
package org.apache.accumulo.manager.compaction;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionQueueSummary;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService;
import org.apache.accumulo.core.tabletserver.thrift.TabletServerClientService.Client;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.LiveTServerSet;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.thrift.transport.TTransportException;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

public class CompactionCoordinatorTest {

  public class TestCoordinator extends CompactionCoordinator {

    private final Client tabletServerClient;
    private final List<RunningCompaction> runningCompactions;

    private Set<ExternalCompactionId> metadataCompactionIds = null;

    protected TestCoordinator(LiveTServerSet tservers, Client tabletServerClient,
        ServerContext context, AuditedSecurityOperation security,
        List<RunningCompaction> runningCompactions) {
      super(context, tservers, security);
      this.tabletServerClient = tabletServerClient;
      this.runningCompactions = runningCompactions;
    }

    @Override
    protected void startDeadCompactionDetector() {}

    @Override
    protected long getTServerCheckInterval() {
      this.shutdown = true;
      return 0L;
    }

    @Override
    protected void startCompactionCleaner(ScheduledThreadPoolExecutor schedExecutor) {}

    @Override
    protected void createCompactionFinalizer(ScheduledThreadPoolExecutor stpe) {}

    @Override
    protected void startRunningCleaner(ScheduledThreadPoolExecutor schedExecutor) {}

    @Override
    protected Client getTabletServerConnection(TServerInstance tserver) throws TTransportException {
      return tabletServerClient;
    }

    @Override
    public void compactionCompleted(TInfo tinfo, TCredentials credentials,
        String externalCompactionId, TKeyExtent textent, TCompactionStats stats)
        throws ThriftSecurityException {}

    @Override
    public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
        TKeyExtent extent) throws ThriftSecurityException {}

    void setMetadataCompactionIds(Set<ExternalCompactionId> mci) {
      metadataCompactionIds = mci;
    }

    @Override
    protected Set<ExternalCompactionId> readExternalCompactionIds() {
      if (metadataCompactionIds == null) {
        return RUNNING_CACHE.keySet();
      } else {
        return metadataCompactionIds;
      }
    }

    public Map<String,TreeMap<Short,TreeSet<TServerInstance>>> getQueues() {
      return CompactionCoordinator.QUEUE_SUMMARIES.QUEUES;
    }

    public Map<TServerInstance,Set<QueueAndPriority>> getIndex() {
      return CompactionCoordinator.QUEUE_SUMMARIES.INDEX;
    }

    public Map<ExternalCompactionId,RunningCompaction> getRunning() {
      return RUNNING_CACHE;
    }

    public void resetInternals() {
      getQueues().clear();
      getIndex().clear();
      getRunning().clear();
      metadataCompactionIds = null;
    }

    @Override
    protected String getTServerAddressString(HostAndPort tserverAddress) {
      return "";
    }

    @Override
    protected List<RunningCompaction> getCompactionsRunningOnCompactors() {
      return runningCompactions;
    }

    @Override
    protected void cancelCompactionOnCompactor(String address, String externalCompactionId) {}

    @Override
    protected void returnTServerClient(Client client) {}

  }

  @Test
  public void testCoordinatorColdStartNoCompactions() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    LiveTServerSet tservers = EasyMock.createNiceMock(LiveTServerSet.class);
    expect(tservers.getCurrentServers()).andReturn(Collections.emptySet()).anyTimes();

    TServerInstance tsi = EasyMock.createNiceMock(TServerInstance.class);
    expect(tsi.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletServerClientService.Client tsc =
        EasyMock.createNiceMock(TabletServerClientService.Client.class);
    expect(tsc.getCompactionQueueInfo(anyObject(), anyObject())).andReturn(Collections.emptyList())
        .anyTimes();

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    EasyMock.replay(context, tservers, tsi, tsc, security);

    var coordinator = new TestCoordinator(tservers, tsc, context, security, new ArrayList<>());
    coordinator.resetInternals();
    assertEquals(0, coordinator.getQueues().size());
    assertEquals(0, coordinator.getIndex().size());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    assertEquals(0, coordinator.getQueues().size());
    assertEquals(0, coordinator.getIndex().size());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.shutdown();

    EasyMock.verify(context, tservers, tsi, tsc, security);
    coordinator.resetInternals();
  }

  @Test
  public void testCoordinatorColdStart() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);
    expect(context.rpcCreds()).andReturn(creds);

    LiveTServerSet tservers = EasyMock.createNiceMock(LiveTServerSet.class);
    TServerInstance instance = EasyMock.createNiceMock(TServerInstance.class);
    expect(tservers.getCurrentServers()).andReturn(Collections.singleton(instance)).once();

    TServerInstance tsi = EasyMock.createNiceMock(TServerInstance.class);
    expect(tsi.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletServerClientService.Client tsc =
        EasyMock.createNiceMock(TabletServerClientService.Client.class);
    TCompactionQueueSummary queueSummary = EasyMock.createNiceMock(TCompactionQueueSummary.class);
    expect(tsc.getCompactionQueueInfo(anyObject(), anyObject()))
        .andReturn(Collections.singletonList(queueSummary)).anyTimes();
    expect(queueSummary.getQueue()).andReturn("R2DQ").anyTimes();
    expect(queueSummary.getPriority()).andReturn((short) 1).anyTimes();

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    EasyMock.replay(context, creds, tservers, instance, tsi, tsc, queueSummary, security);

    var coordinator = new TestCoordinator(tservers, tsc, context, security, new ArrayList<>());
    coordinator.resetInternals();
    assertEquals(0, coordinator.getQueues().size());
    assertEquals(0, coordinator.getIndex().size());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    assertEquals(1, coordinator.getQueues().size());
    QueueAndPriority qp = QueueAndPriority.get("R2DQ".intern(), (short) 1);
    Map<Short,TreeSet<TServerInstance>> m = coordinator.getQueues().get("R2DQ".intern());
    assertNotNull(m);
    assertEquals(1, m.size());
    assertTrue(m.containsKey((short) 1));
    Set<TServerInstance> t = m.get((short) 1);
    assertNotNull(t);
    assertEquals(1, t.size());
    TServerInstance queuedTsi = t.iterator().next();
    assertEquals(tsi.getHostPortSession(), queuedTsi.getHostPortSession());
    assertEquals(1, coordinator.getIndex().size());
    assertTrue(coordinator.getIndex().containsKey(queuedTsi));
    Set<QueueAndPriority> i = coordinator.getIndex().get(queuedTsi);
    assertEquals(1, i.size());
    assertEquals(qp, i.iterator().next());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.shutdown();

    EasyMock.verify(context, tservers, instance, tsi, tsc, queueSummary, security);
    coordinator.resetInternals();
  }

  @Test
  public void testCoordinatorRestartNoRunningCompactions() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);
    expect(context.rpcCreds()).andReturn(creds);

    LiveTServerSet tservers = EasyMock.createNiceMock(LiveTServerSet.class);
    TServerInstance instance = EasyMock.createNiceMock(TServerInstance.class);
    HostAndPort tserverAddress = HostAndPort.fromString("localhost:9997");
    expect(instance.getHostAndPort()).andReturn(tserverAddress).anyTimes();
    expect(tservers.getCurrentServers()).andReturn(Sets.newHashSet(instance)).once();

    expect(instance.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletServerClientService.Client tsc =
        EasyMock.createNiceMock(TabletServerClientService.Client.class);
    TCompactionQueueSummary queueSummary = EasyMock.createNiceMock(TCompactionQueueSummary.class);
    expect(tsc.getCompactionQueueInfo(anyObject(), anyObject()))
        .andReturn(Collections.singletonList(queueSummary)).anyTimes();
    expect(queueSummary.getQueue()).andReturn("R2DQ").anyTimes();
    expect(queueSummary.getPriority()).andReturn((short) 1).anyTimes();

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    EasyMock.replay(context, creds, tservers, instance, tsc, queueSummary, security);

    var coordinator = new TestCoordinator(tservers, tsc, context, security, new ArrayList<>());
    coordinator.resetInternals();
    assertEquals(0, coordinator.getQueues().size());
    assertEquals(0, coordinator.getIndex().size());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    assertEquals(1, coordinator.getQueues().size());
    QueueAndPriority qp = QueueAndPriority.get("R2DQ".intern(), (short) 1);
    Map<Short,TreeSet<TServerInstance>> m = coordinator.getQueues().get("R2DQ".intern());
    assertNotNull(m);
    assertEquals(1, m.size());
    assertTrue(m.containsKey((short) 1));
    Set<TServerInstance> t = m.get((short) 1);
    assertNotNull(t);
    assertEquals(1, t.size());
    TServerInstance queuedTsi = t.iterator().next();
    assertEquals(instance.getHostPortSession(), queuedTsi.getHostPortSession());
    assertEquals(1, coordinator.getIndex().size());
    assertTrue(coordinator.getIndex().containsKey(queuedTsi));
    Set<QueueAndPriority> i = coordinator.getIndex().get(queuedTsi);
    assertEquals(1, i.size());
    assertEquals(qp, i.iterator().next());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.shutdown();

    EasyMock.verify(context, creds, tservers, instance, tsc, queueSummary, security);
    coordinator.resetInternals();
  }

  @Test
  public void testCoordinatorRestartOneRunningCompaction() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);
    expect(context.rpcCreds()).andReturn(creds);

    LiveTServerSet tservers = EasyMock.createNiceMock(LiveTServerSet.class);
    TServerInstance instance = EasyMock.createNiceMock(TServerInstance.class);
    HostAndPort tserverAddress = HostAndPort.fromString("localhost:9997");
    expect(instance.getHostAndPort()).andReturn(tserverAddress).anyTimes();
    expect(tservers.getCurrentServers()).andReturn(Sets.newHashSet(instance)).once();

    List<RunningCompaction> runningCompactions = new ArrayList<>();
    ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());
    TExternalCompactionJob job = EasyMock.createNiceMock(TExternalCompactionJob.class);
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    TKeyExtent extent = new TKeyExtent();
    extent.setTable("1".getBytes());
    runningCompactions.add(new RunningCompaction(job, tserverAddress.toString(), "queue"));

    expect(instance.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletServerClientService.Client tsc =
        EasyMock.createNiceMock(TabletServerClientService.Client.class);
    TCompactionQueueSummary queueSummary = EasyMock.createNiceMock(TCompactionQueueSummary.class);
    expect(tsc.getCompactionQueueInfo(anyObject(), anyObject()))
        .andReturn(Collections.singletonList(queueSummary)).anyTimes();
    expect(queueSummary.getQueue()).andReturn("R2DQ").anyTimes();
    expect(queueSummary.getPriority()).andReturn((short) 1).anyTimes();

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    EasyMock.replay(context, creds, tservers, instance, job, tsc, queueSummary, security);

    var coordinator = new TestCoordinator(tservers, tsc, context, security, runningCompactions);
    coordinator.resetInternals();
    assertEquals(0, coordinator.getQueues().size());
    assertEquals(0, coordinator.getIndex().size());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    assertEquals(1, coordinator.getQueues().size());
    QueueAndPriority qp = QueueAndPriority.get("R2DQ".intern(), (short) 1);
    Map<Short,TreeSet<TServerInstance>> m = coordinator.getQueues().get("R2DQ".intern());
    assertNotNull(m);
    assertEquals(1, m.size());
    assertTrue(m.containsKey((short) 1));
    Set<TServerInstance> t = m.get((short) 1);
    assertNotNull(t);
    assertEquals(1, t.size());
    TServerInstance queuedTsi = t.iterator().next();
    assertEquals(instance.getHostPortSession(), queuedTsi.getHostPortSession());
    assertEquals(1, coordinator.getIndex().size());
    assertTrue(coordinator.getIndex().containsKey(queuedTsi));
    Set<QueueAndPriority> i = coordinator.getIndex().get(queuedTsi);
    assertEquals(1, i.size());
    assertEquals(qp, i.iterator().next());
    assertEquals(1, coordinator.getRunning().size());
    coordinator.shutdown();

    EasyMock.verify(context, creds, tservers, instance, job, tsc, queueSummary, security);
    coordinator.resetInternals();
  }

  @Test
  public void testGetCompactionJob() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);
    expect(context.rpcCreds()).andReturn(creds).anyTimes();

    LiveTServerSet tservers = EasyMock.createNiceMock(LiveTServerSet.class);
    TServerInstance instance = EasyMock.createNiceMock(TServerInstance.class);
    expect(tservers.getCurrentServers()).andReturn(Collections.singleton(instance)).once();
    HostAndPort tserverAddress = HostAndPort.fromString("localhost:9997");
    expect(instance.getHostAndPort()).andReturn(tserverAddress).anyTimes();

    TServerInstance tsi = EasyMock.createNiceMock(TServerInstance.class);
    expect(tsi.getHostPort()).andReturn("localhost:9997").anyTimes();

    TabletServerClientService.Client tsc =
        EasyMock.createNiceMock(TabletServerClientService.Client.class);
    TCompactionQueueSummary queueSummary = EasyMock.createNiceMock(TCompactionQueueSummary.class);
    expect(tsc.getCompactionQueueInfo(anyObject(), anyObject()))
        .andReturn(Collections.singletonList(queueSummary)).anyTimes();
    expect(queueSummary.getQueue()).andReturn("R2DQ").anyTimes();
    expect(queueSummary.getPriority()).andReturn((short) 1).anyTimes();

    ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());
    TExternalCompactionJob job = EasyMock.createNiceMock(TExternalCompactionJob.class);
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    TInfo trace = TraceUtil.traceInfo();
    expect(tsc.reserveCompactionJob(trace, creds, "R2DQ", 1, "localhost:10241", eci.toString()))
        .andReturn(job).anyTimes();

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);
    expect(security.canPerformSystemActions(creds)).andReturn(true);

    EasyMock.replay(context, creds, tservers, instance, job, tsc, queueSummary, security);

    var coordinator = new TestCoordinator(tservers, tsc, context, security, new ArrayList<>());
    coordinator.resetInternals();
    assertEquals(0, coordinator.getQueues().size());
    assertEquals(0, coordinator.getIndex().size());
    assertEquals(0, coordinator.getRunning().size());
    // Use coordinator.run() to populate the internal data structures. This is tested in a different
    // test.
    coordinator.run();
    coordinator.shutdown();

    assertEquals(1, coordinator.getQueues().size());
    QueueAndPriority qp = QueueAndPriority.get("R2DQ".intern(), (short) 1);
    Map<Short,TreeSet<TServerInstance>> m = coordinator.getQueues().get("R2DQ".intern());
    assertNotNull(m);
    assertEquals(1, m.size());
    assertTrue(m.containsKey((short) 1));
    Set<TServerInstance> t = m.get((short) 1);
    assertNotNull(t);
    assertEquals(1, t.size());
    TServerInstance queuedTsi = t.iterator().next();
    assertEquals(tsi.getHostPortSession(), queuedTsi.getHostPortSession());
    assertEquals(1, coordinator.getIndex().size());
    assertTrue(coordinator.getIndex().containsKey(queuedTsi));
    Set<QueueAndPriority> i = coordinator.getIndex().get(queuedTsi);
    assertEquals(1, i.size());
    assertEquals(qp, i.iterator().next());
    assertEquals(0, coordinator.getRunning().size());

    // Get the next job
    TExternalCompactionJob createdJob =
        coordinator.getCompactionJob(trace, creds, "R2DQ", "localhost:10241", eci.toString());
    assertEquals(eci.toString(), createdJob.getExternalCompactionId());

    assertEquals(1, coordinator.getQueues().size());
    assertEquals(1, coordinator.getIndex().size());
    assertEquals(1, coordinator.getRunning().size());
    Entry<ExternalCompactionId,RunningCompaction> entry =
        coordinator.getRunning().entrySet().iterator().next();
    assertEquals(eci.toString(), entry.getKey().toString());
    assertEquals("localhost:10241", entry.getValue().getCompactorAddress());
    assertEquals(eci.toString(), entry.getValue().getJob().getExternalCompactionId());

    EasyMock.verify(context, creds, tservers, instance, job, tsc, queueSummary, security);
    coordinator.resetInternals();

  }

  @Test
  public void testGetCompactionJobNoJobs() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);

    LiveTServerSet tservers = EasyMock.createNiceMock(LiveTServerSet.class);

    TabletServerClientService.Client tsc =
        EasyMock.createNiceMock(TabletServerClientService.Client.class);

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);
    expect(security.canPerformSystemActions(creds)).andReturn(true);

    EasyMock.replay(context, creds, tservers, tsc, security);

    var coordinator = new TestCoordinator(tservers, tsc, context, security, new ArrayList<>());
    coordinator.resetInternals();
    TExternalCompactionJob job = coordinator.getCompactionJob(TraceUtil.traceInfo(), creds, "R2DQ",
        "localhost:10240", UUID.randomUUID().toString());
    assertNull(job.getExternalCompactionId());
    coordinator.shutdown();

    EasyMock.verify(context, creds, tservers, tsc, security);
    coordinator.resetInternals();
  }

  @Test
  public void testCleanUpRunning() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);

    LiveTServerSet tservers = EasyMock.createNiceMock(LiveTServerSet.class);

    TabletServerClientService.Client tsc =
        EasyMock.createNiceMock(TabletServerClientService.Client.class);

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    EasyMock.replay(context, creds, tservers, tsc, security);

    TestCoordinator coordinator =
        new TestCoordinator(tservers, tsc, context, security, new ArrayList<>());
    coordinator.resetInternals();

    var ecid1 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid2 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid3 = ExternalCompactionId.generate(UUID.randomUUID());

    coordinator.getRunning().put(ecid1, new RunningCompaction(new TExternalCompaction()));
    coordinator.getRunning().put(ecid2, new RunningCompaction(new TExternalCompaction()));
    coordinator.getRunning().put(ecid3, new RunningCompaction(new TExternalCompaction()));

    coordinator.cleanUpRunning();

    assertEquals(Set.of(ecid1, ecid2, ecid3), coordinator.getRunning().keySet());

    coordinator.setMetadataCompactionIds(Set.of(ecid1, ecid2));

    coordinator.cleanUpRunning();

    assertEquals(Set.of(ecid1, ecid2), coordinator.getRunning().keySet());

    EasyMock.verify(context, creds, tservers, tsc, security);

  }
}

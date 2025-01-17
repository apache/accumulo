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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TNextCompactionJob;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.Fate;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.FateInstanceType;
import org.apache.accumulo.core.iteratorsImpl.system.SystemIteratorUtil;
import org.apache.accumulo.core.metadata.ReferencedTabletFile;
import org.apache.accumulo.core.metadata.schema.CompactionMetadata;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metrics.MetricsInfo;
import org.apache.accumulo.core.securityImpl.thrift.TCredentials;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.spi.compaction.CompactorGroupId;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionKind;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.trace.TraceUtil;
import org.apache.accumulo.core.util.cache.Caches;
import org.apache.accumulo.core.util.compaction.CompactionJobImpl;
import org.apache.accumulo.core.util.compaction.RunningCompaction;
import org.apache.accumulo.core.util.time.SteadyTime;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.manager.compaction.coordinator.CompactionCoordinator;
import org.apache.accumulo.manager.compaction.queue.CompactionJobPriorityQueue;
import org.apache.accumulo.manager.compaction.queue.ResolvedCompactionJob;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.TableConfiguration;
import org.apache.accumulo.server.security.AuditedSecurityOperation;
import org.apache.accumulo.server.security.SecurityOperation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class CompactionCoordinatorTest {

  // Need a non-null fateInstances reference for CompactionCoordinator.compactionCompleted
  private static final AtomicReference<Map<FateInstanceType,Fate<Manager>>> fateInstances =
      new AtomicReference<>(Map.of());

  private static final CompactorGroupId GROUP_ID = CompactorGroupId.of("R2DQ");

  private final HostAndPort tserverAddr = HostAndPort.fromParts("192.168.1.1", 9090);

  public MetricsInfo getMockMetrics() {
    MetricsInfo metricsInfo = createMock(MetricsInfo.class);
    metricsInfo.addMetricsProducers(anyObject());
    expectLastCall().anyTimes();
    metricsInfo.init(List.of());
    expectLastCall().anyTimes();
    replay(metricsInfo);
    return metricsInfo;
  }

  public class TestCoordinator extends CompactionCoordinator {

    private final List<RunningCompaction> runningCompactions;

    private Set<ExternalCompactionId> metadataCompactionIds = null;

    public TestCoordinator(ServerContext ctx, SecurityOperation security,
        List<RunningCompaction> runningCompactions, Manager manager) {
      super(ctx, security, fateInstances, manager);
      this.runningCompactions = runningCompactions;
    }

    @Override
    protected int countCompactors(String groupName) {
      return 3;
    }

    @Override
    protected void startDeadCompactionDetector() {}

    @Override
    protected void startCompactorZKCleaner(ScheduledThreadPoolExecutor schedExecutor) {}

    @Override
    protected void startInternalStateCleaner(ScheduledThreadPoolExecutor schedExecutor) {
      // This is called from CompactionCoordinator.run(). Counting down
      // the latch will exit the run method
      this.shutdown.countDown();
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

    public Map<ExternalCompactionId,RunningCompaction> getRunning() {
      return RUNNING_CACHE;
    }

    public void resetInternals() {
      getRunning().clear();
      metadataCompactionIds = null;
    }

    @Override
    protected List<RunningCompaction> getCompactionsRunningOnCompactors() {
      return runningCompactions;
    }

    @Override
    protected Set<ServerId> getRunningCompactors() {
      return Set.of();
    }

    @Override
    protected CompactionMetadata reserveCompaction(ResolvedCompactionJob rcJob,
        String compactorAddress, ExternalCompactionId externalCompactionId) {
      return createExternalCompactionMetadata(rcJob, compactorAddress, externalCompactionId);
    }

    @Override
    protected CompactionMetadata createExternalCompactionMetadata(ResolvedCompactionJob job,
        String compactorAddress, ExternalCompactionId externalCompactionId) {
      FateInstanceType type = FateInstanceType.fromTableId(job.getExtent().tableId());
      FateId fateId = FateId.from(type, UUID.randomUUID());
      return new CompactionMetadata(job.getJobFiles(),
          new ReferencedTabletFile(new Path("file:///accumulo/tables/1/default_tablet/F00001.rf")),
          compactorAddress, job.getKind(), job.getPriority(), job.getGroup(), true, fateId);
    }

    @Override
    protected TExternalCompactionJob createThriftJob(String externalCompactionId,
        CompactionMetadata ecm, ResolvedCompactionJob rcJob,
        Optional<CompactionConfig> compactionConfig) {
      return new TExternalCompactionJob(externalCompactionId, rcJob.getExtent().toThrift(),
          List.of(), SystemIteratorUtil.toIteratorConfig(List.of()),
          ecm.getCompactTmpName().getNormalizedPathStr(), ecm.getPropagateDeletes(),
          TCompactionKind.valueOf(ecm.getKind().name()),
          FateId.from(FateInstanceType.fromTableId(rcJob.getExtent().tableId()), UUID.randomUUID())
              .toThrift(),
          Map.of());
    }

    @Override
    protected void cancelCompactionOnCompactor(String address, String externalCompactionId) {}

  }

  @Test
  public void testCoordinatorColdStart() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    MetricsInfo metricsInfo = getMockMetrics();
    expect(context.getMetricsInfo()).andReturn(metricsInfo).anyTimes();

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    Manager manager = EasyMock.createNiceMock(Manager.class);
    expect(manager.getSteadyTime()).andReturn(SteadyTime.from(100000, TimeUnit.NANOSECONDS))
        .anyTimes();

    EasyMock.replay(context, security, manager);

    var coordinator = new TestCoordinator(context, security, new ArrayList<>(), manager);
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    coordinator.shutdown();

    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    EasyMock.verify(context, security, metricsInfo);
  }

  @Test
  public void testCoordinatorRestartOneRunningCompaction() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    MetricsInfo metricsInfo = getMockMetrics();
    expect(context.getMetricsInfo()).andReturn(metricsInfo).anyTimes();

    List<RunningCompaction> runningCompactions = new ArrayList<>();
    ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());
    TExternalCompactionJob job = EasyMock.createNiceMock(TExternalCompactionJob.class);
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    TKeyExtent extent = new TKeyExtent();
    extent.setTable("1".getBytes(UTF_8));
    runningCompactions.add(new RunningCompaction(job, tserverAddr.toString(), GROUP_ID.toString()));

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);

    Manager manager = EasyMock.createNiceMock(Manager.class);
    expect(manager.getSteadyTime()).andReturn(SteadyTime.from(100000, TimeUnit.NANOSECONDS))
        .anyTimes();

    EasyMock.replay(context, job, security, manager);

    var coordinator = new TestCoordinator(context, security, runningCompactions, manager);
    coordinator.resetInternals();
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    coordinator.run();
    coordinator.shutdown();
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(1, coordinator.getRunning().size());

    Map<ExternalCompactionId,RunningCompaction> running = coordinator.getRunning();
    Entry<ExternalCompactionId,RunningCompaction> ecomp = running.entrySet().iterator().next();
    assertEquals(eci, ecomp.getKey());
    RunningCompaction rc = ecomp.getValue();
    assertEquals(GROUP_ID.toString(), rc.getGroupName());
    assertEquals(tserverAddr.toString(), rc.getCompactorAddress());

    EasyMock.verify(context, job, security);
  }

  @Test
  public void testGetCompactionJob() throws Exception {

    TableConfiguration tconf = EasyMock.createNiceMock(TableConfiguration.class);
    expect(tconf.get(Property.TABLE_COMPACTION_CONFIGURER))
        .andReturn(Property.TABLE_COMPACTION_CONFIGURER.getDefaultValue()).anyTimes();

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    expect(context.getTableConfiguration(TableId.of("2a"))).andReturn(tconf).anyTimes();

    MetricsInfo metricsInfo = getMockMetrics();
    expect(context.getMetricsInfo()).andReturn(metricsInfo).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);
    expect(context.rpcCreds()).andReturn(creds).anyTimes();

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);
    expect(security.canPerformSystemActions(creds)).andReturn(true).anyTimes();

    KeyExtent ke = new KeyExtent(TableId.of("2a"), new Text("z"), new Text("b"));
    TabletMetadata tm = EasyMock.createNiceMock(TabletMetadata.class);
    expect(tm.getExtent()).andReturn(ke).anyTimes();
    expect(tm.getFiles()).andReturn(Collections.emptySet()).anyTimes();
    expect(tm.getTableId()).andReturn(ke.tableId()).anyTimes();
    expect(tm.getDirName()).andReturn("t-00001").anyTimes();
    Manager manager = EasyMock.createNiceMock(Manager.class);
    expect(manager.getSteadyTime()).andReturn(SteadyTime.from(100000, TimeUnit.NANOSECONDS))
        .anyTimes();

    EasyMock.replay(tconf, context, creds, tm, security, manager);

    var coordinator = new TestCoordinator(context, security, new ArrayList<>(), manager);
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    // Use coordinator.run() to populate the internal data structures. This is tested in a different
    // test.
    coordinator.run();
    coordinator.shutdown();

    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());

    // Add a job to the job queue
    CompactionJob job =
        new CompactionJobImpl((short) 1, GROUP_ID, Collections.emptyList(), CompactionKind.SYSTEM);
    coordinator.addJobs(tm, Collections.singleton(job));
    CompactionJobPriorityQueue queue = coordinator.getJobQueues().getQueue(GROUP_ID);
    assertEquals(1, queue.getQueuedJobs());

    // Get the next job
    ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());
    TNextCompactionJob nextJob = coordinator.getCompactionJob(new TInfo(), creds,
        GROUP_ID.toString(), "localhost:10241", eci.toString());
    assertEquals(3, nextJob.getCompactorCount());
    TExternalCompactionJob createdJob = nextJob.getJob();
    assertEquals(eci.toString(), createdJob.getExternalCompactionId());
    assertEquals(ke, KeyExtent.fromThrift(createdJob.getExtent()));

    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(1, coordinator.getRunning().size());
    Entry<ExternalCompactionId,RunningCompaction> entry =
        coordinator.getRunning().entrySet().iterator().next();
    assertEquals(eci.toString(), entry.getKey().toString());
    assertEquals("localhost:10241", entry.getValue().getCompactorAddress());
    assertEquals(eci.toString(), entry.getValue().getJob().getExternalCompactionId());

    EasyMock.verify(tconf, context, creds, tm, security);
  }

  @Test
  public void testGetCompactionJobNoJobs() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);
    expect(security.canPerformSystemActions(creds)).andReturn(true);

    Manager manager = EasyMock.createNiceMock(Manager.class);
    expect(manager.getSteadyTime()).andReturn(SteadyTime.from(100000, TimeUnit.NANOSECONDS))
        .anyTimes();

    EasyMock.replay(context, creds, security, manager);

    var coordinator = new TestCoordinator(context, security, new ArrayList<>(), manager);
    TNextCompactionJob nextJob = coordinator.getCompactionJob(TraceUtil.traceInfo(), creds,
        GROUP_ID.toString(), "localhost:10240", UUID.randomUUID().toString());
    assertEquals(3, nextJob.getCompactorCount());
    assertNull(nextJob.getJob().getExternalCompactionId());

    EasyMock.verify(context, creds, security);
  }

  @Test
  public void testCleanUpRunning() throws Exception {

    ServerContext context = EasyMock.createNiceMock(ServerContext.class);
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();

    TCredentials creds = EasyMock.createNiceMock(TCredentials.class);

    AuditedSecurityOperation security = EasyMock.createNiceMock(AuditedSecurityOperation.class);
    Manager manager = EasyMock.createNiceMock(Manager.class);
    expect(manager.getSteadyTime()).andReturn(SteadyTime.from(100000, TimeUnit.NANOSECONDS))
        .anyTimes();

    EasyMock.replay(context, creds, security, manager);

    TestCoordinator coordinator =
        new TestCoordinator(context, security, new ArrayList<>(), manager);

    var ecid1 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid2 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid3 = ExternalCompactionId.generate(UUID.randomUUID());

    coordinator.getRunning().put(ecid1, new RunningCompaction(new TExternalCompaction()));
    coordinator.getRunning().put(ecid2, new RunningCompaction(new TExternalCompaction()));
    coordinator.getRunning().put(ecid3, new RunningCompaction(new TExternalCompaction()));

    coordinator.cleanUpInternalState();

    assertEquals(Set.of(ecid1, ecid2, ecid3), coordinator.getRunning().keySet());

    coordinator.setMetadataCompactionIds(Set.of(ecid1, ecid2));

    coordinator.cleanUpInternalState();

    assertEquals(Set.of(ecid1, ecid2), coordinator.getRunning().keySet());

    EasyMock.verify(context, creds, security);

  }
}

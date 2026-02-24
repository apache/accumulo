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
import static org.easymock.EasyMock.verify;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.thrift.TInfo;
import org.apache.accumulo.core.clientImpl.thrift.ThriftSecurityException;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TExternalCompaction;
import org.apache.accumulo.core.compaction.thrift.TNextCompactionJob;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.google.common.net.HostAndPort;

public class CompactionCoordinatorTest {

  private static final ResourceGroupId GROUP_ID = ResourceGroupId.of("R2DQ");

  private final HostAndPort tserverAddr = HostAndPort.fromParts("192.168.1.1", 9090);

  public MetricsInfo getMockMetrics() {
    MetricsInfo metricsInfo = createMock(MetricsInfo.class);
    metricsInfo.addMetricsProducers(anyObject());
    expectLastCall().anyTimes();
    metricsInfo.init(List.of());
    expectLastCall().anyTimes();
    return metricsInfo;
  }

  public class TestCoordinator extends CompactionCoordinator {

    private final List<RunningCompaction> runningCompactions;

    private Set<ExternalCompactionId> metadataCompactionIds = null;

    public TestCoordinator(Manager manager, List<RunningCompaction> runningCompactions) {
      super(manager, t -> null);
      this.runningCompactions = runningCompactions;
    }

    @Override
    protected int countCompactors(ResourceGroupId groupName) {
      return 3;
    }

    @Override
    protected void startQueueRunningSummaryLogging() {}

    @Override
    protected void startFailureSummaryLogging() {}

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
    protected void startConfigMonitor(ScheduledThreadPoolExecutor schedExecutor) {}

    @Override
    public void compactionCompleted(TInfo tinfo, TCredentials credentials,
        String externalCompactionId, TKeyExtent textent, TCompactionStats stats)
        throws ThriftSecurityException {}

    @Override
    public void compactionFailed(TInfo tinfo, TCredentials credentials, String externalCompactionId,
        TKeyExtent extent, String exceptionClassName, TCompactionState failureState)
        throws ThriftSecurityException {}

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

    public Map<String,TimeOrderedRunningCompactionSet> getLongRunningByGroup() {
      return LONG_RUNNING_COMPACTIONS_BY_RG;
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

  private TableId tableId;
  private Manager manager;
  private ServerContext context;
  private AuditedSecurityOperation security;
  private MetricsInfo metricsInfo;
  private TableConfiguration tconf;
  private TCredentials rpcCreds;

  @BeforeEach
  public void setupMocks(TestInfo testInfo) throws Exception {
    rpcCreds = new TCredentials(null, null, null, null);

    tableId = TableId.of(testInfo.getDisplayName());

    metricsInfo = getMockMetrics();

    security = createMock(AuditedSecurityOperation.class);
    expect(security.canPerformSystemActions(rpcCreds)).andReturn(true).anyTimes();

    context = createMock(ServerContext.class);
    expect(context.getMetricsInfo()).andReturn(getMockMetrics()).anyTimes();
    expect(context.getSecurityOperation()).andReturn(security).anyTimes();
    expect(context.getScheduledExecutor()).andReturn(null).anyTimes();
    expect(context.getCaches()).andReturn(Caches.getInstance()).anyTimes();
    expect(context.getConfiguration()).andReturn(DefaultConfiguration.getInstance()).anyTimes();
    tconf = createMock(TableConfiguration.class);
    expect(tconf.get(Property.TABLE_COMPACTION_CONFIGURER))
        .andReturn(Property.TABLE_COMPACTION_CONFIGURER.getDefaultValue()).anyTimes();
    expect(context.getTableConfiguration(tableId)).andReturn(tconf).anyTimes();

    manager = createMock(Manager.class);
    expect(manager.getContext()).andReturn(context).anyTimes();
    expect(manager.getSteadyTime()).andReturn(SteadyTime.from(100000, TimeUnit.NANOSECONDS))
        .anyTimes();

    replay(manager, context, security, metricsInfo, tconf);
  }

  @AfterEach
  public void verifyMocks() {
    verify(manager, context, security, metricsInfo, tconf);
  }

  @Test
  public void testCoordinatorColdStart() throws Exception {
    var coordinator = new TestCoordinator(manager, new ArrayList<>());
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    assertEquals(0, coordinator.getLongRunningByGroup().size());
    coordinator.run();
    coordinator.shutdown();

    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    assertEquals(0, coordinator.getLongRunningByGroup().size());
  }

  @Test
  public void testCoordinatorRestartOneRunningCompaction() throws Exception {
    List<RunningCompaction> runningCompactions = new ArrayList<>();
    ExternalCompactionId eci = ExternalCompactionId.generate(UUID.randomUUID());

    TExternalCompactionJob job = createMock(TExternalCompactionJob.class);
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).atLeastOnce();
    TKeyExtent extent = new TKeyExtent();
    extent.setTable("1".getBytes(UTF_8));
    runningCompactions.add(new RunningCompaction(job, tserverAddr.toString(), GROUP_ID));
    replay(job);

    var coordinator = new TestCoordinator(manager, runningCompactions);
    coordinator.resetInternals();
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(0, coordinator.getRunning().size());
    assertEquals(0, coordinator.getLongRunningByGroup().size());
    coordinator.run();
    coordinator.shutdown();
    assertEquals(0, coordinator.getJobQueues().getQueuedJobCount());
    assertEquals(1, coordinator.getRunning().size());
    assertEquals(1, coordinator.getLongRunningByGroup().size());

    Map<ExternalCompactionId,RunningCompaction> running = coordinator.getRunning();
    Entry<ExternalCompactionId,RunningCompaction> ecomp = running.entrySet().iterator().next();
    assertEquals(eci, ecomp.getKey());
    RunningCompaction rc = ecomp.getValue();
    assertEquals(GROUP_ID, rc.getGroup());
    assertEquals(tserverAddr.toString(), rc.getCompactorAddress());

    assertTrue(coordinator.getLongRunningByGroup().containsKey(GROUP_ID.toString()));
    assertTrue(coordinator.getLongRunningByGroup().get(GROUP_ID.toString()).size() == 1);
    rc = coordinator.getLongRunningByGroup().get(GROUP_ID.toString()).iterator().next();
    assertEquals(GROUP_ID, rc.getGroup());
    assertEquals(tserverAddr.toString(), rc.getCompactorAddress());

    verify(job);
  }

  @Test
  public void testGetCompactionJob() throws Exception {
    KeyExtent ke = new KeyExtent(tableId, new Text("z"), new Text("b"));

    TabletMetadata tm = createMock(TabletMetadata.class);
    expect(tm.getSelectedFiles()).andReturn(null).atLeastOnce();
    expect(tm.getExtent()).andReturn(ke).atLeastOnce();
    expect(tm.getFiles()).andReturn(Collections.emptySet()).atLeastOnce();
    expect(tm.getDirName()).andReturn("t-00001").atLeastOnce();
    replay(tm);

    var coordinator = new TestCoordinator(manager, new ArrayList<>());
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
    TNextCompactionJob nextJob = coordinator.getCompactionJob(new TInfo(), rpcCreds,
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

    verify(tm);
  }

  @Test
  public void testGetCompactionJobNoJobs() throws Exception {
    var coordinator = new TestCoordinator(manager, new ArrayList<>());
    TNextCompactionJob nextJob = coordinator.getCompactionJob(TraceUtil.traceInfo(), rpcCreds,
        GROUP_ID.toString(), "localhost:10240", UUID.randomUUID().toString());
    assertEquals(3, nextJob.getCompactorCount());
    assertNull(nextJob.getJob().getExternalCompactionId());
  }

  @Test
  public void testCleanUpRunning() throws Exception {
    TExternalCompaction ext1 = createMock(TExternalCompaction.class);
    expect(ext1.getJob()).andReturn(new TExternalCompactionJob()).atLeastOnce();
    expect(ext1.getCompactor()).andReturn("localhost:9133").atLeastOnce();
    expect(ext1.getGroupName()).andReturn(Constants.DEFAULT_RESOURCE_GROUP_NAME).atLeastOnce();
    TExternalCompaction ext2 = createMock(TExternalCompaction.class);
    expect(ext2.getJob()).andReturn(new TExternalCompactionJob()).atLeastOnce();
    expect(ext2.getCompactor()).andReturn("localhost:9133").atLeastOnce();
    expect(ext2.getGroupName()).andReturn(Constants.DEFAULT_RESOURCE_GROUP_NAME).atLeastOnce();
    TExternalCompaction ext3 = createMock(TExternalCompaction.class);
    expect(ext3.getJob()).andReturn(new TExternalCompactionJob()).atLeastOnce();
    expect(ext3.getCompactor()).andReturn("localhost:9133").atLeastOnce();
    expect(ext3.getGroupName()).andReturn(Constants.DEFAULT_RESOURCE_GROUP_NAME).atLeastOnce();
    replay(ext1, ext2, ext3);

    TestCoordinator coordinator = new TestCoordinator(manager, new ArrayList<>());

    var ecid1 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid2 = ExternalCompactionId.generate(UUID.randomUUID());
    var ecid3 = ExternalCompactionId.generate(UUID.randomUUID());

    coordinator.getRunning().put(ecid1, new RunningCompaction(ext1));
    coordinator.getRunning().put(ecid2, new RunningCompaction(ext2));
    coordinator.getRunning().put(ecid3, new RunningCompaction(ext3));
    coordinator.cleanUpInternalState();

    assertEquals(Set.of(ecid1, ecid2, ecid3), coordinator.getRunning().keySet());

    coordinator.setMetadataCompactionIds(Set.of(ecid1, ecid2));
    coordinator.cleanUpInternalState();

    assertEquals(Set.of(ecid1, ecid2), coordinator.getRunning().keySet());

    verify(ext1, ext2, ext3);
  }
}

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
package org.apache.accumulo.tasks;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.tasks.compaction.CompactionTask;
import org.apache.accumulo.core.tasks.thrift.WorkerType;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.RetryableThriftCall.RetriesExceededException;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.mem.LowMemoryDetector;
import org.apache.accumulo.server.rpc.ServerAddress;
import org.apache.accumulo.tasks.jobs.CompactionJob;
import org.apache.accumulo.tasks.jobs.Job;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.net.HostAndPort;

@RunWith(PowerMockRunner.class)
@PrepareForTest({TaskRunner.class})
@SuppressStaticInitializationFor({"org.apache.log4j.LogManager"})
@PowerMockIgnore({"org.slf4j.*", "org.apache.logging.*", "org.apache.log4j.*",
    "org.apache.commons.logging.*", "org.xml.*", "javax.xml.*", "org.w3c.dom.*",
    "com.sun.org.apache.xerces.*"})
public class CompactorTest {

  public class SuccessfulCompaction extends CompactionJob {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    private volatile boolean completedCalled = false;
    private volatile boolean failedCalled = false;
    private TCompactionStatusUpdate latestState = null;

    public SuccessfulCompaction(TaskRunnerProcess worker, CompactionTask msg,
        AtomicReference<ExternalCompactionId> currentCompactionId) throws TException {
      super(worker, msg, currentCompactionId);
    }

    @Override
    public Runnable createJob() throws TException {
      return () -> {
        try {
          started.countDown();
          UtilWaitThread.sleep(1000);
        } catch (Exception e) {
          errorRef.set(e);
        } finally {
          stopped.countDown();
        }
      };
    }

    @Override
    protected void updateCompactionState(TExternalCompactionJob job, TCompactionStatusUpdate update)
        throws RetriesExceededException {
      latestState = update;
    }

    @Override
    protected void updateCompactionFailed(TExternalCompactionJob job)
        throws RetriesExceededException {
      failedCalled = true;
    }

    @Override
    protected void updateCompactionCompleted(TExternalCompactionJob job, TCompactionStats stats)
        throws RetriesExceededException {
      completedCalled = true;
    }

    public boolean isCompletedCalled() {
      return completedCalled;
    }

    public boolean isFailedCalled() {
      return failedCalled;
    }

    public TCompactionStatusUpdate getLatestState() {
      return latestState;
    }

  }

  public class FailedCompaction extends SuccessfulCompaction {

    public FailedCompaction(TaskRunnerProcess worker, CompactionTask msg,
        AtomicReference<ExternalCompactionId> currentCompactionId) throws TException {
      super(worker, msg, currentCompactionId);
    }

    @Override
    public Runnable createJob() throws TException {
      return () -> {
        try {
          started.countDown();
          UtilWaitThread.sleep(1000);
          throw new RuntimeException();
        } catch (Exception e) {
          errorRef.set(e);
        } finally {
          stopped.countDown();
        }
      };
    }
  }

  public class InterruptedCompaction extends SuccessfulCompaction {

    public InterruptedCompaction(TaskRunnerProcess worker, CompactionTask msg,
        AtomicReference<ExternalCompactionId> currentCompactionId) throws TException {
      super(worker, msg, currentCompactionId);
    }

    @Override
    public Runnable createJob() throws TException {
      return () -> {
        try {
          started.countDown();
          final Thread thread = Thread.currentThread();
          Timer t = new Timer();
          TimerTask task = new TimerTask() {
            @Override
            public void run() {
              thread.interrupt();
            }
          };
          t.schedule(task, 250);
          Thread.sleep(1000);
        } catch (Exception e) {
          LOG.error("Compaction failed: {}", e.getMessage());
          errorRef.set(e);
          throw new RuntimeException("Compaction failed", e);
        } finally {
          stopped.countDown();
        }
      };
    }

  }

  public class SuccessfulCompactor extends TaskRunner {

    private final Logger LOG = LoggerFactory.getLogger(SuccessfulCompactor.class);

    private final Supplier<UUID> uuid;
    private final ServerAddress address;
    protected final TExternalCompactionJob job;
    private final ServerContext context;
    private final ExternalCompactionId eci;
    private SuccessfulCompaction compactionJob;

    SuccessfulCompactor(Supplier<UUID> uuid, ServerAddress address, TExternalCompactionJob job,
        ServerContext context, ExternalCompactionId eci) {
      super(new ConfigOpts(),
          new String[] {"-o", Property.TASK_RUNNER_GROUP_NAME.getKey() + "=testQ", "-o",
              Property.TASK_RUNNER_WORKER_TYPE.getKey() + "=COMPACTION"},
          context.getConfiguration());
      this.uuid = uuid;
      this.address = address;
      this.job = job;
      this.context = context;
      this.eci = eci;
    }

    @Override
    protected String getTaskWorkerTypePropertyValue() {
      return WorkerType.COMPACTION.toString();
    }

    @Override
    public AccumuloConfiguration getConfiguration() {
      return context.getConfiguration();
    }

    @Override
    protected void setupSecurity() {}

    @Override
    protected void printStartupMsg() {}

    @Override
    public ServerContext getContext() {
      return this.context;
    }

    @Override
    protected void announceExistence(HostAndPort clientAddress)
        throws KeeperException, InterruptedException {}

    @Override
    protected ServerAddress startThriftClientService() throws UnknownHostException {
      return this.address;
    }

    @Override
    protected Job<?> getNextJob(Supplier<UUID> uuid) throws RetriesExceededException {
      try {
        LOG.info("Attempting to get next job, eci = {}", eci);
        currentTaskId.set(eci);
        compactionJob = createJobForTest();
        return compactionJob;
      } catch (TException e) {
        throw new RuntimeException("Error creating CompactionJob", e);
      } finally {
        this.shutdown = true;
      }
    }

    protected SuccessfulCompaction createJobForTest() throws TException {
      CompactionTask task = new CompactionTask();
      task.setCompactionJob(job);
      return new SuccessfulCompaction(this, task, currentTaskId);
    }

    @Override
    protected Supplier<UUID> getNextId() {
      return uuid;
    }

    public TCompactionState getLatestState() {
      return compactionJob.getLatestState().getState();
    }

    public boolean isCompletedCalled() {
      return compactionJob.isCompletedCalled();
    }

    public boolean isFailedCalled() {
      return compactionJob.isFailedCalled();
    }

  }

  public class FailedCompactor extends SuccessfulCompactor {

    FailedCompactor(Supplier<UUID> uuid, ServerAddress address, TExternalCompactionJob job,
        ServerContext context, ExternalCompactionId eci) {
      super(uuid, address, job, context, eci);
    }

    @Override
    protected SuccessfulCompaction createJobForTest() throws TException {
      CompactionTask task = new CompactionTask();
      task.setCompactionJob(job);
      return new FailedCompaction(this, task, currentTaskId);
    }
  }

  public class InterruptedCompactor extends SuccessfulCompactor {

    InterruptedCompactor(Supplier<UUID> uuid, ServerAddress address, TExternalCompactionJob job,
        ServerContext context, ExternalCompactionId eci) {
      super(uuid, address, job, context, eci);
    }

    @Override
    protected SuccessfulCompaction createJobForTest() throws TException {
      CompactionTask task = new CompactionTask();
      task.setCompactionJob(job);
      return new InterruptedCompaction(this, task, currentTaskId);
    }

  }

  @Test
  public void testCheckTime() throws Exception {
    assertEquals(1, CompactionJob.calculateProgressCheckTime(1024));
    assertEquals(1, CompactionJob.calculateProgressCheckTime(1048576));
    assertEquals(1, CompactionJob.calculateProgressCheckTime(10485760));
    assertEquals(10, CompactionJob.calculateProgressCheckTime(104857600));
    assertEquals(102, CompactionJob.calculateProgressCheckTime(1024 * 1024 * 1024));
  }

  @Test
  public void testCompactionSucceeds() throws Exception {
    UUID uuid = UUID.randomUUID();
    Supplier<UUID> supplier = () -> uuid;

    ExternalCompactionId eci = ExternalCompactionId.generate(supplier.get());

    PowerMock.resetAll();
    PowerMock.suppress(PowerMock.methods(Halt.class, "halt"));
    PowerMock.suppress(PowerMock.constructor(AbstractServer.class));

    ServerAddress client = PowerMock.createNiceMock(ServerAddress.class);
    HostAndPort address = HostAndPort.fromString("localhost:10240");
    expect(client.getAddress()).andReturn(address);

    TExternalCompactionJob job = PowerMock.createNiceMock(TExternalCompactionJob.class);
    KeyExtent ke = new KeyExtent(TableId.of("1"), new Text("b"), new Text("a"));
    TKeyExtent extent = ke.toThrift();
    expect(job.isSetExternalCompactionId()).andReturn(true).anyTimes();
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    expect(job.getExtent()).andReturn(extent).anyTimes();

    var conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_ZK_TIMEOUT, "1d");

    ServerContext context = PowerMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(context.getLowMemoryDetector()).andReturn(new LowMemoryDetector()).anyTimes();
    ZooReaderWriter zrw = PowerMock.createNiceMock(ZooReaderWriter.class);
    ZooKeeper zk = PowerMock.createNiceMock(ZooKeeper.class);
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(zrw.getZooKeeper()).andReturn(zk).anyTimes();
    VolumeManagerImpl vm = PowerMock.createNiceMock(VolumeManagerImpl.class);
    expect(context.getVolumeManager()).andReturn(vm);
    vm.close();

    PowerMock.replayAll();

    SuccessfulCompactor c = new SuccessfulCompactor(supplier, client, job, context, eci);
    c.run();

    PowerMock.verifyAll();
    c.close();

    assertTrue(c.isCompletedCalled());
    assertFalse(c.isFailedCalled());
  }

  @Test
  public void testCompactionFails() throws Exception {
    UUID uuid = UUID.randomUUID();
    Supplier<UUID> supplier = () -> uuid;

    ExternalCompactionId eci = ExternalCompactionId.generate(supplier.get());

    PowerMock.resetAll();
    PowerMock.suppress(PowerMock.methods(Halt.class, "halt"));
    PowerMock.suppress(PowerMock.constructor(AbstractServer.class));

    ServerAddress client = PowerMock.createNiceMock(ServerAddress.class);
    HostAndPort address = HostAndPort.fromString("localhost:10240");
    expect(client.getAddress()).andReturn(address);

    TExternalCompactionJob job = PowerMock.createNiceMock(TExternalCompactionJob.class);
    KeyExtent ke = new KeyExtent(TableId.of("1"), new Text("b"), new Text("a"));
    TKeyExtent extent = ke.toThrift();
    expect(job.isSetExternalCompactionId()).andReturn(true).anyTimes();
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    expect(job.getExtent()).andReturn(extent).anyTimes();

    var conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_ZK_TIMEOUT, "1d");

    ServerContext context = PowerMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(context.getLowMemoryDetector()).andReturn(new LowMemoryDetector()).anyTimes();
    ZooReaderWriter zrw = PowerMock.createNiceMock(ZooReaderWriter.class);
    ZooKeeper zk = PowerMock.createNiceMock(ZooKeeper.class);
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(zrw.getZooKeeper()).andReturn(zk).anyTimes();
    VolumeManagerImpl vm = PowerMock.createNiceMock(VolumeManagerImpl.class);
    expect(context.getVolumeManager()).andReturn(vm);
    vm.close();

    PowerMock.replayAll();

    FailedCompactor c = new FailedCompactor(supplier, client, job, context, eci);
    c.run();

    PowerMock.verifyAll();
    c.close();

    assertFalse(c.isCompletedCalled());
    assertTrue(c.isFailedCalled());
    assertEquals(TCompactionState.FAILED, c.getLatestState());
  }

  @Test
  public void testCompactionInterrupted() throws Exception {
    UUID uuid = UUID.randomUUID();
    Supplier<UUID> supplier = () -> uuid;

    ExternalCompactionId eci = ExternalCompactionId.generate(supplier.get());

    PowerMock.resetAll();
    PowerMock.suppress(PowerMock.methods(Halt.class, "halt"));
    PowerMock.suppress(PowerMock.constructor(AbstractServer.class));

    ServerAddress client = PowerMock.createNiceMock(ServerAddress.class);
    HostAndPort address = HostAndPort.fromString("localhost:10240");
    expect(client.getAddress()).andReturn(address);

    TExternalCompactionJob job = PowerMock.createNiceMock(TExternalCompactionJob.class);
    KeyExtent ke = new KeyExtent(TableId.of("1"), new Text("b"), new Text("a"));
    TKeyExtent extent = ke.toThrift();
    expect(job.isSetExternalCompactionId()).andReturn(true).anyTimes();
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    expect(job.getExtent()).andReturn(extent).anyTimes();

    var conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_ZK_TIMEOUT, "1d");

    ServerContext context = PowerMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
    expect(context.getLowMemoryDetector()).andReturn(new LowMemoryDetector()).anyTimes();
    ZooReaderWriter zrw = PowerMock.createNiceMock(ZooReaderWriter.class);
    ZooKeeper zk = PowerMock.createNiceMock(ZooKeeper.class);
    expect(context.getZooReaderWriter()).andReturn(zrw).anyTimes();
    expect(zrw.getZooKeeper()).andReturn(zk).anyTimes();
    VolumeManagerImpl vm = PowerMock.createNiceMock(VolumeManagerImpl.class);
    expect(context.getVolumeManager()).andReturn(vm);
    vm.close();

    PowerMock.replayAll();

    InterruptedCompactor c = new InterruptedCompactor(supplier, client, job, context, eci);
    c.run();

    PowerMock.verifyAll();
    c.close();

    assertFalse(c.isCompletedCalled());
    assertTrue(c.isFailedCalled());
    assertEquals(TCompactionState.CANCELLED, c.getLatestState());
  }

}

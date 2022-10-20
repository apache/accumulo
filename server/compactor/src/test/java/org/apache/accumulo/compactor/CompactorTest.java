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
package org.apache.accumulo.compactor;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import org.apache.accumulo.core.compaction.thrift.TCompactionState;
import org.apache.accumulo.core.compaction.thrift.TCompactionStatusUpdate;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.metadata.schema.ExternalCompactionId;
import org.apache.accumulo.core.tabletserver.thrift.TCompactionStats;
import org.apache.accumulo.core.tabletserver.thrift.TExternalCompactionJob;
import org.apache.accumulo.core.util.Halt;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.server.AbstractServer;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.compaction.RetryableThriftCall.RetriesExceededException;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.accumulo.server.rpc.ServerAddress;
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

@RunWith(PowerMockRunner.class)
@PrepareForTest({Compactor.class})
@SuppressStaticInitializationFor({"org.apache.log4j.LogManager"})
@PowerMockIgnore({"org.slf4j.*", "org.apache.logging.*", "org.apache.log4j.*",
    "org.apache.commons.logging.*", "org.xml.*", "javax.xml.*", "org.w3c.dom.*",
    "com.sun.org.apache.xerces.*"})
public class CompactorTest {

  public class SuccessfulCompaction implements Runnable {

    protected final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected final LongAdder totalInputEntries;
    protected final LongAdder totalInputBytes;
    protected final CountDownLatch started;
    protected final CountDownLatch stopped;
    protected final AtomicReference<Throwable> err;

    public SuccessfulCompaction(LongAdder totalInputEntries, LongAdder totalInputBytes,
        CountDownLatch started, CountDownLatch stopped, AtomicReference<Throwable> err) {
      this.totalInputEntries = totalInputEntries;
      this.totalInputBytes = totalInputBytes;
      this.err = err;
      this.started = started;
      this.stopped = stopped;
    }

    @Override
    public void run() {
      try {
        started.countDown();
        UtilWaitThread.sleep(1000);
      } catch (Exception e) {
        err.set(e);
      } finally {
        stopped.countDown();
      }
    }
  }

  public class FailedCompaction extends SuccessfulCompaction {

    public FailedCompaction(LongAdder totalInputEntries, LongAdder totalInputBytes,
        CountDownLatch started, CountDownLatch stopped, AtomicReference<Throwable> err) {
      super(totalInputEntries, totalInputBytes, started, stopped, err);
    }

    @Override
    public void run() {
      try {
        started.countDown();
        UtilWaitThread.sleep(1000);
        throw new RuntimeException();
      } catch (Exception e) {
        err.set(e);
      } finally {
        stopped.countDown();
      }
    }
  }

  public class InterruptedCompaction extends SuccessfulCompaction {

    public InterruptedCompaction(LongAdder totalInputEntries, LongAdder totalInputBytes,
        CountDownLatch started, CountDownLatch stopped, AtomicReference<Throwable> err) {
      super(totalInputEntries, totalInputBytes, started, stopped, err);
    }

    @Override
    public void run() {
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
        err.set(e);
        throw new RuntimeException("Compaction failed", e);
      } finally {
        stopped.countDown();
      }
    }

  }

  public class SuccessfulCompactor extends Compactor {

    private final Logger LOG = LoggerFactory.getLogger(SuccessfulCompactor.class);

    private final Supplier<UUID> uuid;
    private final ServerAddress address;
    private final TExternalCompactionJob job;
    private final ServerContext context;
    private final ExternalCompactionId eci;
    private volatile boolean completedCalled = false;
    private volatile boolean failedCalled = false;
    private TCompactionStatusUpdate latestState = null;

    SuccessfulCompactor(Supplier<UUID> uuid, ServerAddress address, TExternalCompactionJob job,
        ServerContext context, ExternalCompactionId eci) {
      super(new CompactorServerOpts(), new String[] {"-q", "testQ"}, context.getConfiguration());
      this.uuid = uuid;
      this.address = address;
      this.job = job;
      this.context = context;
      this.eci = eci;
    }

    @Override
    public AccumuloConfiguration getConfiguration() {
      return context.getConfiguration();
    }

    @Override
    protected void setupSecurity() {}

    @Override
    protected void startGCLogger(ScheduledThreadPoolExecutor schedExecutor) {}

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
    protected ServerAddress startCompactorClientService() throws UnknownHostException {
      return this.address;
    }

    @Override
    protected TExternalCompactionJob getNextJob(Supplier<UUID> uuid)
        throws RetriesExceededException {
      LOG.info("Attempting to get next job, eci = {}", eci);
      currentCompactionId.set(eci);
      this.shutdown = true;
      return job;
    }

    @Override
    protected synchronized void checkIfCanceled() {}

    @Override
    protected Runnable createCompactionJob(TExternalCompactionJob job, LongAdder totalInputEntries,
        LongAdder totalInputBytes, CountDownLatch started, CountDownLatch stopped,
        AtomicReference<Throwable> err) {
      return new SuccessfulCompaction(totalInputEntries, totalInputBytes, started, stopped, err);
    }

    @Override
    protected Supplier<UUID> getNextId() {
      return uuid;
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

    public TCompactionState getLatestState() {
      return latestState.getState();
    }

    public boolean isCompletedCalled() {
      return completedCalled;
    }

    public boolean isFailedCalled() {
      return failedCalled;
    }

  }

  public class FailedCompactor extends SuccessfulCompactor {

    FailedCompactor(Supplier<UUID> uuid, ServerAddress address, TExternalCompactionJob job,
        ServerContext context, ExternalCompactionId eci) {
      super(uuid, address, job, context, eci);
    }

    @Override
    protected Runnable createCompactionJob(TExternalCompactionJob job, LongAdder totalInputEntries,
        LongAdder totalInputBytes, CountDownLatch started, CountDownLatch stopped,
        AtomicReference<Throwable> err) {
      return new FailedCompaction(totalInputEntries, totalInputBytes, started, stopped, err);
    }
  }

  public class InterruptedCompactor extends SuccessfulCompactor {

    InterruptedCompactor(Supplier<UUID> uuid, ServerAddress address, TExternalCompactionJob job,
        ServerContext context, ExternalCompactionId eci) {
      super(uuid, address, job, context, eci);
    }

    @Override
    protected Runnable createCompactionJob(TExternalCompactionJob job, LongAdder totalInputEntries,
        LongAdder totalInputBytes, CountDownLatch started, CountDownLatch stopped,
        AtomicReference<Throwable> err) {
      return new InterruptedCompaction(totalInputEntries, totalInputBytes, started, stopped, err);
    }

  }

  @Test
  public void testCheckTime() throws Exception {
    assertEquals(1, Compactor.calculateProgressCheckTime(1024));
    assertEquals(1, Compactor.calculateProgressCheckTime(1048576));
    assertEquals(1, Compactor.calculateProgressCheckTime(10485760));
    assertEquals(10, Compactor.calculateProgressCheckTime(104857600));
    assertEquals(102, Compactor.calculateProgressCheckTime(1024 * 1024 * 1024));
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
    TKeyExtent extent = PowerMock.createNiceMock(TKeyExtent.class);
    expect(job.isSetExternalCompactionId()).andReturn(true).anyTimes();
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    expect(job.getExtent()).andReturn(extent).anyTimes();
    expect(extent.getTable()).andReturn("testTable".getBytes()).anyTimes();

    var conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_ZK_TIMEOUT, "1d");

    ServerContext context = PowerMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
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
    TKeyExtent extent = PowerMock.createNiceMock(TKeyExtent.class);
    expect(extent.getTable()).andReturn("testTable".getBytes()).anyTimes();

    expect(job.isSetExternalCompactionId()).andReturn(true).anyTimes();
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    expect(job.getExtent()).andReturn(extent).anyTimes();

    var conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_ZK_TIMEOUT, "1d");

    ServerContext context = PowerMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
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
    TKeyExtent extent = PowerMock.createNiceMock(TKeyExtent.class);
    expect(job.isSetExternalCompactionId()).andReturn(true).anyTimes();
    expect(job.getExternalCompactionId()).andReturn(eci.toString()).anyTimes();
    expect(job.getExtent()).andReturn(extent).anyTimes();
    expect(extent.getTable()).andReturn("testTable".getBytes()).anyTimes();

    var conf = new ConfigurationCopy(DefaultConfiguration.getInstance());
    conf.set(Property.INSTANCE_ZK_TIMEOUT, "1d");

    ServerContext context = PowerMock.createNiceMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(conf).anyTimes();
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

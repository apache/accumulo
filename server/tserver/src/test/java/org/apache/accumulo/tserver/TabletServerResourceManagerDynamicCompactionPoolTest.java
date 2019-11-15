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
package org.apache.accumulo.tserver;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleep;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.junit.After;
import org.junit.Test;

public class TabletServerResourceManagerDynamicCompactionPoolTest {

  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final AtomicInteger numRunning = new AtomicInteger(0);
  private final AtomicInteger maxRan = new AtomicInteger(0);

  private class FakeCompaction implements Runnable, Comparable<Runnable> {
    private final TableId id;

    public FakeCompaction(TableId id) {
      this.id = id;
    }

    @Override
    public void run() {
      numRunning.addAndGet(1);
      while (keepRunning.get()) {
        sleep(5);
      }
      numRunning.decrementAndGet();
      maxRan.addAndGet(1);
    }

    @Override
    public int compareTo(Runnable o) {
      if (o instanceof FakeCompaction) {
        return id.canonical().compareTo(((FakeCompaction) o).id.canonical());
      }
      return 1;
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj);
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
  }

  @After
  public void stopRunningThreads() {
    keepRunning.set(false);
  }

  /*
   * Ensure the TabletServerResourceManager increases the thread pool size dynamically
   */
  @Test(timeout = 20_000)
  public void testDynamicThreadPoolUpdates() {
    // create a mock config that substitutes for the system configuration in ZK
    ConfigurationCopy config = new ConfigurationCopy(DefaultConfiguration.getInstance()) {
      @Override
      public boolean isPropertySet(Property prop, boolean cacheAndWatch) {
        return false;
      }
    };
    config.set(Property.TSERV_NATIVEMAP_ENABLED, "false");
    config.set(Property.TSERV_MAJC_DELAY, "100ms");
    config.set(Property.TSERV_MAJC_MAXCONCURRENT, "3");

    ServerConfigurationFactory serverConfFactory = createMock(ServerConfigurationFactory.class);
    expect(serverConfFactory.getSystemConfiguration()).andReturn(config).anyTimes();
    ServerContext context = createMock(ServerContext.class);
    expect(context.getConfiguration()).andReturn(config).anyTimes();
    expect(context.getVolumeManager()).andReturn(null).anyTimes();
    expect(context.getServerConfFactory()).andReturn(serverConfFactory).anyTimes();
    replay(context, serverConfFactory);

    // create a resource manager to test
    keepRunning.set(true);
    TabletServerResourceManager manager = new TabletServerResourceManager(context);

    // start first batch and ensure it runs at most 3 at a time (initial configuration)
    for (int i = 0; i < 10; i++) {
      TableId id = TableId.of("userTableBatch1_" + i);
      manager.executeMajorCompaction(new KeyExtent(id, null, null), new FakeCompaction(id));
    }
    waitForNumRunningToReach(3);

    // increase the number of concurrent threads to 5 and wait for it to eventually reach 5
    config.set(Property.TSERV_MAJC_MAXCONCURRENT, "5");
    waitForNumRunningToReach(5);

    // shut down the first batch (this will run all the remaining queued)
    keepRunning.set(false);
    waitForNumRunningToReach(0);

    // make sure all 10 in the first batch ran, and reset it for the second batch
    assertTrue(maxRan.compareAndSet(10, 0));

    // decrease to 2, but need to wait for it to propagate, or else the running compactions will
    // block the decrease in the threadpool size; the scheduler updates this every 10 seconds, so
    // we'll give it 12 to be sure it updated before we execute any new tasks
    config.set(Property.TSERV_MAJC_MAXCONCURRENT, "2");
    sleep(12_000);

    // start the second batch of 10 tasks, and make sure it stops at 2
    keepRunning.set(true);
    for (int i = 0; i < 10; i++) {
      TableId id = TableId.of("userTableBatch2_" + i);
      manager.executeMajorCompaction(new KeyExtent(id, null, null), new FakeCompaction(id));
    }
    waitForNumRunningToReach(2);

    // shut down second batch (this will run out all the remaining queued)
    keepRunning.set(false);
    waitForNumRunningToReach(0);

    // make sure all 10 in the second batch ran, and reset it
    assertTrue(maxRan.compareAndSet(10, 0));
  }

  private void waitForNumRunningToReach(int expected) {
    while (numRunning.get() != expected) {
      sleep(10);
    }
  }

}

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
package org.apache.accumulo.manager.metrics;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Deprecated
public class ReplicationMetricsTest {

  /**
   * Extend the class to override the current time for testing
   */
  public class ReplicationMetricsTestMetrics extends ReplicationMetrics {
    ReplicationMetricsTestMetrics(Manager manager) {
      super(manager);
    }

  }

  @Test
  public void testAddReplicationQueueTimeMetrics() throws Exception {
    Manager manager = EasyMock.createMock(Manager.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    VolumeManager fileSystem = EasyMock.createMock(VolumeManager.class);
    ReplicationUtil util = EasyMock.createMock(ReplicationUtil.class);
    MeterRegistry meterRegistry = EasyMock.createMock(MeterRegistry.class);
    Timer timer = EasyMock.createMock(Timer.class);

    Path path1 = new Path("hdfs://localhost:9000/accumulo/wal/file1");
    Path path2 = new Path("hdfs://localhost:9000/accumulo/wal/file2");

    // First call will initialize the map of paths to modification time
    EasyMock.expect(manager.getContext()).andReturn(context).anyTimes();
    EasyMock.expect(meterRegistry.timer("replicationQueue")).andReturn(timer).anyTimes();
    EasyMock.expect(meterRegistry.gauge(EasyMock.eq("filesPendingReplication"),
        EasyMock.anyObject(AtomicLong.class))).andReturn(new AtomicLong(0)).anyTimes();
    EasyMock
        .expect(
            meterRegistry.gauge(EasyMock.eq("numPeers"), EasyMock.anyObject(AtomicInteger.class)))
        .andReturn(new AtomicInteger(0)).anyTimes();
    EasyMock.expect(meterRegistry.gauge(EasyMock.eq("maxReplicationThreads"),
        EasyMock.anyObject(AtomicInteger.class))).andReturn(new AtomicInteger(0)).anyTimes();
    EasyMock.expect(util.getPendingReplicationPaths()).andReturn(Set.of(path1, path2));
    EasyMock.expect(manager.getVolumeManager()).andReturn(fileSystem);
    EasyMock.expect(fileSystem.getFileStatus(path1)).andReturn(createStatus(100));
    EasyMock.expect(manager.getVolumeManager()).andReturn(fileSystem);
    EasyMock.expect(fileSystem.getFileStatus(path2)).andReturn(createStatus(200));

    // Second call will recognize the missing path1 and add the latency stat
    EasyMock.expect(util.getPendingReplicationPaths()).andReturn(Set.of(path2));

    timer.record(EasyMock.isA(Duration.class));
    EasyMock.expectLastCall();

    EasyMock.replay(manager, fileSystem, util, meterRegistry, timer);

    ReplicationMetrics metrics = new ReplicationMetricsTestMetrics(manager);

    // Inject our mock objects
    replaceField(metrics, "replicationUtil", util);
    replaceField(metrics, "replicationQueueTimer", timer);

    // Two calls to this will initialize the map and then add metrics
    metrics.addReplicationQueueTimeMetrics();
    metrics.addReplicationQueueTimeMetrics();

    EasyMock.verify(manager, fileSystem, util, meterRegistry, timer);
  }

  private void replaceField(Object instance, String fieldName, Object target)
      throws NoSuchFieldException, IllegalAccessException {
    Field field = instance.getClass().getSuperclass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(instance, target);
  }

  private FileStatus createStatus(long modtime) {
    return new FileStatus(0, false, 0, 0, modtime, 0, null, null, null, null);
  }
}

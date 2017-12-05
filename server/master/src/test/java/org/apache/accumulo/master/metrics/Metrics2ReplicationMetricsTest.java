/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.master.metrics;

import java.lang.reflect.Field;

import org.apache.accumulo.master.Master;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.replication.ReplicationUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableQuantiles;
import org.apache.hadoop.metrics2.lib.MutableStat;
import org.easymock.EasyMock;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class Metrics2ReplicationMetricsTest {
  private long currentTime = 1000L;

  /**
   * Extend the class to override the current time for testing
   */
  public class TestMetrics2ReplicationMetrics extends Metrics2ReplicationMetrics {
    TestMetrics2ReplicationMetrics(Master master, MetricsSystem system) {
      super(master, system);
    }

    @Override
    public long getCurrentTime() {
      return currentTime;
    }
  }

  @Test
  public void testAddReplicationQueueTimeMetrics() throws Exception {
    Master master = EasyMock.createMock(Master.class);
    MetricsSystem system = EasyMock.createMock(MetricsSystem.class);
    VolumeManager fileSystem = EasyMock.createMock(VolumeManager.class);
    ReplicationUtil util = EasyMock.createMock(ReplicationUtil.class);
    MutableStat stat = EasyMock.createMock(MutableStat.class);
    MutableQuantiles quantiles = EasyMock.createMock(MutableQuantiles.class);

    Path path1 = new Path("hdfs://localhost:9000/accumulo/wal/file1");
    Path path2 = new Path("hdfs://localhost:9000/accumulo/wal/file2");

    // First call will initialize the map of paths to modification time
    EasyMock.expect(util.getPendingReplicationPaths()).andReturn(ImmutableSet.of(path1, path2));
    EasyMock.expect(master.getFileSystem()).andReturn(fileSystem);
    EasyMock.expect(fileSystem.getFileStatus(path1)).andReturn(createStatus(100));
    EasyMock.expect(master.getFileSystem()).andReturn(fileSystem);
    EasyMock.expect(fileSystem.getFileStatus(path2)).andReturn(createStatus(200));

    // Second call will recognize the missing path1 and add the latency stat
    EasyMock.expect(util.getPendingReplicationPaths()).andReturn(ImmutableSet.of(path2));

    // Expect a call to reset the min/max
    stat.resetMinMax();
    EasyMock.expectLastCall();

    // Expect the calls of adding the stats
    quantiles.add(currentTime - 100);
    EasyMock.expectLastCall();

    stat.add(currentTime - 100);
    EasyMock.expectLastCall();

    EasyMock.replay(master, system, fileSystem, util, stat, quantiles);

    Metrics2ReplicationMetrics metrics = new TestMetrics2ReplicationMetrics(master, system);

    // Inject our mock objects
    replaceField(metrics, "replicationUtil", util);
    replaceField(metrics, "replicationQueueTimeQuantiles", quantiles);
    replaceField(metrics, "replicationQueueTimeStat", stat);

    // Two calls to this will initialize the map and then add metrics
    metrics.addReplicationQueueTimeMetrics();
    metrics.addReplicationQueueTimeMetrics();

    EasyMock.verify(master, system, fileSystem, util, stat, quantiles);
  }

  private void replaceField(Object instance, String fieldName, Object target) throws NoSuchFieldException, IllegalAccessException {
    Field field = instance.getClass().getSuperclass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(instance, target);
  }

  private FileStatus createStatus(long modtime) {
    return new FileStatus(0, false, 0, 0, modtime, 0, null, null, null, null);
  }
}

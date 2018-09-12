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
package org.apache.accumulo.server.fs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.UncheckedExecutionException;

public class SpaceAwareVolumeChooserTest {
  VolumeManager volumeManager = null;
  VolumeChooserEnvironment chooserEnv = null;
  ServerContext serverContext = null;
  ServerConfigurationFactory serverConfigurationFactory = null;
  AccumuloConfiguration sysConfig = null;
  Volume vol1 = null;
  Volume vol2 = null;
  FileSystem fs1 = null;
  FileSystem fs2 = null;
  FsStatus status1 = null;
  FsStatus status2 = null;

  int iterations = 1000;

  String volumeOne = "hdfs://nn1:8020/apps/accumulo1/tables";
  String volumeTwo = "hdfs://nn2:8020/applications/accumulo/tables";

  // Different volumes with different paths
  String[] tableDirs = {volumeOne, volumeTwo};

  int vol1Count = 0;
  int vol2Count = 0;

  @Before
  public void beforeTest() {
    volumeManager = EasyMock.createMock(VolumeManager.class);
    serverContext = EasyMock.createMock(ServerContext.class);
    serverConfigurationFactory = EasyMock.createMock(ServerConfigurationFactory.class);
    sysConfig = EasyMock.createMock(AccumuloConfiguration.class);
    vol1 = EasyMock.createMock(Volume.class);
    vol2 = EasyMock.createMock(Volume.class);
    fs1 = EasyMock.createMock(FileSystem.class);
    fs2 = EasyMock.createMock(FileSystem.class);
    status1 = EasyMock.createMock(FsStatus.class);
    status2 = EasyMock.createMock(FsStatus.class);
    chooserEnv = new VolumeChooserEnvironment(VolumeChooserEnvironment.ChooserScope.DEFAULT,
        serverContext);

  }

  private void testSpecificSetup(long percentage1, long percentage2, String cacheDuration,
      int timesToCallPreferredVolumeChooser, boolean anyTimes) throws IOException {
    int max = iterations + 1;
    int min = 1;
    int updatePropertyMax = timesToCallPreferredVolumeChooser + iterations;
    if (anyTimes) {
      max = iterations + 1;
      updatePropertyMax = max + 1;
    }
    // Volume 1 is percentage1 full
    EasyMock.expect(status1.getRemaining()).andReturn(percentage1).times(min, max);
    EasyMock.expect(status1.getCapacity()).andReturn(100L).times(min, max);

    // Volume 2 is percentage2 full
    EasyMock.expect(status2.getRemaining()).andReturn(percentage2).times(min, max);
    EasyMock.expect(status2.getCapacity()).andReturn(100L).times(min, max);

    EasyMock.expect(sysConfig.get(SpaceAwareVolumeChooser.HDFS_SPACE_RECOMPUTE_INTERVAL))
        .andReturn(cacheDuration).times(1);
    EasyMock
        .expect(sysConfig.get(PreferredVolumeChooser
            .getPropertyNameForScope(VolumeChooserEnvironment.ChooserScope.DEFAULT)))
        .andReturn(String.join(",", tableDirs)).times(timesToCallPreferredVolumeChooser);

    EasyMock.expect(serverContext.getVolumeManager()).andReturn(volumeManager).times(min,
        Math.max(max, updatePropertyMax));
    EasyMock.expect(serverContext.getServerConfFactory()).andReturn(serverConfigurationFactory)
        .times(min, updatePropertyMax);
    EasyMock.expect(serverConfigurationFactory.getSystemConfiguration()).andReturn(sysConfig)
        .times(1, updatePropertyMax);

    EasyMock.expect(volumeManager.getVolumeByPath(new Path(volumeOne))).andReturn(vol1).times(min,
        max);
    EasyMock.expect(volumeManager.getVolumeByPath(new Path(volumeTwo))).andReturn(vol2).times(min,
        max);
    EasyMock.expect(vol1.getFileSystem()).andReturn(fs1).times(min, max);
    EasyMock.expect(vol2.getFileSystem()).andReturn(fs2).times(min, max);
    EasyMock.expect(fs1.getStatus()).andReturn(status1).times(min, max);
    EasyMock.expect(fs2.getStatus()).andReturn(status2).times(min, max);

    EasyMock.replay(serverContext, vol1, vol2, fs1, fs2, status1, status2, volumeManager,
        serverConfigurationFactory, sysConfig);
  }

  @After
  public void afterTest() {

    EasyMock.verify(serverContext, vol1, vol2, fs1, fs2, status1, status2, volumeManager,
        serverConfigurationFactory, sysConfig);

    volumeManager = null;
    serverContext = null;
    vol1 = null;
    vol2 = null;
    fs1 = null;
    fs2 = null;
    status1 = null;
    status2 = null;
    vol1Count = 0;
    vol2Count = 0;
  }

  @Test
  public void testEvenWeightsWithCaching() throws IOException {

    testSpecificSetup(10L, 10L, null, iterations, false);

    makeChoices();

    assertEquals(iterations / 2, vol1Count, iterations / 10);
    assertEquals(iterations / 2, vol2Count, iterations / 10);

  }

  @Test
  public void testEvenWeightsNoCaching() throws IOException {

    testSpecificSetup(10L, 10L, "0", iterations, true);

    makeChoices();

    assertEquals(iterations / 2, vol1Count, iterations / 10);
    assertEquals(iterations / 2, vol2Count, iterations / 10);

  }

  @Test(expected = UncheckedExecutionException.class)
  public void testNoFreeSpace() throws IOException {

    testSpecificSetup(0L, 0L, null, 1, false);

    makeChoices();
  }

  @Test
  public void testNinetyTen() throws IOException {

    testSpecificSetup(90L, 10L, null, iterations, false);

    makeChoices();

    assertEquals(iterations * .9, vol1Count, iterations / 10);
    assertEquals(iterations * .1, vol2Count, iterations / 10);

  }

  @Test
  public void testTenNinety() throws IOException {

    testSpecificSetup(10L, 90L, null, iterations, false);

    makeChoices();

    assertEquals(iterations * .1, vol1Count, iterations / 10);
    assertEquals(iterations * .9, vol2Count, iterations / 10);

  }

  @Test
  public void testWithNoCaching() throws IOException {

    testSpecificSetup(10L, 90L, "0", iterations, true);

    makeChoices();

    assertEquals(iterations * .1, vol1Count, iterations / 10);
    assertEquals(iterations * .9, vol2Count, iterations / 10);

  }

  private void makeChoices() {
    SpaceAwareVolumeChooser chooser = new SpaceAwareVolumeChooser();
    for (int i = 0; i < iterations; i++) {
      String choice = chooser.choose(chooserEnv, tableDirs);
      if (choice.equals(volumeOne)) {
        vol1Count += 1;
      }

      if (choice.equals(volumeTwo)) {
        vol2Count += 1;
      }
    }

  }
}

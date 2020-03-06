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
package org.apache.accumulo.server.fs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.util.concurrent.UncheckedExecutionException;

public class SpaceAwareVolumeChooserTest {

  VolumeChooserEnvironment chooserEnv = null;
  ServiceEnvironment serviceEnv = null;
  Configuration sysConfig = null;
  FileSystem fs1 = null;
  FileSystem fs2 = null;
  FsStatus status1 = null;
  FsStatus status2 = null;

  int iterations = 1000;

  String volumeOne = "hdfs://nn1:8020/apps/accumulo1/tables";
  String volumeTwo = "hdfs://nn2:8020/applications/accumulo/tables";

  // Different volumes with different paths
  Set<String> tableDirs = Set.of(volumeOne, volumeTwo);

  int vol1Count = 0;
  int vol2Count = 0;

  @Before
  public void beforeTest() {
    serviceEnv = EasyMock.createMock(ServiceEnvironment.class);
    sysConfig = EasyMock.createMock(Configuration.class);
    fs1 = EasyMock.createMock(FileSystem.class);
    fs2 = EasyMock.createMock(FileSystem.class);
    status1 = EasyMock.createMock(FsStatus.class);
    status2 = EasyMock.createMock(FsStatus.class);
    chooserEnv = EasyMock.createMock(VolumeChooserEnvironment.class);
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

    EasyMock.expect(sysConfig.getCustom(SpaceAwareVolumeChooser.RECOMPUTE_INTERVAL))
        .andReturn(cacheDuration).times(1);

    EasyMock
        .expect(
            sysConfig.getCustom("volume.preferred." + ChooserScope.DEFAULT.name().toLowerCase()))
        .andReturn(String.join(",", tableDirs)).times(timesToCallPreferredVolumeChooser);

    EasyMock.expect(serviceEnv.getConfiguration()).andReturn(sysConfig).times(1, updatePropertyMax);

    EasyMock.expect(fs1.getStatus()).andReturn(status1).times(min, max);
    EasyMock.expect(fs2.getStatus()).andReturn(status2).times(min, max);

    EasyMock.expect(chooserEnv.getFileSystem(volumeOne)).andReturn(fs1).times(min, max);
    EasyMock.expect(chooserEnv.getFileSystem(volumeTwo)).andReturn(fs2).times(min, max);
    EasyMock.expect(chooserEnv.getScope()).andReturn(ChooserScope.DEFAULT).times(min, max * 2);
    EasyMock.expect(chooserEnv.getServiceEnv()).andReturn(serviceEnv).times(min, max);

    EasyMock.replay(serviceEnv, fs1, fs2, status1, status2, sysConfig, chooserEnv);
  }

  @After
  public void afterTest() {

    EasyMock.verify(serviceEnv, fs1, fs2, status1, status2, sysConfig, chooserEnv);

    serviceEnv = null;
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

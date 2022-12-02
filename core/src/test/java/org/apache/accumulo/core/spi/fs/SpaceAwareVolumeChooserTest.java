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
package org.apache.accumulo.core.spi.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;

import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.spi.common.ServiceEnvironment.Configuration;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment.Scope;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.UncheckedExecutionException;

public class SpaceAwareVolumeChooserTest {

  VolumeChooserEnvironment chooserEnv = null;
  ServiceEnvironment serviceEnv = null;
  Configuration sysConfig = null;

  double free1;
  double free2;

  int iterations = 1000;

  String volumeOne = "hdfs://nn1:8020/apps/accumulo1/tables";
  String volumeTwo = "hdfs://nn2:8020/applications/accumulo/tables";

  // Different volumes with different paths
  Set<String> tableDirs = Set.of(volumeOne, volumeTwo);

  int vol1Count = 0;
  int vol2Count = 0;

  @BeforeEach
  public void beforeTest() {
    serviceEnv = EasyMock.createMock(ServiceEnvironment.class);
    sysConfig = EasyMock.createMock(Configuration.class);
    chooserEnv = EasyMock.createMock(VolumeChooserEnvironment.class);
  }

  private void testSpecificSetup(long percentage1, long percentage2, String cacheDuration,
      int timesToCallPreferredVolumeChooser, boolean anyTimes) {
    int max = iterations + 1;
    int min = 1;
    int updatePropertyMax = timesToCallPreferredVolumeChooser + iterations;
    if (anyTimes) {
      max = iterations + 1;
      updatePropertyMax = max + 1;
    }

    free1 = percentage1 / (double) 100;
    free2 = percentage2 / (double) 100;

    EasyMock.expect(sysConfig.getCustom(SpaceAwareVolumeChooser.RECOMPUTE_INTERVAL))
        .andReturn(cacheDuration).times(1);

    EasyMock.expect(sysConfig.getCustom("volume.preferred." + Scope.DEFAULT.name().toLowerCase()))
        .andReturn(String.join(",", tableDirs)).times(timesToCallPreferredVolumeChooser);

    EasyMock.expect(serviceEnv.getConfiguration()).andReturn(sysConfig).times(1, updatePropertyMax);

    EasyMock.expect(chooserEnv.getChooserScope()).andReturn(Scope.DEFAULT).times(min, max * 2);
    EasyMock.expect(chooserEnv.getServiceEnv()).andReturn(serviceEnv).times(min, max);

    EasyMock.replay(serviceEnv, sysConfig, chooserEnv);
  }

  @AfterEach
  public void afterTest() {

    EasyMock.verify(serviceEnv, sysConfig, chooserEnv);

    serviceEnv = null;
    vol1Count = 0;
    vol2Count = 0;
  }

  @Test
  public void testEvenWeightsWithCaching() {

    testSpecificSetup(10L, 10L, null, iterations, false);

    makeChoices();

    assertEquals(iterations / 2.0, vol1Count, iterations / 10.0);
    assertEquals(iterations / 2.0, vol2Count, iterations / 10.0);

  }

  @Test
  public void testEvenWeightsNoCaching() {

    testSpecificSetup(10L, 10L, "0", iterations, true);

    makeChoices();

    assertEquals(iterations / 2.0, vol1Count, iterations / 10.0);
    assertEquals(iterations / 2.0, vol2Count, iterations / 10.0);

  }

  @Test
  public void testNoFreeSpace() {
    testSpecificSetup(0L, 0L, null, 1, false);
    assertThrows(UncheckedExecutionException.class, this::makeChoices);
  }

  @Test
  public void testNinetyTen() {

    testSpecificSetup(90L, 10L, null, iterations, false);

    makeChoices();

    assertEquals(iterations * .9, vol1Count, iterations / 10.0);
    assertEquals(iterations * .1, vol2Count, iterations / 10.0);

  }

  @Test
  public void testTenNinety() {

    testSpecificSetup(10L, 90L, null, iterations, false);

    makeChoices();

    assertEquals(iterations * .1, vol1Count, iterations / 10.0);
    assertEquals(iterations * .9, vol2Count, iterations / 10.0);

  }

  @Test
  public void testWithNoCaching() {

    testSpecificSetup(10L, 90L, "0", iterations, true);

    makeChoices();

    assertEquals(iterations * .1, vol1Count, iterations / 10.0);
    assertEquals(iterations * .9, vol2Count, iterations / 10.0);

  }

  private void makeChoices() {
    SpaceAwareVolumeChooser chooser = new SpaceAwareVolumeChooser() {
      @Override
      protected double getFreeSpace(String uri) {
        if (uri.equals(volumeOne)) {
          return free1;
        }
        if (uri.equals(volumeTwo)) {
          return free2;
        }
        throw new IllegalArgumentException();
      }
    };
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

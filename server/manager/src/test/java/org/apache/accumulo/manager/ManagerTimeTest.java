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
package org.apache.accumulo.manager;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.util.time.SteadyTime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ManagerTimeTest {

  @Test
  public void testSteadyTime() {
    long time = 20_000;
    var steadyTime = SteadyTime.from(time, TimeUnit.NANOSECONDS);

    // make sure calling serialize on instance matches static helper
    byte[] serialized = ManagerTime.serialize(steadyTime);
    assertArrayEquals(serialized, ManagerTime.serialize(steadyTime));

    // Verify deserialization matches original object
    var deserialized = ManagerTime.deserialize(serialized);
    assertEquals(steadyTime, deserialized);
    assertEquals(0, steadyTime.compareTo(deserialized));
  }

  @ParameterizedTest
  // Test with both a 0 and positive previous value. This simulates the value
  // read out of zookeeper for the time and should never be negative as it is
  // based on elapsed time from previous manager runs
  @ValueSource(longs = {0, 50_000})
  public void testSteadyTimeFromSkew(long previousTime) throws InterruptedException {
    List<Long> times = List.of(-100_000L, -100L, 0L, 20_000L, System.nanoTime());

    for (Long time : times) {
      // ManagerTime builds the skew amount by subtracting the current nanotime
      // from the previous persisted time in ZK. The skew can be negative or positive because
      // it will depend on if the current nanotime is negative or positive as
      // nanotime is allowed to be negative
      var skewAmount =
          ManagerTime.updateSkew(SteadyTime.from(previousTime, TimeUnit.NANOSECONDS), time);

      // Build a SteadyTime using the skewAmount
      // SteadyTime should never be negative
      var original = ManagerTime.fromSkew(time, skewAmount);

      // Simulate a future time and create another SteadyTime from the skew which should
      // now be after the original
      time = time + 10000;
      var futureSkew = ManagerTime.fromSkew(time, skewAmount);

      // future should be after the original
      assertTrue(futureSkew.compareTo(original) > 0);
    }
  }

  @ParameterizedTest
  // Test with both a 0 and positive previous value. This simulates the value
  // read out of zookeeper for the time and should never be negative as it is
  // based on elapsed time from previous manager runs
  @ValueSource(longs = {0, 50_000})
  public void testSteadyTimeFromSkewCurrent(long previousTime) throws InterruptedException {
    // Also test fromSkew(skewAmount) method which only uses System.nanoTime()
    var skewAmount = ManagerTime.updateSkew(SteadyTime.from(previousTime, TimeUnit.NANOSECONDS));

    // Build a SteadyTime using the skewAmount and current time
    var original = ManagerTime.fromSkew(skewAmount);

    // sleep a bit so time elapses
    Thread.sleep(10);
    var futureSkew = ManagerTime.fromSkew(skewAmount);

    // future should be after the original
    assertTrue(futureSkew.compareTo(original) > 0);
  }

  @ParameterizedTest
  // Test with both a 0 and positive previous value. This simulates the value
  // read out of zookeeper for the time and should never be negative as it is
  // based on elapsed time from previous manager runs
  @ValueSource(longs = {0, 50_000})
  public void testSteadyTimeUpdateSkew(long previousTime) throws InterruptedException {

    var steadyTime = SteadyTime.from(previousTime, TimeUnit.NANOSECONDS);
    List<Long> times = List.of(-100_000L, -100L, 0L, 20_000L, System.nanoTime());

    // test updateSkew with various times and previous times
    for (Long time : times) {
      var expected = steadyTime.getNanos() - time;

      // test that updateSkew computes the update as current steadyTime - time
      var skewAmount = ManagerTime.updateSkew(steadyTime, time);
      assertEquals(expected, skewAmount.toNanos());
    }

    // test updateSkew with current system time
    var skew = ManagerTime.updateSkew(steadyTime);
    // sleep a bit so time elapses
    Thread.sleep(10);
    var updatedSkew = ManagerTime.updateSkew(steadyTime);
    // Updating the skew time subtracts the current nanotime from
    // the previous value so the updated value should be less than
    // a previously created SteadyTime based on the same skew
    assertTrue(skew.toNanos() - updatedSkew.toNanos() > 0);
    assertTrue(skew.compareTo(updatedSkew) > 0);
  }
}

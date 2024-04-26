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

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.manager.ManagerTime.SteadyTime;
import org.junit.jupiter.api.Test;

public class ManagerTimeTest {

  @Test
  public void testSteadyTime() {
    long time = System.nanoTime();
    var steadyTime = SteadyTime.from(time);
    assertEquals(time, steadyTime.getTimeNs());
    assertEquals(TimeUnit.NANOSECONDS.toMillis(time), steadyTime.getTimeMillis());

    // make sure calling serialize on instance matches static helper
    byte[] serialized = steadyTime.serialize();
    assertArrayEquals(serialized, SteadyTime.serialize(steadyTime));

    // Verify deserialization matches original object
    var deserialized = SteadyTime.deserialize(serialized);
    assertEquals(steadyTime, deserialized);
    assertEquals(0, steadyTime.compareTo(deserialized));
  }

  @Test
  public void testSteadyTimeSkew() {
    var time = System.nanoTime();
    long skewAmount = 200000;
    var skewed = ManagerTime.fromSkew(skewAmount);

    // Skew time appends the skew amount to the current nanotime
    // so a comparsion here should always be greater than the previous computation
    assertTrue(skewed.getTimeNs() > time + skewAmount);
    assertTrue(skewed.compareTo(SteadyTime.from(time + skewAmount)) > 0);

    var steadyTime = SteadyTime.from(System.nanoTime());
    var newSkew = ManagerTime.updatedSkew(steadyTime.serialize());
    // Updating the skew time subtracts the current nanotime from
    // the previous value so the updated value should be less than
    // a previously created SteadyTime based on the same skew
    assertTrue(skewed.getTimeNs() > newSkew);
    assertTrue(skewed.compareTo(SteadyTime.from(newSkew)) > 0);
  }
}

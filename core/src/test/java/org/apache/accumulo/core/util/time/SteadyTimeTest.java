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
package org.apache.accumulo.core.util.time;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

public class SteadyTimeTest {

  @Test
  public void testSteadyTime() {
    long time = 20_000;
    var steadyTime = SteadyTime.from(time, TimeUnit.NANOSECONDS);

    assertEquals(time, steadyTime.getNanos());
    assertEquals(TimeUnit.NANOSECONDS.toMillis(time), steadyTime.getMillis());
    assertEquals(Duration.ofNanos(time), steadyTime.getDuration());

    // Verify equals and compareTo work correctly for same
    var steadyTime2 = SteadyTime.from(time, TimeUnit.NANOSECONDS);
    assertEquals(steadyTime, steadyTime2);
    assertEquals(0, steadyTime.compareTo(steadyTime2));

    // Check equals/compareto different objects
    var steadyTime3 = SteadyTime.from(time + 100, TimeUnit.NANOSECONDS);
    assertNotEquals(steadyTime, steadyTime3);
    assertTrue(steadyTime.compareTo(steadyTime3) < 1);

    // Negatives are not allowed
    assertThrows(IllegalArgumentException.class, () -> SteadyTime.from(-100, TimeUnit.NANOSECONDS));
  }

}

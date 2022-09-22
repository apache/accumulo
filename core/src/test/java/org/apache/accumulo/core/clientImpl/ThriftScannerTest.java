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
package org.apache.accumulo.core.clientImpl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Test calls for {@link ThriftScanner}.
 */
public class ThriftScannerTest {

  private static boolean withinTenPercent(long expected, long actual) {
    long delta = Math.max(expected / 10, 1);
    return actual >= (expected - delta) && actual <= (expected + delta);
  }

  @Test
  public void testPauseIncrease() throws Exception {
    long newPause = ThriftScanner.pause(5L, 5000L, false);
    assertTrue(withinTenPercent(10L, newPause),
        "New pause should be within [9,11], but was " + newPause);
  }

  @Test
  public void testMaxPause() throws Exception {
    long maxPause = 1L;
    long nextPause = ThriftScanner.pause(5L, maxPause, false);
    assertTrue(withinTenPercent(maxPause, nextPause),
        "New pause should be within [0,2], but was " + nextPause);
  }
}

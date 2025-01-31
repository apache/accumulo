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
package org.apache.accumulo.core.client.admin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;

public class TabletMergeabilityInfoTest {

  @Test
  public void testValidation() {
    // ALWAYS requires an insertion time
    assertThrows(IllegalArgumentException.class,
        () -> new TabletMergeabilityInfo(TabletMergeability.always(), Optional.empty(),
            () -> Duration.ofDays(10)));

    // NEVER requires empty insertion time
    assertThrows(IllegalArgumentException.class,
        () -> new TabletMergeabilityInfo(TabletMergeability.never(), Optional.of(Duration.ZERO),
            () -> Duration.ofDays(10)));

    // Delay can't be null
    assertThrows(IllegalArgumentException.class,
        () -> new TabletMergeabilityInfo(TabletMergeability.after(Duration.ofMillis(-10)),
            Optional.of(Duration.ofHours(1)), () -> Duration.ofDays(10)));

    // InsertionTime can't be negative
    assertThrows(IllegalArgumentException.class,
        () -> new TabletMergeabilityInfo(TabletMergeability.always(),
            Optional.of(Duration.ofHours(-1)), () -> Duration.ofDays(10)));

    // No negative supplier for current time
    assertThrows(NullPointerException.class,
        () -> new TabletMergeabilityInfo(TabletMergeability.always(),
            Optional.of(Duration.ofHours(1)), null));
  }

  @Test
  public void testNever() {
    var tmi = new TabletMergeabilityInfo(TabletMergeability.never(), Optional.empty(),
        () -> Duration.ofDays(10));
    assertFalse(tmi.isMergeable());
    assertTrue(tmi.getTabletMergeability().isNever());
    assertTrue(tmi.getElapsed().isEmpty());
    assertTrue(tmi.getDelay().isEmpty());
    assertTrue(tmi.getRemaining().isEmpty());
  }

  @Test
  public void testAlways() {
    var tmi = new TabletMergeabilityInfo(TabletMergeability.always(),
        Optional.of(Duration.ofDays(1)), () -> Duration.ofDays(10));
    assertTrue(tmi.isMergeable());
    assertTrue(tmi.getTabletMergeability().isAlways());
    assertTrue(tmi.getElapsed().orElseThrow().toNanos() > 0);
    assertEquals(Duration.ZERO, tmi.getRemaining().orElseThrow());
  }

  @Test
  public void testDelay() {
    // test values when isMergeable() is false
    var tmi = new TabletMergeabilityInfo(TabletMergeability.after(Duration.ofDays(2)),
        Optional.of(Duration.ofDays(9)), () -> Duration.ofDays(10));
    assertFalse(tmi.isMergeable());
    assertEquals(Duration.ofDays(2), tmi.getDelay().orElseThrow());
    assertEquals(Duration.ofDays(1), tmi.getElapsed().orElseThrow());
    assertEquals(tmi.getDelay().map(delay -> delay.minus(tmi.getElapsed().orElseThrow())),
        tmi.getRemaining());

    // test values when isMergeable() is true
    var tmi2 = new TabletMergeabilityInfo(TabletMergeability.after(Duration.ofDays(6)),
        Optional.of(Duration.ofDays(1)), () -> Duration.ofDays(10));
    assertTrue(tmi2.isMergeable());
    assertEquals(Duration.ofDays(6), tmi2.getDelay().orElseThrow());
    assertEquals(Duration.ofDays(9), tmi2.getElapsed().orElseThrow());
    assertEquals(Duration.ZERO, tmi2.getRemaining().orElseThrow());
  }

}

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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

public class NanoTimeTest {
  @Test
  public void testMultipleTimes() {
    List<NanoTime> ntimes = new ArrayList<>();

    NanoTime prev = NanoTime.now();
    ntimes.add(prev);

    for (int i = 0; i < 100; i++) {
      NanoTime next = NanoTime.now();
      while (prev.equals(next)) {
        next = NanoTime.now();
      }

      ntimes.add(next);
      prev = next;
    }

    long curr = System.nanoTime();
    while (curr == System.nanoTime()) {}

    var start = NanoTime.now();

    while (start.equals(NanoTime.now())) {}

    for (int i = 1; i < ntimes.size(); i++) {
      var last = ntimes.get(i - 1);
      var next = ntimes.get(i);
      assertTrue(last.compareTo(next) < 0);
      assertTrue(next.compareTo(last) > 0);
      assertTrue(next.compareTo(next) == 0);
      assertTrue(next.elapsed().toNanos() > 0);
      assertEquals(next, next);
      assertEquals(next.hashCode(), next.hashCode());
      assertNotEquals(last, next);
      assertNotEquals(last.hashCode(), next.hashCode());

      var duration1 = next.elapsed();
      var duration2 = start.subtract(last);
      var duration3 = start.subtract(next);

      assertTrue(duration2.compareTo(duration3) > 0);
      assertTrue(duration1.compareTo(duration3) > 0);
    }

    var copy = List.copyOf(ntimes);
    Collections.shuffle(ntimes);
    Collections.sort(ntimes);
    assertEquals(copy, ntimes);
  }

  @Test
  public void testBoundry() {
    // tests crossing the Long.MAX_VALUE boundry
    long origin = Long.MAX_VALUE - 1000;

    List<NanoTime> ntimes = new ArrayList<>();

    // add times that start positive and then go negative
    for (int i = 0; i < 20; i++) {
      var nt = i * 100 + origin;
      ntimes.add(new NanoTime(nt));
    }

    for (int i = 1; i < ntimes.size(); i++) {
      var last = ntimes.get(i - 1);
      var next = ntimes.get(i);
      assertEquals(100, next.subtract(last).toNanos());
      assertEquals(-100, last.subtract(next).toNanos());
      assertTrue(next.compareTo(last) > 0);
      assertTrue(last.compareTo(next) < 0);
      assertTrue(next.compareTo(next) == 0);
    }

    var copy = List.copyOf(ntimes);
    Collections.shuffle(ntimes);
    Collections.sort(ntimes);
    assertEquals(copy, ntimes);
  }

  @Test
  public void testNowPlus() {

    List<NanoTime> ntimes = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ntimes.add(NanoTime.nowPlus(Duration.ofHours(i)));
    }

    for (int i = 1; i < ntimes.size(); i++) {
      var last = ntimes.get(i - 1);
      var next = ntimes.get(i);

      var duration = next.subtract(last);

      assertTrue(duration.compareTo(Duration.ofHours(1)) >= 0);
      // This could fail if the test process were paused for more than 3 minutes
      assertTrue(duration.compareTo(Duration.ofMinutes(63)) < 0);
      assertTrue(next.elapsed().compareTo(Duration.ZERO) < 0);
    }

    var copy = List.copyOf(ntimes);
    Collections.shuffle(ntimes);
    Collections.sort(ntimes);
    assertEquals(copy, ntimes);

    ntimes.clear();

    // nano time can compute elapsed times in a 290 year period which should wrap Long.MAX_VALUE no
    // matter where it starts
    for (int i = 0; i < 290; i++) {
      ntimes.add(NanoTime.nowPlus(Duration.ofDays(365 * i)));
    }

    for (int i = 1; i < ntimes.size(); i++) {
      var last = ntimes.get(i - 1);
      var next = ntimes.get(i);

      var duration = next.subtract(last);

      assertTrue(duration.compareTo(Duration.ofDays(365)) >= 0);
      assertTrue(duration.compareTo(Duration.ofDays(366)) < 0);
      assertTrue(next.elapsed().compareTo(Duration.ZERO) < 0);
    }

    copy = List.copyOf(ntimes);
    Collections.shuffle(ntimes);
    Collections.sort(ntimes);
    assertEquals(copy, ntimes);
  }

}

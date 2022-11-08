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
package org.apache.accumulo.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatTest {

  static double delta = 0.0000001;

  Stat zero;
  Stat stat;

  @BeforeEach
  public void setUp() {
    zero = new Stat();
    zero.addStat(0);

    stat = new Stat();

    // The mean and sd for this set were checked against wolfram alpha
    for (Long l : new long[] {9792, 5933, 4766, 5770, 3763, 3677, 5002}) {
      stat.addStat(l);
    }
  }

  @Test
  public void testGetMin() {
    assertEquals(0, zero.min());
    assertEquals(3677, stat.min());
  }

  @Test
  public void testGetMax() {
    assertEquals(0, zero.max());
    assertEquals(9792, stat.max());
  }

  @Test
  public void testGetAverage() {
    assertEquals(0, zero.mean(), delta);
    assertEquals(5529, stat.mean(), delta);
  }

  @Test
  public void testGetSum() {
    assertEquals(0, zero.sum());
    assertEquals(38703, stat.sum());
  }

  @Test
  public void testClear() {
    zero.clear();
    stat.clear();

    assertEquals(0, zero.max());
    assertEquals(zero.max(), stat.max());
    assertEquals(0, zero.min());
    assertEquals(zero.min(), stat.min());
    assertEquals(0, zero.sum());
    assertEquals(zero.sum(), stat.sum());

    assertEquals(Double.NaN, zero.mean(), 0);
    assertEquals(zero.mean(), stat.mean(), 0);
  }
}

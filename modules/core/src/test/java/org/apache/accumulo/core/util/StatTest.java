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
package org.apache.accumulo.core.util;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

public class StatTest {

  static double delta = 0.0000001;

  Stat zero;
  Stat stat;

  @Before
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
    assertEquals(0, zero.getMin());
    assertEquals(3677, stat.getMin());
  }

  @Test
  public void testGetMax() {
    assertEquals(0, zero.getMax());
    assertEquals(9792, stat.getMax());
  }

  @Test
  public void testGetAverage() {
    assertEquals(0, zero.getAverage(), delta);
    assertEquals(5529, stat.getAverage(), delta);
  }

  @Test
  public void testGetStdDev() {
    assertEquals(0, zero.getStdDev(), delta);
    assertEquals(2073.7656569632, stat.getStdDev(), delta);
  }

  @Test
  public void testGetSum() {
    assertEquals(0, zero.getSum());
    assertEquals(38703, stat.getSum());
  }

  @Test
  public void testClear() {
    zero.clear();
    stat.clear();

    assertEquals(0, zero.getMax());
    assertEquals(zero.getMax(), stat.getMax());
    assertEquals(0, zero.getMin());
    assertEquals(zero.getMin(), stat.getMin());
    assertEquals(0, zero.getSum());
    assertEquals(zero.getSum(), stat.getSum());

    assertEquals(Double.NaN, zero.getAverage(), 0);
    assertEquals(zero.getAverage(), stat.getAverage(), 0);
    assertEquals(Double.NaN, zero.getStdDev(), 0);
    assertEquals(zero.getStdDev(), stat.getStdDev(), 0);
  }
}

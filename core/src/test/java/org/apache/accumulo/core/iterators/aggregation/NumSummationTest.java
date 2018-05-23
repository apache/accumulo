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
package org.apache.accumulo.core.iterators.aggregation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated since 1.4
 */
@Deprecated
public class NumSummationTest {

  private static final Logger log = LoggerFactory.getLogger(NumSummationTest.class);

  public byte[] init(int n) {
    byte[] b = new byte[n];
    for (int i = 0; i < b.length; i++)
      b[i] = 0;
    return b;
  }

  @Test
  public void test1() {
    try {
      long[] la = {1L, 2L, 3L};
      byte[] b = NumArraySummation.longArrayToBytes(la);
      long[] la2 = NumArraySummation.bytesToLongArray(b);

      assertEquals(la.length, la2.length);
      for (int i = 0; i < la.length; i++) {
        assertEquals(i + ": " + la[i] + " does not equal " + la2[i], la[i], la2[i]);
      }
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void test2() {
    try {
      NumArraySummation nas = new NumArraySummation();
      long[] la = {1L, 2L, 3L};
      nas.collect(new Value(NumArraySummation.longArrayToBytes(la)));
      long[] la2 = {3L, 2L, 1L, 0L};
      nas.collect(new Value(NumArraySummation.longArrayToBytes(la2)));
      la = NumArraySummation.bytesToLongArray(nas.aggregate().get());
      assertEquals(4, la.length);
      for (int i = 0; i < la.length - 1; i++) {
        assertEquals(4, la[i]);
      }
      assertEquals(0, la[la.length - 1]);
      nas.reset();
      la = NumArraySummation.bytesToLongArray(nas.aggregate().get());
      assertEquals(0, la.length);
    } catch (Exception e) {
      log.error("{}", e.getMessage(), e);
      fail();
    }
  }

  @Test
  public void test3() {
    try {
      NumArraySummation nas = new NumArraySummation();
      long[] la = {Long.MAX_VALUE, Long.MIN_VALUE, 3L, -5L, 5L, 5L};
      nas.collect(new Value(NumArraySummation.longArrayToBytes(la)));
      long[] la2 = {1L, -3L, 2L, 10L};
      nas.collect(new Value(NumArraySummation.longArrayToBytes(la2)));
      la = NumArraySummation.bytesToLongArray(nas.aggregate().get());
      assertEquals(6, la.length);
      for (int i = 2; i < la.length; i++) {
        assertEquals(5, la[i]);
      }
      assertEquals("max long plus one was " + la[0], la[0], Long.MAX_VALUE);
      assertEquals("min long minus 3 was " + la[1], la[1], Long.MIN_VALUE);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void test4() {
    try {
      long l = 5L;
      byte[] b = NumSummation.longToBytes(l);
      long l2 = NumSummation.bytesToLong(b);

      assertEquals(l, l2);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void test5() {
    try {
      NumSummation ns = new NumSummation();
      for (long l = -5L; l < 8L; l++) {
        ns.collect(new Value(NumSummation.longToBytes(l)));
      }
      long l = NumSummation.bytesToLong(ns.aggregate().get());
      assertEquals("l was " + l, 13, l);

      ns.collect(new Value(NumSummation.longToBytes(Long.MAX_VALUE)));
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertEquals("l was " + l, l, Long.MAX_VALUE);

      ns.collect(new Value(NumSummation.longToBytes(Long.MIN_VALUE)));
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertEquals("l was " + l, l, -1);

      ns.collect(new Value(NumSummation.longToBytes(Long.MIN_VALUE)));
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertEquals("l was " + l, l, Long.MIN_VALUE);

      ns.collect(new Value(NumSummation.longToBytes(Long.MIN_VALUE)));
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertEquals("l was " + l, l, Long.MIN_VALUE);

      ns.reset();
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertEquals("l was " + l, 0, l);
    } catch (IOException | RuntimeException e) {
      fail();
    }
  }
}

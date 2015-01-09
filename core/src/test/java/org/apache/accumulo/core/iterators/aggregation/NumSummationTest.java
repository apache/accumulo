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

import junit.framework.TestCase;

import org.apache.accumulo.core.data.Value;

/**
 * @deprecated since 1.4
 */
@Deprecated
public class NumSummationTest extends TestCase {
  public byte[] init(int n) {
    byte[] b = new byte[n];
    for (int i = 0; i < b.length; i++)
      b[i] = 0;
    return b;
  }

  public void test1() {
    try {
      long[] la = {1l, 2l, 3l};
      byte[] b = NumArraySummation.longArrayToBytes(la);
      long[] la2 = NumArraySummation.bytesToLongArray(b);

      assertTrue(la.length == la2.length);
      for (int i = 0; i < la.length; i++) {
        assertTrue(i + ": " + la[i] + " does not equal " + la2[i], la[i] == la2[i]);
      }
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  public void test2() {
    try {
      NumArraySummation nas = new NumArraySummation();
      long[] la = {1l, 2l, 3l};
      nas.collect(new Value(NumArraySummation.longArrayToBytes(la)));
      long[] la2 = {3l, 2l, 1l, 0l};
      nas.collect(new Value(NumArraySummation.longArrayToBytes(la2)));
      la = NumArraySummation.bytesToLongArray(nas.aggregate().get());
      assertTrue(la.length == 4);
      for (int i = 0; i < la.length - 1; i++) {
        assertTrue(la[i] == 4);
      }
      assertTrue(la[la.length - 1] == 0);
      nas.reset();
      la = NumArraySummation.bytesToLongArray(nas.aggregate().get());
      assertTrue(la.length == 0);
    } catch (Exception e) {
      e.printStackTrace();
      assertTrue(false);
    }
  }

  public void test3() {
    try {
      NumArraySummation nas = new NumArraySummation();
      long[] la = {Long.MAX_VALUE, Long.MIN_VALUE, 3l, -5l, 5l, 5l};
      nas.collect(new Value(NumArraySummation.longArrayToBytes(la)));
      long[] la2 = {1l, -3l, 2l, 10l};
      nas.collect(new Value(NumArraySummation.longArrayToBytes(la2)));
      la = NumArraySummation.bytesToLongArray(nas.aggregate().get());
      assertTrue(la.length == 6);
      for (int i = 2; i < la.length; i++) {
        assertTrue(la[i] == 5);
      }
      assertTrue("max long plus one was " + la[0], la[0] == Long.MAX_VALUE);
      assertTrue("min long minus 3 was " + la[1], la[1] == Long.MIN_VALUE);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  public void test4() {
    try {
      long l = 5l;
      byte[] b = NumSummation.longToBytes(l);
      long l2 = NumSummation.bytesToLong(b);

      assertTrue(l == l2);
    } catch (Exception e) {
      assertTrue(false);
    }
  }

  public void test5() {
    try {
      NumSummation ns = new NumSummation();
      for (long l = -5l; l < 8l; l++) {
        ns.collect(new Value(NumSummation.longToBytes(l)));
      }
      long l = NumSummation.bytesToLong(ns.aggregate().get());
      assertTrue("l was " + l, l == 13);

      ns.collect(new Value(NumSummation.longToBytes(Long.MAX_VALUE)));
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertTrue("l was " + l, l == Long.MAX_VALUE);

      ns.collect(new Value(NumSummation.longToBytes(Long.MIN_VALUE)));
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertTrue("l was " + l, l == -1);

      ns.collect(new Value(NumSummation.longToBytes(Long.MIN_VALUE)));
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertTrue("l was " + l, l == Long.MIN_VALUE);

      ns.collect(new Value(NumSummation.longToBytes(Long.MIN_VALUE)));
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertTrue("l was " + l, l == Long.MIN_VALUE);

      ns.reset();
      l = NumSummation.bytesToLong(ns.aggregate().get());
      assertTrue("l was " + l, l == 0);
    } catch (Exception e) {
      assertTrue(false);
    }
  }
}

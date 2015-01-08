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
package org.apache.accumulo.test.randomwalk.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.accumulo.test.randomwalk.Fixture;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.hadoop.io.Text;

/**
 * When multiple instance of this test suite are run, all instances will operate on the same set of table names.
 *
 *
 */

public class ConcurrentFixture extends Fixture {

  @Override
  public void setUp(State state) throws Exception {}

  @Override
  public void tearDown(State state) throws Exception {
    state.remove(CheckBalance.LAST_UNBALANCED_TIME);
    state.remove(CheckBalance.UNBALANCED_COUNT);
  }

  /**
   *
   * @param rand
   *          A Random to use
   * @return A two element list with first being smaller than the second, but either value (or both) can be null
   */
  public static List<Text> generateRange(Random rand) {
    ArrayList<Text> toRet = new ArrayList<Text>(2);

    long firstLong = rand.nextLong();

    long secondLong = rand.nextLong();
    Text first = null, second = null;

    // Having all negative values = null might be too frequent
    if (firstLong >= 0)
      first = new Text(String.format("%016x", firstLong & 0x7fffffffffffffffl));
    if (secondLong >= 0)
      second = new Text(String.format("%016x", secondLong & 0x7fffffffffffffffl));

    if (first != null && second != null && first.compareTo(second) > 0) {
      Text swap = first;
      first = second;
      second = swap;
    }

    toRet.add(first);
    toRet.add(second);

    return toRet;
  }
}

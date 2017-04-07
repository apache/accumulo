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
package org.apache.accumulo.monitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.apache.accumulo.monitor.ZooKeeperStatus.ZooKeeperState;
import org.junit.Assert;
import org.junit.Test;

public class ZooKeeperStatusTest {

  @Test
  public void zkHostSortingTest() {
    List<String> expectedHosts = Arrays.asList("rack1node1", "rack2node1", "rack4node1", "rack4node4");

    // Add the states in a not correctly sorted order
    TreeSet<ZooKeeperState> states = new TreeSet<>();
    states.add(new ZooKeeperState("rack4node4", "leader", 10));
    states.add(new ZooKeeperState("rack4node1", "follower", 10));
    states.add(new ZooKeeperState("rack1node1", "follower", 10));
    states.add(new ZooKeeperState("rack2node1", "follower", 10));

    List<String> actualHosts = new ArrayList<>(4);
    for (ZooKeeperState state : states) {
      actualHosts.add(state.keeper);
    }

    // Assert we have 4 of each
    Assert.assertEquals(expectedHosts.size(), actualHosts.size());

    // Assert the ordering is correct
    for (int i = 0; i < expectedHosts.size(); i++) {
      Assert.assertEquals(expectedHosts.get(i), actualHosts.get(i));
    }

  }

}

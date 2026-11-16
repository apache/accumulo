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
package org.apache.accumulo.manager.multi;

import static org.apache.accumulo.manager.multi.ManagerAssignment.computeAssignments;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

public class ManagerAssignmentTest {
  @Test
  public void testComputeAssignments() {
    assertEquals(Map.of(), computeAssignments(Map.of(), Set.of("a", "b")));

    var hp1 = HostAndPort.fromParts("127.0.0.1", 5000);
    var hp2 = HostAndPort.fromParts("127.0.0.1", 5001);
    var hp3 = HostAndPort.fromParts("127.0.0.1", 5002);
    var hp4 = HostAndPort.fromParts("127.0.0.1", 5003);

    runTest(Map.of(hp1, Set.of()), Set.of("a"), 1, 1, 1);
    runTest(Map.of(hp1, Set.of()), Set.of("a", "b"), 2, 2, 1);

    Map<HostAndPort,Set<String>> threeEmptyHost =
        Map.of(hp1, Set.of(), hp2, Set.of(), hp3, Set.of());
    runTest(threeEmptyHost, Set.of("a"), 0, 1, 1);
    runTest(threeEmptyHost, Set.of("a", "b"), 0, 1, 2);
    runTest(threeEmptyHost, Set.of("a", "b", "c"), 1, 1, 3);
    runTest(threeEmptyHost, Set.of("a", "b", "c", "d"), 1, 2, 1);
    runTest(threeEmptyHost, Set.of("a", "b", "c", "d", "e"), 1, 2, 2);
    runTest(threeEmptyHost, Set.of("a", "b", "c", "d", "e", "f"), 2, 2, 3);
    runTest(threeEmptyHost, Set.of("a", "b", "c", "d", "e", "f", "g"), 2, 3, 1);

    Map<HostAndPort,Set<String>> fourEmptyHost =
        Map.of(hp1, Set.of(), hp2, Set.of(), hp3, Set.of(), hp4, Set.of());
    runTest(fourEmptyHost, Set.of("a"), 0, 1, 1);
    runTest(fourEmptyHost, Set.of("a", "b"), 0, 1, 2);
    runTest(fourEmptyHost, Set.of("a", "b", "c"), 0, 1, 3);
    runTest(fourEmptyHost, Set.of("a", "b", "c", "d"), 1, 1, 4);
    runTest(fourEmptyHost, Set.of("a", "b", "c", "d", "e"), 1, 2, 1);

    var assignments =
        runTest(Map.of(hp1, Set.of("a", "z"), hp2, Set.of("b", "c", "x"), hp3, Set.of("d", "e")),
            Set.of("a", "b", "c", "d", "e", "f"), 2, 2, 3);
    assertEquals(Map.of(hp1, Set.of("a", "f"), hp2, Set.of("b", "c"), hp3, Set.of("d", "e")),
        assignments);

    assignments =
        runTest(Map.of(hp1, Set.of("a", "b", "c"), hp2, Set.of("d", "x"), hp3, Set.of("e")),
            Set.of("a", "b", "c", "d", "e", "f"), 2, 2, 3);
    assertEquals(2, Sets.intersection(assignments.get(hp1), Set.of("a", "b", "c")).size());
    assertTrue(assignments.get(hp2).contains("d"));
    assertTrue(assignments.get(hp3).contains("e"));

    assignments =
        runTest(Map.of(hp1, Set.of("a", "b", "c"), hp2, Set.of("d", "e", "f", "g"), hp3, Set.of()),
            Set.of("a", "b", "c", "d", "e", "f", "g"), 2, 3, 1);
    int iss1 = Sets.intersection(assignments.get(hp1), Set.of("a", "b", "c")).size();
    int iss2 = Sets.intersection(assignments.get(hp2), Set.of("d", "e", "f", "g")).size();
    assertTrue((iss1 == 2 && iss2 == 3) || (iss1 == 3 && iss2 == 2));
  }

  @Test
  public void testBug() {
    var hp1 = HostAndPort.fromParts("127.0.0.1", 5000);
    var hp2 = HostAndPort.fromParts("127.0.0.1", 5001);
    var hp3 = HostAndPort.fromParts("127.0.0.1", 5002);
    var hp4 = HostAndPort.fromParts("127.0.0.1", 5003);
    var hp5 = HostAndPort.fromParts("127.0.0.1", 5004);

    var assignments = runTest(
        Map.of(hp1, Set.of("a", "b", "c"), hp2, Set.of("d", "e", "f"), hp3, Set.of("g", "h", "i"),
            hp4, Set.of(), hp5, Set.of()),
        Set.of("a", "b", "c", "d", "e", "f", "g", "h", "i"), 1, 2, 4);
    assertEquals(2, Sets.intersection(assignments.get(hp1), Set.of("a", "b", "c")).size());
    assertEquals(2, Sets.intersection(assignments.get(hp2), Set.of("d", "e", "f")).size());
    assertEquals(2, Sets.intersection(assignments.get(hp3), Set.of("g", "h", "i")).size());
  }

  private Map<HostAndPort,Set<String>> runTest(Map<HostAndPort,Set<String>> currentAssignments,
      Set<String> parts, int expectedMin, int expectedMax, int expectedHostWithMax) {
    var assignments = computeAssignments(currentAssignments, parts);
    assertEquals(currentAssignments.keySet(), assignments.keySet());
    HashSet<String> partsCopy = new HashSet<>(parts);

    // ensure every partition is assigned and only assigned once
    assignments.values().forEach(hostAssignments -> {
      assertTrue(partsCopy.containsAll(hostAssignments), () -> partsCopy + " " + hostAssignments);
      partsCopy.removeAll(hostAssignments);
      assertTrue(hostAssignments.size() == expectedMin || hostAssignments.size() == expectedMax);
    });

    assertEquals(expectedHostWithMax,
        assignments.values().stream().mapToInt(Set::size).filter(s -> s == expectedMax).count());

    return assignments;
  }
}

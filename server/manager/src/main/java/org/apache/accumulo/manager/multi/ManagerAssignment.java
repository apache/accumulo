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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

public abstract class ManagerAssignment {

  /**
   * Generic utility code to assign partitions to host. It evenly spread the partitions across host
   * and tried to keep partition already on a host in place if possible.
   */
  public static <T> Map<HostAndPort,Set<T>> computeAssignments(Map<HostAndPort,Set<T>> current,
      Set<T> desiredPartitions) {
    if (current.isEmpty()) {
      return Map.of();
    }

    int minAssignments = desiredPartitions.size() / current.size();
    int maxAssignments = minAssignments + Math.min(desiredPartitions.size() % current.size(), 1);
    int hostThatCanHaveMax = desiredPartitions.size() % current.size();

    Map<HostAndPort,Set<T>> assignments = new HashMap<>();
    Set<T> assigned = new HashSet<>();

    Set<HostAndPort> hostThatHaveMax = new HashSet<>();
    // Make a pass through to keep anything in place that can be kept
    for (var entry : current.entrySet()) {
      HostAndPort hp = entry.getKey();
      Set<T> currParts = entry.getValue();
      var hostAssignments = new HashSet<T>();
      var iter =
          Sets.intersection(desiredPartitions, Sets.difference(currParts, assigned)).iterator();

      int canAssign = hostThatHaveMax.size() < hostThatCanHaveMax ? maxAssignments : minAssignments;

      while (iter.hasNext() && hostAssignments.size() < canAssign) {
        hostAssignments.add(iter.next());
      }

      if (hostAssignments.size() >= maxAssignments) {
        hostThatHaveMax.add(hp);
      }

      assignments.put(hp, hostAssignments);
      assigned.addAll(hostAssignments);
    }

    // Make a second pass to get every partition assigned
    for (var hp : current.keySet()) {
      var hostAssignments = assignments.get(hp);
      int canAssign = hostThatHaveMax.size() < hostThatCanHaveMax ? maxAssignments : minAssignments;
      var iter = Sets.difference(desiredPartitions, assigned).iterator();
      while (iter.hasNext() && hostAssignments.size() < canAssign) {
        hostAssignments.add(iter.next());
      }
      if (hostAssignments.size() >= maxAssignments) {
        hostThatHaveMax.add(hp);
      }
      assigned.addAll(hostAssignments);
    }

    return assignments;
  }
}

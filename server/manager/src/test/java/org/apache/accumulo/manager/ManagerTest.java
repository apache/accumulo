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
package org.apache.accumulo.manager;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.metadata.TServerInstance;
import org.junit.jupiter.api.Test;

public class ManagerTest {

  @Test
  public void cleanByHostTest() {
    final TServerInstance server1 = new TServerInstance("host1:1234[SESSION1]");
    final TServerInstance server2 = new TServerInstance("host1:1234[SESSION2]");
    final TServerInstance server3 = new TServerInstance("host1:1235[SESSION3]");
    final TServerInstance server4 = new TServerInstance("host2:1234[SESSION4]");

    Set<TServerInstance> servers = new HashSet<>(List.of(server1, server2, server3, server4));
    // check deleted set removes by host port
    Manager.cleanListByHostAndPort(servers, Set.of(new TServerInstance("host1:1234[SESSION5]")),
        Set.of());
    assertEquals(Set.of(server3, server4), servers);

    servers = new HashSet<>(List.of(server1, server2, server3, server4));
    // check added set removes by host port
    Manager.cleanListByHostAndPort(servers, Set.of(),
        Set.of(new TServerInstance("host1:1234[SESSION5]")));
    assertEquals(Set.of(server3, server4), servers);

    // check using both sets
    servers = new HashSet<>(List.of(server1, server2, server3, server4));
    Manager.cleanListByHostAndPort(servers, Set.of(new TServerInstance("host1:1235[SESSION5]")),
        Set.of(new TServerInstance("host1:1234[SESSION6]")));
    assertEquals(Set.of(server4), servers);

    // Test empty sets
    servers = new HashSet<>(List.of(server1, server2, server3, server4));
    Manager.cleanListByHostAndPort(servers, Set.of(), Set.of());
    assertEquals(Set.of(server1, server2, server3, server4), servers);
    servers.clear();
    Manager.cleanListByHostAndPort(servers, Set.of(),
        Set.of(new TServerInstance("host1:1234[SESSION5]")));
    assertEquals(Set.of(), servers);
  }
}

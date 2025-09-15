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

import static org.apache.accumulo.manager.TabletGroupWatcher.findServerIgnoringSession;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.TreeMap;

import org.apache.accumulo.core.metadata.TServerInstance;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class TabletGroupWatcherTest {
  @Test
  public void testFindingServer() {
    TreeMap<TServerInstance,String> servers = new TreeMap<>();

    servers.put(new TServerInstance("192.168.1.2:9997", 50L), "tserver1");
    servers.put(new TServerInstance("192.168.1.4:9997", -90L), "tserver2");

    assertNull(findServerIgnoringSession(servers, HostAndPort.fromString("192.168.1.1:9997")));
    assertNull(findServerIgnoringSession(servers, HostAndPort.fromString("192.168.1.2:9996")));
    assertNull(findServerIgnoringSession(servers, HostAndPort.fromString("192.168.1.2:9998")));
    assertNull(findServerIgnoringSession(servers, HostAndPort.fromString("192.168.1.3:9997")));
    assertNull(findServerIgnoringSession(servers, HostAndPort.fromString("192.168.1.5:9997")));

    assertEquals(new TServerInstance("192.168.1.2:9997", 50L),
        findServerIgnoringSession(servers, HostAndPort.fromString("192.168.1.2:9997")));
    assertEquals(new TServerInstance("192.168.1.4:9997", -90L),
        findServerIgnoringSession(servers, HostAndPort.fromString("192.168.1.4:9997")));
  }
}

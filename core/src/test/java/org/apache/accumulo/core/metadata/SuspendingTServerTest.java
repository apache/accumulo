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
package org.apache.accumulo.core.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.util.time.SteadyTime;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class SuspendingTServerTest {
  @Test
  public void testToFromValue() {
    SteadyTime suspensionTime = SteadyTime.from(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    TServerInstance ser1 = new TServerInstance(HostAndPort.fromParts("server1", 8555), "s001");

    var val1 = SuspendingTServer.toValue(ser1, suspensionTime);
    var st1 = SuspendingTServer.fromValue(val1);
    assertEquals(HostAndPort.fromParts("server1", 8555), st1.server);
    assertEquals(suspensionTime, st1.suspensionTime);
    assertEquals(val1, st1.toValue());
    var st2 = new SuspendingTServer(HostAndPort.fromParts("server1", 8555), suspensionTime);
    assertEquals(st1, st2);
    assertEquals(st1.hashCode(), st2.hashCode());
    assertEquals(st1.toString(), st2.toString());
    assertEquals(st1.toValue(), st2.toValue());

    // Create three SuspendingTServer objs that differ in one field. Ensure each field is considered
    // in equality checks.
    var st3 = new SuspendingTServer(HostAndPort.fromParts("server2", 8555), suspensionTime);
    var st4 = new SuspendingTServer(HostAndPort.fromParts("server1", 9555), suspensionTime);
    SteadyTime suspensionTime2 =
        SteadyTime.from(System.currentTimeMillis() + 100, TimeUnit.MILLISECONDS);
    var st5 = new SuspendingTServer(HostAndPort.fromParts("server1", 8555), suspensionTime2);
    for (var stne : List.of(st3, st4, st5)) {
      assertNotEquals(st1, stne);
      assertNotEquals(st1.toValue(), stne.toValue());
      assertNotEquals(st1.toString(), stne.toString());
      assertNotEquals(st1.hashCode(), stne.hashCode());
    }
  }
}

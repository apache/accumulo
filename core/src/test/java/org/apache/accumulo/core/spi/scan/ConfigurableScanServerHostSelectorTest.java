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
package org.apache.accumulo.core.spi.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.clientImpl.ServerIdUtil;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelectorTest.InitParams;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelectorTest.SelectorParams;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelectorTest.TestScanServerAttempt;
import org.apache.accumulo.core.spi.scan.ScanServerAttempt.Result;
import org.junit.jupiter.api.Test;

public class ConfigurableScanServerHostSelectorTest {

  private final ServerId ss1 = ServerIdUtil.sserver("host1", 2000);
  private final ServerId ss2 = ServerIdUtil.sserver("host1", 2001);
  private final ServerId ss3 = ServerIdUtil.sserver("host1", 2002);
  private final ServerId ss4 = ServerIdUtil.sserver("host1", 2003);
  private final ServerId ss5 = ServerIdUtil.sserver("host2", 2000);
  private final ServerId ss6 = ServerIdUtil.sserver("host2", 2001);
  private final ServerId ss7 = ServerIdUtil.sserver("host2", 2002);
  private final ServerId ss8 = ServerIdUtil.sserver("host2", 2003);
  private final ServerId ss9 = ServerIdUtil.sserver("host3", 2000);

  private final Set<ServerId> host1Servers = Set.of(ss1, ss2, ss3, ss4);
  private final Set<ServerId> host2Servers = Set.of(ss5, ss6, ss7, ss8);
  private final Set<ServerId> host3Servers = Set.of(ss9);

  @Test
  public void test() {

    boolean firstHostSeen = false;
    boolean secondHostSeen = false;
    boolean thirdHostSeen = false;

    final TabletId tId = ConfigurableScanServerSelectorTest.nti("1", "m");

    final ConfigurableScanServerHostSelector selector = new ConfigurableScanServerHostSelector();
    selector.init(new InitParams(
        Set.of(ss1.toHostPortString(), ss2.toHostPortString(), ss3.toHostPortString(),
            ss4.toHostPortString(), ss5.toHostPortString(), ss6.toHostPortString(),
            ss7.toHostPortString(), ss8.toHostPortString(), ss9.toHostPortString())));

    ScanServerSelections selection = selector.selectServers(new SelectorParams(tId));
    assertNotNull(selection);
    final ServerId firstServer = selection.getScanServer(tId);
    assertNotNull(firstServer);

    final Set<ServerId> remainingServers = new HashSet<>();
    if (host1Servers.contains(firstServer)) {
      remainingServers.addAll(host1Servers);
      firstHostSeen = true;
    } else if (host2Servers.contains(firstServer)) {
      remainingServers.addAll(host2Servers);
      secondHostSeen = true;
    } else if (host3Servers.contains(firstServer)) {
      remainingServers.addAll(host3Servers);
      thirdHostSeen = true;
    } else {
      fail("First server unknown: " + firstServer);
    }

    assertTrue(remainingServers.remove(firstServer));
    final List<ScanServerAttempt> attempts = new ArrayList<>();
    attempts.add(new TestScanServerAttempt(firstServer, Result.BUSY));
    while (!remainingServers.isEmpty()) {
      selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
      ServerId selectedServer = selection.getScanServer(tId);
      assertEquals(selectedServer.getHost(), firstServer.getHost());
      assertTrue(remainingServers.remove(selectedServer));
      attempts.add(new TestScanServerAttempt(selectedServer, Result.BUSY));
    }

    // At this point we should have exhausted all of the scan servers on the first selected host
    selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
    ServerId secondServer = selection.getScanServer(tId);
    assertFalse(secondServer.getHost().equals(firstServer.getHost()));

    if (host1Servers.contains(secondServer)) {
      if (firstHostSeen) {
        fail("First host reused");
      }
      remainingServers.addAll(host1Servers);
      firstHostSeen = true;
    } else if (host2Servers.contains(secondServer)) {
      if (secondHostSeen) {
        fail("Second host reused");
      }
      remainingServers.addAll(host2Servers);
      secondHostSeen = true;
    } else if (host3Servers.contains(secondServer)) {
      if (thirdHostSeen) {
        fail("Third host reused");
      }
      remainingServers.addAll(host3Servers);
      thirdHostSeen = true;
    } else {
      fail("Second server unknown: " + secondServer);
    }

    assertTrue(remainingServers.remove(secondServer));
    attempts.add(new TestScanServerAttempt(secondServer, Result.BUSY));
    while (!remainingServers.isEmpty()) {
      selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
      ServerId selectedServer = selection.getScanServer(tId);
      assertEquals(selectedServer.getHost(), secondServer.getHost());
      assertTrue(remainingServers.remove(selectedServer));
      attempts.add(new TestScanServerAttempt(selectedServer, Result.BUSY));
    }

    // At this point we should have exhausted all of the scan servers on the first and second
    // selected host
    selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
    ServerId thirdServer = selection.getScanServer(tId);
    assertFalse(thirdServer.getHost().equals(firstServer.getHost()));
    assertFalse(thirdServer.getHost().equals(secondServer.getHost()));

    if (host1Servers.contains(thirdServer)) {
      if (firstHostSeen) {
        fail("First host reused");
      }
      remainingServers.addAll(host1Servers);
      firstHostSeen = true;
    } else if (host2Servers.contains(thirdServer)) {
      if (secondHostSeen) {
        fail("Second host reused");
      }
      remainingServers.addAll(host2Servers);
      secondHostSeen = true;
    } else if (host3Servers.contains(thirdServer)) {
      if (thirdHostSeen) {
        fail("Third host reused");
      }
      remainingServers.addAll(host3Servers);
      thirdHostSeen = true;
    } else {
      fail("Third server unknown: " + thirdServer);
    }

    assertTrue(remainingServers.remove(thirdServer));
    attempts.add(new TestScanServerAttempt(thirdServer, Result.BUSY));
    while (!remainingServers.isEmpty()) {
      selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
      ServerId selectedServer = selection.getScanServer(tId);
      assertEquals(selectedServer.getHost(), thirdServer.getHost());
      assertTrue(remainingServers.remove(selectedServer));
      attempts.add(new TestScanServerAttempt(selectedServer, Result.BUSY));
    }

    // at this point all servers should be exhausted
    assertTrue(firstHostSeen);
    assertTrue(secondHostSeen);
    assertTrue(thirdHostSeen);
    selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
    assertNull(selection.getScanServer(tId));

  }
}

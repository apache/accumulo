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

import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelectorTest.InitParams;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelectorTest.SelectorParams;
import org.apache.accumulo.core.spi.scan.ConfigurableScanServerSelectorTest.TestScanServerAttempt;
import org.apache.accumulo.core.spi.scan.ScanServerAttempt.Result;
import org.junit.jupiter.api.Test;

import com.google.common.net.HostAndPort;

public class ConfigurableScanServerHostSelectorTest {

  private final String ss1 = "host1:2000";
  private final String ss2 = "host1:2001";
  private final String ss3 = "host1:2002";
  private final String ss4 = "host1:2003";
  private final String ss5 = "host2:2000";
  private final String ss6 = "host2:2001";
  private final String ss7 = "host2:2002";
  private final String ss8 = "host2:2003";
  private final String ss9 = "host3:2000";

  private final Set<String> host1Servers = Set.of(ss1, ss2, ss3, ss4);
  private final Set<String> host2Servers = Set.of(ss5, ss6, ss7, ss8);
  private final Set<String> host3Servers = Set.of(ss9);

  @Test
  public void test() {

    boolean firstHostSeen = false;
    boolean secondHostSeen = false;
    boolean thirdHostSeen = false;

    final TabletId tId = ConfigurableScanServerSelectorTest.nti("1", "m");

    final ConfigurableScanServerHostSelector selector = new ConfigurableScanServerHostSelector();
    selector.init(new InitParams(Set.of(ss1, ss2, ss3, ss4, ss5, ss6, ss7, ss8, ss9)));

    ScanServerSelections selection = selector.selectServers(new SelectorParams(tId));
    assertNotNull(selection);
    final String firstServer = selection.getScanServer(tId);
    assertNotNull(firstServer);
    final HostAndPort firstHP = HostAndPort.fromString(firstServer);

    final Set<String> remainingServers = new HashSet<>();
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
      String selectedServer = selection.getScanServer(tId);
      HostAndPort selectedHP = HostAndPort.fromString(selectedServer);
      assertEquals(selectedHP.getHost(), firstHP.getHost());
      assertTrue(remainingServers.remove(selectedServer));
      attempts.add(new TestScanServerAttempt(selectedServer, Result.BUSY));
    }

    // At this point we should have exhausted all of the scan servers on the first selected host
    selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
    String secondServer = selection.getScanServer(tId);
    final HostAndPort secondHP = HostAndPort.fromString(secondServer);
    assertFalse(secondHP.getHost().equals(firstHP.getHost()));

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
      String selectedServer = selection.getScanServer(tId);
      HostAndPort selectedHP = HostAndPort.fromString(selectedServer);
      assertEquals(selectedHP.getHost(), secondHP.getHost());
      assertTrue(remainingServers.remove(selectedServer));
      attempts.add(new TestScanServerAttempt(selectedServer, Result.BUSY));
    }

    // At this point we should have exhausted all of the scan servers on the first and second
    // selected host
    selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
    String thirdServer = selection.getScanServer(tId);
    final HostAndPort thirdHP = HostAndPort.fromString(thirdServer);
    assertFalse(thirdHP.getHost().equals(firstHP.getHost()));
    assertFalse(thirdHP.getHost().equals(secondHP.getHost()));

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
      String selectedServer = selection.getScanServer(tId);
      HostAndPort selectedHP = HostAndPort.fromString(selectedServer);
      assertEquals(selectedHP.getHost(), thirdHP.getHost());
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

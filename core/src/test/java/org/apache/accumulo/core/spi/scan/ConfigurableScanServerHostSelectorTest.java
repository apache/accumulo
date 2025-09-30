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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
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
  private final String ssa = "host4:2000";
  private final String ssb = "host4:2001";

  private final Set<String> host1Servers = Set.of(ss1, ss2, ss3, ss4);
  private final Set<String> host2Servers = Set.of(ss5, ss6, ss7, ss8);
  private final Set<String> host3Servers = Set.of(ss9);
  private final Set<String> host4Servers = Set.of(ssa, ssb);

  private final Map<String,Set<String>> hostsServers = Map.of("host1", host1Servers, "host2",
      host2Servers, "host3", host3Servers, "host4", host4Servers);

  @Test
  public void test() {

    String defaultProfile =
        "{'isDefault':true,'maxBusyTimeout':'5m','busyTimeoutMultiplier':4,'timeToWaitForScanServers':'120s',"
            + "'attemptPlans':[{'servers':1, 'busyTimeout':'10ms'},{'servers':2, 'busyTimeout':'20ms'},{'servers':3, 'busyTimeout':'30ms'}]}";

    var opts = Map.of("profiles", "[" + defaultProfile + "]".replace('\'', '"'));

    Set<String> hostSeen = new HashSet<>();

    final TabletId tId = ConfigurableScanServerSelectorTest.nti("1", "m");

    final ConfigurableScanServerHostSelector selector = new ConfigurableScanServerHostSelector();
    selector
        .init(new InitParams(Set.of(ss1, ss2, ss3, ss4, ss5, ss6, ss7, ss8, ss9, ssa, ssb), opts));

    ScanServerSelections selection = selector.selectServers(new SelectorParams(tId));
    assertNotNull(selection);
    final String firstServer = selection.getScanServer(tId);
    assertNotNull(firstServer);
    final HostAndPort firstHP = HostAndPort.fromString(firstServer);

    assertTrue(hostsServers.containsKey(firstHP.getHost()));
    final Set<String> remainingServers = new HashSet<>(hostsServers.get(firstHP.getHost()));
    hostSeen.add(firstHP.getHost());

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
      assertEquals(Duration.ofMillis(10), selection.getBusyTimeout());
    }

    // At this point we should have exhausted all of the scan servers on the first selected host
    selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
    String secondServer = selection.getScanServer(tId);
    final HostAndPort secondHP = HostAndPort.fromString(secondServer);
    assertNotEquals(secondHP.getHost(), firstHP.getHost());

    assertTrue(hostsServers.containsKey(secondHP.getHost()));
    remainingServers.addAll(hostsServers.get(secondHP.getHost()));
    assertTrue(hostSeen.add(secondHP.getHost()));

    assertTrue(remainingServers.remove(secondServer));
    attempts.add(new TestScanServerAttempt(secondServer, Result.BUSY));
    while (!remainingServers.isEmpty()) {
      selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
      String selectedServer = selection.getScanServer(tId);
      HostAndPort selectedHP = HostAndPort.fromString(selectedServer);
      assertEquals(selectedHP.getHost(), secondHP.getHost());
      assertTrue(remainingServers.remove(selectedServer));
      attempts.add(new TestScanServerAttempt(selectedServer, Result.BUSY));
      assertEquals(Duration.ofMillis(20), selection.getBusyTimeout());
    }

    // At this point we should have exhausted all of the scan servers on the first and second
    // selected host
    selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
    String thirdServer = selection.getScanServer(tId);
    final HostAndPort thirdHP = HostAndPort.fromString(thirdServer);
    assertNotEquals(thirdHP.getHost(), firstHP.getHost());
    assertNotEquals(thirdHP.getHost(), secondHP.getHost());

    assertTrue(hostsServers.containsKey(thirdHP.getHost()));
    remainingServers.addAll(hostsServers.get(thirdHP.getHost()));
    assertTrue(hostSeen.add(thirdHP.getHost()));

    assertTrue(remainingServers.remove(thirdServer));
    attempts.add(new TestScanServerAttempt(thirdServer, Result.BUSY));
    while (!remainingServers.isEmpty()) {
      selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));
      String selectedServer = selection.getScanServer(tId);
      HostAndPort selectedHP = HostAndPort.fromString(selectedServer);
      assertEquals(selectedHP.getHost(), thirdHP.getHost());
      assertTrue(remainingServers.remove(selectedServer));
      attempts.add(new TestScanServerAttempt(selectedServer, Result.BUSY));
      assertEquals(Duration.ofMillis(30), selection.getBusyTimeout());
    }

    // at this point all servers should be exhausted
    assertEquals(3, hostSeen.size());
    selection = selector.selectServers(new SelectorParams(tId, Map.of(tId, attempts), Map.of()));

    // in the case where all servers have failed, any server can be selected, however it should be
    // one of the 3 host chosen before.
    assertNotNull(selection.getScanServer(tId));
    assertTrue(hostSeen.contains(HostAndPort.fromString(selection.getScanServer(tId)).getHost()));

  }
}

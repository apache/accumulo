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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class DefaultScanServerDispatcherTest {

  static class InitParams implements ScanServerDispatcher.InitParameters {

    private final Map<String,String> opts;
    private final Set<String> scanServers;

    InitParams(Map<String,String> opts, Set<String> scanServers) {
      this.opts = opts;
      this.scanServers = scanServers;
    }

    InitParams(Set<String> scanServers) {
      this.opts = Map.of();
      this.scanServers = scanServers;
    }

    InitParams(Set<String> scanServers, Map<String,String> opts) {
      this.opts = opts;
      this.scanServers = scanServers;
    }

    @Override
    public Map<String,String> getOptions() {
      return opts;
    }

    @Override
    public ServiceEnvironment getServiceEnv() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Supplier<Set<String>> getScanServers() {
      return () -> scanServers;
    }
  }

  static class DaParams implements ScanServerDispatcher.DispatcherParameters {

    private final Collection<TabletId> tablets;
    private final Map<TabletId,Collection<? extends ScanServerDispatcher.ScanAttempt>> attempts;
    private final Map<String,String> hints;

    DaParams(TabletId tablet) {
      this.tablets = Set.of(tablet);
      this.attempts = Map.of();
      this.hints = Map.of();
    }

    DaParams(TabletId tablet,
        Map<TabletId,Collection<? extends ScanServerDispatcher.ScanAttempt>> attempts,
        Map<String,String> hints) {
      this.tablets = Set.of(tablet);
      this.attempts = attempts;
      this.hints = hints;
    }

    @Override
    public Collection<TabletId> getTablets() {
      return tablets;
    }

    @Override
    public Collection<? extends ScanServerDispatcher.ScanAttempt> getAttempts(TabletId tabletId) {
      return attempts.getOrDefault(tabletId, Set.of());
    }

    @Override
    public Map<String,String> getHints() {
      return hints;
    }
  }

  static class TestScanAttempt implements ScanServerDispatcher.ScanAttempt {

    private final String server;
    private final long endTime;
    private final Result result;

    TestScanAttempt(String server, long endTime, Result result) {
      this.server = server;
      this.endTime = endTime;
      this.result = result;
    }

    @Override
    public String getServer() {
      return server;
    }

    @Override
    public long getEndTime() {
      return endTime;
    }

    @Override
    public Result getResult() {
      return result;
    }
  }

  public static TabletId nti(String tableId, String endRow) {
    return new TabletIdImpl(
        new KeyExtent(TableId.of(tableId), endRow == null ? null : new Text(endRow), null));
  }

  @Test
  public void testBasic() {
    DefaultScanServerDispatcher dispatcher = new DefaultScanServerDispatcher();
    dispatcher.init(new InitParams(
        Set.of("ss1:1", "ss2:2", "ss3:3", "ss4:4", "ss5:5", "ss6:6", "ss7:7", "ss8:8")));

    Set<String> servers = new HashSet<>();

    for (int i = 0; i < 100; i++) {
      var tabletId = nti("1", "m");

      ScanServerDispatcher.Actions actions = dispatcher.determineActions(new DaParams(tabletId));

      servers.add(actions.getScanServer(tabletId));
    }

    assertEquals(3, servers.size());
  }

  private void runBusyTest(int numServers, int busyAttempts, int expectedServers,
      long expectedBusyTimeout) {
    runBusyTest(numServers, busyAttempts, expectedServers, expectedBusyTimeout, Map.of());
  }

  private void runBusyTest(int numServers, int busyAttempts, int expectedServers,
      long expectedBusyTimeout, Map<String,String> opts) {
    runBusyTest(numServers, busyAttempts, expectedServers, expectedBusyTimeout, opts, Map.of());
  }

  private void runBusyTest(int numServers, int busyAttempts, int expectedServers,
      long expectedBusyTimeout, Map<String,String> opts, Map<String,String> hints) {
    DefaultScanServerDispatcher dispatcher = new DefaultScanServerDispatcher();

    var servers = Stream.iterate(1, i -> i <= numServers, i -> i + 1).map(i -> "s" + i + ":" + i)
        .collect(Collectors.toSet());

    dispatcher.init(new InitParams(servers, opts));

    Set<String> serversSeen = new HashSet<>();

    var tabletId = nti("1", "m");

    var tabletAttempts = Stream.iterate(1, i -> i <= busyAttempts, i -> i + 1)
        .map(i -> (new TestScanAttempt("ss" + i + ":" + i, i,
            ScanServerDispatcher.ScanAttempt.Result.BUSY)))
        .collect(Collectors.toList());

    Map<TabletId,Collection<? extends ScanServerDispatcher.ScanAttempt>> attempts = new HashMap<>();
    attempts.put(tabletId, tabletAttempts);

    for (int i = 0; i < 100 * numServers; i++) {
      ScanServerDispatcher.Actions actions =
          dispatcher.determineActions(new DaParams(tabletId, attempts, hints));

      assertEquals(expectedBusyTimeout, actions.getBusyTimeout().toMillis());
      assertEquals(0, actions.getDelay().toMillis());

      serversSeen.add(actions.getScanServer(tabletId));
    }

    assertEquals(expectedServers, serversSeen.size());
  }

  @Test
  public void testBusy() {
    runBusyTest(1000, 0, 3, 33);
    runBusyTest(1000, 1, 13, 33);
    runBusyTest(1000, 2, 1000, 33);
    runBusyTest(1000, 3, 1000, 33 * 8);
    runBusyTest(1000, 4, 1000, 33 * 8 * 8);
    runBusyTest(1000, 6, 1000, 33 * 8 * 8 * 8 * 8);
    runBusyTest(1000, 7, 1000, 300000);
    runBusyTest(1000, 20, 1000, 300000);

    runBusyTest(27, 0, 3, 33);
    runBusyTest(27, 1, 13, 33);
    runBusyTest(27, 2, 27, 33);
    runBusyTest(27, 3, 27, 33 * 8);

    runBusyTest(6, 0, 3, 33);
    runBusyTest(6, 1, 6, 33);
    runBusyTest(6, 2, 6, 33);
    runBusyTest(6, 3, 6, 33 * 8);

    for (int i = 0; i < 2; i++) {
      runBusyTest(1, i, 1, 33);
      runBusyTest(2, i, 2, 33);
      runBusyTest(3, i, 3, 33);
    }
  }

  @Test
  public void testCoverage() {
    DefaultScanServerDispatcher dispatcher = new DefaultScanServerDispatcher();
    var servers = Stream.iterate(1, i -> i <= 20, i -> i + 1).map(i -> "s" + i + ":" + i)
        .collect(Collectors.toSet());
    dispatcher.init(new InitParams(servers));

    Map<String,Long> allServersSeen = new HashMap<>();

    SecureRandom rand = new SecureRandom();

    for (int t = 0; t < 10000; t++) {
      Set<String> serversSeen = new HashSet<>();

      String endRow = Long.toString(Math.abs(Math.max(rand.nextLong(), Long.MIN_VALUE + 1)), 36);

      var tabletId = t % 1000 == 0 ? nti("" + t, null) : nti("" + t, endRow);

      for (int i = 0; i < 100; i++) {
        ScanServerDispatcher.Actions actions = dispatcher.determineActions(new DaParams(tabletId));
        serversSeen.add(actions.getScanServer(tabletId));
        allServersSeen.merge(actions.getScanServer(tabletId), 1L, Long::sum);
      }

      assertEquals(3, serversSeen.size());
    }

    assertEquals(20, allServersSeen.size());

    var stats = allServersSeen.values().stream().mapToLong(l -> l - 50000).summaryStatistics();

    assertTrue(stats.getMin() > -5000 && stats.getMax() < 5000);
  }

  @Test
  public void testOpts() {

    String defaultProfile = "{'isDefault':true,'maxBusyTimeout':'5m','busyTimeoutMultiplier':4, "
        + "'attemptPlans':[{'servers':'5', 'busyTimeout':'5ms'},"
        + "{'servers':'20', 'busyTimeout':'33ms'}," + "{'servers':'50%', 'busyTimeout':'100ms'},"
        + "{'servers':'100%', 'busyTimeout':'200ms'}]" + "}";

    String profile1 = "{'scanTypeActivations':['long','st9'],'maxBusyTimeout':'30m',"
        + "'busyTimeoutMultiplier':4, " + "'attemptPlans':[{'servers':'2', 'busyTimeout':'10s'},"
        + "{'servers':'4', 'busyTimeout':'2m'}," + "{'servers':'10%', 'busyTimeout':'5m'}]" + "}";

    String profile2 =
        "{'scanTypeActivations':['mega'],'maxBusyTimeout':'60m'," + "'busyTimeoutMultiplier':2, "
            + "'attemptPlans':[{'servers':'100%', 'busyTimeout':'10m'}]" + "}";

    var opts = Map.of("profiles",
        "[" + defaultProfile + ", " + profile1 + "," + profile2 + "]".replace('\'', '"'));

    runBusyTest(1000, 0, 5, 5, opts);
    runBusyTest(1000, 1, 20, 33, opts);
    runBusyTest(1000, 2, 500, 100, opts);
    runBusyTest(1000, 3, 1000, 200, opts);
    runBusyTest(1000, 4, 1000, 200 * 4, opts);
    runBusyTest(1000, 5, 1000, 200 * 4 * 4, opts);
    runBusyTest(1000, 8, 1000, 200 * 4 * 4 * 4 * 4 * 4, opts);
    runBusyTest(1000, 9, 1000, 300000, opts);
    runBusyTest(1000, 10, 1000, 300000, opts);

    var hints = Map.of("scan_type", "long");

    runBusyTest(1000, 0, 2, 10000, opts, hints);
    runBusyTest(1000, 1, 4, 120000, opts, hints);
    runBusyTest(1000, 2, 100, 300000, opts, hints);
    runBusyTest(1000, 3, 100, 1200000, opts, hints);
    runBusyTest(1000, 4, 100, 1800000, opts, hints);
    runBusyTest(1000, 50, 100, 1800000, opts, hints);

    hints = Map.of("scan_type", "st9");
    runBusyTest(1000, 0, 2, 10000, opts, hints);

    hints = Map.of("scan_type", "mega");
    runBusyTest(1000, 0, 1000, 600000, opts, hints);
    runBusyTest(1000, 1, 1000, 1200000, opts, hints);

    // test case where no profile is activated by a scan_type, so should use default profile
    hints = Map.of("scan_type", "st7");
    runBusyTest(1000, 0, 5, 5, opts, hints);

  }

  @Test
  public void testUnknownOpts() {
    var opts = Map.of("abc", "3");
    var exception =
        assertThrows(IllegalArgumentException.class, () -> runBusyTest(1000, 0, 5, 66, opts));
    assertTrue(exception.getMessage().contains("abc"));
  }

  @Test
  public void testIncorrectOptions() {

    String defaultProfile = "{'isDefault':true,'maxBusyTimeout':'5m','busyTimeoutMultiplier':4, "
        + "'attemptPlans':[{'servers':'5', 'busyTimeout':'5ms'},"
        + "{'servers':'20', 'busyTimeout':'33ms'}," + "{'servers':'50%', 'busyTimeout':'100ms'},"
        + "{'servers':'100%', 'busyTimeout':'200ms'}]" + "}";

    String profile1 = "{'scanTypeActivations':['long','mega'],'maxBusyTimeout':'30m',"
        + "'busyTimeoutMultiplier':4, " + "'attemptPlans':[{'servers':'2', 'busyTimeout':'10s'},"
        + "{'servers':'4', 'busyTimeout':'2m'}," + "{'servers':'10%', 'busyTimeout':'5m'}]" + "}";

    String profile2 =
        "{'scanTypeActivations':['mega'],'maxBusyTimeout':'60m'," + "'busyTimeoutMultiplier':2, "
            + "'attemptPlans':[{'servers':'100%', 'busyTimeout':'10m'}]" + "}";

    var opts1 = Map.of("profiles",
        "[" + defaultProfile + ", " + profile1 + "," + profile2 + "]".replace('\'', '"'));

    // two profiles activate on the scan type "mega", so should fail
    var exception =
        assertThrows(IllegalArgumentException.class, () -> runBusyTest(1000, 0, 5, 66, opts1));
    assertTrue(exception.getMessage().contains("mega"));

    var opts2 = Map.of("profiles", "[" + profile1 + "]".replace('\'', '"'));

    // missing a default profile, so should fail
    exception =
        assertThrows(IllegalArgumentException.class, () -> runBusyTest(1000, 0, 5, 66, opts2));

    assertTrue(exception.getMessage().contains("default"));

  }

  @Test
  public void testNoScanServers() {
    DefaultScanServerDispatcher dispatcher = new DefaultScanServerDispatcher();
    dispatcher.init(new InitParams(Set.of()));

    var tabletId = nti("1", "m");
    ScanServerDispatcher.Actions actions = dispatcher.determineActions(new DaParams(tabletId));
    assertNull(actions.getScanServer(tabletId));
    assertEquals(Duration.ZERO, actions.getDelay());
    assertEquals(Duration.ZERO, actions.getBusyTimeout());
  }
}

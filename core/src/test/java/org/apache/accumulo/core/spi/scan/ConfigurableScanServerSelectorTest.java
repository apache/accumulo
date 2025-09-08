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

import static org.apache.accumulo.core.util.LazySingletons.RANDOM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.client.TimedOutException;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;

public class ConfigurableScanServerSelectorTest {

  static class InitParams implements ScanServerSelector.InitParameters {

    private final Map<String,String> opts;
    private final Supplier<Map<ServerId,ResourceGroupId>> scanServers;

    InitParams(Set<String> scanServers) {
      this(scanServers, Map.of());
    }

    InitParams(Set<String> scanServers, Map<String,String> opts) {
      this.opts = opts;
      var scanServersMap = new HashMap<ServerId,ResourceGroupId>();
      scanServers.forEach(sserv -> scanServersMap
          .put(ServerId.sserver(HostAndPort.fromString(sserv)), ResourceGroupId.DEFAULT));
      this.scanServers = () -> scanServersMap;
    }

    InitParams(Map<ServerId,ResourceGroupId> scanServers, Map<String,String> opts) {
      this.opts = opts;
      this.scanServers = () -> scanServers;
    }

    InitParams(Supplier<Map<ServerId,ResourceGroupId>> scanServers, Map<String,String> opts) {
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
    public Supplier<Collection<ScanServerInfo>> getScanServers() {
      return () -> scanServers.get().entrySet().stream().map(entry -> new ScanServerInfo() {

        @Override
        public ServerId getServer() {
          return entry.getKey();
        }
      }).collect(Collectors.toSet());
    }
  }

  static class SelectorParams implements ScanServerSelector.SelectorParameters {

    private final Collection<TabletId> tablets;
    private final Map<TabletId,Collection<? extends ScanServerAttempt>> attempts;
    private final Map<String,String> hints;

    SelectorParams(TabletId tablet) {
      this.tablets = Set.of(tablet);
      this.attempts = Map.of();
      this.hints = Map.of();
    }

    SelectorParams(TabletId tablet, Map<TabletId,Collection<? extends ScanServerAttempt>> attempts,
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
    public Collection<? extends ScanServerAttempt> getAttempts(TabletId tabletId) {
      return attempts.getOrDefault(tabletId, Set.of());
    }

    @Override
    public Map<String,String> getHints() {
      return hints;
    }

    @Override
    public <T> Optional<T> waitUntil(Supplier<Optional<T>> condition, Duration maxWaitTime,
        String description) {
      Preconditions.checkArgument(maxWaitTime.compareTo(Duration.ZERO) > 0);
      throw new TimedOutException("Timed out w/o checking condition");
    }

  }

  static class TestScanServerAttempt implements ScanServerAttempt {

    private final ServerId server;
    private final Result result;

    TestScanServerAttempt(ServerId server, Result result) {
      this.server = server;
      this.result = result;
    }

    TestScanServerAttempt(String server, Result result) {
      this.server = ServerId.sserver(HostAndPort.fromString(server));
      this.result = result;
    }

    @Override
    public ServerId getServer() {
      return server;
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
    ConfigurableScanServerSelector selector = new ConfigurableScanServerSelector();
    selector.init(new InitParams(Set.of("ss1:1101", "ss2:1102", "ss3:1103", "ss4:1104", "ss5:1105",
        "ss6:1106", "ss7:1107", "ss8:1108")));

    Set<ServerId> servers = new HashSet<>();

    for (int i = 0; i < 100; i++) {
      var tabletId = nti("1", "m");

      ScanServerSelections actions = selector.selectServers(new SelectorParams(tabletId));

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
    ConfigurableScanServerSelector selector = new ConfigurableScanServerSelector();

    var servers = Stream.iterate(1, i -> i <= numServers, i -> i + 1)
        .map(i -> "s" + i + ":" + (1100 + i)).collect(Collectors.toSet());

    selector.init(new InitParams(servers, opts));

    Set<ServerId> serversSeen = new HashSet<>();

    var tabletId = nti("1", "m");

    var tabletAttempts = Stream.iterate(1, i -> i <= busyAttempts, i -> i + 1)
        .map(i -> (new TestScanServerAttempt("ss" + i + ":" + (1100 + i),
            ScanServerAttempt.Result.BUSY)))
        .collect(Collectors.toList());

    Map<TabletId,Collection<? extends ScanServerAttempt>> attempts = new HashMap<>();
    attempts.put(tabletId, tabletAttempts);

    for (int i = 0; i < 100 * numServers; i++) {
      ScanServerSelections actions =
          selector.selectServers(new SelectorParams(tabletId, attempts, hints));

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
    ConfigurableScanServerSelector selector = new ConfigurableScanServerSelector();
    var servers = Stream.iterate(1, i -> i <= 20, i -> i + 1).map(i -> "s" + i + ":" + (1100 + i))
        .collect(Collectors.toSet());
    selector.init(new InitParams(servers));

    Map<ServerId,Long> allServersSeen = new HashMap<>();

    for (int t = 0; t < 10000; t++) {
      Set<ServerId> serversSeen = new HashSet<>();

      String endRow =
          Long.toString(Math.abs(Math.max(RANDOM.get().nextLong(), Long.MIN_VALUE + 1)), 36);

      var tabletId = t % 1000 == 0 ? nti("" + t, null) : nti("" + t, endRow);

      for (int i = 0; i < 100; i++) {
        ScanServerSelections actions = selector.selectServers(new SelectorParams(tabletId));
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

    String defaultProfile = """
        {'isDefault':true,'maxBusyTimeout':'5m',
         'busyTimeoutMultiplier':4,
         'attemptPlans':
           [
             {'servers':'5', 'busyTimeout':'5ms'},{'servers':'20', 'busyTimeout':'33ms'},
             {'servers':'50%', 'busyTimeout':'100ms'},{'servers':'100%', 'busyTimeout':'200ms'}
           ]
        }
        """;

    String profile1 = """
        {'scanTypeActivations':['long','st9'],
         'maxBusyTimeout':'30m',
         'busyTimeoutMultiplier':4,
         'attemptPlans':
           [
             {'servers':'2', 'busyTimeout':'10s'},
             {'servers':'4', 'busyTimeout':'2m'},
             {'servers':'10%', 'busyTimeout':'5m'}
           ]
        }
        """;

    String profile2 = """
        {'scanTypeActivations':['mega'],
         'maxBusyTimeout':'60m',
         'busyTimeoutMultiplier':2,
         'attemptPlans':
           [
             {'servers':'100%', 'busyTimeout':'10m'}
           ]
        }
        """;

    // Intentionally put the default profile in 2nd position. There was a bug where config parsing
    // would fail if the default did not come first.
    var opts = Map.of("profiles",
        ("[" + profile1 + ", " + defaultProfile + "," + profile2 + "]").replace('\'', '"'));

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
        + "{'servers':'20', 'busyTimeout':'33ms'},{'servers':'50%', 'busyTimeout':'100ms'},"
        + "{'servers':'100%', 'busyTimeout':'200ms'}]}";

    String profile1 = "{'scanTypeActivations':['long','mega'],'maxBusyTimeout':'30m',"
        + "'busyTimeoutMultiplier':4, 'attemptPlans':[{'servers':'2', 'busyTimeout':'10s'},"
        + "{'servers':'4', 'busyTimeout':'2m'},{'servers':'10%', 'busyTimeout':'5m'}]}";

    String profile2 =
        "{'scanTypeActivations':['mega'],'maxBusyTimeout':'60m','busyTimeoutMultiplier':2, "
            + "'attemptPlans':[{'servers':'100%', 'busyTimeout':'10m'}]}";

    var opts1 = Map.of("profiles",
        ("[" + defaultProfile + ", " + profile1 + "," + profile2 + "]").replace('\'', '"'));

    // two profiles activate on the scan type "mega", so should fail
    var exception =
        assertThrows(IllegalArgumentException.class, () -> runBusyTest(1000, 0, 5, 66, opts1));
    assertTrue(exception.getMessage().contains("mega"));

    var opts2 = Map.of("profiles", ("[" + profile1 + "]").replace('\'', '"'));

    // missing a default profile, so should fail
    exception =
        assertThrows(IllegalArgumentException.class, () -> runBusyTest(1000, 0, 5, 66, opts2));

    assertTrue(exception.getMessage().contains("default"));

  }

  @Test
  public void testNoScanServers() {
    // run a 1st test assuming default config does not fallback to tservers, so should timeout
    var selector1 = new ConfigurableScanServerSelector();
    selector1.init(new InitParams(Set.of()));
    var tabletId = nti("1", "m");
    assertThrows(TimedOutException.class,
        () -> selector1.selectServers(new SelectorParams(tabletId)));

    // run a 2nd test with config that does fall back to tablet servers
    String profile = "{'isDefault':'true','maxBusyTimeout':'60m','busyTimeoutMultiplier':2,"
        + " 'timeToWaitForScanServers': '0s',"
        + "'attemptPlans':[{'servers':'100%', 'busyTimeout':'10m'}]}";
    var opts1 = Map.of("profiles", ("[" + profile + "]").replace('\'', '"'));
    var selector2 = new ConfigurableScanServerSelector();
    selector2.init(new InitParams(Set.of(), opts1));
    ScanServerSelections actions = selector2.selectServers(new SelectorParams(tabletId));
    assertNull(actions.getScanServer(tabletId));
    assertEquals(Duration.ZERO, actions.getDelay());
    assertEquals(Duration.ZERO, actions.getBusyTimeout());
  }

  @Test
  public void testGroups() {

    String defaultProfile =
        "{'isDefault':true,'maxBusyTimeout':'5m','busyTimeoutMultiplier':4, 'attemptPlans':"
            + "[{'servers':'100%', 'busyTimeout':'60s'}]}";

    String profile1 = "{'scanTypeActivations':['long','st9'],'maxBusyTimeout':'30m','group':'g1',"
        + "'busyTimeoutMultiplier':4, 'attemptPlans':[{'servers':'100%', 'busyTimeout':'60s'}]}";

    String profile2 =
        "{'scanTypeActivations':['mega'],'maxBusyTimeout':'60m','busyTimeoutMultiplier':2, 'group':'g2',"
            + "'attemptPlans':[{'servers':'100%', 'busyTimeout':'10m'}]}";

    var opts = Map.of("profiles",
        ("[" + defaultProfile + ", " + profile1 + "," + profile2 + "]").replace('\'', '"'));

    ConfigurableScanServerSelector selector = new ConfigurableScanServerSelector();
    var dg = ResourceGroupId.DEFAULT;
    selector.init(new InitParams(
        Map.of(ServerId.sserver("ss1", 1101), dg, ServerId.sserver("ss2", 1102), dg,
            ServerId.sserver("ss3", 1103), dg,
            ServerId.sserver(ResourceGroupId.of("g1"), "ss4", 1104), ResourceGroupId.of("g1"),
            ServerId.sserver(ResourceGroupId.of("g1"), "ss5", 1105), ResourceGroupId.of("g1"),
            ServerId.sserver(ResourceGroupId.of("g2"), "ss6", 1106), ResourceGroupId.of("g2"),
            ServerId.sserver(ResourceGroupId.of("g2"), "ss7", 1107), ResourceGroupId.of("g2"),
            ServerId.sserver(ResourceGroupId.of("g2"), "ss8", 1108), ResourceGroupId.of("g2")),
        opts));

    Set<String> servers = new HashSet<>();

    for (int i = 0; i < 1000; i++) {
      var tabletId = nti("1", "m" + i);

      ScanServerSelections actions = selector.selectServers(new SelectorParams(tabletId));

      servers.add(actions.getScanServer(tabletId).toHostPortString());
    }

    assertEquals(Set.of("ss1:1101", "ss2:1102", "ss3:1103"), servers);

    // config should map this scan type to the group of scan servers g1
    var hints = Map.of("scan_type", "long");

    servers.clear();

    for (int i = 0; i < 1000; i++) {
      var tabletId = nti("1", "m" + i);

      ScanServerSelections actions =
          selector.selectServers(new SelectorParams(tabletId, Map.of(), hints));

      servers.add(actions.getScanServer(tabletId).toHostPortString());
    }

    assertEquals(Set.of("ss4:1104", "ss5:1105"), servers);

    // config should map this scan type to the group of scan servers g2
    hints = Map.of("scan_type", "mega");

    servers.clear();

    for (int i = 0; i < 1000; i++) {
      var tabletId = nti("1", "m" + i);

      ScanServerSelections actions =
          selector.selectServers(new SelectorParams(tabletId, Map.of(), hints));

      servers.add(actions.getScanServer(tabletId).toHostPortString());
    }

    assertEquals(Set.of("ss6:1106", "ss7:1107", "ss8:1108"), servers);

    // config does map this scan type to anything, so should use the default group of scan servers
    hints = Map.of("scan_type", "rust");

    servers.clear();

    for (int i = 0; i < 1000; i++) {
      var tabletId = nti("1", "m" + i);

      ScanServerSelections actions =
          selector.selectServers(new SelectorParams(tabletId, Map.of(), hints));

      servers.add(actions.getScanServer(tabletId).toHostPortString());
    }

    assertEquals(Set.of("ss1:1101", "ss2:1102", "ss3:1103"), servers);
  }

  @Test
  public void testWaitForScanServers() {
    // this test ensures that when there are no scan servers that the ConfigurableScanServerSelector
    // will wait for scan servers

    String defaultProfile =
        "{'isDefault':true,'maxBusyTimeout':'5m','busyTimeoutMultiplier':4,'timeToWaitForScanServers':'120s',"
            + "'attemptPlans':[{'servers':'100%', 'busyTimeout':'60s'}]}";

    var opts = Map.of("profiles", ("[" + defaultProfile + "]").replace('\'', '"'));

    ConfigurableScanServerSelector selector = new ConfigurableScanServerSelector();

    AtomicReference<Map<ServerId,ResourceGroupId>> scanServers = new AtomicReference<>(Map.of());

    selector.init(new InitParams(scanServers::get, opts));

    var tabletId = nti("1", "m");

    var dg = ResourceGroupId.DEFAULT;

    var params = new SelectorParams(tabletId, Map.of(), Map.of()) {
      @Override
      public <T> Optional<T> waitUntil(Supplier<Optional<T>> condition, Duration maxWaitTime,
          String description) {
        // make some scan servers available now that wait was called
        scanServers.set(Map.of(ServerId.sserver("ss1", 1101), dg, ServerId.sserver("ss2", 1102), dg,
            ServerId.sserver("ss3", 1103), dg));

        Optional<T> optional = condition.get();

        while (optional.isEmpty()) {
          UtilWaitThread.sleep(10);
          optional = condition.get();
        }

        return optional;
      }
    };

    ScanServerSelections actions = selector.selectServers(params);

    assertTrue(Set.of(ServerId.sserver("ss1", 1101), ServerId.sserver("ss2", 1102),
        ServerId.sserver("ss3", 1103)).contains(actions.getScanServer(tabletId)));
    assertFalse(scanServers.get().isEmpty());
  }
}

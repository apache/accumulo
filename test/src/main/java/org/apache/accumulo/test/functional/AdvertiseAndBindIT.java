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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.test.compaction.ExternalCompactionTestUtils.QUEUE1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.HostAndPort;
import org.apache.accumulo.core.util.compaction.ExternalCompactionUtil;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterControl;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class AdvertiseAndBindIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumCompactors(1);
    cfg.setNumScanServers(1);
    cfg.setNumTservers(1);
    cfg.setProperty(Property.COMPACTOR_PORTSEARCH, "true");
    cfg.setProperty(Property.TSERV_PORTSEARCH, "true");
    cfg.setServerClass(ServerType.COMPACTION_COORDINATOR, CompactionCoordinator.class);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s");
  }

  @Override
  public void setUp() throws Exception {
    // Override the setup method so that Mini is
    // not started before each test. We are going to
    // manage this manually.
  }

  @Test
  public void testAdvertiseAndBindArguments() throws Exception {
    final String localHostName = InetAddress.getLocalHost().getHostName();

    createMiniAccumulo();
    assertNotNull(cluster);

    // Accumulo will use the default bind address of "0.0.0.0"
    // when it's not specified. When the bind address is the
    // default, then Accumulo will use the hostname for the
    // advertise address.
    cluster.start();
    getCluster().getClusterControl().start(ServerType.COMPACTION_COORDINATOR, Map.of(), 1);
    getCluster().getClusterControl().start(ServerType.COMPACTOR, Map.of(), 1,
        new String[] {"-q", QUEUE1});
    getCluster().getClusterControl().start(ServerType.SCAN_SERVER, Map.of(), 1,
        new String[] {"-g", "DEFAULT"});
    Wait.waitFor(() -> !getServerContext().instanceOperations().getCompactors().isEmpty(), 60_000);
    Wait.waitFor(() -> !getServerContext().instanceOperations().getScanServers().isEmpty(), 60_000);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(false);
      zkAddrs.values().forEach(hp -> assertTrue(hp.getHost().equals(localHostName)));
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set only the bind address
    restartClusterWithArguments(null, "127.0.0.1", false);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(false);
      zkAddrs.values().forEach(hp -> assertTrue(hp.getHost().equals("127.0.0.1")));
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set only the advertise address
    restartClusterWithArguments("localhost", null, false);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(false);
      zkAddrs.values().forEach(hp -> assertTrue(hp.getHost().equals("localhost")));
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set advertise and bind address
    restartClusterWithArguments("localhost", "127.0.0.1", false);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(false);
      zkAddrs.values().forEach(hp -> assertTrue(hp.getHost().equals("localhost")));
    } finally {
      cluster.stop();
    }

    // Set advertise with port and bind address
    // skip the coordinator because MiniAccumuloClusterControl.start will
    // try to connect to it
    restartClusterWithArguments("192.168.1.2:59000", "127.0.0.1", true);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(true);
      zkAddrs.values().forEach(hp -> assertTrue(hp.toString().equals("192.168.1.2:59000")));
    } finally {
      cluster.stop();
    }

  }

  @Test
  public void testAdvertiseAndBindProperties() throws Exception {

    final String localHostName = InetAddress.getLocalHost().getHostName();

    createMiniAccumulo();
    assertNotNull(cluster);

    // Accumulo will use the default bind address of "0.0.0.0"
    // when it's not specified. When the bind address is the
    // default, then Accumulo will use the hostname for the
    // advertise address.
    cluster.start();
    getCluster().getClusterControl().start(ServerType.COMPACTION_COORDINATOR, Map.of(), 1);
    getCluster().getClusterControl().start(ServerType.COMPACTOR, Map.of(), 1,
        new String[] {"-q", QUEUE1});
    getCluster().getClusterControl().start(ServerType.SCAN_SERVER, Map.of(), 1,
        new String[] {"-g", "DEFAULT"});
    Wait.waitFor(() -> !getServerContext().instanceOperations().getCompactors().isEmpty(), 60_000);
    Wait.waitFor(() -> !getServerContext().instanceOperations().getScanServers().isEmpty(), 60_000);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(false);
      zkAddrs.values().forEach(hp -> assertTrue(hp.getHost().equals(localHostName)));
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set only the bind address
    restartClusterWithProperties(Map.of(Property.RPC_PROCESS_BIND_ADDRESS.getKey(), "127.0.0.1"),
        false);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(false);
      zkAddrs.values().forEach(hp -> assertTrue(hp.getHost().equals("127.0.0.1")));
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set only the advertise address
    restartClusterWithProperties(
        Map.of(Property.RPC_PROCESS_ADVERTISE_ADDRESS.getKey(), "localhost"), false);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(false);
      zkAddrs.values().forEach(hp -> assertTrue(hp.getHost().equals("localhost")));
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set advertise and bind address
    restartClusterWithProperties(Map.of(Property.RPC_PROCESS_BIND_ADDRESS.getKey(), "127.0.0.1",
        Property.RPC_PROCESS_ADVERTISE_ADDRESS.getKey(), "localhost"), false);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(false);
      zkAddrs.values().forEach(hp -> assertTrue(hp.getHost().equals("localhost")));
    } finally {
      cluster.stop();
    }

    // Set advertise with port and bind address
    // skip the coordinator because MiniAccumuloClusterControl.start will
    // try to connect to it
    restartClusterWithProperties(Map.of(Property.RPC_PROCESS_BIND_ADDRESS.getKey(), "127.0.0.1",
        Property.RPC_PROCESS_ADVERTISE_ADDRESS.getKey(), "192.168.1.1:10005"), true);
    try {
      Map<ServerType,HostAndPort> zkAddrs = getAdvertiseAddressFromZooKeeper(true);
      zkAddrs.values().forEach(hp -> assertTrue(hp.toString().equals("192.168.1.1:10005")));
    } finally {
      cluster.stop();
    }

  }

  private void restartClusterWithArguments(String advertiseAddress, String bindAddress,
      boolean skipCoordinator) throws Exception {
    List<String> args = new ArrayList<>();
    if (advertiseAddress != null) {
      args.add("-o");
      args.add(Property.RPC_PROCESS_ADVERTISE_ADDRESS.getKey() + "=" + advertiseAddress);
    }
    if (bindAddress != null) {
      args.add("-o");
      args.add(Property.RPC_PROCESS_BIND_ADDRESS.getKey() + "=" + bindAddress);
    }
    // cluster.start will not end up overwriting the accumulo.properties file
    // with any property changes after the initial start. The only way to pass
    // new or updated property settings on a process restart is to use the
    // start method that takes configuration overrides.
    MiniAccumuloClusterControl control = getCluster().getClusterControl();
    control.start(ServerType.ZOOKEEPER);
    control.start(ServerType.TABLET_SERVER, Map.of(), 1, args.toArray(new String[] {}));
    control.start(ServerType.MANAGER, Map.of(), 1, args.toArray(new String[] {}));
    control.start(ServerType.GARBAGE_COLLECTOR, Map.of(), 1, args.toArray(new String[] {}));
    // Calling cluster.start here will set the Manager goal state
    // and call verifyUp
    cluster.start();
    if (!skipCoordinator) {
      control.start(ServerType.COMPACTION_COORDINATOR, Map.of(), 1, args.toArray(new String[] {}));
    }
    List<String> compactorArgs = new ArrayList<>(args);
    compactorArgs.add("-q");
    compactorArgs.add(QUEUE1);
    control.start(ServerType.COMPACTOR, Map.of(), 1, compactorArgs.toArray(new String[] {}));
    List<String> sserverArgs = new ArrayList<>(args);
    sserverArgs.add("-g");
    sserverArgs.add("DEFAULT");
    control.start(ServerType.SCAN_SERVER, Map.of(), 1, sserverArgs.toArray(new String[] {}));
    Wait.waitFor(() -> !getServerContext().instanceOperations().getCompactors().isEmpty(), 60_000);
    Wait.waitFor(() -> !getServerContext().instanceOperations().getScanServers().isEmpty(), 60_000);
  }

  private void restartClusterWithProperties(Map<String,String> properties, boolean skipCoordinator)
      throws Exception {
    // cluster.start will not end up overwriting the accumulo.properties file
    // with any property changes after the initial start. The only way to pass
    // new or updated property settings on a process restart is to use the
    // start method that takes configuration overrides.
    MiniAccumuloClusterControl control = getCluster().getClusterControl();
    control.start(ServerType.ZOOKEEPER);
    control.start(ServerType.TABLET_SERVER, properties, 1);
    control.start(ServerType.MANAGER, properties, 1);
    control.start(ServerType.GARBAGE_COLLECTOR, properties, 1);
    // Calling cluster.start here will set the Manager goal state
    // and call verifyUp
    cluster.start();
    if (!skipCoordinator) {
      control.start(ServerType.COMPACTION_COORDINATOR, properties, 1);
    }
    control.start(ServerType.COMPACTOR, properties, 1, new String[] {"-q", QUEUE1});
    control.start(ServerType.SCAN_SERVER, properties, 1, new String[] {"-g", "DEFAULT"});
    Wait.waitFor(() -> !getServerContext().instanceOperations().getCompactors().isEmpty(), 60_000);
    Wait.waitFor(() -> !getServerContext().instanceOperations().getScanServers().isEmpty(), 60_000);
  }

  private Map<ServerType,HostAndPort> getAdvertiseAddressFromZooKeeper(boolean skipCoordinator)
      throws InterruptedException {
    Map<ServerType,HostAndPort> addresses = new HashMap<>();

    List<String> mgrs = getServerContext().instanceOperations().getManagerLocations();
    assertEquals(1, mgrs.size());
    addresses.put(ServerType.MANAGER, HostAndPort.fromString(mgrs.get((0))));

    if (!skipCoordinator) {
      Optional<HostAndPort> coordAddr =
          ExternalCompactionUtil.findCompactionCoordinator(getServerContext());
      while (coordAddr.isEmpty()) {
        Thread.sleep(50);
        coordAddr = ExternalCompactionUtil.findCompactionCoordinator(getServerContext());
      }
      addresses.put(ServerType.COMPACTION_COORDINATOR, coordAddr.orElseThrow());
    }

    List<String> tservers = getServerContext().instanceOperations().getTabletServers();
    assertEquals(1, tservers.size());
    addresses.put(ServerType.TABLET_SERVER, HostAndPort.fromString(tservers.get((0))));

    Set<String> compactors = getServerContext().instanceOperations().getCompactors();
    assertEquals(1, compactors.size());
    addresses.put(ServerType.COMPACTOR, HostAndPort.fromString(compactors.iterator().next()));

    Set<String> sservers = getServerContext().instanceOperations().getScanServers();
    assertEquals(1, sservers.size());
    addresses.put(ServerType.SCAN_SERVER, HostAndPort.fromString(sservers.iterator().next()));

    return addresses;
  }

}

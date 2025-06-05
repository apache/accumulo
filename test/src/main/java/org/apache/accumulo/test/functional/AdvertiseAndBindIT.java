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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.coordinator.CompactionCoordinator;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.HostAndPort;
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
    Wait.waitFor(() -> !getServerContext().instanceOperations().getCompactors().isEmpty());
    Wait.waitFor(() -> !getServerContext().instanceOperations().getScanServers().isEmpty());
    try {
      assertEquals(localHostName, getServerAddressesFromZookeeper());
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set only the bind address
    restartClusterWithArguments(null, "127.0.0.1");
    try {
      assertEquals("127.0.0.1", getServerAddressesFromZookeeper());
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set only the advertise address
    restartClusterWithArguments("localhost", null);
    try {
      assertEquals("localhost", getServerAddressesFromZookeeper());
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set advertise and bind address
    restartClusterWithArguments("localhost", "127.0.0.1");
    try {
      assertEquals("localhost", getServerAddressesFromZookeeper());
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
    Wait.waitFor(() -> !getServerContext().instanceOperations().getCompactors().isEmpty());
    Wait.waitFor(() -> !getServerContext().instanceOperations().getScanServers().isEmpty());
    try {
      assertEquals(localHostName, getServerAddressesFromZookeeper());
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set only the bind address
    restartClusterWithProperties(Map.of(Property.RPC_PROCESS_BIND_ADDRESS.getKey(), "127.0.0.1"));
    try {
      assertEquals("127.0.0.1", getServerAddressesFromZookeeper());
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set only the advertise address
    restartClusterWithProperties(
        Map.of(Property.RPC_PROCESS_ADVERTISE_ADDRESS.getKey(), "localhost"));
    try {
      assertEquals("localhost", getServerAddressesFromZookeeper());
    } finally {
      cluster.stop();
      Thread.sleep(20_000); // wait 2x the ZK timeout to ensure ZK entries removed
    }

    // Set advertise and bind address
    restartClusterWithProperties(Map.of(Property.RPC_PROCESS_BIND_ADDRESS.getKey(), "127.0.0.1",
        Property.RPC_PROCESS_ADVERTISE_ADDRESS.getKey(), "localhost"));
    try {
      assertEquals("localhost", getServerAddressesFromZookeeper());
    } finally {
      cluster.stop();
    }

  }

  private void restartClusterWithArguments(String advertiseAddress, String bindAddress)
      throws Exception {
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
    control.start(ServerType.COMPACTION_COORDINATOR, Map.of(), 1, args.toArray(new String[] {}));
    List<String> compactorArgs = new ArrayList<>(args);
    compactorArgs.add("-q");
    compactorArgs.add(QUEUE1);
    control.start(ServerType.COMPACTOR, Map.of(), 1, compactorArgs.toArray(new String[] {}));
    List<String> sserverArgs = new ArrayList<>(args);
    sserverArgs.add("-g");
    sserverArgs.add("DEFAULT");
    control.start(ServerType.SCAN_SERVER, Map.of(), 1, sserverArgs.toArray(new String[] {}));
    Wait.waitFor(() -> !getServerContext().instanceOperations().getCompactors().isEmpty());
    Wait.waitFor(() -> !getServerContext().instanceOperations().getScanServers().isEmpty());
  }

  private void restartClusterWithProperties(Map<String,String> properties) throws Exception {
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
    control.start(ServerType.COMPACTION_COORDINATOR, properties, 1);
    control.start(ServerType.COMPACTOR, properties, 1, new String[] {"-q", QUEUE1});
    control.start(ServerType.SCAN_SERVER, properties, 1, new String[] {"-g", "DEFAULT"});
    Wait.waitFor(() -> !getServerContext().instanceOperations().getCompactors().isEmpty());
    Wait.waitFor(() -> !getServerContext().instanceOperations().getScanServers().isEmpty());
  }

  private String getServerAddressesFromZookeeper() {
    Set<String> names = new HashSet<>();
    List<String> mgrs = getServerContext().instanceOperations().getManagerLocations();
    assertEquals(1, mgrs.size());
    names.add(HostAndPort.fromString(mgrs.get((0))).getHost());
    List<String> tservers = getServerContext().instanceOperations().getTabletServers();
    assertEquals(1, tservers.size());
    names.add(HostAndPort.fromString(tservers.get((0))).getHost());
    Set<String> compactors = getServerContext().instanceOperations().getCompactors();
    assertEquals(1, compactors.size());
    names.add(HostAndPort.fromString(compactors.iterator().next()).getHost());
    Set<String> sservers = getServerContext().instanceOperations().getScanServers();
    assertEquals(1, sservers.size());
    names.add(HostAndPort.fromString(sservers.iterator().next()).getHost());

    assertEquals(1, names.size());
    return names.iterator().next();

  }
}

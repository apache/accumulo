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
package org.apache.accumulo.test;

import static org.apache.accumulo.test.harness.AccumuloITBase.MINI_CLUSTER_ONLY;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.lock.ServiceLockPaths.ServiceLockPath;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.accumulo.test.harness.AccumuloClusterHarness;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Test class that verifies "HA-capable" servers put up their thrift servers before acquiring their
 * ZK lock.
 */
@Tag(MINI_CLUSTER_ONLY)
public class ThriftServerBindsBeforeZooKeeperLockIT extends AccumuloClusterHarness {
  private static final Logger LOG =
      LoggerFactory.getLogger(ThriftServerBindsBeforeZooKeeperLockIT.class);

  @Override
  public boolean canRunTest(ClusterType type) {
    return type == ClusterType.MINI;
  }

  @SuppressFBWarnings(value = "URLCONNECTION_SSRF_FD", justification = "url is not from user")
  @Test
  public void testMonitorService() throws Exception {
    final MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
    Collection<ProcessReference> monitors = cluster.getProcesses().get(ServerType.MONITOR);
    // Need to start one monitor and let it become active.
    if (monitors == null || monitors.isEmpty()) {
      getClusterControl().start(ServerType.MONITOR, "localhost");
    }

    String[] monitorLocation = {null};
    Wait.waitFor(() -> {
      try {
        monitorLocation[0] = MonitorUtil.getLocation(getServerContext());
      } catch (Exception e) {
        LOG.debug("Failed to find active monitor location, retrying", e);
      }
      return monitorLocation[0] != null;
    }, 30_000, 250, "Active monitor location was not published to ZooKeeper");

    LOG.debug("Found active monitor");

    int[] freePort = {PortUtils.getRandomFreePort()};
    Process[] monitor = {null};
    try {
      LOG.debug("Starting standby monitor on {}", freePort[0]);
      monitor[0] = startProcess(cluster, ServerType.MONITOR, freePort[0]);

      Wait.waitFor(() -> {
        var url = new URI("http://localhost:" + freePort[0]).toURL();
        try {
          HttpURLConnection cnxn = (HttpURLConnection) url.openConnection();
          cnxn.setConnectTimeout(1000);
          cnxn.setReadTimeout(1000);
          try {
            final int responseCode = cnxn.getResponseCode();
            String errorText;
            // This is our "assertion", but we want to re-check it if it's not what we expect
            if (responseCode == HttpURLConnection.HTTP_OK) {
              return true;
            } else {
              errorText = FunctionalTestUtils.readAll(cnxn.getErrorStream());
            }
            LOG.debug("Unexpected responseCode and/or error text, will retry: '{}' '{}'",
                responseCode, errorText);
          } finally {
            cnxn.disconnect();
          }
        } catch (Exception e) {
          LOG.debug("Caught exception trying to fetch monitor info", e);
        }
        // Make sure the process is still up. Possible the "randomFreePort" we got wasn't actually
        // free and the process died trying to bind it. Pick a new port and restart it in that case.
        if (!monitor[0].isAlive()) {
          freePort[0] = PortUtils.getRandomFreePort();
          LOG.debug("Monitor died, restarting it listening on {}", freePort[0]);
          monitor[0] = startProcess(cluster, ServerType.MONITOR, freePort[0]);
        }
        return false;
      }, 30_000, 250, "Standby monitor did not serve requests");
    } finally {
      if (monitor[0] != null) {
        monitor[0].destroyForcibly();
      }
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SOCKET",
      justification = "unencrypted socket is okay for testing")
  @Test
  public void testManagerService() throws Exception {
    final MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      // Wait for the Manager to grab its lock
      Wait.waitFor(() -> {
        try {
          ServiceLockPath managerLockPath = getServerContext().getServerPaths().getManager(true);
          return managerLockPath != null;
        } catch (Exception e) {
          LOG.debug("Failed to find active manager location, retrying", e);
          return false;
        }
      }, 30_000, 250, "Active manager lock was not acquired");

      LOG.debug("Found active manager");

      int[] freePort = {PortUtils.getRandomFreePort()};
      Process[] manager = {null};
      try {
        LOG.debug("Starting standby manager on {}", freePort[0]);
        manager[0] = startProcess(cluster, ServerType.MANAGER, freePort[0]);

        Wait.waitFor(() -> {
          try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", freePort[0]), 1000);
            return s.isConnected();
          } catch (Exception e) {
            LOG.debug("Caught exception trying to connect to Manager", e);
          }
          // Make sure the process is still up. Possible the "randomFreePort" we got wasn't
          // actually free and the process died trying to bind it. Pick a new port and restart it.
          if (!manager[0].isAlive()) {
            freePort[0] = PortUtils.getRandomFreePort();
            LOG.debug("Manager died, restarting it listening on {}", freePort[0]);
            manager[0] = startProcess(cluster, ServerType.MANAGER, freePort[0]);
          }
          return false;
        }, 30_000, 250, "Standby manager did not accept connections");
      } finally {
        if (manager[0] != null) {
          manager[0].destroyForcibly();
        }
      }
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SOCKET",
      justification = "unencrypted socket is okay for testing")
  @Test
  public void testGarbageCollectorPorts() throws Exception {
    final MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {

      // Wait for the Manager to grab its lock
      Wait.waitFor(() -> {
        try {
          ServiceLockPath slp = getServerContext().getServerPaths().getGarbageCollector(true);
          return slp != null;
        } catch (Exception e) {
          LOG.debug("Failed to find active gc location, retrying", e);
          return false;
        }
      }, 30_000, 250, "Active garbage collector lock was not acquired");

      LOG.debug("Found active gc");

      int[] freePort = {PortUtils.getRandomFreePort()};
      Process[] manager = {null};
      try {
        LOG.debug("Starting standby gc on {}", freePort[0]);
        manager[0] = startProcess(cluster, ServerType.GARBAGE_COLLECTOR, freePort[0]);

        Wait.waitFor(() -> {
          try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("localhost", freePort[0]), 1000);
            return s.isConnected();
          } catch (Exception e) {
            LOG.debug("Caught exception trying to connect to GC", e);
          }
          // Make sure the process is still up. Possible the "randomFreePort" we got wasn't
          // actually free and the process died trying to bind it. Pick a new port and restart it.
          if (!manager[0].isAlive()) {
            freePort[0] = PortUtils.getRandomFreePort();
            LOG.debug("GC died, restarting it listening on {}", freePort[0]);
            manager[0] = startProcess(cluster, ServerType.GARBAGE_COLLECTOR, freePort[0]);
          }
          return false;
        }, 30_000, 250, "Standby garbage collector did not accept connections");
      } finally {
        if (manager[0] != null) {
          manager[0].destroyForcibly();
        }
      }
    }
  }

  private Process startProcess(MiniAccumuloClusterImpl cluster, ServerType serverType, int port)
      throws IOException {
    final Property property;
    final Class<?> service = switch (serverType) {
      case MONITOR -> {
        property = Property.MONITOR_PORT;
        yield Monitor.class;
      }
      case MANAGER -> {
        property = Property.MANAGER_CLIENTPORT;
        yield Manager.class;
      }
      case GARBAGE_COLLECTOR -> {
        property = Property.GC_PORT;
        yield SimpleGarbageCollector.class;
      }
      default -> throw new IllegalArgumentException("Irrelevant server type for test");
    };

    return cluster._exec(service, serverType, Map.of(property.getKey(), Integer.toString(port)))
        .getProcess();
  }
}

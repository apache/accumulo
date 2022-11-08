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

import static org.apache.accumulo.harness.AccumuloITBase.MINI_CLUSTER_ONLY;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.manager.Manager;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.miniclusterImpl.ProcessReference;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
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

    while (true) {
      try {
        MonitorUtil.getLocation(getServerContext());
        break;
      } catch (Exception e) {
        LOG.debug("Failed to find active monitor location, retrying", e);
        Thread.sleep(1000);
      }
    }

    LOG.debug("Found active monitor");

    int freePort = PortUtils.getRandomFreePort();
    String monitorUrl = "http://localhost:" + freePort;
    Process monitor = null;
    try {
      LOG.debug("Starting standby monitor on {}", freePort);
      monitor = startProcess(cluster, ServerType.MONITOR, freePort);

      while (true) {
        URL url = new URL(monitorUrl);
        try {
          HttpURLConnection cnxn = (HttpURLConnection) url.openConnection();
          final int responseCode = cnxn.getResponseCode();
          String errorText;
          // This is our "assertion", but we want to re-check it if it's not what we expect
          if (responseCode == HttpURLConnection.HTTP_OK) {
            return;
          } else {
            errorText = FunctionalTestUtils.readAll(cnxn.getErrorStream());
          }
          LOG.debug("Unexpected responseCode and/or error text, will retry: '{}' '{}'",
              responseCode, errorText);
        } catch (Exception e) {
          LOG.debug("Caught exception trying to fetch monitor info", e);
        }
        // Wait before trying again
        Thread.sleep(1000);
        // Make sure the process is still up. Possible the "randomFreePort" we got wasn't actually
        // free and the process
        // died trying to bind it. Pick a new port and restart it in that case.
        if (!monitor.isAlive()) {
          freePort = PortUtils.getRandomFreePort();
          monitorUrl = "http://localhost:" + freePort;
          LOG.debug("Monitor died, restarting it listening on {}", freePort);
          monitor = startProcess(cluster, ServerType.MONITOR, freePort);
        }
      }
    } finally {
      if (monitor != null) {
        monitor.destroyForcibly();
      }
    }
  }

  @SuppressFBWarnings(value = "UNENCRYPTED_SOCKET",
      justification = "unencrypted socket is okay for testing")
  @Test
  public void testManagerService() throws Exception {
    final MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      final InstanceId instanceID = client.instanceOperations().getInstanceId();

      // Wait for the Manager to grab its lock
      while (true) {
        try {
          List<String> locks = cluster.getServerContext().getZooReader()
              .getChildren(Constants.ZROOT + "/" + instanceID + Constants.ZMANAGER_LOCK);
          if (!locks.isEmpty()) {
            break;
          }
        } catch (Exception e) {
          LOG.debug("Failed to find active manager location, retrying", e);
          Thread.sleep(1000);
        }
      }

      LOG.debug("Found active manager");

      int freePort = PortUtils.getRandomFreePort();
      Process manager = null;
      try {
        LOG.debug("Starting standby manager on {}", freePort);
        manager = startProcess(cluster, ServerType.MANAGER, freePort);

        while (true) {
          try (Socket s = new Socket("localhost", freePort)) {
            if (s.isConnected()) {
              // Pass
              return;
            }
          } catch (Exception e) {
            LOG.debug("Caught exception trying to connect to Manager", e);
          }
          // Wait before trying again
          Thread.sleep(1000);
          // Make sure the process is still up. Possible the "randomFreePort" we got wasn't
          // actually
          // free and the process
          // died trying to bind it. Pick a new port and restart it in that case.
          if (!manager.isAlive()) {
            freePort = PortUtils.getRandomFreePort();
            LOG.debug("Manager died, restarting it listening on {}", freePort);
            manager = startProcess(cluster, ServerType.MANAGER, freePort);
          }
        }
      } finally {
        if (manager != null) {
          manager.destroyForcibly();
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
      InstanceId instanceID = client.instanceOperations().getInstanceId();

      // Wait for the Manager to grab its lock
      while (true) {
        try {
          List<String> locks = cluster.getServerContext().getZooReader()
              .getChildren(Constants.ZROOT + "/" + instanceID + Constants.ZGC_LOCK);
          if (!locks.isEmpty()) {
            break;
          }
        } catch (Exception e) {
          LOG.debug("Failed to find active gc location, retrying", e);
          Thread.sleep(1000);
        }
      }

      LOG.debug("Found active gc");

      int freePort = PortUtils.getRandomFreePort();
      Process manager = null;
      try {
        LOG.debug("Starting standby gc on {}", freePort);
        manager = startProcess(cluster, ServerType.GARBAGE_COLLECTOR, freePort);

        while (true) {
          try (Socket s = new Socket("localhost", freePort)) {
            if (s.isConnected()) {
              // Pass
              return;
            }
          } catch (Exception e) {
            LOG.debug("Caught exception trying to connect to GC", e);
          }
          // Wait before trying again
          Thread.sleep(1000);
          // Make sure the process is still up. Possible the "randomFreePort" we got wasn't
          // actually
          // free and the process
          // died trying to bind it. Pick a new port and restart it in that case.
          if (!manager.isAlive()) {
            freePort = PortUtils.getRandomFreePort();
            LOG.debug("GC died, restarting it listening on {}", freePort);
            manager = startProcess(cluster, ServerType.GARBAGE_COLLECTOR, freePort);
          }
        }
      } finally {
        if (manager != null) {
          manager.destroyForcibly();
        }
      }
    }
  }

  private Process startProcess(MiniAccumuloClusterImpl cluster, ServerType serverType, int port)
      throws IOException {
    final Property property;
    final Class<?> service;
    switch (serverType) {
      case MONITOR:
        property = Property.MONITOR_PORT;
        service = Monitor.class;
        break;
      case MANAGER:
        property = Property.MANAGER_CLIENTPORT;
        service = Manager.class;
        break;
      case GARBAGE_COLLECTOR:
        property = Property.GC_PORT;
        service = SimpleGarbageCollector.class;
        break;
      default:
        throw new IllegalArgumentException("Irrelevant server type for test");
    }

    return cluster._exec(service, serverType, Map.of(property.getKey(), Integer.toString(port)))
        .getProcess();
  }
}

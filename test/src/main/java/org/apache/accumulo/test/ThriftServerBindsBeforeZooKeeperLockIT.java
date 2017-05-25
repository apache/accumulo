/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.MonitorUtil;
import org.apache.accumulo.fate.zookeeper.ZooReader;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.master.Master;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloClusterImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.monitor.Monitor;
import org.apache.accumulo.server.util.PortUtils;
import org.apache.accumulo.test.categories.MiniClusterOnlyTests;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * Test class that verifies "HA-capable" servers put up their thrift servers before acquiring their ZK lock.
 */
@Category({MiniClusterOnlyTests.class})
public class ThriftServerBindsBeforeZooKeeperLockIT extends AccumuloClusterHarness {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftServerBindsBeforeZooKeeperLockIT.class);

  @Override
  public boolean canRunTest(ClusterType type) {
    return ClusterType.MINI == type;
  }

  @Test
  public void testMonitorService() throws Exception {
    final MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
    Collection<ProcessReference> monitors = cluster.getProcesses().get(ServerType.MONITOR);
    // Need to start one monitor and let it become active.
    if (null == monitors || 0 == monitors.size()) {
      getClusterControl().start(ServerType.MONITOR, "localhost");
    }

    final ZooKeeperInstance inst = new ZooKeeperInstance(cluster.getClientConfig());
    while (true) {
      try {
        MonitorUtil.getLocation(inst);
        break;
      } catch (Exception e) {
        LOG.debug("Failed to find active monitor location, retrying", e);
        Thread.sleep(1000);
      }
    }

    LOG.debug("Found active monitor");

    while (true) {
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
            if (HttpURLConnection.HTTP_OK == responseCode) {
              return;
            } else {
              errorText = FunctionalTestUtils.readAll(cnxn.getErrorStream());
            }
            LOG.debug("Unexpected responseCode and/or error text, will retry: '{}' '{}'", responseCode, errorText);
          } catch (Exception e) {
            LOG.debug("Caught exception trying to fetch monitor info", e);
          }
          // Wait before trying again
          Thread.sleep(1000);
          // Make sure the process is still up. Possible the "randomFreePort" we got wasn't actually free and the process
          // died trying to bind it. Pick a new port and restart it in that case.
          if (!monitor.isAlive()) {
            freePort = PortUtils.getRandomFreePort();
            monitorUrl = "http://localhost:" + freePort;
            LOG.debug("Monitor died, restarting it listening on {}", freePort);
            monitor = startProcess(cluster, ServerType.MONITOR, freePort);
          }
        }
      } finally {
        if (null != monitor) {
          monitor.destroyForcibly();
        }
      }
    }
  }

  @Test
  public void testMasterService() throws Exception {
    final MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
    final ZooKeeperInstance inst = new ZooKeeperInstance(cluster.getClientConfig());

    // Wait for the Master to grab its lock
    while (true) {
      final ZooReader reader = new ZooReader(inst.getZooKeepers(), 30000);
      try {
        List<String> locks = reader.getChildren(Constants.ZROOT + "/" + inst.getInstanceID() + Constants.ZMASTER_LOCK);
        if (locks.size() > 0) {
          break;
        }
      } catch (Exception e) {
        LOG.debug("Failed to find active master location, retrying", e);
        Thread.sleep(1000);
      }
    }

    LOG.debug("Found active master");

    while (true) {
      int freePort = PortUtils.getRandomFreePort();
      Process master = null;
      try {
        LOG.debug("Starting standby master on {}", freePort);
        master = startProcess(cluster, ServerType.MASTER, freePort);

        while (true) {
          Socket s = null;
          try {
            s = new Socket("localhost", freePort);
            if (s.isConnected()) {
              // Pass
              return;
            }
          } catch (Exception e) {
            LOG.debug("Caught exception trying to connect to Master", e);
          } finally {
            if (null != s) {
              s.close();
            }
          }
          // Wait before trying again
          Thread.sleep(1000);
          // Make sure the process is still up. Possible the "randomFreePort" we got wasn't actually free and the process
          // died trying to bind it. Pick a new port and restart it in that case.
          if (!master.isAlive()) {
            freePort = PortUtils.getRandomFreePort();
            LOG.debug("Master died, restarting it listening on {}", freePort);
            master = startProcess(cluster, ServerType.MASTER, freePort);
          }
        }
      } finally {
        if (null != master) {
          master.destroyForcibly();
        }
      }
    }
  }

  @Test
  public void testGarbageCollectorPorts() throws Exception {
    final MiniAccumuloClusterImpl cluster = (MiniAccumuloClusterImpl) getCluster();
    final ZooKeeperInstance inst = new ZooKeeperInstance(cluster.getClientConfig());

    // Wait for the Master to grab its lock
    while (true) {
      final ZooReader reader = new ZooReader(inst.getZooKeepers(), 30000);
      try {
        List<String> locks = reader.getChildren(Constants.ZROOT + "/" + inst.getInstanceID() + Constants.ZGC_LOCK);
        if (locks.size() > 0) {
          break;
        }
      } catch (Exception e) {
        LOG.debug("Failed to find active gc location, retrying", e);
        Thread.sleep(1000);
      }
    }

    LOG.debug("Found active gc");

    while (true) {
      int freePort = PortUtils.getRandomFreePort();
      Process master = null;
      try {
        LOG.debug("Starting standby gc on {}", freePort);
        master = startProcess(cluster, ServerType.GARBAGE_COLLECTOR, freePort);

        while (true) {
          Socket s = null;
          try {
            s = new Socket("localhost", freePort);
            if (s.isConnected()) {
              // Pass
              return;
            }
          } catch (Exception e) {
            LOG.debug("Caught exception trying to connect to GC", e);
          } finally {
            if (null != s) {
              s.close();
            }
          }
          // Wait before trying again
          Thread.sleep(1000);
          // Make sure the process is still up. Possible the "randomFreePort" we got wasn't actually free and the process
          // died trying to bind it. Pick a new port and restart it in that case.
          if (!master.isAlive()) {
            freePort = PortUtils.getRandomFreePort();
            LOG.debug("GC died, restarting it listening on {}", freePort);
            master = startProcess(cluster, ServerType.GARBAGE_COLLECTOR, freePort);
          }
        }
      } finally {
        if (null != master) {
          master.destroyForcibly();
        }
      }
    }
  }

  private Process startProcess(MiniAccumuloClusterImpl cluster, ServerType serverType, int port) throws IOException {
    final Property property;
    final Class<?> service;
    switch (serverType) {
      case MONITOR:
        property = Property.MONITOR_PORT;
        service = Monitor.class;
        break;
      case MASTER:
        property = Property.MASTER_CLIENTPORT;
        service = Master.class;
        break;
      case GARBAGE_COLLECTOR:
        property = Property.GC_PORT;
        service = SimpleGarbageCollector.class;
        break;
      default:
        throw new IllegalArgumentException("Irrelevant server type for test");
    }

    return cluster._exec(service, serverType, ImmutableMap.of(property.getKey(), Integer.toString(port)));
  }
}

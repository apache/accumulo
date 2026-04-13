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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.lock.ServiceLockPaths.AddressSelector;
import org.apache.accumulo.core.rpc.clients.ManagerClient;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class MultipleManagerStartupWaitIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Set this lower so that locks timeout faster
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MIN_COUNT, "2");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MAX_WAIT, "5m");
    super.configure(cfg, hadoopCoreSite);
  }

  @Override
  public void setUp() throws Exception {
    // Overriding setup here so that the cluster
    // is not started. We are going to start in
    // manually in the test method
  }

  @Test
  public void testManagerWait() throws Exception {

    AtomicReference<Throwable> startError = new AtomicReference<>();
    Thread clusterThread = new Thread(() -> {
      try {
        super.setUp();
      } catch (Exception e) {
        startError.set(e);
      }
    });
    clusterThread.start();

    // Wait a few seconds for processes to start
    // ServiceLock is set during start after starting
    // the processes and before verifyUp is called.-
    Wait.waitFor(() -> getCluster() != null);
    Wait.waitFor(() -> getCluster().getServerContext() != null);
    Wait.waitFor(() -> getCluster().getServerContext().getServiceLock() != null);

    // One Manager should be up and have acquired the assistant manager lock
    Wait.waitFor(() -> getCluster().getServerContext().getServerPaths()
        .getAssistantManagers(AddressSelector.all(), true).size() == 1);

    // The Primary Manager lock should not be acquired yet
    assertNull(getCluster().getServerContext().getServerPaths().getManager(true));

    // Start the 2nd Manager
    getCluster().getConfig().getClusterServerConfiguration().setNumManagers(2);
    // Don't call Cluster.start, it's synchronized and the thread
    // we started is blocked on it until the primary manager lock
    // is acquired
    getCluster().getClusterControl().start(ServerType.MANAGER);

    // Wait for both Managers to acquire the assistant manager locks
    Wait.waitFor(() -> getCluster().getServerContext().getServerPaths()
        .getAssistantManagers(AddressSelector.all(), true).size() == 2);

    // The Primary Manager lock should now be acquired yet
    Wait.waitFor(() -> getCluster().getServerContext().getServerPaths().getManager(true) != null);

    var managers = getCluster().getServerContext().getServerPaths()
        .getAssistantManagers(AddressSelector.all(), true);
    assertEquals(2, managers.size());
    Set<String> managerHosts = new HashSet<>();
    managers.forEach(m -> managerHosts.add(m.getServer()));

    // There is a time period where the primary manager has the lock but has not yet set the address
    // on the lock, when the address is not set an empty set would be returned. So this waits for
    // the address to be set.
    Wait.waitFor(() -> ManagerClient.getPrimaryManagerLocation(cluster.getServerContext()) != null);
    var primaryAddr = ManagerClient.getPrimaryManagerLocation(cluster.getServerContext());
    assertTrue(managerHosts.contains(primaryAddr),
        () -> "mangerHosts:" + managerHosts + " primaryAddr:" + primaryAddr);
    assertNull(startError.get());
    clusterThread.join();
  }
}

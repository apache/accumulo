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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

public class MultipleManagerStartupWaitIT extends ConfigurableMacBase {

  @Override
  protected void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    // Set this lower so that locks timeout faster
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "5s");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MIN_COUNT, "2");
    cfg.setProperty(Property.MANAGER_STARTUP_MANAGER_AVAIL_MAX_WAIT, "10s");
    super.configure(cfg, hadoopCoreSite);
  }

  @Override
  public void setUp() throws Exception {
    // Overriding setup here so that the cluster
    // is not started. We are going to start in
    // manually in the test method
  }

  private List<String> getAssistantManagers() throws KeeperException, InterruptedException {
    String zAsstMgrPath = Constants.ZMANAGER_ASSISTANT_LOCK + "/" + ResourceGroupId.DEFAULT;
    return getCluster().getServerContext().getZooSession().getChildren(zAsstMgrPath, null);
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

    // Wait a few seconds for processes to start and
    // for ServerContext to be created
    Thread.sleep(10_000);

    // One Manager should be up and have acquired the assistant manager lock
    Wait.waitFor(() -> getAssistantManagers().size() == 1);

    // The Primary Manager lock should not be acquired yet
    assertNull(getCluster().getServerContext().getServerPaths().getManager(true));

    // Start the 2nd Manager
    getCluster().getConfig().getClusterServerConfiguration().setNumManagers(2);
    getCluster().start();

    // Wait for both Managers to acquire the assistant manager locks
    Wait.waitFor(() -> getAssistantManagers().size() == 2);

    // The Primary Manager lock should now be acquired yet
    assertNotNull(getCluster().getServerContext().getServerPaths().getManager(true));

    List<String> managers = getAssistantManagers();
    assertEquals(2, managers.size());

    Set<ServerId> primary =
        getCluster().getServerContext().instanceOperations().getServers(ServerId.Type.MANAGER);
    assertNotNull(primary);
    assertEquals(1, primary.size());
    assertTrue(managers.contains(primary.iterator().next().toHostPortString()));
    assertNull(startError.get());
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.fate.zookeeper.ZooReaderWriter;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.accumulo.core.lock.ServiceLock;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.manager.LiveManagerSet;
import org.apache.accumulo.manager.Manager;
import org.junit.jupiter.api.Test;

public class MultipleManagerLockIT extends ConfigurableMacBase implements LiveManagerSet.Listener {

  private final AtomicInteger changeCount = new AtomicInteger(0);
  private Collection<String> zooKeeperManagerAddrs = null;

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void managersChanged(Collection<String> addresses) {
    System.out.println("callback called: " + addresses);
    changeCount.incrementAndGet();
    zooKeeperManagerAddrs = addresses;
  }

  @Test
  public void test() throws Exception {

    Process secondManager = null;
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      final ZooReaderWriter writer = getCluster().getServerContext().getZooReaderWriter();
      final String root = "/accumulo/" + client.instanceOperations().getInstanceId();
      final LiveManagerSet live = new LiveManagerSet(getServerContext(), this);
      live.startListeningForManagerServerChanges();

      // wait for manager
      while (changeCount.get() == 0) {
        Thread.sleep(100);
      }
      int changes = changeCount.get();
      assertEquals(1, changes);

      // create a second manager
      secondManager = exec(Manager.class);

      List<String> managers = ((ClientContext) client).getManagerLocations();
      while (managers.size() != 2) {
        UtilWaitThread.sleep(100);
        managers = ((ClientContext) client).getManagerLocations();
      }
      System.out.println("managers: " + managers.toString());

      String primary = ((ClientContext) client).getPrimaryManagerLocation();
      System.out.println("primary: " + primary);
      assertTrue(managers.contains(primary));
      assertEquals(changes + 1, changeCount.get());
      assertEquals(managers, zooKeeperManagerAddrs);
      changes = changeCount.get();

      // Remove the primary manager lock manually. This will cause the
      // primary manager to shutdown and the other manager should become
      // the primary.
      List<String> childLocks = writer.getChildren(root + Constants.ZMANAGER_LOCK);
      assertEquals(2, childLocks.size());
      childLocks =
          ServiceLock.validateAndSort(ServiceLock.path(root + Constants.ZMANAGER_LOCK), childLocks);
      System.out.println("primary lock children: " + childLocks.toString());
      String currentLock = childLocks.get(0);
      System.out.println("current primary lock is: " + currentLock);
      String nextLock = childLocks.get(1);
      System.out.println("deleting current manager lock, next primary lock should be: " + nextLock);
      writer.recursiveDelete(root + Constants.ZMANAGERS + "/" + primary, NodeMissingPolicy.FAIL);

      // wait for primary to change
      String nextPrimary = ((ClientContext) client).getPrimaryManagerLocation();
      while (nextPrimary == null || primary.equals(nextPrimary)) {
        Thread.sleep(100);
        nextPrimary = ((ClientContext) client).getPrimaryManagerLocation();
      }
      System.out.println("next primary manager is: " + nextPrimary);

      managers = ((ClientContext) client).getManagerLocations();
      while (managers.size() != 1) {
        Thread.sleep(100);
        managers = ((ClientContext) client).getManagerLocations();
      }
      System.out.println("managers: " + managers.toString());

      assertEquals(1, managers.size());
      assertTrue(managers.contains(nextPrimary));

      childLocks = writer.getChildren(root + Constants.ZMANAGER_LOCK);
      System.out.println("primary lock children: " + childLocks.toString());
      assertEquals(1, childLocks.size());

      while (changeCount.get() != (changes + 1)) {
        Thread.sleep(500);
      }
      assertEquals(managers, zooKeeperManagerAddrs);
      changes = changeCount.get();

    } finally {
      if (secondManager != null) {
        secondManager.destroy();
      }
    }
  }

}

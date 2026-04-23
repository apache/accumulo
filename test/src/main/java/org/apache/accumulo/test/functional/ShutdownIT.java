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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.servers.ServerId;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloClusterImpl;
import org.apache.accumulo.server.util.adminCommand.StopAll;
import org.apache.accumulo.server.util.adminCommand.StopServers;
import org.apache.accumulo.test.TestIngest;
import org.apache.accumulo.test.TestRandomDeletes;
import org.apache.accumulo.test.VerifyIngest;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.Test;

public class ShutdownIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Test
  public void shutdownDuringIngest() throws Exception {
    Process ingest = cluster
        .exec(TestIngest.class, "-c", cluster.getClientPropsPath(), "--createTable").getProcess();
    Thread.sleep(100);
    assertEquals(0, cluster.exec(StopAll.class).getProcess().waitFor());
    ingest.destroy();
  }

  @Test
  public void shutdownDuringQuery() throws Exception {
    assertEquals(0,
        cluster.exec(TestIngest.class, "-c", cluster.getClientPropsPath(), "--createTable")
            .getProcess().waitFor());
    Process verify =
        cluster.exec(VerifyIngest.class, "-c", cluster.getClientPropsPath()).getProcess();
    Thread.sleep(100);
    assertEquals(0, cluster.exec(StopAll.class).getProcess().waitFor());
    verify.destroy();
  }

  @Test
  public void shutdownDuringDelete() throws Exception {
    assertEquals(0,
        cluster.exec(TestIngest.class, "-c", cluster.getClientPropsPath(), "--createTable")
            .getProcess().waitFor());
    Process deleter =
        cluster.exec(TestRandomDeletes.class, "-c", cluster.getClientPropsPath()).getProcess();
    Thread.sleep(100);
    assertEquals(0, cluster.exec(StopAll.class).getProcess().waitFor());
    deleter.destroy();
  }

  @Test
  public void shutdownDuringDeleteTable() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      for (int i = 0; i < 10; i++) {
        c.tableOperations().create("table" + i);
      }
      final AtomicReference<Exception> ref = new AtomicReference<>();
      Thread async = new Thread(() -> {
        try {
          for (int i = 0; i < 10; i++) {
            Thread.sleep(100);
            c.tableOperations().delete("table" + i);
          }
        } catch (Exception ex) {
          ref.set(ex);
        }
      });
      async.start();
      Thread.sleep(100);
      assertEquals(0, cluster.exec(StopAll.class).getProcess().waitFor());
      // give the backfound delete operations a bit to run
      Thread.sleep(3000);
      // The delete operations should get stuck or run, but should not throw an exception
      if (ref.get() != null) {
        throw ref.get();
      }
    }
  }

  @Test
  public void stopDuringStart() throws Exception {
    assertEquals(0, cluster.exec(StopAll.class).getProcess().waitFor());
  }

  @Test
  public void adminStop() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      runAdminStopTest(c, cluster);
    }
  }

  static void runAdminStopTest(AccumuloClient c, MiniAccumuloClusterImpl cluster)
      throws InterruptedException, IOException {
    int x = cluster.exec(TestIngest.class, "-c", cluster.getClientPropsPath(), "--createTable")
        .getProcess().waitFor();
    assertEquals(0, x);
    Set<ServerId> tabletServers = c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
    assertEquals(2, tabletServers.size());
    ServerId doomed = tabletServers.iterator().next();
    log.info("Stopping " + doomed);
    assertEquals(0,
        cluster.exec(StopServers.class, doomed.toHostPortString()).getProcess().waitFor());
    Wait.waitFor(() -> c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER).size() == 1);
    tabletServers = c.instanceOperations().getServers(ServerId.Type.TABLET_SERVER);
    assertEquals(1, tabletServers.size());
    assertNotEquals(tabletServers.iterator().next(), doomed);
  }
}

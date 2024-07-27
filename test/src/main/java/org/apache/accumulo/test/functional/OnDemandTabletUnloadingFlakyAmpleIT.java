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

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.test.ample.FlakyAmpleManager;
import org.apache.accumulo.test.ample.FlakyAmpleTserver;
import org.apache.accumulo.test.util.Wait;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This test causes ondemand tablet to unload in a tablet server using FlakyAmple. When tablets
 * ondemand tablets unload a conditional mutation is written and in this test that will happen with
 * FlakyAmple.
 */
public class OnDemandTabletUnloadingFlakyAmpleIT extends SharedMiniClusterBase {
  @BeforeAll
  public static void beforeAll() throws Exception {

    SharedMiniClusterBase.startMiniClusterWithConfig((cfg, core) -> {
      cfg.getClusterServerConfiguration().setNumDefaultScanServers(1);
      cfg.getClusterServerConfiguration().setNumDefaultTabletServers(1);
      cfg.setProperty(Property.MANAGER_TABLET_GROUP_WATCHER_INTERVAL, "1s");
      cfg.setProperty(Property.TSERV_ONDEMAND_UNLOADER_INTERVAL, "3s");
      cfg.setProperty("table.custom.ondemand.unloader.inactivity.threshold.seconds", "3");
      cfg.setServerClass(ServerType.TABLET_SERVER, FlakyAmpleTserver.class);
      cfg.setServerClass(ServerType.MANAGER, FlakyAmpleManager.class);
    });
  }

  @AfterAll
  public static void afterAll() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  @Test
  public void testOndemandFlakyUnload() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      String tableName = super.getUniqueNames(1)[0];

      c.tableOperations().create(tableName);

      var tableId = getCluster().getServerContext().getTableId(tableName);

      try (var writer = c.createBatchWriter(tableName)) {
        var m = new Mutation("ondemand");
        m.put("test", "data", "o");
        writer.addMutation(m);
      }

      // wait for tablet to unload, should be able to unload even if the conditional mutation flakes
      // out
      Wait.waitFor(() -> {
        var ample = ((ClientContext) c).getAmple();
        return ample.readTablets().forTable(tableId).build().stream()
            .allMatch(tm -> tm.getLocation() == null);
      });

      // verify can read data after unload, should cause tablet to reload
      try (var scanner = c.createScanner(tableName)) {
        assertEquals(1, scanner.stream().count());
      }
    }
  }
}

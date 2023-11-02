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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletHostingGoal;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class AssignLocationModeIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(2);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(Property.TSERV_LAST_LOCATION_MODE, "assignment");
  }

  @Test
  public void test() throws Exception {
    try (AccumuloClient c =
        getCluster().createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD))) {
      String tableName = super.getUniqueNames(1)[0];
      NewTableConfiguration ntc =
          new NewTableConfiguration().withInitialHostingGoal(TabletHostingGoal.ALWAYS);
      c.tableOperations().create(tableName, ntc);
      String tableId = c.tableOperations().tableIdMap().get(tableName);
      // wait for the table to be online
      TabletMetadata newTablet;
      do {
        UtilWaitThread.sleep(250);
        newTablet = ManagerAssignmentIT.getTabletMetadata(c, tableId, null);
      } while (!newTablet.hasCurrent());
      // this would be null if the mode was not "assign"
      assertEquals(newTablet.getLocation().getHostPort(), newTablet.getLast().getHostPort());

      // put something in it
      try (BatchWriter bw = c.createBatchWriter(tableName)) {
        Mutation m = new Mutation("a");
        m.put("b", "c", "d");
        bw.addMutation(m);
      }
      // assert that the default mode is "assign"
      assertEquals("assignment", c.instanceOperations().getSystemConfiguration()
          .get(Property.TSERV_LAST_LOCATION_MODE.getKey()));

      // last location should not be set yet
      TabletMetadata unflushed = ManagerAssignmentIT.getTabletMetadata(c, tableId, null);
      assertEquals(newTablet.getLocation().getHostPort(), unflushed.getLocation().getHostPort());
      assertEquals(newTablet.getLocation().getHostPort(), unflushed.getLast().getHostPort());
      assertTrue(newTablet.hasCurrent());

      // take the tablet offline
      c.tableOperations().offline(tableName, true);
      TabletMetadata offline = ManagerAssignmentIT.getTabletMetadata(c, tableId, null);
      assertNull(offline.getLocation());
      assertFalse(offline.hasCurrent());
      assertEquals(newTablet.getLocation().getHostPort(), offline.getLast().getHostPort());

      // put it back online, should have the same last location
      c.tableOperations().online(tableName, true);
      TabletMetadata online = ManagerAssignmentIT.getTabletMetadata(c, tableId, null);
      assertTrue(online.hasCurrent());
      assertNotNull(online.getLocation());
      assertEquals(newTablet.getLast().getHostPort(), online.getLast().getHostPort());

      c.instanceOperations().setProperty(Property.TSERV_LAST_LOCATION_MODE.getKey(), "none");
      // Wait for property to sync across servers and wait 2x TabletGroupWatcher interval (5s)
      // to ensure that TGW did *not* update TabletMetadata (because the Tablet is hosted)
      UtilWaitThread.sleep(1000);
      online = ManagerAssignmentIT.getTabletMetadata(c, tableId, null);
      assertTrue(online.hasCurrent());
      assertNotNull(online.getLocation());
      assertEquals(newTablet.getLast().getHostPort(), online.getLast().getHostPort());

      // Unload the Tablet
      c.tableOperations().offline(tableName, true);
      offline = ManagerAssignmentIT.getTabletMetadata(c, tableId, null);
      assertNull(offline.getLocation());
      assertFalse(offline.hasCurrent());
      // Check that last is null
      assertNull(offline.getLast());
    }
  }

}

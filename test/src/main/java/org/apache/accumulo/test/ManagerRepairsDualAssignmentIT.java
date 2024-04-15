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

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TabletAvailability;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class ManagerRepairsDualAssignmentIT extends ConfigurableMacBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(5);
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.MANAGER_RECOVERY_DELAY, "5s");
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void test() throws Exception {
    // make some tablets, spread 'em around
    try (AccumuloClient c = Accumulo.newClient().from(getClientProperties()).build()) {
      ServerContext serverContext = cluster.getServerContext();
      String table = this.getUniqueNames(1)[0];
      c.securityOperations().grantTablePermission("root", AccumuloTable.METADATA.tableName(),
          TablePermission.WRITE);
      c.securityOperations().grantTablePermission("root", AccumuloTable.ROOT.tableName(),
          TablePermission.WRITE);
      SortedSet<Text> partitions = new TreeSet<>();
      for (String part : "a b c d e f g h i j k l m n o p q r s t u v w x y z".split(" ")) {
        partitions.add(new Text(part));
      }
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(partitions)
          .withInitialTabletAvailability(TabletAvailability.HOSTED);
      c.tableOperations().create(table, ntc);
      // scan the metadata table and get the two table location states
      final Set<TabletMetadata.Location> states = new HashSet<>();
      final Set<TabletMetadata> oldLocations = new HashSet<>();

      while (states.size() < 2) {
        Thread.sleep(250);
        oldLocations.clear();
        try (
            var tablets = serverContext.getAmple().readTablets().forLevel(DataLevel.USER).build()) {
          tablets.iterator().forEachRemaining(tm -> {
            if (tm.hasCurrent()) {
              states.add(tm.getLocation());
              oldLocations.add(tm);
            }
          });
        }

      }
      assertEquals(2, states.size());
      // Kill a tablet server... we don't care which one... wait for everything to be reassigned
      cluster.killProcess(ServerType.TABLET_SERVER,
          cluster.getProcesses().get(ServerType.TABLET_SERVER).iterator().next());
      // Find out which tablet server remains
      while (true) {
        Thread.sleep(1000);
        states.clear();
        AtomicBoolean allAssigned = new AtomicBoolean(true);
        try (
            var tablets = serverContext.getAmple().readTablets().forLevel(DataLevel.USER).build()) {
          tablets.iterator().forEachRemaining(tm -> {
            if (tm.hasCurrent()) {
              states.add(tm.getLocation());
            } else {
              allAssigned.set(false);
            }
          });
        }
        System.out.println(states + " size " + states.size() + " allAssigned " + allAssigned);
        if (states.size() != 2 && allAssigned.get()) {
          break;
        }
      }
      assertEquals(1, states.size());
      // pick an assigned tablet and assign it to the old tablet
      TabletMetadata moved = null;
      for (TabletMetadata old : oldLocations) {
        if (!states.contains(old.getLocation())) {
          moved = old;
        }
      }
      assertNotNull(moved);
      // throw a mutation in as if we were the dying tablet
      TabletMutator tabletMutator = serverContext.getAmple().mutateTablet(moved.getExtent());
      tabletMutator.putLocation(moved.getLocation());
      tabletMutator.mutate();
      // wait for the manager to fix the problem
      waitForCleanStore(serverContext, DataLevel.USER);
      // now jam up the metadata table
      tabletMutator = serverContext.getAmple()
          .mutateTablet(new KeyExtent(AccumuloTable.METADATA.tableId(), null, null));
      tabletMutator.putLocation(moved.getLocation());
      tabletMutator.mutate();
      waitForCleanStore(serverContext, DataLevel.METADATA);
    }
  }

  private void waitForCleanStore(ServerContext serverContext, DataLevel level)
      throws InterruptedException {
    while (true) {
      try (var tablets = serverContext.getAmple().readTablets().forLevel(level).build()) {
        tablets.iterator().forEachRemaining(t -> {});
      } catch (Exception ex) {
        System.out.println(ex.getMessage());
        Thread.sleep(250);
        continue;
      }
      break;
    }
  }

}

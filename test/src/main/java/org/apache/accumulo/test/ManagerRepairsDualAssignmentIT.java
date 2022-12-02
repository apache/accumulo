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
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.TabletLocationState;
import org.apache.accumulo.core.metadata.schema.Ample.DataLevel;
import org.apache.accumulo.core.metadata.schema.Ample.TabletMutator;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.LocationType;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.manager.state.ClosableIterator;
import org.apache.accumulo.server.manager.state.TabletStateStore;
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
      ClientContext context = (ClientContext) c;
      ServerContext serverContext = cluster.getServerContext();
      String table = this.getUniqueNames(1)[0];
      c.securityOperations().grantTablePermission("root", MetadataTable.NAME,
          TablePermission.WRITE);
      c.securityOperations().grantTablePermission("root", RootTable.NAME, TablePermission.WRITE);
      SortedSet<Text> partitions = new TreeSet<>();
      for (String part : "a b c d e f g h i j k l m n o p q r s t u v w x y z".split(" ")) {
        partitions.add(new Text(part));
      }
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(partitions);
      c.tableOperations().create(table, ntc);
      // scan the metadata table and get the two table location states
      Set<TServerInstance> states = new HashSet<>();
      Set<TabletLocationState> oldLocations = new HashSet<>();
      TabletStateStore store = TabletStateStore.getStoreForLevel(DataLevel.USER, context);
      while (states.size() < 2) {
        UtilWaitThread.sleep(250);
        oldLocations.clear();
        for (TabletLocationState tls : store) {
          if (tls.current != null) {
            states.add(tls.current);
            oldLocations.add(tls);
          }
        }
      }
      assertEquals(2, states.size());
      // Kill a tablet server... we don't care which one... wait for everything to be reassigned
      cluster.killProcess(ServerType.TABLET_SERVER,
          cluster.getProcesses().get(ServerType.TABLET_SERVER).iterator().next());
      // Find out which tablet server remains
      while (true) {
        UtilWaitThread.sleep(1000);
        states.clear();
        boolean allAssigned = true;
        for (TabletLocationState tls : store) {
          if (tls != null && tls.current != null) {
            states.add(tls.current);
          } else {
            allAssigned = false;
          }
        }
        System.out.println(states + " size " + states.size() + " allAssigned " + allAssigned);
        if (states.size() != 2 && allAssigned) {
          break;
        }
      }
      assertEquals(1, states.size());
      // pick an assigned tablet and assign it to the old tablet
      TabletLocationState moved = null;
      for (TabletLocationState old : oldLocations) {
        if (!states.contains(old.current)) {
          moved = old;
        }
      }
      assertNotEquals(null, moved);
      // throw a mutation in as if we were the dying tablet
      TabletMutator tabletMutator = serverContext.getAmple().mutateTablet(moved.extent);
      tabletMutator.putLocation(moved.current, LocationType.CURRENT);
      tabletMutator.mutate();
      // wait for the manager to fix the problem
      waitForCleanStore(store);
      // now jam up the metadata table
      tabletMutator =
          serverContext.getAmple().mutateTablet(new KeyExtent(MetadataTable.ID, null, null));
      tabletMutator.putLocation(moved.current, LocationType.CURRENT);
      tabletMutator.mutate();
      waitForCleanStore(TabletStateStore.getStoreForLevel(DataLevel.METADATA, context));
    }
  }

  private void waitForCleanStore(TabletStateStore store) {
    while (true) {
      try (ClosableIterator<TabletLocationState> iter = store.iterator()) {
        iter.forEachRemaining(t -> {});
      } catch (Exception ex) {
        System.out.println(ex);
        UtilWaitThread.sleep(250);
        continue;
      }
      break;
    }
  }

}

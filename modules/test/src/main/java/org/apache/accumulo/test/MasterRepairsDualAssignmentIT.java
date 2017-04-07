/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.master.state.ClosableIterator;
import org.apache.accumulo.server.master.state.MetaDataStateStore;
import org.apache.accumulo.server.master.state.RootTabletStateStore;
import org.apache.accumulo.server.master.state.TServerInstance;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class MasterRepairsDualAssignmentIT extends ConfigurableMacBase {

  @Override
  public int defaultTimeoutSeconds() {
    return 5 * 60;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setProperty(Property.MASTER_RECOVERY_DELAY, "5s");
    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void test() throws Exception {
    // make some tablets, spread 'em around
    Connector c = getConnector();
    ClientContext context = new ClientContext(c.getInstance(), new Credentials("root", new PasswordToken(ROOT_PASSWORD)), getClientConfig());
    String table = this.getUniqueNames(1)[0];
    c.securityOperations().grantTablePermission("root", MetadataTable.NAME, TablePermission.WRITE);
    c.securityOperations().grantTablePermission("root", RootTable.NAME, TablePermission.WRITE);
    c.tableOperations().create(table);
    SortedSet<Text> partitions = new TreeSet<>();
    for (String part : "a b c d e f g h i j k l m n o p q r s t u v w x y z".split(" ")) {
      partitions.add(new Text(part));
    }
    c.tableOperations().addSplits(table, partitions);
    // scan the metadata table and get the two table location states
    Set<TServerInstance> states = new HashSet<>();
    Set<TabletLocationState> oldLocations = new HashSet<>();
    MetaDataStateStore store = new MetaDataStateStore(context, null);
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
    cluster.killProcess(ServerType.TABLET_SERVER, cluster.getProcesses().get(ServerType.TABLET_SERVER).iterator().next());
    Set<TServerInstance> replStates = new HashSet<>();
    // Find out which tablet server remains
    while (true) {
      UtilWaitThread.sleep(1000);
      states.clear();
      replStates.clear();
      boolean allAssigned = true;
      for (TabletLocationState tls : store) {
        if (tls != null && tls.current != null) {
          states.add(tls.current);
        } else if (tls != null && tls.extent.equals(new KeyExtent(ReplicationTable.ID, null, null))) {
          replStates.add(tls.current);
        } else {
          allAssigned = false;
        }
      }
      System.out.println(states + " size " + states.size() + " allAssigned " + allAssigned);
      if (states.size() != 2 && allAssigned == true)
        break;
    }
    assertEquals(1, replStates.size());
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
    BatchWriter bw = c.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    Mutation assignment = new Mutation(moved.extent.getMetadataEntry());
    moved.current.putLocation(assignment);
    bw.addMutation(assignment);
    bw.close();
    // wait for the master to fix the problem
    waitForCleanStore(store);
    // now jam up the metadata table
    bw = c.createBatchWriter(MetadataTable.NAME, new BatchWriterConfig());
    assignment = new Mutation(new KeyExtent(MetadataTable.ID, null, null).getMetadataEntry());
    moved.current.putLocation(assignment);
    bw.addMutation(assignment);
    bw.close();
    waitForCleanStore(new RootTabletStateStore(context, null));
  }

  private void waitForCleanStore(MetaDataStateStore store) {
    while (true) {
      try (ClosableIterator<TabletLocationState> iter = store.iterator()) {
        Iterators.size(iter);
      } catch (Exception ex) {
        System.out.println(ex);
        UtilWaitThread.sleep(250);
        continue;
      }
      break;
    }
  }

}

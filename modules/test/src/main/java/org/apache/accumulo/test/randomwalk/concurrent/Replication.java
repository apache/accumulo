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
package org.apache.accumulo.test.randomwalk.concurrent;

import static org.apache.accumulo.core.conf.Property.MASTER_REPLICATION_SCAN_INTERVAL;
import static org.apache.accumulo.core.conf.Property.REPLICATION_NAME;
import static org.apache.accumulo.core.conf.Property.REPLICATION_PEERS;
import static org.apache.accumulo.core.conf.Property.REPLICATION_PEER_PASSWORD;
import static org.apache.accumulo.core.conf.Property.REPLICATION_PEER_USER;
import static org.apache.accumulo.core.conf.Property.REPLICATION_WORK_ASSIGNMENT_SLEEP;
import static org.apache.accumulo.core.conf.Property.REPLICATION_WORK_PROCESSOR_DELAY;
import static org.apache.accumulo.core.conf.Property.REPLICATION_WORK_PROCESSOR_PERIOD;
import static org.apache.accumulo.core.conf.Property.TABLE_REPLICATION;
import static org.apache.accumulo.core.conf.Property.TABLE_REPLICATION_TARGET;
import static org.apache.accumulo.server.replication.ReplicaSystemFactory.getPeerConfigurationValue;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.test.randomwalk.Environment;
import org.apache.accumulo.test.randomwalk.State;
import org.apache.accumulo.test.randomwalk.Test;
import org.apache.accumulo.tserver.replication.AccumuloReplicaSystem;
import org.apache.hadoop.io.Text;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

public class Replication extends Test {

  final int ROWS = 1000;
  final int COLS = 50;

  @Override
  public void visit(State state, Environment env, Properties props) throws Exception {
    final Connector c = env.getConnector();
    final Instance inst = c.getInstance();
    final String instName = inst.getInstanceName();
    final InstanceOperations iOps = c.instanceOperations();
    final TableOperations tOps = c.tableOperations();

    // Replicate to ourselves
    iOps.setProperty(REPLICATION_NAME.getKey(), instName);
    iOps.setProperty(REPLICATION_PEERS.getKey() + instName, getPeerConfigurationValue(AccumuloReplicaSystem.class, instName + "," + inst.getZooKeepers()));
    iOps.setProperty(REPLICATION_PEER_USER.getKey() + instName, env.getUserName());
    iOps.setProperty(REPLICATION_PEER_PASSWORD.getKey() + instName, env.getPassword());
    // Tweak some replication parameters to make the replication go faster
    iOps.setProperty(MASTER_REPLICATION_SCAN_INTERVAL.getKey(), "1s");
    iOps.setProperty(REPLICATION_WORK_ASSIGNMENT_SLEEP.getKey(), "1s");
    iOps.setProperty(REPLICATION_WORK_PROCESSOR_DELAY.getKey(), "1s");
    iOps.setProperty(REPLICATION_WORK_PROCESSOR_PERIOD.getKey(), "1s");

    // Ensure the replication table is online
    ReplicationTable.setOnline(c);
    boolean online = ReplicationTable.isOnline(c);
    for (int i = 0; i < 10; i++) {
      if (online)
        break;
      sleepUninterruptibly(2, TimeUnit.SECONDS);
      online = ReplicationTable.isOnline(c);
    }
    assertTrue("Replication table was not online", online);

    // Make a source and destination table
    final String sourceTable = ("repl-source-" + UUID.randomUUID()).replace('-', '_');
    final String destTable = ("repl-dest-" + UUID.randomUUID()).replace('-', '_');
    final String tables[] = new String[] {sourceTable, destTable};

    for (String tableName : tables) {
      log.debug("creating " + tableName);
      tOps.create(tableName);
    }

    // Point the source to the destination
    final String destID = tOps.tableIdMap().get(destTable);
    tOps.setProperty(sourceTable, TABLE_REPLICATION.getKey(), "true");
    tOps.setProperty(sourceTable, TABLE_REPLICATION_TARGET.getKey() + instName, destID);

    // zookeeper propagation wait
    sleepUninterruptibly(5, TimeUnit.SECONDS);

    // Maybe split the tables
    Random rand = new Random(System.currentTimeMillis());
    for (String tableName : tables) {
      if (rand.nextBoolean()) {
        splitTable(tOps, tableName);
      }
    }

    // write some checkable data
    BatchWriter bw = c.createBatchWriter(sourceTable, null);
    for (int row = 0; row < ROWS; row++) {
      Mutation m = new Mutation(itos(row));
      for (int col = 0; col < COLS; col++) {
        m.put("", itos(col), "");
      }
      bw.addMutation(m);
    }
    bw.close();

    // attempt to force the WAL to roll so replication begins
    final Set<String> origRefs = c.replicationOperations().referencedFiles(sourceTable);
    // write some data we will ignore
    while (true) {
      final Set<String> updatedFileRefs = c.replicationOperations().referencedFiles(sourceTable);
      updatedFileRefs.retainAll(origRefs);
      log.debug("updateFileRefs size " + updatedFileRefs.size());
      if (updatedFileRefs.isEmpty()) {
        break;
      }
      bw = c.createBatchWriter(sourceTable, null);
      for (int row = 0; row < ROWS; row++) {
        Mutation m = new Mutation(itos(row));
        for (int col = 0; col < COLS; col++) {
          m.put("ignored", itos(col), "");
        }
        bw.addMutation(m);
      }
      bw.close();
    }

    // wait a little while for replication to take place
    sleepUninterruptibly(30, TimeUnit.SECONDS);

    // check the data
    Scanner scanner = c.createScanner(destTable, Authorizations.EMPTY);
    scanner.fetchColumnFamily(new Text(""));
    int row = 0;
    int col = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertEquals(row, Integer.parseInt(entry.getKey().getRow().toString()));
      assertEquals(col, Integer.parseInt(entry.getKey().getColumnQualifier().toString()));
      col++;
      if (col == COLS) {
        row++;
        col = 0;
      }
    }
    assertEquals(ROWS, row);
    assertEquals(0, col);

    // cleanup
    for (String tableName : tables) {
      log.debug("Deleting " + tableName);
      tOps.delete(tableName);
    }
  }

  // junit isn't a dependency
  private void assertEquals(int expected, int actual) {
    if (expected != actual)
      throw new RuntimeException(String.format("%d fails to match expected value %d", actual, expected));
  }

  // junit isn't a dependency
  private void assertTrue(String string, boolean test) {
    if (!test)
      throw new RuntimeException(string);
  }

  private static String itos(int i) {
    return String.format("%08d", i);
  }

  private void splitTable(TableOperations tOps, String tableName) throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    for (int i = 1; i <= 9; i++) {
      splits.add(new Text(itos(i * (ROWS / 10))));
    }
    log.debug("Adding splits to " + tableName);
    tOps.addSplits(tableName, splits);
  }
}

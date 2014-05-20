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
package org.apache.accumulo.test.replication;

import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.replication.ReplicaSystemFactory;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * Tests for replication that should be run at every build -- basic functionality
 */
public class ReplicationWithMakerTest extends ConfigurableMacIT {

  @Override
  public int defaultTimeoutSeconds() {
    return 30;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "1M");
    // Run the process in the master which writes replication records from metadata to replication
    // repeatedly without pause
    cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "0");
    cfg.setNumTservers(1);
  }

  @Test
  public void singleTableSingleTarget() throws Exception {
    // We want to kill the GC so it doesn't come along and close Status records and mess up the comparisons
    // against expected Status messages.
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.GARBAGE_COLLECTOR)) {
      cluster.killProcess(ServerType.GARBAGE_COLLECTOR, proc);
    }

    Connector conn = getConnector();
    String table1 = "table1";

    // replication shouldn't exist when we begin
    Assert.assertFalse(conn.tableOperations().exists(ReplicationTable.NAME));

    // Create a table
    conn.tableOperations().create(table1);

    int attempts = 5;
    while (attempts > 0) {
      try {
        // Enable replication on table1
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
        // Replicate table1 to cluster1 in the table with id of '4'
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGETS.getKey() + "cluster1", "4");
        conn.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
            ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, "100000"));
        break;
      } catch (Exception e) {
        attempts--;
        if (attempts <= 0) {
          throw e;
        }
        UtilWaitThread.sleep(500);
      }
    }

    // Write some data to table1
    BatchWriter bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 2000; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 50; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    // Make sure the replication table exists at this point
    boolean exists = conn.tableOperations().exists(ReplicationTable.NAME);
    attempts = 5;
    do {
      if (!exists) {
        UtilWaitThread.sleep(200);
        exists = conn.tableOperations().exists(ReplicationTable.NAME);
        attempts--;
      }
    } while (!exists && attempts > 0);
    Assert.assertTrue("Replication table was never created", exists);

    // ACCUMULO-2743 The Observer in the tserver has to be made aware of the change to get the combiner (made by the master)
    for (int i = 0; i < 5 && !conn.tableOperations().listIterators(ReplicationTable.NAME).keySet().contains(ReplicationTable.COMBINER_NAME); i++) {
      UtilWaitThread.sleep(1000);
    }

    Assert.assertTrue("Combiner was never set on replication table",
        conn.tableOperations().listIterators(ReplicationTable.NAME).keySet().contains(ReplicationTable.COMBINER_NAME));

    // Trigger the minor compaction, waiting for it to finish.
    // This should write the entry to metadata that the file has data
    conn.tableOperations().flush(table1, null, null, true);

    // Make sure that we have one status element, should be a new file
    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    Entry<Key,Value> entry = null;
    Status expectedStatus = StatusUtil.openWithUnknownLength();
    attempts = 5;
    // This record will move from new to new with infinite length because of the minc (flush)
    while (null == entry && attempts > 0) {
      try {
        entry = Iterables.getOnlyElement(s);
        Status actual = Status.parseFrom(entry.getValue().get());
        if (actual.getInfiniteEnd() != expectedStatus.getInfiniteEnd()) {
          entry = null;
          // the master process didn't yet fire and write the new mutation, wait for it to do
          // so and try to read it again
          Thread.sleep(1000);
        }
      } catch (NoSuchElementException e) {
        entry = null;
        Thread.sleep(500);
      } catch (IllegalArgumentException e) {
        // saw this contain 2 elements once
        s = ReplicationTable.getScanner(conn);
        StatusSection.limit(s);
        for (Entry<Key,Value> content : s) {
          log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
        }
        throw e;
      } finally {
        attempts--;
      }
    }

    Assert.assertNotNull("Could not find expected entry in replication table", entry);
    Status actual = Status.parseFrom(entry.getValue().get());
    Assert.assertTrue("Expected to find a replication entry that is open with infinite length: " + ProtobufUtil.toString(actual), !actual.getClosed() && actual.getInfiniteEnd());

    // Try a couple of times to watch for the work record to be created
    boolean notFound = true;
    for (int i = 0; i < 10 && notFound; i++) {
      s = ReplicationTable.getScanner(conn);
      WorkSection.limit(s);
      int elementsFound = Iterables.size(s);
      if (0 < elementsFound) {
        Assert.assertEquals(1, elementsFound);
        notFound = false;
      }
      Thread.sleep(500);
    }

    // If we didn't find the work record, print the contents of the table
    if (notFound) {
      s = ReplicationTable.getScanner(conn);
      for (Entry<Key,Value> content : s) {
        log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
      }
      Assert.assertFalse("Did not find the work entry for the status entry", notFound);
    }

    // Write some more data so that we over-run the single WAL
    bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 2000; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 50; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    conn.tableOperations().compact(ReplicationTable.NAME, null, null, true, true);

    s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    Assert.assertEquals(2, Iterables.size(s));

    // We should eventually get 2 work records recorded, need to account for a potential delay though
    // might see: status1 -> work1 -> status2 -> (our scans) -> work2
    notFound = true;
    for (int i = 0; i < 10 && notFound; i++) {
      s = ReplicationTable.getScanner(conn);
      WorkSection.limit(s);
      int elementsFound = Iterables.size(s);
      if (2 == elementsFound) {
        notFound = false;
      }
      Thread.sleep(500);
    }

    // If we didn't find the work record, print the contents of the table
    if (notFound) {
      s = ReplicationTable.getScanner(conn);
      for (Entry<Key,Value> content : s) {
        log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
      }
      Assert.assertFalse("Did not find the work entries for the status entries", notFound);
    }
  }

  @Test
  public void correctClusterNameInWorkEntry() throws Exception {
    Connector conn = getConnector();
    String table1 = "table1";

    // replication shouldn't exist when we begin
    Assert.assertFalse(conn.tableOperations().exists(ReplicationTable.NAME));

    // Create two tables
    conn.tableOperations().create(table1);

    int attempts = 5;
    while (attempts > 0) {
      try {
        // Enable replication on table1
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
        // Replicate table1 to cluster1 in the table with id of '4'
        conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGETS.getKey() + "cluster1", "4");
        attempts = 0;
      } catch (Exception e) {
        attempts--;
        if (attempts <= 0) {
          throw e;
        }
        UtilWaitThread.sleep(500);
      }
    }

    // Write some data to table1
    BatchWriter bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 2000; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 50; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    String tableId = conn.tableOperations().tableIdMap().get(table1);
    Assert.assertNotNull("Table ID was null", tableId);

    // Make sure the replication table exists at this point
    boolean exists = conn.tableOperations().exists(ReplicationTable.NAME);
    attempts = 5;
    do {
      if (!exists) {
        UtilWaitThread.sleep(500);
        exists = conn.tableOperations().exists(ReplicationTable.NAME);
        attempts--;
      }
    } while (!exists && attempts > 0);
    Assert.assertTrue("Replication table did not exist", exists);

    for (int i = 0; i < 5 && !conn.securityOperations().hasTablePermission("root", ReplicationTable.NAME, TablePermission.READ); i++) {
      Thread.sleep(1000);
    }

    Assert.assertTrue(conn.securityOperations().hasTablePermission("root", ReplicationTable.NAME, TablePermission.READ));

    boolean notFound = true;
    Scanner s;
    for (int i = 0; i < 10 && notFound; i++) {
      s = ReplicationTable.getScanner(conn);
      WorkSection.limit(s);
      try {
        Entry<Key,Value> e = Iterables.getOnlyElement(s);
        Text expectedColqual = new ReplicationTarget("cluster1", "4", tableId).toText();
        Assert.assertEquals(expectedColqual, e.getKey().getColumnQualifier());
        notFound = false;
      } catch (NoSuchElementException e) {} catch (IllegalArgumentException e) {
        s = ReplicationTable.getScanner(conn);
        for (Entry<Key,Value> content : s) {
          log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
        }
        Assert.fail("Found more than one work section entry");
      }

      Thread.sleep(500);
    }

    if (notFound) {
      s = ReplicationTable.getScanner(conn);
      for (Entry<Key,Value> content : s) {
        log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
      }
      Assert.assertFalse("Did not find the work entry for the status entry", notFound);
    }
  }
}

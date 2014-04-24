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
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
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
    cfg.setProperty(Property.MASTER_REPLICATION_STATUS_SCAN_INTERVAL, "1s");
    cfg.setNumTservers(1);
  }

  @Test
  public void singleTableSingleTarget() throws Exception {
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

    // Make sure the replication table exists at this point and grant ourselves read access
    Assert.assertTrue(conn.tableOperations().exists(ReplicationTable.NAME));
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.READ);

    // Make sure that we have one status element, should be a new file
    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    Entry<Key,Value> entry;
    try{
      entry = Iterables.getOnlyElement(s);
    } catch (IllegalArgumentException e) {
      // saw this contain 2 elements once
      s = ReplicationTable.getScanner(conn);
      StatusSection.limit(s);
      for (Entry<Key,Value> content : s) {
        log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
      }
      throw e;
    }
    Assert.assertEquals(StatusUtil.newFile(), Status.parseFrom(entry.getValue().get()));

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

    notFound = true;
    for (int i = 0; i < 10 && notFound; i++) {
      s = ReplicationTable.getScanner(conn);
      WorkSection.limit(s);
      int elementsFound = Iterables.size(s);
      if (0 < elementsFound) {
        Assert.assertEquals(2, elementsFound);
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

    Assert.assertTrue(conn.tableOperations().exists(ReplicationTable.NAME));
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.READ);

    boolean notFound = true;
    Scanner s;
    for (int i = 0; i < 10 && notFound; i++) {
      s = ReplicationTable.getScanner(conn);
      WorkSection.limit(s);
      try {
        Entry<Key,Value> e = Iterables.getOnlyElement(s);
        Text expectedColqual = ReplicationTarget.toText(new ReplicationTarget("cluster1", "4"));
        Assert.assertEquals(expectedColqual, e.getKey().getColumnQualifier());
        notFound = false;
      } catch (NoSuchElementException e) {
      } catch (IllegalArgumentException e) {
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

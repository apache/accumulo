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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class ReplicationWithGCIT extends ConfigurableMacIT {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "1M");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void replicationRecordsAreClosedAfterGarbageCollection() throws Exception {
    Collection<ProcessReference> gcProcs = cluster.getProcesses().get(ServerType.GARBAGE_COLLECTOR);
    for (ProcessReference ref : gcProcs) {
      cluster.killProcess(ServerType.GARBAGE_COLLECTOR, ref);
    }

    Connector conn = getConnector();

    if (conn.tableOperations().exists(ReplicationTable.NAME)) {
      conn.tableOperations().delete(ReplicationTable.NAME);
    }

    ReplicationTable.create(conn);
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);

    String table1 = "table1", table2 = "table2", table3 = "table3";

    conn.tableOperations().create(table1);
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGETS.getKey() + "cluster1", "1");
    Thread.sleep(1000);

    // Write some data to table1
    BatchWriter bw = conn.createBatchWriter(table1, new BatchWriterConfig());
    for (int rows = 0; rows < 200; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 500; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    conn.tableOperations().create(table2);
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table2, Property.TABLE_REPLICATION_TARGETS.getKey() + "cluster1", "1");
    Thread.sleep(1000);

    // Write some data to table2
    bw = conn.createBatchWriter(table2, new BatchWriterConfig());
    for (int rows = 0; rows < 200; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 500; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    conn.tableOperations().create(table3);
    conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table3, Property.TABLE_REPLICATION_TARGETS.getKey() + "cluster1", "1");
    Thread.sleep(1000);

    // Write some data to table3
    bw = conn.createBatchWriter(table3, new BatchWriterConfig());
    for (int rows = 0; rows < 200; rows++) {
      Mutation m = new Mutation(Integer.toString(rows));
      for (int cols = 0; cols < 500; cols++) {
        String value = Integer.toString(cols);
        m.put(value, "", value);
      }
      bw.addMutation(m);
    }

    bw.close();

    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    for (Entry<Key,Value> entry : s) {
      Status status = Status.parseFrom(entry.getValue().get());
      Assert.assertFalse(status.getClosed());
    }

    //TODO This isn't enough to ensure that we'll get the Status records closed
    // the only way is to observe the current WALs, kill the tserver(s), restart them,
    // and then ensure that the WALs we previously observed all move to closed.
    Process gc = cluster.exec(SimpleGarbageCollector.class);
    try {
      boolean allClosed = true;
      for (int i = 0; i < 10; i++) {
        UtilWaitThread.sleep(1000);
        allClosed = true;

        s = ReplicationTable.getScanner(conn);
        StatusSection.limit(s);
        Iterator<Entry<Key,Value>> iter = s.iterator();
        
        while (allClosed && iter.hasNext()) {
          Status status = Status.parseFrom(iter.next().getValue().get());
          allClosed &= status.getClosed();
        }

        if (allClosed) {
          return;
        }
      }
    } finally {
      gc.destroy();
      gc.waitFor();
    }

    s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    for (Entry<Key,Value> entry : s) {
      System.out.println(entry.getKey().toStringNoTruncate() + " " + Status.parseFrom(entry.getValue().get()).toString().replace("\n", ", "));
    }
    Assert.fail("Expected all replication records to be closed");
  }

}

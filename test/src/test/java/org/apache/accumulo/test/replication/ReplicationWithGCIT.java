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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.StatusUtil;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.gc.SimpleGarbageCollector;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.minicluster.impl.ProcessReference;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.accumulo.tserver.TabletServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
    cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "0");
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private Set<String> metadataWals(Connector conn) throws Exception {
    Scanner s = conn.createScanner(MetadataTable.NAME, new Authorizations());
    s.fetchColumnFamily(LogColumnFamily.NAME);
    Set<String> metadataWals = new HashSet<>();
    for (Entry<Key,Value> entry : s) {
      LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());
      for (String log : logEntry.logSet) {
        metadataWals.add(new Path(log).toString());
      }
    }
    return metadataWals;
  }

  @Test(timeout = 4 * 60 * 1000)
  public void replicationRecordsAreClosedAfterGarbageCollection() throws Exception {
    Collection<ProcessReference> gcProcs = cluster.getProcesses().get(ServerType.GARBAGE_COLLECTOR);
    for (ProcessReference ref : gcProcs) {
      cluster.killProcess(ServerType.GARBAGE_COLLECTOR, ref);
    }

    final Connector conn = getConnector();

    if (conn.tableOperations().exists(ReplicationTable.NAME)) {
      conn.tableOperations().delete(ReplicationTable.NAME);
    }

    ReplicationTable.create(conn);
    conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);

    final AtomicBoolean keepRunning = new AtomicBoolean(true);
    final Set<String> metadataWals = new HashSet<>();

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        // Should really be able to interrupt here, but the Scanner throws a fit to the logger
        // when that happens
        while (keepRunning.get()) {
          try {
            metadataWals.addAll(metadataWals(conn));
          } catch (Exception e) {
            log.error("Metadata table doesn't exist");
          }
        }
      }

    });

    t.start();

    String table1 = "table1", table2 = "table2", table3 = "table3";

    conn.tableOperations().create(table1);
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGETS.getKey() + "cluster1", "1");

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

    // Flush everything to try to make the replication records
    for (String table : Arrays.asList(table1, table2, table3)) {
      conn.tableOperations().flush(table, null, null, true);
    }

    keepRunning.set(false);
    t.join(5000);

    // write a Long.MAX_VALUE into each repl entry
    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    bw = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
    Status finishedReplStatus = StatusUtil.replicated(Long.MAX_VALUE);
    for (Entry<Key,Value> entry : s) {
      Status status = Status.parseFrom(entry.getValue().get());
      Assert.assertFalse(status.getClosed());

      // Fake that each one is fully replicated
      Mutation m = new Mutation(entry.getKey().getRow());
      m.put(entry.getKey().getColumnFamily().toString(), entry.getKey().getColumnQualifier().toString(), new Value(finishedReplStatus.toByteArray()));
      bw.addMutation(m);
    }
    bw.close();

    // System.out.println("**** WALs from metadata");
    // for (String metadataWal : metadataWals) {
    // System.out.println(metadataWal);
    // }
    //
    // System.out.println("**** WALs from replication");
    // s = ReplicationTable.getScanner(conn);
    // StatusSection.limit(s);
    // for (Entry<Key,Value> entry : s) {
    // System.out.println(entry.getKey().toStringNoTruncate() + " " + Status.parseFrom(entry.getValue().get()).toString().replace("\n", ", "));
    // }

    s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    bw = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
    for (Entry<Key,Value> entry : s) {
      Status status = Status.parseFrom(entry.getValue().get());
      Assert.assertFalse(status.getClosed());

      // Fake that each one is fully replicated
      Mutation m = new Mutation(entry.getKey().getRow());
      m.put(entry.getKey().getColumnFamily().toString(), entry.getKey().getColumnQualifier().toString(), StatusUtil.newFileValue());
      bw.addMutation(m);
    }
    bw.close();

    // System.out.println("**** WALs from replication");
    // s = ReplicationTable.getScanner(conn);
    // StatusSection.limit(s);
    // for (Entry<Key,Value> entry : s) {
    // System.out.println(entry.getKey().toStringNoTruncate() + " " + Status.parseFrom(entry.getValue().get()).toString().replace("\n", ", "));
    // }

    // Kill the tserver(s) and restart them
    // to ensure that the WALs we previously observed all move to closed.
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }

    cluster.exec(TabletServer.class);
    Process gc = cluster.exec(SimpleGarbageCollector.class);

    // Make sure we can read all the tables (recovery complete)
    for (String table : Arrays.asList(table1, table2, table3)) {
      s = conn.createScanner(table, new Authorizations());
      for (@SuppressWarnings("unused")
      Entry<Key,Value> entry : s) {}
    }

    // for (int i = 0; i < 5; i++) {
    // s = conn.createScanner(MetadataTable.NAME, new Authorizations());
    // s.setRange(ReplicationSection.getRange());
    // System.out.println("**** Metadata");
    // for (Entry<Key,Value> entry : s) {
    // System.out.println(entry.getKey().toStringNoTruncate() + " " + Status.parseFrom(entry.getValue().get()).toString().replace("\n", ", "));
    // }
    //
    // s = ReplicationTable.getScanner(conn);
    // StatusSection.limit(s);
    // System.out.println("**** Replication status");
    // for (Entry<Key,Value> entry : s) {
    // System.out.println(entry.getKey().toStringNoTruncate() + " " + Status.parseFrom(entry.getValue().get()).toString().replace("\n", ", "));
    // }
    //
    // Thread.sleep(1000);
    // }

    try {
      boolean allClosed = true;
      for (int i = 0; i < 10; i++) {
        // System.out.println();
        allClosed = true;

        s = ReplicationTable.getScanner(conn);
        StatusSection.limit(s);
        Iterator<Entry<Key,Value>> iter = s.iterator();

        while (allClosed && iter.hasNext()) {
          Entry<Key,Value> entry = iter.next();
          String wal = entry.getKey().getRow().toString();
          // System.out.println(entry.getKey().toStringNoTruncate() + " " + Status.parseFrom(entry.getValue().get()).toString().replace("\n", ", "));
          if (metadataWals.contains(wal)) {
            // System.out.println("Checked");
            Status status = Status.parseFrom(entry.getValue().get());
            allClosed &= status.getClosed();
          } else {
            // System.out.println("Ignored");
          }
        }

        if (allClosed) {
          return;
        }

        UtilWaitThread.sleep(1000);
      }
    } finally {
      gc.destroy();
      gc.waitFor();
    }

    // System.out.println("****** Replication table iterators");
    // System.out.println(conn.tableOperations().listIterators(ReplicationTable.NAME));
    // for (Entry<String,String> entry : conn.tableOperations().getProperties(ReplicationTable.NAME)) {
    // System.out.println(entry.getKey()+ "=" + entry.getValue());
    // }
    // System.out.println();

    System.out.println("****** Final Replication logs before failure");
    s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    for (Entry<Key,Value> entry : s) {
      System.out.println(entry.getKey().toStringNoTruncate() + " " + Status.parseFrom(entry.getValue().get()).toString().replace("\n", ", "));
    }
    Assert.fail("Expected all replication records to be closed");
  }
}

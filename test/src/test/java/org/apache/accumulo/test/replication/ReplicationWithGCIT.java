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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.replication.ReplicaSystemFactory;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.ReplicationSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.protobuf.ProtobufUtil;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.ReplicationSchema.WorkSection;
import org.apache.accumulo.core.replication.ReplicationTarget;
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
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.protobuf.TextFormat;

/**
 * 
 */
public class ReplicationWithGCIT extends ConfigurableMacIT {
  private static final Logger log = LoggerFactory.getLogger(ReplicationWithGCIT.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "1M");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "0");
    cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s");
    cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s");
    cfg.setProperty(Property.REPLICATION_NAME, "master");
    cfg.setProperty(Property.REPLICATION_WORK_PROCESSOR_DELAY, "1s");
    cfg.setProperty(Property.REPLICATION_WORK_PROCESSOR_PERIOD, "1s");
    cfg.setNumTservers(1);
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
    conn.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
        ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, null));

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

    s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    bw = conn.createBatchWriter(ReplicationTable.NAME, new BatchWriterConfig());
    for (Entry<Key,Value> entry : s) {
      Status status = Status.parseFrom(entry.getValue().get());
      Assert.assertFalse(status.getClosed());

      // Fake that each one is fully replicated
      Mutation m = new Mutation(entry.getKey().getRow());
      m.put(entry.getKey().getColumnFamily().toString(), entry.getKey().getColumnQualifier().toString(),
          StatusUtil.fileCreatedValue(System.currentTimeMillis()));
      bw.addMutation(m);
    }
    bw.close();

    // Kill the tserver(s) and restart them
    // to ensure that the WALs we previously observed all move to closed.
    for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
      cluster.killProcess(ServerType.TABLET_SERVER, proc);
    }

    cluster.exec(TabletServer.class);

    // Starting the gc will run CloseWriteAheadLogReferences which will first close Statuses
    // in the metadata table, and then in the replication table
    Process gc = cluster.exec(SimpleGarbageCollector.class);

    // Make sure we can read all the tables (recovery complete)
    for (String table : Arrays.asList(table1, table2, table3)) {
      s = conn.createScanner(table, new Authorizations());
      for (@SuppressWarnings("unused")
      Entry<Key,Value> entry : s) {}
    }

    try {
      boolean allClosed = true;

      // We should either find all closed records or no records
      // After they're closed, they are candidates for deletion
      for (int i = 0; i < 10; i++) {
        s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        s.setRange(Range.prefix(ReplicationSection.getRowPrefix()));
        Iterator<Entry<Key,Value>> iter = s.iterator();

        long recordsFound = 0l;
        while (allClosed && iter.hasNext()) {
          Entry<Key,Value> entry = iter.next();
          String wal = entry.getKey().getRow().toString();
          if (metadataWals.contains(wal)) {
            Status status = Status.parseFrom(entry.getValue().get());
            log.info("{}={}", entry.getKey().toStringNoTruncate(), ProtobufUtil.toString(status));
            allClosed &= status.getClosed();
            recordsFound++;
          }
        }

        log.info("Found {} records from the metadata table", recordsFound);
        if (allClosed) {
          break;
        }
      }

      if (!allClosed) {
        s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        s.setRange(Range.prefix(ReplicationSection.getRowPrefix()));
        for (Entry<Key,Value> entry : s) {
          log.info(entry.getKey().toStringNoTruncate() + " " + ProtobufUtil.toString(Status.parseFrom(entry.getValue().get())));
        }
        Assert.fail("Expected all replication records in the metadata table to be closed");
      }

      for (int i = 0; i < 10; i++) {
        allClosed = true;

        s = ReplicationTable.getScanner(conn);
        Iterator<Entry<Key,Value>> iter = s.iterator();

        long recordsFound = 0l;
        while (allClosed && iter.hasNext()) {
          Entry<Key,Value> entry = iter.next();
          String wal = entry.getKey().getRow().toString();
          if (metadataWals.contains(wal)) {
            Status status = Status.parseFrom(entry.getValue().get());
            log.info("{}={}", entry.getKey().toStringNoTruncate(), ProtobufUtil.toString(status));
            allClosed &= status.getClosed();
            recordsFound++;
          }
        }

        log.info("Found {} records from the replication table", recordsFound);
        if (allClosed) {
          break;
        }

        UtilWaitThread.sleep(1000);
      }

      if (!allClosed) {
        s = ReplicationTable.getScanner(conn);
        StatusSection.limit(s);
        for (Entry<Key,Value> entry : s) {
          log.info(entry.getKey().toStringNoTruncate() + " " + TextFormat.shortDebugString(Status.parseFrom(entry.getValue().get())));
        }
        Assert.fail("Expected all replication records in the replication table to be closed");
      }

    } finally {
      gc.destroy();
      gc.waitFor();
    }

  }

  @Test(timeout = 5 * 60 * 1000)
  public void replicatedStatusEntriesAreDeleted() throws Exception {
    // Just stop it now, we'll restart it after we restart the tserver
    for (ProcessReference proc : getCluster().getProcesses().get(ServerType.GARBAGE_COLLECTOR)) {
      getCluster().killProcess(ServerType.GARBAGE_COLLECTOR, proc);
    }

    final Connector conn = getConnector();
    log.info("Got connector to MAC");
    String table1 = "table1";

    // replication shouldn't exist when we begin
    Assert.assertFalse(conn.tableOperations().exists(ReplicationTable.NAME));

    ReplicationTablesPrinterThread thread = new ReplicationTablesPrinterThread(conn, System.out);
    thread.start();

    try {
      // Create two tables
      conn.tableOperations().create(table1);

      int attempts = 5;
      while (attempts > 0) {
        try {
          // Enable replication on table1
          conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION.getKey(), "true");
          // Replicate table1 to cluster1 in the table with id of '4'
          conn.tableOperations().setProperty(table1, Property.TABLE_REPLICATION_TARGETS.getKey() + "cluster1", "4");
          // Use the MockReplicaSystem impl and sleep for 5seconds
          conn.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
              ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, "5000"));
          attempts = 0;
        } catch (Exception e) {
          attempts--;
          if (attempts <= 0) {
            throw e;
          }
          UtilWaitThread.sleep(500);
        }
      }

      String tableId = conn.tableOperations().tableIdMap().get(table1);
      Assert.assertNotNull("Could not determine table id for " + table1, tableId);

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
      attempts = 10;
      do {
        if (!exists) {
          UtilWaitThread.sleep(1000);
          exists = conn.tableOperations().exists(ReplicationTable.NAME);
          attempts--;
        }
      } while (!exists && attempts > 0);
      Assert.assertTrue("Replication table did not exist", exists);

      // Grant ourselves the write permission for later
      conn.securityOperations().grantTablePermission("root", ReplicationTable.NAME, TablePermission.WRITE);

      // Find the WorkSection record that will be created for that data we ingested
      boolean notFound = true;
      Scanner s;
      for (int i = 0; i < 10 && notFound; i++) {
        try {
          s = ReplicationTable.getScanner(conn);
          WorkSection.limit(s);
          Entry<Key,Value> e = Iterables.getOnlyElement(s);
          Text expectedColqual = new ReplicationTarget("cluster1", "4", tableId).toText();
          Assert.assertEquals(expectedColqual, e.getKey().getColumnQualifier());
          notFound = false;
        } catch (NoSuchElementException e) {

        } catch (IllegalArgumentException e) {
          // Somehow we got more than one element. Log what they were
          s = ReplicationTable.getScanner(conn);
          for (Entry<Key,Value> content : s) {
            log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
          }
          Assert.fail("Found more than one work section entry");
        } catch (RuntimeException e) {
          // Catch a propagation issue, fail if it's not what we expect
          Throwable cause = e.getCause();
          if (cause instanceof AccumuloSecurityException) {
            AccumuloSecurityException sec = (AccumuloSecurityException) cause;
            switch (sec.getSecurityErrorCode()) {
              case PERMISSION_DENIED:
                // retry -- the grant didn't happen yet
                log.warn("Sleeping because permission was denied");
              default:
                throw e;
            }
          } else {
            throw e;
          }
        }

        Thread.sleep(1000);
      }

      if (notFound) {
        s = ReplicationTable.getScanner(conn);
        for (Entry<Key,Value> content : s) {
          log.info(content.getKey().toStringNoTruncate() + " => " + content.getValue());
        }
        Assert.assertFalse("Did not find the work entry for the status entry", notFound);
      }

      /**
       * By this point, we should have data ingested into a table, with at least one WAL as a candidate for replication. Compacting the table should close all
       * open WALs, which should ensure all records we're going to replicate have entries in the replication table, and nothing will exist in the metadata table
       * anymore
       */

      log.info("Killing tserver");
      // Kill the tserver(s) and restart them
      // to ensure that the WALs we previously observed all move to closed.
      for (ProcessReference proc : cluster.getProcesses().get(ServerType.TABLET_SERVER)) {
        cluster.killProcess(ServerType.TABLET_SERVER, proc);
      }

      log.info("Starting tserver");
      cluster.exec(TabletServer.class);

      log.info("Waiting to read tables");

      // Make sure we can read all the tables (recovery complete)
      for (String table : new String[] {MetadataTable.NAME, table1}) {
        s = conn.createScanner(table, new Authorizations());
        for (@SuppressWarnings("unused")
        Entry<Key,Value> entry : s) {}
      }

      log.info("Checking for replication entries in replication");
      // Then we need to get those records over to the replication table
      boolean foundResults = false;
      for (int i = 0; i < 5; i++) {
        s = ReplicationTable.getScanner(conn);
        int count = 0;
        for (Entry<Key,Value> entry : s) {
          count++;
          log.info("{}={}", entry.getKey().toStringNoTruncate(), entry.getValue());
        }
        if (count > 0) {
          foundResults = true;
          break;
        }
        Thread.sleep(1000);
      }

      Assert.assertTrue("Did not find any replication entries in the replication table", foundResults);

      getCluster().exec(SimpleGarbageCollector.class);

      // Wait for a bit since the GC has to run (should be running after a one second delay)
      Thread.sleep(5000);

      // We expect no records in the metadata table after compaction. We have to poll
      // because we have to wait for the StatusMaker's next iteration which will clean
      // up the dangling *closed* records after we create the record in the replication table.
      // We need the GC to close the file (CloseWriteAheadLogReferences) before we can remove the record
      log.info("Checking metadata table for replication entries");
      foundResults = true;
      for (int i = 0; i < 5; i++) {
        s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        s.setRange(ReplicationSection.getRange());
        long size = 0;
        for (Entry<Key,Value> e : s) {
          size++;
          log.info("{}={}", e.getKey().toStringNoTruncate(), ProtobufUtil.toString(Status.parseFrom(e.getValue().get())));
        }
        if (size == 0) {
          foundResults = false;
          break;
        }
        Thread.sleep(1000);
        log.info("");
      }

      Assert.assertFalse("Replication status messages were not cleaned up from metadata table", foundResults);

      /**
       * After we close out and subsequently delete the metadata record, this will propagate to the replication table, which will cause those records to be
       * deleted after repliation occurs
       */

      int recordsFound = 0;
      for (int i = 0; i < 10; i++) {
        s = ReplicationTable.getScanner(conn);
        recordsFound = 0;
        for (Entry<Key,Value> entry : s) {
          recordsFound++;
          log.info(entry.getKey().toStringNoTruncate() + " " + Status.parseFrom(entry.getValue().get()).toString().replace("\n", ", "));
        }

        if (0 == recordsFound) {
          break;
        } else {
          Thread.sleep(1000);
          log.info("");
        }
      }

      Assert.assertEquals("Found unexpected replication records in the replication table", 0, recordsFound);
    } finally {
      thread.join(200);
    }
  }
}

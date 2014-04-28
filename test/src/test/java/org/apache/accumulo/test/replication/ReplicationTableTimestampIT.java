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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

/**
 * Integration Tests that attempt to evaluate the accuracy of the internal bookkeeping performed on the accumulo "master" instance. Does not send data to any
 * remote instance, merely tracks what is stored locally.
 */
public class ReplicationTableTimestampIT extends ConfigurableMacIT {
  @Override
  public int defaultTimeoutSeconds() {
    return 300;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s");
    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "1M");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "1s");
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  private Multimap<String,String> getLogs(Connector conn) throws TableNotFoundException {
    Multimap<String,String> logs = HashMultimap.create();
    Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.fetchColumnFamily(LogColumnFamily.NAME);
    scanner.setRange(new Range());
    for (Entry<Key,Value> entry : scanner) {
      if (Thread.interrupted()) {
        return logs;
      }

      LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());

      for (String log : logEntry.logSet) {
        logs.put(new Path(log).toString(), logEntry.extent.getTableId().toString());
      }
    }
    return logs;
  }

  @Test
  public void closedReplicationStatusStayClosed() throws Exception {
    final Connector conn = getConnector();
    String table1 = "table1", table2 = "table2", table3 = "table3";
    final Multimap<String,String> logs = HashMultimap.create();
    final AtomicBoolean keepRunning = new AtomicBoolean(true);

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        // Should really be able to interrupt here, but the Scanner throws a fit to the logger
        // when that happens
        while (keepRunning.get()) {
          try {
            logs.putAll(getLogs(conn));
          } catch (TableNotFoundException e) {
            log.error("Metadata table doesn't exist");
          }
        }
      }
    
    });

    t.start();

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

    keepRunning.set(false);
    t.join(5000);

    Scanner s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    Set<String> replFiles = new HashSet<>();
    for (Entry<Key,Value> entry : s) {
      replFiles.add(entry.getKey().getRow().toString());
    }

    // We might have a WAL that was use solely for the replication table
    // We want to remove that from our list as it should not appear in the replication table
    String replicationTableId = conn.tableOperations().tableIdMap().get(ReplicationTable.NAME);
    Iterator<Entry<String,String>> observedLogs = logs.entries().iterator();
    while (observedLogs.hasNext()) {
      Entry<String,String> observedLog = observedLogs.next();
      if (replicationTableId.equals(observedLog.getValue())) {
        observedLogs.remove();
      }
    }

    // We should have *some* reference to each log that was seen in the metadata table
    // They might not yet all be closed though (might be newfile)
    Assert.assertEquals("Metadata log distribution: " + logs, logs.keySet(), replFiles);

    LinkedListMultimap<String,Entry<Key,Value>> kvByRow = LinkedListMultimap.create();
    s = ReplicationTable.getScanner(conn);
    StatusSection.limit(s);
    for (Entry<Key,Value> entry : s) {
      kvByRow.put(entry.getKey().getRow().toString(), entry);
    }

    for (String row : kvByRow.keySet()) {
      ArrayList<Entry<Key,Value>> kvs = new ArrayList<>(kvByRow.get(row));
      Collections.sort(kvs, new Comparator<Entry<Key,Value>>() {
        @Override
        public int compare(Entry<Key,Value> o1, Entry<Key,Value> o2) {
          return (new Long(o1.getKey().getTimestamp())).compareTo(new Long(o2.getKey().getTimestamp()));
        }
      });

      Key closedKey = null;
      boolean observedClosed = false;
      for (Entry<Key,Value> kv : kvs) {
        Status status = Status.parseFrom(kv.getValue().get());

        // Once we get a closed record, every subsequent record should *also* be closed
        // A file cannot be "re-opened"
        if (!observedClosed) {
          if (status.getClosed()) {
            closedKey = kv.getKey();
            observedClosed = true;
          }
        } else {
          Assert.assertTrue("Found a non-closed Status (" + kv.getKey().toStringNoTruncate() + ") after a closed Status (" + closedKey.toStringNoTruncate() + ") was observed", status.getClosed());
        }
      }
      
    }

    for (String replFile : replFiles) {
      Path p = new Path(replFile);
      FileSystem fs = p.getFileSystem(new Configuration());
      Assert.assertTrue("File does not exist anymore, it was likely incorrectly garbage collected: " + p, fs.exists(p));
    }
  }

}

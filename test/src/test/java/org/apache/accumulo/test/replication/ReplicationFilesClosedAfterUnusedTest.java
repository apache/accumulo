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

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.replication.ReplicaSystemFactory;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.LogColumnFamily;
import org.apache.accumulo.core.replication.ReplicationSchema.StatusSection;
import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.replication.ReplicationTable;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class ReplicationFilesClosedAfterUnusedTest extends ConfigurableMacIT {
  private static final Logger log = LoggerFactory.getLogger(ReplicationFilesClosedAfterUnusedTest.class);

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.TSERV_WALOG_MAX_SIZE, "1M");
    cfg.setProperty(Property.GC_CYCLE_START, "1s");
    cfg.setProperty(Property.GC_CYCLE_DELAY, "0");
    cfg.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "0s");
    cfg.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "0s");
    cfg.setProperty(Property.REPLICATION_NAME, "master");
    cfg.setNumTservers(1);
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test(timeout = 60000)
  public void test() throws Exception {
    Connector conn = getConnector();

    String table = "table";
    conn.tableOperations().create(table);
    String tableId = conn.tableOperations().tableIdMap().get(table);

    Assert.assertNotNull(tableId);

    log.info("Writing to {}", tableId);

    conn.tableOperations().setProperty(table, Property.TABLE_REPLICATION.getKey(), "true");
    conn.tableOperations().setProperty(table, Property.TABLE_REPLICATION_TARGETS.getKey() + "cluster1", "1");
    // just sleep
    conn.instanceOperations().setProperty(Property.REPLICATION_PEERS.getKey() + "cluster1",
        ReplicaSystemFactory.getPeerConfigurationValue(MockReplicaSystem.class, "50000"));

    // Write a mutation to make a log file
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    Mutation m = new Mutation("one");
    m.put("", "", "");
    bw.addMutation(m);
    bw.close();

    // Write another to make sure the logger rolls itself?
    bw = conn.createBatchWriter(table, new BatchWriterConfig());
    m = new Mutation("three");
    m.put("", "", "");
    bw.addMutation(m);
    bw.close();

    Scanner s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    s.fetchColumnFamily(TabletsSection.LogColumnFamily.NAME);
    s.setRange(TabletsSection.getRange(tableId));
    Set<String> wals = new HashSet<>();
    for (Entry<Key,Value> entry : s) {
      LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());
      for (String file : logEntry.logSet) {
        Path p = new Path(file);
        wals.add(p.toString());
      }
    }

    log.warn("Found wals {}", wals);

    // for (int j = 0; j < 5; j++) {
    bw = conn.createBatchWriter(table, new BatchWriterConfig());
    m = new Mutation("three");
    byte[] bytes = new byte[1024 * 1024];
    m.put("1".getBytes(), new byte[0], bytes);
    m.put("2".getBytes(), new byte[0], bytes);
    m.put("3".getBytes(), new byte[0], bytes);
    m.put("4".getBytes(), new byte[0], bytes);
    m.put("5".getBytes(), new byte[0], bytes);
    bw.addMutation(m);
    bw.close();

    conn.tableOperations().flush(table, null, null, true);

    while (!conn.tableOperations().exists(ReplicationTable.NAME)) {
      UtilWaitThread.sleep(500);
    }

    for (int i = 0; i < 5; i++) {
      s = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
      s.fetchColumnFamily(LogColumnFamily.NAME);
      s.setRange(TabletsSection.getRange(tableId));
      for (Entry<Key,Value> entry : s) {
        log.info(entry.getKey().toStringNoTruncate() + "=" + entry.getValue());
      }

      s = ReplicationTable.getScanner(conn);
      StatusSection.limit(s);
      Text buff = new Text();
      boolean allReferencedLogsClosed = true;
      int recordsFound = 0;
      for (Entry<Key,Value> e : s) {
        recordsFound++;
        allReferencedLogsClosed = true;
        StatusSection.getFile(e.getKey(), buff);
        String file = buff.toString();
        if (wals.contains(file)) {
          Status stat = Status.parseFrom(e.getValue().get());
          if (!stat.getClosed()) {
            log.info("{} wasn't closed", file);
            allReferencedLogsClosed = false;
          }
        }
      }

      if (recordsFound > 0 && allReferencedLogsClosed) {
        return;
      }

      Thread.sleep(1000);
    }

    Assert.fail("We had a file that was referenced but didn't get closed");
  }

}

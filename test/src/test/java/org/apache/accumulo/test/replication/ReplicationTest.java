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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.replication.ReplicationSchema;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.tabletserver.log.LogEntry;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * 
 */
public class ReplicationTest extends ConfigurableMacIT {

  @Override
  public int defaultTimeoutSeconds() {
    return 30;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    cfg.setNumTservers(1);
  }

  @Test
  public void noReplicationTableCreatedImmediately() throws Exception {
    Connector conn = getConnector();
    Set<String> tables = conn.tableOperations().list();

    Assert.assertEquals(Sets.newHashSet(RootTable.NAME, MetadataTable.NAME), tables);
  }

  @Test
  public void readRootTable() throws Exception {
    Connector conn = getConnector();
    Scanner s = conn.createScanner(RootTable.NAME, new Authorizations());
    Assert.assertNotEquals(0, Iterables.size(s));
  }

  @Test
  public void readMetadataTable() throws Exception {
    Connector conn = getConnector();
    Scanner s = conn.createScanner(MetadataTable.NAME, new Authorizations());
    Assert.assertEquals(0, Iterables.size(s));
  }

  @Test
  public void correctRecordsCompleteFile() throws Exception {
    Connector conn = getConnector();
    String table = "table1";
    conn.tableOperations().create(table);
    // If we have more than one tserver, this is subject to a race condition.
    conn.tableOperations().setProperty(table, Property.TABLE_REPLICATION.getKey(), "true");

    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation(Integer.toString(i));
      m.put(new byte[0], new byte[0], new byte[0]);
      bw.addMutation(m);
    }

    bw.close();

    conn.tableOperations().flush(table, null, null, true);

    Set<String> replRows = Sets.newHashSet();
    final String replRowPrefix = ReplicationSchema.ReplicationSection.getRowPrefix();
    for (Entry<Key,Value> entry : conn.createScanner(ReplicationTable.NAME, new Authorizations())) {
      Key k = entry.getKey();
      String row = k.getRow().toString();

      if (row.startsWith(replRowPrefix)) {
        int offset = row.indexOf(replRowPrefix.charAt(replRowPrefix.length() - 1));

        String fileUri = row.substring(offset + 1);
        try {
          new URI(fileUri);
        } catch (URISyntaxException e) {
          Assert.fail("Expected a valid URI: " + fileUri);
        }

        replRows.add(fileUri);
      } // else, ignored
    }

    Set<String> wals = Sets.newHashSet();
    Scanner s = conn.createScanner(MetadataTable.NAME, new Authorizations());
    s.fetchColumnFamily(MetadataSchema.TabletsSection.LogColumnFamily.NAME);
    for (Entry<Key,Value> entry : s) {
      LogEntry logEntry = LogEntry.fromKeyValue(entry.getKey(), entry.getValue());
      wals.add(logEntry.filename);
    }

    // We only have one file that should need replication (no trace table)
    // We should find an entry in tablet and in the repl row
    Assert.assertEquals("Rows found: " + replRows, 1, replRows.size());

    // This should be the same set of WALs that we also are using
    Assert.assertEquals(replRows, wals);
  }

  public void noRecordsWithoutReplication() throws Exception {
    Connector conn = getConnector();
    List<String> tables = new ArrayList<>();

    // replication shouldn't exist when we begin
    Assert.assertFalse(conn.tableOperations().exists(ReplicationTable.NAME));

    for (int i = 0; i < 5; i++) {
      String name = "table" + i;
      tables.add(name);
      conn.tableOperations().create(name);
    }

    // nor after we create some tables (that aren't being replicated)
    Assert.assertFalse(conn.tableOperations().exists(ReplicationTable.NAME));

    for (String table : tables) {
      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());

      for (int j = 0; j < 5; j++) {
        Mutation m = new Mutation(Integer.toString(j));
        for (int k = 0; k < 5; k++) {
          String value = Integer.toString(k);
          m.put(value, "", value);
        }
        bw.addMutation(m);
      }

      bw.close();
    }

    // After writing data, still no replication table
    Assert.assertFalse(conn.tableOperations().exists(ReplicationTable.NAME));

    for (String table : tables) {
      conn.tableOperations().compact(table, null, null, true, true);
    }

    // After compacting data, still no replication table
    Assert.assertFalse(conn.tableOperations().exists(ReplicationTable.NAME));

    for (String table : tables) {
      conn.tableOperations().delete(table);
    }

    // After deleting tables, still no replication table
    Assert.assertFalse(conn.tableOperations().exists(ReplicationTable.NAME));
  }
}

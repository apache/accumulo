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
import java.util.Map.Entry;
import java.util.Arrays;
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
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

/**
 * 
 */
public class ReplicationTest extends ConfigurableMacIT {
  private static final Logger log = Logger.getLogger(ReplicationTest.class);

  @Override
  public int defaultTimeoutSeconds() {
    return 30;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.REPLICATION_ENABLED, "true");
    cfg.setNumTservers(1);
  }

  @Test
  public void replicationTableCreated() throws Exception {
    Connector conn = getConnector();
    Set<String> tables = conn.tableOperations().list();

    Assert.assertEquals(Sets.newHashSet(RootTable.NAME, MetadataTable.NAME, ReplicationTable.NAME), tables);
  }

  @Test
  public void readSystemTables() throws Exception {
    Connector conn = getConnector();
    Scanner s;
    for (String table : Arrays.asList(RootTable.NAME, MetadataTable.NAME, ReplicationTable.NAME)) {
      System.out.println("Table: " + table);
      s = conn.createScanner(table, new Authorizations());
      for (Entry<Key,Value> entry : s) {
        System.out.println(entry.getKey().toStringNoTruncate() + " " + entry.getValue());
      }
//      Assert.assertNotEquals("Expected to find entries in " + table, 0, Iterables.size(s));
    }

  }
  
  @Test
  public void correctRecordsCompleteFile() throws Exception {
    Connector conn = getConnector();
    String table = "table1";
    conn.tableOperations().create(table);

    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    for (int i = 0; i < 10; i++) {
      Mutation m = new Mutation(Integer.toString(i));
      m.put(new byte[0], new byte[0], new byte[0]);
      bw.addMutation(m);
    }

    bw.close();

    conn.tableOperations().flush(table, null, null, true);

    int replRowCount = 0;
    Set<String> replRows = Sets.newHashSet();
    final String replRowPrefix = ReplicationSchema.ReplicationSection.getRowPrefix(); 
    for (Entry<Key,Value> entry : conn.createScanner(ReplicationTable.NAME, new Authorizations())) {
      Key k = entry.getKey();
      String row = k.getRow().toString();

      if (row.startsWith(replRowPrefix)) {
        replRowCount++;
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
    Assert.assertEquals("Rows found: "+ replRows, 1, replRowCount);

    // This should be the same set of WALs that we also are using
    Assert.assertEquals(replRows, wals);
  }
}

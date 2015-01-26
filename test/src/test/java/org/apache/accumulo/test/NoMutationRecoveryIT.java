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
package org.apache.accumulo.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.cluster.ClusterServerType;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.FunctionalTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Test;

// Verify that a recovery of a log without any mutations removes the log reference
public class NoMutationRecoveryIT extends AccumuloClusterIT {

  @Override
  public int defaultTimeoutSeconds() {
    return 10 * 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.useMiniDFS(true);
    cfg.setNumTservers(1);
  }

  public boolean equals(Entry<Key,Value> a, Entry<Key,Value> b) {
    // comparison, without timestamp
    Key akey = a.getKey();
    Key bkey = b.getKey();
    return akey.compareTo(bkey, PartialKey.ROW_COLFAM_COLQUAL_COLVIS) == 0 && a.getValue().equals(b.getValue());
  }

  @Test
  public void test() throws Exception {
    Connector conn = getConnector();
    final String table = getUniqueNames(1)[0];
    conn.tableOperations().create(table);
    String tableId = conn.tableOperations().tableIdMap().get(table);
    update(conn, table, new Text("row"), new Text("cf"), new Text("cq"), new Value("value".getBytes()));
    Entry<Key,Value> logRef = getLogRef(conn, MetadataTable.NAME);
    conn.tableOperations().flush(table, null, null, true);
    assertEquals("should not have any refs", 0, FunctionalTestUtils.count(getLogRefs(conn, MetadataTable.NAME, Range.prefix(tableId))));
    conn.securityOperations().grantTablePermission(conn.whoami(), MetadataTable.NAME, TablePermission.WRITE);
    update(conn, MetadataTable.NAME, logRef);
    assertTrue(equals(logRef, getLogRef(conn, MetadataTable.NAME)));
    conn.tableOperations().flush(MetadataTable.NAME, null, null, true);
    conn.tableOperations().flush(RootTable.NAME, null, null, true);

    ClusterControl control = cluster.getClusterControl();
    control.stopAllServers(ClusterServerType.TABLET_SERVER);
    control.startAllServers(ClusterServerType.TABLET_SERVER);

    Scanner s = conn.createScanner(table, Authorizations.EMPTY);
    int count = 0;
    for (Entry<Key,Value> e : s) {
      assertEquals(e.getKey().getRow().toString(), "row");
      assertEquals(e.getKey().getColumnFamily().toString(), "cf");
      assertEquals(e.getKey().getColumnQualifier().toString(), "cq");
      assertEquals(e.getValue().toString(), "value");
      count++;
    }
    assertEquals(1, count);
    for (Entry<Key,Value> ref : getLogRefs(conn, MetadataTable.NAME)) {
      assertFalse(equals(ref, logRef));
    }
  }

  private void update(Connector conn, String name, Entry<Key,Value> logRef) throws Exception {
    Key k = logRef.getKey();
    update(conn, name, k.getRow(), k.getColumnFamily(), k.getColumnQualifier(), logRef.getValue());
  }

  private Iterable<Entry<Key,Value>> getLogRefs(Connector conn, String table) throws Exception {
    return getLogRefs(conn, table, new Range());
  }

  private Iterable<Entry<Key,Value>> getLogRefs(Connector conn, String table, Range r) throws Exception {
    Scanner s = conn.createScanner(table, Authorizations.EMPTY);
    s.fetchColumnFamily(MetadataSchema.TabletsSection.LogColumnFamily.NAME);
    s.setRange(r);
    return s;
  }

  private Entry<Key,Value> getLogRef(Connector conn, String table) throws Exception {
    return getLogRefs(conn, table).iterator().next();
  }

  private void update(Connector conn, String table, Text row, Text cf, Text cq, Value value) throws Exception {
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    Mutation m = new Mutation(row);
    m.put(cf, cq, value);
    bw.addMutation(m);
    bw.close();
  }

}

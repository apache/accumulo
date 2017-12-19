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

import java.util.Map.Entry;

import org.apache.accumulo.cluster.ClusterControl;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterators;

// Accumulo3010
public class RecoveryCompactionsAreFlushesIT extends AccumuloClusterHarness {

  @Override
  public int defaultTimeoutSeconds() {
    return 60;
  }

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    // file system supports recovery
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
  }

  @Test
  public void test() throws Exception {
    // create a table
    String tableName = getUniqueNames(1)[0];
    Connector c = getConnector();
    c.tableOperations().create(tableName);
    c.tableOperations().setProperty(tableName, Property.TABLE_MAJC_RATIO.getKey(), "100");
    c.tableOperations().setProperty(tableName, Property.TABLE_FILE_MAX.getKey(), "3");
    // create 3 flush files
    BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig());
    Mutation m = new Mutation("a");
    m.put("b", "c", new Value("v".getBytes()));
    for (int i = 0; i < 3; i++) {
      bw.addMutation(m);
      bw.flush();
      c.tableOperations().flush(tableName, null, null, true);
    }
    // create an unsaved mutation
    bw.addMutation(m);
    bw.close();

    ClusterControl control = cluster.getClusterControl();

    // kill the tablet servers
    control.stopAllServers(ServerType.TABLET_SERVER);

    // recover
    control.startAllServers(ServerType.TABLET_SERVER);

    // ensure the table is readable
    Iterators.size(c.createScanner(tableName, Authorizations.EMPTY).iterator());

    // ensure that the recovery was not a merging minor compaction
    try (Scanner s = c.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      s.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
      for (Entry<Key,Value> entry : s) {
        String filename = entry.getKey().getColumnQualifier().toString();
        String parts[] = filename.split("/");
        Assert.assertFalse(parts[parts.length - 1].startsWith("M"));
      }
    }
  }

}

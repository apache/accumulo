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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.fate.util.UtilWaitThread.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterators;

public class MetadataIT extends AccumuloClusterHarness {

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setNumTservers(1);
  }

  @Override
  public int defaultTimeoutSeconds() {
    return 2 * 60;
  }

  @Test
  public void testFlushAndCompact() throws Exception {
    Connector c = getConnector();
    String tableNames[] = getUniqueNames(2);

    // create a table to write some data to metadata table
    c.tableOperations().create(tableNames[0]);

    try (Scanner rootScanner = c.createScanner(RootTable.NAME, Authorizations.EMPTY)) {
      rootScanner.setRange(MetadataSchema.TabletsSection.getRange());
      rootScanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);

      Set<String> files1 = new HashSet<>();
      for (Entry<Key,Value> entry : rootScanner)
        files1.add(entry.getKey().getColumnQualifier().toString());

      c.tableOperations().create(tableNames[1]);
      c.tableOperations().flush(MetadataTable.NAME, null, null, true);

      Set<String> files2 = new HashSet<>();
      for (Entry<Key,Value> entry : rootScanner)
        files2.add(entry.getKey().getColumnQualifier().toString());

      // flush of metadata table should change file set in root table
      Assert.assertTrue(files2.size() > 0);
      Assert.assertNotEquals(files1, files2);

      c.tableOperations().compact(MetadataTable.NAME, null, null, false, true);

      Set<String> files3 = new HashSet<>();
      for (Entry<Key,Value> entry : rootScanner)
        files3.add(entry.getKey().getColumnQualifier().toString());

      // compaction of metadata table should change file set in root table
      Assert.assertNotEquals(files2, files3);
    }
  }

  @Test
  public void mergeMeta() throws Exception {
    Connector c = getConnector();
    String[] names = getUniqueNames(5);
    SortedSet<Text> splits = new TreeSet<>();
    for (String id : "1 2 3 4 5".split(" ")) {
      splits.add(new Text(id));
    }
    c.tableOperations().addSplits(MetadataTable.NAME, splits);
    for (String tableName : names) {
      c.tableOperations().create(tableName);
    }
    c.tableOperations().merge(MetadataTable.NAME, null, null);
    try (Scanner s = c.createScanner(RootTable.NAME, Authorizations.EMPTY)) {
      s.setRange(MetadataSchema.DeletesSection.getRange());
      while (Iterators.size(s.iterator()) == 0) {
        sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
      assertEquals(0, c.tableOperations().listSplits(MetadataTable.NAME).size());
    }
  }

  @Test
  public void batchScanTest() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    // batch scan regular metadata table
    int count = 0;
    try (BatchScanner s = c.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 1)) {

      s.setRanges(Collections.singleton(new Range()));
      for (Entry<Key,Value> e : s) {
        if (e != null)
          count++;
      }
    }

    assertTrue(count > 0);

    // batch scan root metadata table
    try (BatchScanner s = c.createBatchScanner(RootTable.NAME, Authorizations.EMPTY, 1)) {
      s.setRanges(Collections.singleton(new Range()));
      count = 0;
      for (Entry<Key,Value> e : s) {
        if (e != null)
          count++;
      }
      assertTrue(count > 0);
    }
  }

}

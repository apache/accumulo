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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.harness.AccumuloClusterIT;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaSplitIT extends AccumuloClusterIT {
  private static final Logger log = LoggerFactory.getLogger(MetaSplitIT.class);

  private Collection<Text> metadataSplits = null;

  @Override
  public int defaultTimeoutSeconds() {
    return 3 * 60;
  }

  @Before
  public void saveMetadataSplits() throws Exception {
    if (ClusterType.STANDALONE == getClusterType()) {
      Connector conn = getConnector();
      Collection<Text> splits = conn.tableOperations().listSplits(MetadataTable.NAME);
      // We expect a single split
      if (!splits.equals(Arrays.asList(new Text("~")))) {
        log.info("Existing splits on metadata table. Saving them, and applying single original split of '~'");
        metadataSplits = splits;
        conn.tableOperations().merge(MetadataTable.NAME, null, null);
        conn.tableOperations().addSplits(MetadataTable.NAME, new TreeSet<Text>(Collections.singleton(new Text("~"))));
      }
    }
  }

  @After
  public void restoreMetadataSplits() throws Exception {
    if (null != metadataSplits) {
      log.info("Restoring split on metadata table");
      Connector conn = getConnector();
      conn.tableOperations().merge(MetadataTable.NAME, null, null);
      conn.tableOperations().addSplits(MetadataTable.NAME, new TreeSet<Text>(metadataSplits));
    }
  }

  @Test(expected = AccumuloException.class)
  public void testRootTableSplit() throws Exception {
    TableOperations opts = getConnector().tableOperations();
    SortedSet<Text> splits = new TreeSet<Text>();
    splits.add(new Text("5"));
    opts.addSplits(RootTable.NAME, splits);
  }

  @Test
  public void testRootTableMerge() throws Exception {
    TableOperations opts = getConnector().tableOperations();
    opts.merge(RootTable.NAME, null, null);
  }

  private void addSplits(TableOperations opts, String... points) throws Exception {
    SortedSet<Text> splits = new TreeSet<Text>();
    for (String point : points) {
      splits.add(new Text(point));
    }
    opts.addSplits(MetadataTable.NAME, splits);
  }

  @Test
  public void testMetadataTableSplit() throws Exception {
    TableOperations opts = getConnector().tableOperations();
    for (int i = 1; i <= 10; i++) {
      opts.create(Integer.toString(i));
    }
    try {
      opts.merge(MetadataTable.NAME, new Text("01"), new Text("02"));
      checkMetadataSplits(1, opts);
      addSplits(opts, "4 5 6 7 8".split(" "));
      checkMetadataSplits(6, opts);
      opts.merge(MetadataTable.NAME, new Text("6"), new Text("9"));
      checkMetadataSplits(4, opts);
      addSplits(opts, "44 55 66 77 88".split(" "));
      checkMetadataSplits(9, opts);
      opts.merge(MetadataTable.NAME, new Text("5"), new Text("7"));
      checkMetadataSplits(6, opts);
      opts.merge(MetadataTable.NAME, null, null);
      checkMetadataSplits(0, opts);
    } finally {
      for (int i = 1; i <= 10; i++) {
        opts.delete(Integer.toString(i));
      }
    }
  }

  private static void checkMetadataSplits(int numSplits, TableOperations opts) throws AccumuloSecurityException, TableNotFoundException, AccumuloException,
      InterruptedException {
    for (int i = 0; i < 10; i++) {
      if (opts.listSplits(MetadataTable.NAME).size() == numSplits) {
        break;
      }
      Thread.sleep(2000);
    }
    Collection<Text> splits = opts.listSplits(MetadataTable.NAME);
    assertEquals("Actual metadata table splits: " + splits, numSplits, splits.size());
  }

}

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

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class MetaSplitTest {
  
  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }
  
  @Test(expected = AccumuloException.class)
  public void testRootTableSplit() throws Exception {
    Connector connector = cluster.getConnector("root", secret);
    TableOperations opts = connector.tableOperations();
    SortedSet<Text> splits = new TreeSet<Text>();
    splits.add(new Text("5"));
    opts.addSplits(RootTable.NAME, splits);
  }
  
  @Test
  public void testRootTableMerge() throws Exception {
    Connector connector = cluster.getConnector("root", secret);
    TableOperations opts = connector.tableOperations();
    opts.merge(RootTable.NAME, null, null);
  }
  
  private void addSplits(TableOperations opts, String... points) throws Exception {
    SortedSet<Text> splits = new TreeSet<Text>();
    for (String point : points) {
      splits.add(new Text(point));
    }
    opts.addSplits(MetadataTable.NAME, splits);
  }
  
  @Test(timeout = 60000)
  public void testMetadataTableSplit() throws Exception {
    Connector connector = cluster.getConnector("root", secret);
    TableOperations opts = connector.tableOperations();
    for (int i = 1; i <= 10; i++) {
      opts.create("" + i);
    }
    opts.merge(MetadataTable.NAME, new Text("01"), new Text("02"));
    assertEquals(1, opts.listSplits(MetadataTable.NAME).size());
    addSplits(opts, "4 5 6 7 8".split(" "));
    assertEquals(6, opts.listSplits(MetadataTable.NAME).size());
    opts.merge(MetadataTable.NAME, new Text("6"), new Text("9"));
    assertEquals(4, opts.listSplits(MetadataTable.NAME).size());
    addSplits(opts, "44 55 66 77 88".split(" "));
    assertEquals(9, opts.listSplits(MetadataTable.NAME).size());
    opts.merge(MetadataTable.NAME, new Text("5"), new Text("7"));
    assertEquals(6, opts.listSplits(MetadataTable.NAME).size());
    opts.merge(MetadataTable.NAME, null, null);
    assertEquals(0, opts.listSplits(MetadataTable.NAME).size());
  }
  
}

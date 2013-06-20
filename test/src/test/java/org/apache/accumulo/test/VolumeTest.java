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
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.MetadataTable;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class VolumeTest {
  
  private static final Text EMPTY = new Text();
  private static final Value EMPTY_VALUE = new Value(new byte[]{});
  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  public static Path v1;
  public static Path v2;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    // Run MAC on two locations in the local file system
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    Map<String,String> siteConfig = cfg.getSiteConfig();
    v1 = new Path("file://" + folder.newFolder("v1").getAbsolutePath().toString());
    v2 = new Path("file://" + folder.newFolder("v2").getAbsolutePath().toString());
    siteConfig.put(Property.INSTANCE_VOLUMES.getKey(), v1.toString() + "," + v2.toString());
    cfg.setSiteConfig(siteConfig);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }
  
  @Test
  public void test() throws Exception {
    // create a table
    Connector connector = cluster.getConnector("root", secret);
    String tableName = "test";
    connector.tableOperations().create(tableName);
    SortedSet<Text> partitions = new TreeSet<Text>();
    // with some splits
    for (String s : "d,m,t".split(","))
      partitions.add(new Text(s));
    connector.tableOperations().addSplits(tableName, partitions);
    // scribble over the splits
    BatchWriter bw = connector.createBatchWriter(tableName, new BatchWriterConfig());
    String[] rows = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",");
    for (String s : rows) {
      Mutation m = new Mutation(new Text(s));
      m.put(EMPTY, EMPTY, EMPTY_VALUE);
      bw.addMutation(m);
    }
    bw.close();
    // write the data to disk, read it back
    connector.tableOperations().flush(tableName, null, null, true);
    Scanner scanner = connector.createScanner(tableName, Authorizations.EMPTY);
    int i = 0;
    for (Entry<Key,Value> entry : scanner) {
      assertEquals(rows[i++], entry.getKey().getRow().toString());
    }
    // verify the new files are written to the different volumes
    scanner = connector.createScanner("!METADATA", Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(MetadataTable.DATAFILE_COLUMN_FAMILY);
    int fileCount = 0;
    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString()); 
      assertTrue(inV1 || inV2);
      fileCount++;
    }
    assertEquals(4, fileCount);
  }
  
}

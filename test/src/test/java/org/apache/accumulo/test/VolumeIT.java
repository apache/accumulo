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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.test.functional.ConfigurableMacIT;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;

public class VolumeIT extends ConfigurableMacIT {

  private static final Text EMPTY = new Text();
  private static final Value EMPTY_VALUE = new Value(new byte[] {});
  public static File volDirBase;
  public static Path v1;
  public static Path v2;

  @BeforeClass
  public static void createVolumeDirs() throws IOException {
    volDirBase = createSharedTestDir(VolumeIT.class.getName() + "-volumes");
    File v1f = new File(volDirBase, "v1");
    File v2f = new File(volDirBase, "v2");
    v1f.mkdir();
    v2f.mkdir();
    v1 = new Path("file://" + v1f.getAbsolutePath());
    v2 = new Path("file://" + v2f.getAbsolutePath());
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg) {
    // Run MAC on two locations in the local file system
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1.toString() + "," + v2.toString());
    super.configure(cfg);
  }

  @Test
  public void test() throws Exception {
    // create a table
    Connector connector = getConnector();
    String tableName = getTableNames(1)[0];
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
    scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
    scanner.setRange(new Range("1", "1<"));
    scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
    int fileCount = 0;

    for (Entry<Key,Value> entry : scanner) {
      boolean inV1 = entry.getKey().getColumnQualifier().toString().contains(v1.toString());
      boolean inV2 = entry.getKey().getColumnQualifier().toString().contains(v2.toString());
      assertTrue(inV1 || inV2);
      fileCount++;
    }
    assertEquals(4, fileCount);
    List<DiskUsage> diskUsage = connector.tableOperations().getDiskUsage(Collections.singleton(tableName));
    assertEquals(1, diskUsage.size());
    long usage = diskUsage.get(0).getUsage().longValue();
    System.out.println("usage " + usage);
    assertTrue(usage > 700 && usage < 800);
  }

}

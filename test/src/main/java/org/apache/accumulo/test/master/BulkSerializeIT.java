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
package org.apache.accumulo.test.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.Bulk;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.master.util.BulkSerialize;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.conf.ServerConfigurationFactory;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.accumulo.server.fs.VolumeManagerImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class BulkSerializeIT extends AccumuloClusterHarness {
  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    cfg.setProperty(Property.TSERV_NATIVEMAP_ENABLED, "false");
  }

  @Test
  public void writeReadLoadMapping() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    AccumuloConfiguration aconf = new ServerConfigurationFactory(c.getInstance())
        .getSystemConfiguration();
    VolumeManager vm = VolumeManagerImpl.get(aconf);
    FileSystem fs = getCluster().getFileSystem();

    String rootPath = cluster.getTemporaryPath().toString();
    String sourceDir = rootPath + "/bulk_test" + getUniqueNames(1)[0];
    fs.delete(new Path(sourceDir), true);

    SortedMap<KeyExtent,Bulk.Files> mapping = generateMapping();
    Table.ID tableId = Table.ID.of(c.tableOperations().tableIdMap().get(tableName));

    BulkSerialize.writeLoadMapping(mapping, sourceDir, tableId, tableName, vm);
    SortedMap<KeyExtent,Bulk.Files> readMapping = BulkSerialize.readLoadMapping(sourceDir, tableId,
        vm);

    assertTrue("Read bulk mapping size " + readMapping.size() + " != 3",
        mapping.size() == readMapping.size());

    KeyExtent firstRead = readMapping.firstKey();
    KeyExtent lastRead = readMapping.lastKey();
    assertEquals("Bulk mapping is incorrect, comparing first keys", mapping.firstKey(), firstRead);
    assertEquals("Bulk mapping is incorrect, comparing last keys ", mapping.lastKey(), lastRead);

    checkFiles(mapping, readMapping);
  }

  @Test
  public void writeReadRenames() throws Exception {
    Connector c = getConnector();
    String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    AccumuloConfiguration aconf = new ServerConfigurationFactory(c.getInstance())
        .getSystemConfiguration();
    VolumeManager vm = VolumeManagerImpl.get(aconf);
    FileSystem fs = getCluster().getFileSystem();

    String rootPath = cluster.getTemporaryPath().toString();
    String sourceDir = rootPath + "/bulk_test" + getUniqueNames(1)[0];
    fs.delete(new Path(sourceDir), true);

    Map<String,String> renames = new HashMap<>();
    for (String f : "f1 f2 f3 f4 f5".split(" "))
      renames.put(new File(sourceDir, "old" + f).getAbsolutePath(),
          new File(sourceDir, "new" + f).getAbsolutePath());
    Table.ID tableId = Table.ID.of(c.tableOperations().tableIdMap().get(tableName));

    BulkSerialize.writeRenameMap(renames, sourceDir, tableId, vm);
    Map<String,String> readMap = BulkSerialize.readRenameMap(sourceDir, tableId, vm);

    assertEquals("Read renames file wrong size", renames.size(), readMap.size());
    assertEquals("Read renames file different from what was written.", renames, readMap);
  }

  private void checkFiles(SortedMap<KeyExtent,Bulk.Files> mapping,
      SortedMap<KeyExtent,Bulk.Files> readMapping) {
    mapping.forEach((ke, files) -> {
      Bulk.Files readFiles = readMapping.get(ke);
      assertEquals("Unexpected file", files, readFiles);
    });
  }

  public SortedMap<KeyExtent,Bulk.Files> generateMapping() {
    SortedMap<KeyExtent,Bulk.Files> mapping = new TreeMap<>();
    Bulk.Files testFiles = new Bulk.Files();
    Bulk.Files testFiles2 = new Bulk.Files();
    Bulk.Files testFiles3 = new Bulk.Files();
    long c = 0L;
    for (String f : "f1 f2 f3".split(" ")) {
      c++;
      testFiles.add(new Bulk.FileInfo(f, c, c));
    }
    c = 0L;
    for (String f : "g1 g2 g3".split(" ")) {
      c++;
      testFiles2.add(new Bulk.FileInfo(f, c, c));
    }
    for (String f : "h1 h2 h3".split(" ")) {
      c++;
      testFiles3.add(new Bulk.FileInfo(f, c, c));
    }

    // add out of order to test sorting
    mapping.put(new KeyExtent(Table.ID.of("9"), new Text("d"), new Text("c")), testFiles);
    mapping.put(new KeyExtent(Table.ID.of("5"), new Text("c"), new Text("b")), testFiles2);
    mapping.put(new KeyExtent(Table.ID.of("4"), new Text("b"), new Text("a")), testFiles3);

    return mapping;
  }
}

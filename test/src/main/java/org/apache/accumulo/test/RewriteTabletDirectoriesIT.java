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

import static org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily.DIRECTORY_COLUMN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.RandomizeVolumes;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.junit.Test;

// ACCUMULO-3263
public class RewriteTabletDirectoriesIT extends ConfigurableMacBase {

  @Override
  public int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private Path v1, v2;

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    File baseDir = cfg.getDir();
    File volDirBase = new File(baseDir, "volumes");
    File v1f = new File(volDirBase, "v1");
    File v2f = new File(volDirBase, "v2");
    v1 = new Path("file://" + v1f.getAbsolutePath());
    v2 = new Path("file://" + v2f.getAbsolutePath());

    // Use a VolumeChooser which should be more fair
    cfg.setProperty(Property.GENERAL_VOLUME_CHOOSER, FairVolumeChooser.class.getName());
    // Run MAC on two locations in the local file system
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1.toString());
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());
    super.configure(cfg, hadoopCoreSite);
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    c.securityOperations().grantTablePermission(c.whoami(), MetadataTable.NAME, TablePermission.WRITE);
    final String tableName = getUniqueNames(1)[0];
    c.tableOperations().create(tableName);

    // Write some data to a table and add some splits
    BatchWriter bw = c.createBatchWriter(tableName, null);
    final SortedSet<Text> splits = new TreeSet<>();
    for (String split : "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z".split(",")) {
      splits.add(new Text(split));
      Mutation m = new Mutation(new Text(split));
      m.put(new byte[] {}, new byte[] {}, new byte[] {});
      bw.addMutation(m);
    }
    bw.close();
    c.tableOperations().addSplits(tableName, splits);

    try (BatchScanner scanner = c.createBatchScanner(MetadataTable.NAME, Authorizations.EMPTY, 1)) {
      DIRECTORY_COLUMN.fetch(scanner);
      Table.ID tableId = Table.ID.of(c.tableOperations().tableIdMap().get(tableName));
      assertNotNull("TableID for " + tableName + " was null", tableId);
      scanner.setRanges(Collections.singletonList(TabletsSection.getRange(tableId)));
      // verify the directory entries are all on v1, make a few entries relative
      bw = c.createBatchWriter(MetadataTable.NAME, null);
      int count = 0;
      for (Entry<Key,Value> entry : scanner) {
        assertTrue("Expected " + entry.getValue() + " to contain " + v1, entry.getValue().toString().contains(v1.toString()));
        count++;
        if (count % 2 == 0) {
          String parts[] = entry.getValue().toString().split("/");
          Key key = entry.getKey();
          Mutation m = new Mutation(key.getRow());
          m.put(key.getColumnFamily(), key.getColumnQualifier(), new Value((Path.SEPARATOR + parts[parts.length - 1]).getBytes()));
          bw.addMutation(m);
        }
      }
      bw.close();
      assertEquals(splits.size() + 1, count);

      // This should fail: only one volume
      assertEquals(1, cluster.exec(RandomizeVolumes.class, "-z", cluster.getZooKeepers(), "-i", c.getInstance().getInstanceName(), "-t", tableName).waitFor());

      cluster.stop();

      // add the 2nd volume
      Configuration conf = new Configuration(false);
      conf.addResource(new Path(cluster.getConfig().getConfDir().toURI().toString(), "accumulo-site.xml"));
      conf.set(Property.INSTANCE_VOLUMES.getKey(), v1.toString() + "," + v2.toString());
      BufferedOutputStream fos = new BufferedOutputStream(new FileOutputStream(new File(cluster.getConfig().getConfDir(), "accumulo-site.xml")));
      conf.writeXml(fos);
      fos.close();

      // initialize volume
      assertEquals(0, cluster.exec(Initialize.class, "--add-volumes").waitFor());
      cluster.start();
      c = getConnector();

      // change the directory entries
      assertEquals(0, cluster.exec(Admin.class, "randomizeVolumes", "-t", tableName).waitFor());

      // verify a more equal sharing
      int v1Count = 0, v2Count = 0;
      for (Entry<Key,Value> entry : scanner) {
        if (entry.getValue().toString().contains(v1.toString())) {
          v1Count++;
        }
        if (entry.getValue().toString().contains(v2.toString())) {
          v2Count++;
        }
      }

      log.info("Count for volume1: {}", v1Count);
      log.info("Count for volume2: {}", v2Count);

      assertEquals(splits.size() + 1, v1Count + v2Count);
      // a fair chooser will differ by less than count(volumes)
      assertTrue("Expected the number of files to differ between volumes by less than 10. " + v1Count + " " + v2Count, Math.abs(v1Count - v2Count) < 2);
      // verify we can read the old data
      count = 0;
      for (Entry<Key,Value> entry : c.createScanner(tableName, Authorizations.EMPTY)) {
        assertTrue("Found unexpected entry in table: " + entry, splits.contains(entry.getKey().getRow()));
        count++;
      }
      assertEquals(splits.size(), count);
    }
  }
}

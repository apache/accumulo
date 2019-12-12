/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.Ample;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.harness.AccumuloClusterHarness;
import org.apache.accumulo.master.upgrade.Upgrader9to10;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class UpgradeRelativePathsIT extends AccumuloClusterHarness {
  File baseDir;
  Path v1;

  @Override
  public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    baseDir = cfg.getDir();
    File volDirBase = new File(baseDir, "volumes");
    File v1f = new File(volDirBase, "v1");
    v1 = new Path("file://" + v1f.getAbsolutePath());
    cfg.setProperty(Property.INSTANCE_VOLUMES_UPGRADE_RELATIVE.getKey(), v1.toString());
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1.toString());
  }

  @Test
  public void testUpgradeRelative() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName,
          new NewTableConfiguration().withoutDefaultIterators());

      createRelativePaths(client, tableName, false);

      Upgrader9to10.upgradeRelativePaths(getServerContext(), Ample.DataLevel.USER);

      verifyPaths(client.createScanner(MetadataTable.NAME, Authorizations.EMPTY), 2);
    }
  }

  @Test
  public void testUpgradeRelativeDottedType() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      String tableName = getUniqueNames(1)[0];
      client.tableOperations().create(tableName,
          new NewTableConfiguration().withoutDefaultIterators());

      createRelativePaths(client, tableName, true);

      Upgrader9to10.upgradeRelativePaths(getServerContext(), Ample.DataLevel.USER);

      verifyPaths(client.createScanner(MetadataTable.NAME, Authorizations.EMPTY), 4);
    }
  }

  // copied from VolumeIT
  private void createRelativePaths(AccumuloClient client, String tableName, boolean dottedType)
      throws Exception {
    TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

    SortedSet<Text> partitions = new TreeSet<>();
    // with some splits
    for (String s : "c,g,k,p,s,v".split(","))
      partitions.add(new Text(s));

    client.tableOperations().addSplits(tableName, partitions);

    BatchWriter bw = client.createBatchWriter(tableName);

    // create two files in each tablet
    for (String s : VolumeChooserIT.alpha_rows) {
      Mutation m = new Mutation(s);
      m.put("cf1", "cq1", "1");
      bw.addMutation(m);
    }

    bw.flush();
    client.tableOperations().flush(tableName, null, null, true);

    for (String s : VolumeChooserIT.alpha_rows) {
      Mutation m = new Mutation(s);
      m.put("cf1", "cq1", "2");
      bw.addMutation(m);
    }

    bw.close();
    client.tableOperations().flush(tableName, null, null, true);

    client.tableOperations().offline(tableName, true);

    client.securityOperations().grantTablePermission("root", MetadataTable.NAME,
        TablePermission.WRITE);

    try (Scanner metaScanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      metaScanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
      metaScanner.setRange(new KeyExtent(tableId, null, null).toMetadataRange());

      try (BatchWriter mbw = client.createBatchWriter(MetadataTable.NAME)) {
        for (Map.Entry<Key,Value> entry : metaScanner) {
          String cq = entry.getKey().getColumnQualifier().toString();
          if (cq.startsWith(v1.toString())) {
            Path path = new Path(cq);
            String relPath;
            if (dottedType)
              relPath = "../" + tableId.canonical() + "/" + path.getParent().getName() + "/"
                  + path.getName();
            else
              relPath = "/" + path.getParent().getName() + "/" + path.getName();

            Mutation fileMut = new Mutation(entry.getKey().getRow());
            fileMut.putDelete(entry.getKey().getColumnFamily(),
                entry.getKey().getColumnQualifier());
            fileMut.put(entry.getKey().getColumnFamily().toString(), relPath,
                entry.getValue().toString());
            mbw.addMutation(fileMut);
          }
        }
      }
    }
  }

  private void verifyPaths(Scanner metaScanner, int depth) {
    metaScanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
    for (Map.Entry<Key,Value> entry : metaScanner) {
      String cq = entry.getKey().getColumnQualifier().toString();
      Path path = new Path(cq);
      assertTrue("relative path not replaced " + path, path.depth() > depth);
    }
    metaScanner.close();
  }
}

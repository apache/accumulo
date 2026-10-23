/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import static java.util.stream.Collectors.toCollection;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.ExternalTabletFile;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.util.adminCommand.StopAll;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class VolumeIT extends VolumeITBase {

  @Test
  public void test() throws Exception {
    // create a table
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      // create set of splits
      TreeSet<Text> splits =
          Stream.of("d", "m", "t").map(Text::new).collect(toCollection(TreeSet::new));
      // create table with splits, write some data, and verify read
      VolumeChooserIT.createAndVerifyTable(client, tableName, splits, true);
      // verify the new files are written to the different volumes
      try (Scanner scanner =
          client.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {
        scanner.setRange(new Range("1", "1<"));
        scanner.fetchColumnFamily(DataFileColumnFamily.NAME);
        int fileCount = 0;

        for (Entry<Key,Value> entry : scanner) {
          boolean inV1 = StoredTabletFile.of(entry.getKey().getColumnQualifier()).getMetadataPath()
              .contains(v1.toString());
          boolean inV2 = StoredTabletFile.of(entry.getKey().getColumnQualifier()).getMetadataPath()
              .contains(v2.toString());
          assertTrue(inV1 || inV2);
          fileCount++;
        }
        assertEquals(4, fileCount);
        List<DiskUsage> diskUsage =
            client.tableOperations().getDiskUsage(Collections.singleton(tableName));
        assertEquals(1, diskUsage.size());
        long usage = diskUsage.get(0).getUsage();
        log.debug("usage {}", usage);
        assertTrue(usage > 700 && usage < 900);
      }
    }
  }

  @Test
  public void testAddVolumes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String[] tableNames = getUniqueNames(2);

      InstanceId uuid = verifyAndShutdownCluster(client, tableNames[0]);

      updateConfig(config -> config.setProperty(Property.INSTANCE_VOLUMES.getKey(),
          v1 + "," + v2 + "," + v3));

      // initialize volume
      assertEquals(0, cluster.exec(Initialize.class, "--add-volumes").getProcess().waitFor());

      checkVolumesInitialized(Arrays.asList(v1, v2, v3), uuid);

      // start cluster and verify that new volume is used
      cluster.start();

      verifyVolumesUsed(client, tableNames[1], false, v1, v2, v3);
    }
  }

  // grab uuid before shutting down cluster
  private InstanceId verifyAndShutdownCluster(AccumuloClient c, String tableName) throws Exception {
    InstanceId uuid = c.instanceOperations().getInstanceId();

    verifyVolumesUsed(c, tableName, false, v1, v2);

    assertEquals(0, cluster.exec(StopAll.class).getProcess().waitFor());
    cluster.stop();

    return uuid;
  }

  @Test
  public void testNonConfiguredVolumes() throws Exception {

    String[] tableNames = getUniqueNames(2);

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {

      InstanceId uuid = verifyAndShutdownCluster(client, tableNames[0]);

      updateConfig(config -> config.setProperty(Property.INSTANCE_VOLUMES.getKey(), v2 + "," + v3));

      // initialize volume
      assertEquals(0, cluster.exec(Initialize.class, "--add-volumes").getProcess().waitFor());

      checkVolumesInitialized(Arrays.asList(v1, v2, v3), uuid);

      // start cluster and verify that new volume is used
      cluster.start();

      // verify we can still read the tables (tableNames[0] is likely to have a file still on v1)
      verifyData(expected, client.createScanner(tableNames[0], Authorizations.EMPTY));

      // v1 should not have any data for tableNames[1]
      verifyVolumesUsed(client, tableNames[1], false, v2, v3);
    }
  }

  // check that all volumes are initialized
  private void checkVolumesInitialized(List<Path> volumes, InstanceId uuid) throws Exception {
    for (Path volumePath : volumes) {
      FileSystem fs = volumePath.getFileSystem(cluster.getServerContext().getHadoopConf());
      Path vp = new Path(volumePath, Constants.INSTANCE_ID_DIR);
      FileStatus[] iids = fs.listStatus(vp);
      assertEquals(1, iids.length);
      assertEquals(uuid.canonical(), iids[0].getPath().getName());
    }
  }

  @Test
  public void testRemoveVolumes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String[] tableNames = getUniqueNames(2);

      verifyVolumesUsed(client, tableNames[0], false, v1, v2);

      assertEquals(0, cluster.exec(StopAll.class).getProcess().waitFor());
      cluster.stop();

      updateConfig(config -> config.setProperty(Property.INSTANCE_VOLUMES.getKey(), v2.toString()));

      // start cluster and verify that volume was decommissioned
      cluster.start();

      client.tableOperations().compact(tableNames[0], null, null, true, true);

      verifyVolumesUsed(client, tableNames[0], true, v2);

      client.tableOperations().compact(SystemTables.ROOT.tableName(),
          new CompactionConfig().setWait(true));

      // check that root tablet is not on volume 1
      int count = 0;
      for (StoredTabletFile file : ((ClientContext) client).getAmple().readTablet(RootTable.EXTENT)
          .getFiles()) {
        assertTrue(file.getMetadataPath().startsWith(v2.toString()));
        count++;
      }

      assertTrue(count > 0);

      client.tableOperations().clone(tableNames[0], tableNames[1], true, new HashMap<>(),
          new HashSet<>());

      client.tableOperations().flush(SystemTables.METADATA.tableName(), null, null, true);
      client.tableOperations().flush(SystemTables.ROOT.tableName(), null, null, true);

      verifyVolumesUsed(client, tableNames[0], true, v2);
      verifyVolumesUsed(client, tableNames[1], true, v2);
    }
  }

  @Test
  public void testExternalFileSurvivesVolumeReplacement() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      verifyVolumesUsed(client, tableName, false, v1, v2);
      TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));

      TabletMetadata tablet;
      StoredTabletFile originalFile;
      DataFileValue fileValue;
      try (TabletsMetadata tablets = getServerContext().getAmple().readTablets().forTable(tableId)
          .fetch(ColumnType.FILES).build()) {
        tablet = tablets.iterator().next();
        var firstFile = tablet.getFilesMap().entrySet().iterator().next();
        originalFile = firstFile.getKey();
        fileValue = firstFile.getValue();
      }

      Path externalPath =
          new Path(new Path(v1.getParent(), "external-files"), getUniqueNames(1)[0] + ".rf");
      FileSystem fs = externalPath.getFileSystem(getServerContext().getHadoopConf());
      assertTrue(fs.mkdirs(externalPath.getParent()) || fs.exists(externalPath.getParent()));
      IOUtils.copyBytes(fs.open(originalFile.getPath()), fs.create(externalPath),
          getServerContext().getHadoopConf(), true);

      ExternalTabletFile externalFile =
          ExternalTabletFile.of(externalPath, originalFile.getRange());
      getServerContext().getAmple().mutateTablet(tablet.getExtent()).deleteFile(originalFile)
          .putFile(externalFile, fileValue).mutate();

      assertEquals(0, cluster.exec(StopAll.class).getProcess().waitFor());
      cluster.stop();

      java.nio.file.Path v1f = java.nio.file.Path.of(v1.toUri());
      java.nio.file.Path v8f = java.nio.file.Path.of(v1.getParent().toUri()).resolve("v8");
      Files.move(v1f, v8f, StandardCopyOption.ATOMIC_MOVE);
      Path v8 = new Path(v8f.toUri());

      java.nio.file.Path v2f = java.nio.file.Path.of(v2.toUri());
      java.nio.file.Path v9f = java.nio.file.Path.of(v2.getParent().toUri()).resolve("v9");
      Files.move(v2f, v9f, StandardCopyOption.ATOMIC_MOVE);
      Path v9 = new Path(v9f.toUri());

      updateConfig(config -> {
        config.setProperty(Property.INSTANCE_VOLUMES.getKey(), v8 + "," + v9);
        config.setProperty(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey(),
            v1 + " " + v8 + "," + v2 + " " + v9);
      });
      cluster.start();

      Wait.waitFor(() -> {
        boolean externalFound = false;
        try (TabletsMetadata tablets = getServerContext().getAmple().readTablets().forTable(tableId)
            .fetch(ColumnType.FILES).build()) {
          for (TabletMetadata currentTablet : tablets) {
            for (StoredTabletFile file : currentTablet.getFiles()) {
              if (file.isExternal()) {
                externalFound |= file.getPath().equals(externalPath);
              } else if (file.getMetadataPath().startsWith(v1.toString())
                  || file.getMetadataPath().startsWith(v2.toString())) {
                return false;
              }
            }
          }
        }
        return externalFound;
      }, 30000, 250, "External reference was lost or volume paths were not replaced");

      verifyData(expected, client.createScanner(tableName, Authorizations.EMPTY));
      assertTrue(fs.exists(externalPath));
    }
  }

  @Test
  public void testDirtyReplaceVolumes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      testReplaceVolume(client, false, false);
    }
  }

  @Test
  public void testCleanReplaceVolumesWithRangedFiles() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      testReplaceVolume(client, true, true);
    }
  }

  @Test
  public void testDirtyReplaceVolumesWithRangedFiles() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      testReplaceVolume(client, false, true);
    }
  }

}

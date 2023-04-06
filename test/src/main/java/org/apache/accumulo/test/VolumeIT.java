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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.DiskUsage;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.InstanceId;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.init.Initialize;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.log.WalStateManager.WalMarkerException;
import org.apache.accumulo.server.log.WalStateManager.WalState;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class VolumeIT extends ConfigurableMacBase {

  private File volDirBase;
  private Path v1, v2, v3;
  private List<String> expected = new ArrayList<>();

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    File baseDir = cfg.getDir();
    volDirBase = new File(baseDir, "volumes");
    File v1f = new File(volDirBase, "v1");
    File v2f = new File(volDirBase, "v2");
    v1 = new Path("file://" + v1f.getAbsolutePath());
    v2 = new Path("file://" + v2f.getAbsolutePath());
    File v3f = new File(volDirBase, "v3");
    v3 = new Path("file://" + v3f.getAbsolutePath());
    // setup expected rows
    for (int i = 0; i < 100; i++) {
      String row = String.format("%06d", i * 100 + 3);
      expected.add(row + ":cf1:cq1:1");
    }

    // Run MAC on two locations in the local file system
    cfg.setProperty(Property.INSTANCE_VOLUMES, v1 + "," + v2);
    cfg.setProperty(Property.INSTANCE_ZK_TIMEOUT, "15s");
    cfg.setClientProperty(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(), "15s");

    // use raw local file system so walogs sync and flush will work
    hadoopCoreSite.set("fs.file.impl", RawLocalFileSystem.class.getName());

    super.configure(cfg, hadoopCoreSite);
  }

  @Test
  public void test() throws Exception {
    // create a table
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String tableName = getUniqueNames(1)[0];
      // create set of splits
      SortedSet<Text> partitions = new TreeSet<>();
      for (String s : "d,m,t".split(",")) {
        partitions.add(new Text(s));
      }
      // create table with splits
      NewTableConfiguration ntc = new NewTableConfiguration().withSplits(partitions);
      client.tableOperations().create(tableName, ntc);
      // scribble over the splits
      VolumeChooserIT.writeDataToTable(client, tableName, VolumeChooserIT.alpha_rows);
      // write the data to disk, read it back
      client.tableOperations().flush(tableName, null, null, true);
      try (Scanner scanner = client.createScanner(tableName, Authorizations.EMPTY)) {
        int i = 0;
        for (Entry<Key,Value> entry : scanner) {
          assertEquals(VolumeChooserIT.alpha_rows[i++], entry.getKey().getRow().toString());
        }
      }
      // verify the new files are written to the different volumes
      try (Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
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
        List<DiskUsage> diskUsage =
            client.tableOperations().getDiskUsage(Collections.singleton(tableName));
        assertEquals(1, diskUsage.size());
        long usage = diskUsage.get(0).getUsage();
        log.debug("usage {}", usage);
        assertTrue(usage > 700 && usage < 900);
      }
    }
  }

  private void verifyData(List<String> expected, Scanner createScanner) {

    List<String> actual = new ArrayList<>();

    for (Entry<Key,Value> entry : createScanner) {
      Key k = entry.getKey();
      actual.add(k.getRow() + ":" + k.getColumnFamily() + ":" + k.getColumnQualifier() + ":"
          + entry.getValue());
    }

    Collections.sort(expected);
    Collections.sort(actual);

    createScanner.close();
    assertEquals(expected, actual);
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

    assertEquals(0, cluster.exec(Admin.class, "stopAll").getProcess().waitFor());
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

  private void writeData(String tableName, AccumuloClient client) throws AccumuloException,
      AccumuloSecurityException, TableExistsException, TableNotFoundException {

    TreeSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < 100; i++) {
      splits.add(new Text(String.format("%06d", i * 100)));
    }

    NewTableConfiguration ntc = new NewTableConfiguration().withSplits(splits);
    client.tableOperations().create(tableName, ntc);

    try (BatchWriter bw = client.createBatchWriter(tableName)) {
      for (int i = 0; i < 100; i++) {
        String row = String.format("%06d", i * 100 + 3);
        Mutation m = new Mutation(row);
        m.put("cf1", "cq1", "1");
        bw.addMutation(m);
      }
    }
  }

  private void verifyVolumesUsed(AccumuloClient client, String tableName, boolean shouldExist,
      Path... paths) throws Exception {

    if (!client.tableOperations().exists(tableName)) {
      assertFalse(shouldExist);

      writeData(tableName, client);

      verifyData(expected, client.createScanner(tableName, Authorizations.EMPTY));

      client.tableOperations().flush(tableName, null, null, true);
    }

    verifyData(expected, client.createScanner(tableName, Authorizations.EMPTY));

    TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
    try (Scanner metaScanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY)) {
      metaScanner.fetchColumnFamily(DataFileColumnFamily.NAME);
      metaScanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());

      int[] counts = new int[paths.length];

      outer: for (Entry<Key,Value> entry : metaScanner) {
        String path = entry.getKey().getColumnQualifier().toString();

        for (int i = 0; i < paths.length; i++) {
          if (path.startsWith(paths[i].toString())) {
            counts[i]++;
            continue outer;
          }
        }

        fail("Unexpected volume " + path);
      }

      // keep retrying until WAL state information in ZooKeeper stabilizes or until test times out
      retry: while (true) {
        WalStateManager wals = new WalStateManager(getServerContext());
        try {
          outer: for (Entry<Path,WalState> entry : wals.getAllState().entrySet()) {
            for (Path path : paths) {
              if (entry.getKey().toString().startsWith(path.toString())) {
                continue outer;
              }
            }
            log.warn("Unexpected volume " + entry.getKey() + " (" + entry.getValue() + ")");
            continue retry;
          }
        } catch (WalMarkerException e) {
          Throwable cause = e.getCause();
          if (cause instanceof NoNodeException) {
            // ignore WALs being cleaned up
            continue retry;
          }
          throw e;
        }
        break;
      }

      // if a volume is chosen randomly for each tablet, then the probability that a volume will not
      // be chosen for any tablet is ((num_volumes -
      // 1)/num_volumes)^num_tablets. For 100 tablets and 3 volumes the probability that only 2
      // volumes would be chosen is 2.46e-18

      int sum = 0;
      for (int count : counts) {
        assertTrue(count > 0);
        sum += count;
      }

      assertEquals(100, sum);
    }
  }

  @Test
  public void testRemoveVolumes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      String[] tableNames = getUniqueNames(2);

      verifyVolumesUsed(client, tableNames[0], false, v1, v2);

      assertEquals(0, cluster.exec(Admin.class, "stopAll").getProcess().waitFor());
      cluster.stop();

      updateConfig(config -> config.setProperty(Property.INSTANCE_VOLUMES.getKey(), v2.toString()));

      // start cluster and verify that volume was decommissioned
      cluster.start();

      client.tableOperations().compact(tableNames[0], null, null, true, true);

      verifyVolumesUsed(client, tableNames[0], true, v2);

      client.tableOperations().compact(RootTable.NAME, new CompactionConfig().setWait(true));

      // check that root tablet is not on volume 1
      int count = 0;
      for (StoredTabletFile file : ((ClientContext) client).getAmple().readTablet(RootTable.EXTENT)
          .getFiles()) {
        assertTrue(file.getMetaUpdateDelete().startsWith(v2.toString()));
        count++;
      }

      assertTrue(count > 0);

      client.tableOperations().clone(tableNames[0], tableNames[1], true, new HashMap<>(),
          new HashSet<>());

      client.tableOperations().flush(MetadataTable.NAME, null, null, true);
      client.tableOperations().flush(RootTable.NAME, null, null, true);

      verifyVolumesUsed(client, tableNames[0], true, v2);
      verifyVolumesUsed(client, tableNames[1], true, v2);
    }
  }

  private void testReplaceVolume(AccumuloClient client, boolean cleanShutdown) throws Exception {
    String[] tableNames = getUniqueNames(3);

    verifyVolumesUsed(client, tableNames[0], false, v1, v2);

    // write to 2nd table, but do not flush data to disk before shutdown
    try (AccumuloClient c2 =
        cluster.createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD))) {
      writeData(tableNames[1], c2);
    }

    if (cleanShutdown) {
      assertEquals(0, cluster.exec(Admin.class, "stopAll").getProcess().waitFor());
    }

    cluster.stop();

    File v1f = new File(v1.toUri());
    File v8f = new File(new File(v1.getParent().toUri()), "v8");
    assertTrue(v1f.renameTo(v8f), "Failed to rename " + v1f + " to " + v8f);
    Path v8 = new Path(v8f.toURI());

    File v2f = new File(v2.toUri());
    File v9f = new File(new File(v2.getParent().toUri()), "v9");
    assertTrue(v2f.renameTo(v9f), "Failed to rename " + v2f + " to " + v9f);
    Path v9 = new Path(v9f.toURI());

    updateConfig(config -> {
      config.setProperty(Property.INSTANCE_VOLUMES.getKey(), v8 + "," + v9);
      config.setProperty(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey(),
          v1 + " " + v8 + "," + v2 + " " + v9);
    });

    // start cluster and verify that volumes were replaced
    cluster.start();

    verifyVolumesUsed(client, tableNames[0], true, v8, v9);
    verifyVolumesUsed(client, tableNames[1], true, v8, v9);

    // verify writes to new dir
    client.tableOperations().compact(tableNames[0], null, null, true, true);
    client.tableOperations().compact(tableNames[1], null, null, true, true);

    verifyVolumesUsed(client, tableNames[0], true, v8, v9);
    verifyVolumesUsed(client, tableNames[1], true, v8, v9);

    client.tableOperations().compact(RootTable.NAME, new CompactionConfig().setWait(true));

    // check that root tablet is not on volume 1 or 2
    int count = 0;
    for (StoredTabletFile file : ((ClientContext) client).getAmple().readTablet(RootTable.EXTENT)
        .getFiles()) {
      assertTrue(file.getMetaUpdateDelete().startsWith(v8.toString())
          || file.getMetaUpdateDelete().startsWith(v9.toString()));
      count++;
    }

    assertTrue(count > 0);

    client.tableOperations().clone(tableNames[1], tableNames[2], true, new HashMap<>(),
        new HashSet<>());

    client.tableOperations().flush(MetadataTable.NAME, null, null, true);
    client.tableOperations().flush(RootTable.NAME, null, null, true);

    verifyVolumesUsed(client, tableNames[0], true, v8, v9);
    verifyVolumesUsed(client, tableNames[1], true, v8, v9);
    verifyVolumesUsed(client, tableNames[2], true, v8, v9);
  }

  @Test
  public void testCleanReplaceVolumes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      testReplaceVolume(client, true);
    }
  }

  @Test
  public void testDirtyReplaceVolumes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      testReplaceVolume(client, false);
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths provided by test")
  private void updateConfig(Consumer<PropertiesConfiguration> updater) throws Exception {
    var file = new File(cluster.getAccumuloPropertiesPath());
    var config = new PropertiesConfiguration();
    try (FileReader out = new FileReader(file, UTF_8)) {
      config.read(out);
    }
    updater.accept(config);
    try (FileWriter out = new FileWriter(file, UTF_8)) {
      config.write(out);
    }
  }
}

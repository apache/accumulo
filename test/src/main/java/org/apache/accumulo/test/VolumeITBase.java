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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.security.SystemCredentials;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.util.FileMetadataUtil;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class VolumeITBase extends ConfigurableMacBase {
  protected Path v1;
  protected Path v2;
  protected Path v3;
  protected List<String> expected = new ArrayList<>();

  private static Text getTabletMidPoint(Text row) {
    return row != null ? new Text(String.format("%06d", Integer.parseInt(row.toString()) - 50))
        : null;
  }

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration hadoopCoreSite) {
    java.nio.file.Path volDirBase = cfg.getDir().toPath().resolve("volumes");
    java.nio.file.Path v1f = volDirBase.resolve("v1");
    java.nio.file.Path v2f = volDirBase.resolve("v2");
    v1 = new Path("file://" + v1f.toAbsolutePath());
    v2 = new Path("file://" + v2f.toAbsolutePath());
    java.nio.file.Path v3f = volDirBase.resolve("v3");
    v3 = new Path("file://" + v3f.toAbsolutePath());
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

  protected void verifyData(List<String> expected, Scanner createScanner) {

    List<String> actual = new ArrayList<>();

    for (Map.Entry<Key,Value> entry : createScanner) {
      Key k = entry.getKey();
      actual.add(k.getRow() + ":" + k.getColumnFamily() + ":" + k.getColumnQualifier() + ":"
          + entry.getValue());
    }

    Collections.sort(expected);
    Collections.sort(actual);

    createScanner.close();
    assertEquals(expected, actual);
  }

  protected TreeSet<Text> generateSplits() {
    TreeSet<Text> splits = new TreeSet<>();
    for (int i = 1; i < 100; i++) {
      splits.add(new Text(String.format("%06d", i * 100)));
    }
    return splits;
  }

  private void writeData(String tableName, AccumuloClient client) throws AccumuloException,
      AccumuloSecurityException, TableExistsException, TableNotFoundException {

    TreeSet<Text> splits = generateSplits();

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

  protected void verifyVolumesUsed(AccumuloClient client, String tableName, boolean shouldExist,
      Path... paths) throws Exception {
    verifyVolumesUsed(client, tableName, shouldExist, false, paths);
  }

  private void verifyVolumesUsed(AccumuloClient client, String tableName, boolean shouldExist,
      boolean rangedFiles, Path... paths) throws Exception {

    if (!client.tableOperations().exists(tableName)) {
      assertFalse(shouldExist);

      writeData(tableName, client);

      verifyData(expected, client.createScanner(tableName, Authorizations.EMPTY));

      client.tableOperations().flush(tableName, null, null, true);
    }

    verifyData(expected, client.createScanner(tableName, Authorizations.EMPTY));

    TableId tableId = TableId.of(client.tableOperations().tableIdMap().get(tableName));
    try (Scanner metaScanner =
        client.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {
      metaScanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
      metaScanner.setRange(new KeyExtent(tableId, null, null).toMetaRange());

      int[] counts = new int[paths.length];

      outer: for (Map.Entry<Key,Value> entry : metaScanner) {
        String path = StoredTabletFile.of(entry.getKey().getColumnQualifier()).getMetadataPath();

        for (int i = 0; i < paths.length; i++) {
          if (path.contains(paths[i].toString())) {
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
          outer: for (Map.Entry<Path,WalStateManager.WalState> entry : wals.getAllState()
              .entrySet()) {
            for (Path path : paths) {
              if (entry.getKey().toString().startsWith(path.toString())) {
                continue outer;
              }
            }
            log.warn("Unexpected volume " + entry.getKey() + " (" + entry.getValue() + ")");
            UtilWaitThread.sleep(100);
            continue retry;
          }
        } catch (WalStateManager.WalMarkerException e) {
          Throwable cause = e.getCause();
          if (cause instanceof KeeperException.NoNodeException) {
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

      // When ranged files exist we there should be twice as many
      // as the test split each file into 2
      int numTablets = generateSplits().size() + 1;
      int expectedCount = rangedFiles ? numTablets * 2 : numTablets;
      assertEquals(expectedCount, sum);
    }
  }

  protected void testReplaceVolume(AccumuloClient client, boolean cleanShutdown,
      boolean rangedFiles) throws Exception {
    String[] tableNames = getUniqueNames(3);

    verifyVolumesUsed(client, tableNames[0], false, v1, v2);

    // write to 2nd table, but do not flush data to disk before shutdown
    try (AccumuloClient c2 =
        cluster.createAccumuloClient("root", new PasswordToken(ROOT_PASSWORD))) {
      writeData(tableNames[1], c2);
    }

    // If flag is true then for each file split and create two files
    // to verify volume replacement works on files with ranges
    if (rangedFiles) {
      splitFilesWithRange(client, tableNames[0]);
      splitFilesWithRange(client, tableNames[1]);
    }

    if (cleanShutdown) {
      assertEquals(0, cluster.exec(Admin.class, "stopAll").getProcess().waitFor());
    }

    cluster.stop();

    java.nio.file.Path v1f = java.nio.file.Path.of(v1.toUri());
    java.nio.file.Path v8f = java.nio.file.Path.of(v1.getParent().toUri()).resolve("v8");
    Files.move(v1f, v8f, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    assertTrue(Files.exists(v8f), "Failed to rename " + v1f + " to " + v8f);
    Path v8 = new Path(v8f.toUri());

    java.nio.file.Path v2f = java.nio.file.Path.of(v2.toUri());
    java.nio.file.Path v9f = java.nio.file.Path.of(v2.getParent().toUri()).resolve("v9");
    Files.move(v2f, v9f, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
    assertTrue(Files.exists(v9f), "Failed to rename " + v2f + " to " + v9f);
    Path v9 = new Path(v9f.toUri());

    updateConfig(config -> {
      config.setProperty(Property.INSTANCE_VOLUMES.getKey(), v8 + "," + v9);
      config.setProperty(Property.INSTANCE_VOLUMES_REPLACEMENTS.getKey(),
          v1 + " " + v8 + "," + v2 + " " + v9);
    });

    // start cluster and verify that volumes were replaced
    cluster.start();

    verifyVolumesUsed(client, tableNames[0], true, rangedFiles, v8, v9);
    verifyVolumesUsed(client, tableNames[1], true, rangedFiles, v8, v9);

    // verify writes to new dir
    client.tableOperations().compact(tableNames[0], null, null, true, true);
    client.tableOperations().compact(tableNames[1], null, null, true, true);

    // Always pass false for ranged files as compaction will clean them up if exist
    verifyVolumesUsed(client, tableNames[0], true, false, v8, v9);
    verifyVolumesUsed(client, tableNames[1], true, false, v8, v9);

    client.tableOperations().compact(SystemTables.ROOT.tableName(),
        new CompactionConfig().setWait(true));

    // check that root tablet is not on volume 1 or 2
    int count = 0;
    for (StoredTabletFile file : ((ClientContext) client).getAmple().readTablet(RootTable.EXTENT)
        .getFiles()) {
      assertTrue(file.getMetadataPath().startsWith(v8.toString())
          || file.getMetadataPath().startsWith(v9.toString()));
      count++;
    }

    assertTrue(count > 0);

    client.tableOperations().clone(tableNames[1], tableNames[2], true, new HashMap<>(),
        new HashSet<>());

    client.tableOperations().flush(SystemTables.METADATA.tableName(), null, null, true);
    client.tableOperations().flush(SystemTables.ROOT.tableName(), null, null, true);

    verifyVolumesUsed(client, tableNames[0], true, v8, v9);
    verifyVolumesUsed(client, tableNames[1], true, v8, v9);
    verifyVolumesUsed(client, tableNames[2], true, v8, v9);
  }

  @Test
  public void testCleanReplaceVolumes() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      testReplaceVolume(client, true, false);
    }
  }

  @SuppressFBWarnings(value = "PATH_TRAVERSAL_IN", justification = "paths provided by test")
  protected void updateConfig(Consumer<PropertiesConfiguration> updater) throws Exception {
    var file = java.nio.file.Path.of(cluster.getAccumuloPropertiesPath());
    var config = new PropertiesConfiguration();
    try (BufferedReader out = Files.newBufferedReader(file, UTF_8)) {
      config.read(out);
    }
    updater.accept(config);
    try (BufferedWriter out = Files.newBufferedWriter(file, UTF_8)) {
      config.write(out);
    }
  }

  // Go through each tablet file in metadata and split the files into two files
  // by adding two new entries that covers half of the file. This will test that
  // files with ranges work properly with volume replacement
  private void splitFilesWithRange(AccumuloClient client, String tableName) throws Exception {
    client.securityOperations().grantTablePermission(cluster.getConfig().getRootUserName(),
        SystemTables.METADATA.tableName(), TablePermission.WRITE);
    final ServerContext ctx = getServerContext();
    ctx.setCredentials(new SystemCredentials(client.instanceOperations().getInstanceId(), "root",
        new PasswordToken(ROOT_PASSWORD)));

    AtomicInteger i = new AtomicInteger();
    FileMetadataUtil.mutateTabletFiles(ctx, tableName, null, null, (tm, mutator, file, value) -> {
      i.incrementAndGet();

      // Create a mutation to delete the existing file metadata entry with infinite range
      mutator.deleteFile(file);

      // Find the midpoint and create two new files, each with a range covering half the file
      Text tabletMidPoint = VolumeITBase.getTabletMidPoint(tm.getExtent().endRow());
      // Handle edge case for last tablet
      if (tabletMidPoint == null) {
        tabletMidPoint = new Text(
            String.format("%06d", Integer.parseInt(tm.getExtent().prevEndRow().toString()) + 50));
      }

      final DataFileValue newValue = new DataFileValue(Integer.max(1, (int) (value.getSize() / 2)),
          Integer.max(1, (int) (value.getNumEntries() / 2)));
      mutator.putFile(StoredTabletFile.of(file.getPath(),
          new Range(tm.getExtent().prevEndRow(), false, tabletMidPoint, true)), newValue);
      mutator.putFile(StoredTabletFile.of(file.getPath(),
          new Range(tabletMidPoint, false, tm.getExtent().endRow(), true)), newValue);
    });
  }
}

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
package org.apache.accumulo.test.functional;

import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoServiceFactory;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.LoadPlan.RangeType;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Tests new bulk import technique. For the old technique see {@link BulkOldIT}
 *
 * @since 2.0
 */
public class BulkNewIT extends SharedMiniClusterBase {

  @BeforeClass
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new Callback());
  }

  @AfterClass
  public static void teardown() {
    SharedMiniClusterBase.stopMiniCluster();
  }

  private static class Callback implements MiniClusterConfigurationCallback {
    @Override
    public void configureMiniCluster(MiniAccumuloConfigImpl cfg, Configuration conf) {
      cfg.setMemory(ServerType.TABLET_SERVER, 512, MemoryUnit.MEGABYTE);

      // use raw local file system
      conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
    }
  }

  @Override
  protected int defaultTimeoutSeconds() {
    return 4 * 60;
  }

  private String tableName;
  private AccumuloConfiguration aconf;
  private FileSystem fs;
  private String rootPath;

  @Before
  public void setupBulkTest() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      tableName = getUniqueNames(1)[0];
      c.tableOperations().create(tableName);
      aconf = getCluster().getServerContext().getConfiguration();
      fs = getCluster().getFileSystem();
      rootPath = getCluster().getTemporaryPath().toString();
    }
  }

  private String getDir(String testName) throws Exception {
    String dir = rootPath + testName + getUniqueNames(1)[0];
    fs.delete(new Path(dir), true);
    return dir;
  }

  private void testSingleTabletSingleFile(AccumuloClient c, boolean offline, boolean setTime)
      throws Exception {
    addSplits(c, tableName, "0333");

    if (offline) {
      c.tableOperations().offline(tableName);
    }

    String dir = getDir("/testSingleTabletSingleFileNoSplits-");

    String h1 = writeData(dir + "/f1.", aconf, 0, 332);

    c.tableOperations().importDirectory(dir).to(tableName).tableTime(setTime).load();

    if (offline) {
      c.tableOperations().online(tableName);
    }

    verifyData(c, tableName, 0, 332, setTime);
    verifyMetadata(c, tableName, Map.of("0333", Set.of(h1), "null", Set.of()));
  }

  @Test
  public void testSingleTabletSingleFile() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      testSingleTabletSingleFile(client, false, false);
    }
  }

  @Test
  public void testSetTime() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      tableName = "testSetTime_table1";
      NewTableConfiguration newTableConf = new NewTableConfiguration();
      // set logical time type so we can set time on bulk import
      newTableConf.setTimeType(TimeType.LOGICAL);
      client.tableOperations().create(tableName, newTableConf);
      testSingleTabletSingleFile(client, false, true);
    }
  }

  @Test
  public void testSingleTabletSingleFileOffline() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      testSingleTabletSingleFile(client, true, false);
    }
  }

  @Test
  public void testMaxTablets() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      tableName = "testMaxTablets_table1";
      NewTableConfiguration newTableConf = new NewTableConfiguration();
      // set logical time type so we can set time on bulk import
      var props = Map.of(Property.TABLE_BULK_MAX_TABLETS.getKey(), "2");
      newTableConf.setProperties(props);
      client.tableOperations().create(tableName, newTableConf);

      // test max tablets hit while inspecting bulk files
      var thrown = assertThrows(RuntimeException.class, () -> testBulkFileMax(false));
      var c = thrown.getCause();
      assertTrue("Wrong exception: " + c, c instanceof ExecutionException);
      assertTrue("Wrong exception: " + c.getCause(),
          c.getCause() instanceof IllegalArgumentException);
      var msg = c.getCause().getMessage();
      assertTrue("Bad File not in exception: " + msg, msg.contains("bad-file.rf"));

      // test max tablets hit using load plan on the server side
      c = assertThrows(AccumuloException.class, () -> testBulkFileMax(true));
      msg = c.getMessage();
      assertTrue("Bad File not in exception: " + msg, msg.contains("bad-file.rf"));
    }
  }

  private void testSingleTabletSingleFileNoSplits(AccumuloClient c, boolean offline)
      throws Exception {
    if (offline) {
      c.tableOperations().offline(tableName);
    }

    String dir = getDir("/testSingleTabletSingleFileNoSplits-");

    String h1 = writeData(dir + "/f1.", aconf, 0, 333);

    c.tableOperations().importDirectory(dir).to(tableName).load();

    if (offline) {
      c.tableOperations().online(tableName);
    }

    verifyData(c, tableName, 0, 333, false);
    verifyMetadata(c, tableName, Map.of("null", Set.of(h1)));
  }

  @Test
  public void testSingleTabletSingleFileNoSplits() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      testSingleTabletSingleFileNoSplits(client, false);
    }
  }

  @Test
  public void testSingleTabletSingleFileNoSplitsOffline() throws Exception {
    try (AccumuloClient client = Accumulo.newClient().from(getClientProps()).build()) {
      testSingleTabletSingleFileNoSplits(client, true);
    }
  }

  @Test
  public void testBadPermissions() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      addSplits(c, tableName, "0333");

      String dir = getDir("/testBadPermissions-");

      writeData(dir + "/f1.", aconf, 0, 333);

      Path rFilePath = new Path(dir, "f1." + RFile.EXTENSION);
      FsPermission originalPerms = fs.getFileStatus(rFilePath).getPermission();
      fs.setPermission(rFilePath, FsPermission.valueOf("----------"));
      try {
        c.tableOperations().importDirectory(dir).to(tableName).load();
      } catch (Exception e) {
        Throwable cause = e.getCause();
        if (!(cause instanceof FileNotFoundException)
            && !(cause.getCause() instanceof FileNotFoundException)) {
          fail("Expected FileNotFoundException but threw " + e.getCause());
        }
      } finally {
        fs.setPermission(rFilePath, originalPerms);
      }

      originalPerms = fs.getFileStatus(new Path(dir)).getPermission();
      fs.setPermission(new Path(dir), FsPermission.valueOf("dr--r--r--"));
      try {
        c.tableOperations().importDirectory(dir).to(tableName).load();
      } catch (AccumuloException ae) {
        if (!(ae.getCause() instanceof FileNotFoundException)) {
          fail("Expected FileNotFoundException but threw " + ae.getCause());
        }
      } finally {
        fs.setPermission(new Path(dir), originalPerms);
      }
    }
  }

  private void testBulkFile(boolean offline, boolean usePlan) throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      addSplits(c, tableName, "0333 0666 0999 1333 1666");

      if (offline) {
        c.tableOperations().offline(tableName);
      }

      String dir = getDir("/testBulkFile-");

      Map<String,Set<String>> hashes = new HashMap<>();
      for (String endRow : Arrays.asList("0333 0666 0999 1333 1666 null".split(" "))) {
        hashes.put(endRow, new HashSet<>());
      }

      // Add a junk file, should be ignored
      FSDataOutputStream out = fs.create(new Path(dir, "junk"));
      out.writeChars("ABCDEFG\n");
      out.close();

      // 1 Tablet 0333-null
      String h1 = writeData(dir + "/f1.", aconf, 0, 333);
      hashes.get("0333").add(h1);

      // 2 Tablets 0666-0334, 0999-0667
      String h2 = writeData(dir + "/f2.", aconf, 334, 999);
      hashes.get("0666").add(h2);
      hashes.get("0999").add(h2);

      // 2 Tablets 1333-1000, 1666-1334
      String h3 = writeData(dir + "/f3.", aconf, 1000, 1499);
      hashes.get("1333").add(h3);
      hashes.get("1666").add(h3);

      // 2 Tablets 1666-1334, >1666
      String h4 = writeData(dir + "/f4.", aconf, 1500, 1999);
      hashes.get("1666").add(h4);
      hashes.get("null").add(h4);

      if (usePlan) {
        LoadPlan loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, null, row(333))
            .loadFileTo("f2.rf", RangeType.TABLE, row(333), row(999))
            .loadFileTo("f3.rf", RangeType.FILE, row(1000), row(1499))
            .loadFileTo("f4.rf", RangeType.FILE, row(1500), row(1999)).build();
        c.tableOperations().importDirectory(dir).to(tableName).plan(loadPlan).load();
      } else {
        c.tableOperations().importDirectory(dir).to(tableName).load();
      }

      if (offline) {
        c.tableOperations().online(tableName);
      }

      verifyData(c, tableName, 0, 1999, false);
      verifyMetadata(c, tableName, hashes);
    }
  }

  private void testBulkFileMax(boolean usePlan) throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      addSplits(c, tableName, "0333 0666 0999 1333 1666");

      String dir = getDir("/testBulkFileMax-");

      Map<String,Set<String>> hashes = new HashMap<>();
      for (String endRow : Arrays.asList("0333 0666 0999 1333 1666 null".split(" "))) {
        hashes.put(endRow, new HashSet<>());
      }

      // Add a junk file, should be ignored
      FSDataOutputStream out = fs.create(new Path(dir, "junk"));
      out.writeChars("ABCDEFG\n");
      out.close();

      // 1 Tablet 0333-null
      String h1 = writeData(dir + "/f1.", aconf, 0, 333);
      hashes.get("0333").add(h1);

      // 3 Tablets 0666-0334, 0999-0667, 1333-1000
      String h2 = writeData(dir + "/bad-file.", aconf, 334, 1333);
      hashes.get("0666").add(h2);
      hashes.get("0999").add(h2);
      hashes.get("1333").add(h2);

      // 1 Tablet 1666-1334
      String h3 = writeData(dir + "/f3.", aconf, 1334, 1499);
      hashes.get("1666").add(h3);

      // 2 Tablets 1666-1334, >1666
      String h4 = writeData(dir + "/f4.", aconf, 1500, 1999);
      hashes.get("1666").add(h4);
      hashes.get("null").add(h4);

      if (usePlan) {
        LoadPlan loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, null, row(333))
            .loadFileTo("bad-file.rf", RangeType.TABLE, row(333), row(1333))
            .loadFileTo("f3.rf", RangeType.FILE, row(1334), row(1499))
            .loadFileTo("f4.rf", RangeType.FILE, row(1500), row(1999)).build();
        c.tableOperations().importDirectory(dir).to(tableName).plan(loadPlan).load();
      } else {
        c.tableOperations().importDirectory(dir).to(tableName).load();
      }

      verifyData(c, tableName, 0, 1999, false);
      verifyMetadata(c, tableName, hashes);
    }
  }

  @Test
  public void testBulkFile() throws Exception {
    testBulkFile(false, false);
  }

  @Test
  public void testBulkFileOffline() throws Exception {
    testBulkFile(true, false);
  }

  @Test
  public void testLoadPlan() throws Exception {
    testBulkFile(false, true);
  }

  @Test
  public void testLoadPlanOffline() throws Exception {
    testBulkFile(true, true);
  }

  @Test
  public void testBadLoadPlans() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      addSplits(c, tableName, "0333 0666 0999 1333 1666");

      String dir = getDir("/testBulkFile-");

      writeData(dir + "/f1.", aconf, 0, 333);
      writeData(dir + "/f2.", aconf, 0, 666);

      // Create a plan with more files than exists in dir
      LoadPlan loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, null, row(333))
          .loadFileTo("f2.rf", RangeType.TABLE, null, row(666))
          .loadFileTo("f3.rf", RangeType.TABLE, null, row(666)).build();
      try {
        c.tableOperations().importDirectory(dir).to(tableName).plan(loadPlan).load();
        fail();
      } catch (IllegalArgumentException e) {
        // ignore
      }

      // Create a plan with less files than exists in dir
      loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, null, row(333)).build();
      try {
        c.tableOperations().importDirectory(dir).to(tableName).plan(loadPlan).load();
        fail();
      } catch (IllegalArgumentException e) {
        // ignore
      }

      // Create a plan with tablet boundary that does not exits
      loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, null, row(555))
          .loadFileTo("f2.rf", RangeType.TABLE, null, row(555)).build();
      try {
        c.tableOperations().importDirectory(dir).to(tableName).plan(loadPlan).load();
        fail();
      } catch (AccumuloException e) {
        // ignore
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyDir() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String dir = getDir("/testBulkFile-");
      FileSystem fs = getCluster().getFileSystem();
      fs.mkdirs(new Path(dir));
      c.tableOperations().importDirectory(dir).to(tableName).load();
    }
  }

  private void addSplits(AccumuloClient client, String tableName, String splitString)
      throws Exception {
    SortedSet<Text> splits = new TreeSet<>();
    for (String split : splitString.split(" ")) {
      splits.add(new Text(split));
    }
    client.tableOperations().addSplits(tableName, splits);
  }

  private void verifyData(AccumuloClient client, String table, int start, int end, boolean setTime)
      throws Exception {
    try (Scanner scanner = client.createScanner(table, Authorizations.EMPTY)) {

      Iterator<Entry<Key,Value>> iter = scanner.iterator();

      for (int i = start; i <= end; i++) {
        if (!iter.hasNext()) {
          throw new Exception("row " + i + " not found");
        }

        Entry<Key,Value> entry = iter.next();

        String row = String.format("%04d", i);

        if (!entry.getKey().getRow().equals(new Text(row))) {
          throw new Exception("unexpected row " + entry.getKey() + " " + i);
        }

        if (Integer.parseInt(entry.getValue().toString()) != i) {
          throw new Exception("unexpected value " + entry + " " + i);
        }

        if (setTime) {
          assertEquals(1L, entry.getKey().getTimestamp());
        }
      }

      if (iter.hasNext()) {
        throw new Exception("found more than expected " + iter.next());
      }
    }
  }

  private void verifyMetadata(AccumuloClient client, String tableName,
      Map<String,Set<String>> expectedHashes) {

    Set<String> endRowsSeen = new HashSet<>();

    String id = client.tableOperations().tableIdMap().get(tableName);
    try (TabletsMetadata tablets = TabletsMetadata.builder().forTable(TableId.of(id))
        .fetch(FILES, LOADED, PREV_ROW).build(client)) {
      for (TabletMetadata tablet : tablets) {
        assertTrue(tablet.getLoaded().isEmpty());

        Set<String> fileHashes = tablet.getFiles().stream().map(f -> hash(f.getMetaUpdateDelete()))
            .collect(Collectors.toSet());

        String endRow = tablet.getEndRow() == null ? "null" : tablet.getEndRow().toString();

        assertEquals(expectedHashes.get(endRow), fileHashes);

        endRowsSeen.add(endRow);
      }

      assertEquals(expectedHashes.keySet(), endRowsSeen);
    }
  }

  @SuppressFBWarnings(value = {"PATH_TRAVERSAL_IN", "WEAK_MESSAGE_DIGEST_SHA1"},
      justification = "path provided by test; sha-1 is okay for test")
  private String hash(String filename) {
    try {
      byte[] data = Files.readAllBytes(Paths.get(filename.replaceFirst("^file:", "")));
      byte[] hash = MessageDigest.getInstance("SHA1").digest(data);
      return new BigInteger(1, hash).toString(16);
    } catch (IOException | NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  private static String row(int r) {
    return String.format("%04d", r);
  }

  private String writeData(String file, AccumuloConfiguration aconf, int s, int e)
      throws Exception {
    FileSystem fs = getCluster().getFileSystem();
    String filename = file + RFile.EXTENSION;
    try (FileSKVWriter writer = FileOperations.getInstance().newWriterBuilder()
        .forFile(filename, fs, fs.getConf(), CryptoServiceFactory.newDefaultInstance())
        .withTableConfiguration(aconf).build()) {
      writer.startDefaultLocalityGroup();
      for (int i = s; i <= e; i++) {
        writer.append(new Key(new Text(row(i))), new Value(Integer.toString(i)));
      }
    }

    return hash(filename);
  }
}

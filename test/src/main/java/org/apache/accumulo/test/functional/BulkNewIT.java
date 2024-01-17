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
package org.apache.accumulo.test.functional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.FILES;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.LOADED;
import static org.apache.accumulo.core.metadata.schema.TabletMetadata.ColumnType.PREV_ROW;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.LoadPlan.RangeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.constraints.Constraint;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.metadata.AccumuloTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.UnreferencedTabletFile;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.MetadataTime;
import org.apache.accumulo.core.metadata.schema.TabletMetadata;
import org.apache.accumulo.core.metadata.schema.TabletsMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.accumulo.harness.MiniClusterConfigurationCallback;
import org.apache.accumulo.harness.SharedMiniClusterBase;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.accumulo.miniclusterImpl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.constraints.MetadataConstraints;
import org.apache.accumulo.server.constraints.SystemEnvironment;
import org.apache.accumulo.test.util.Wait;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.util.concurrent.MoreExecutors;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class BulkNewIT extends SharedMiniClusterBase {

  @Override
  protected Duration defaultTimeout() {
    return Duration.ofMinutes(4);
  }

  @BeforeAll
  public static void setup() throws Exception {
    SharedMiniClusterBase.startMiniClusterWithConfig(new Callback());
  }

  @AfterAll
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

  private String tableName;
  private AccumuloConfiguration aconf;
  private FileSystem fs;
  private String rootPath;

  @BeforeEach
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
    testSingleTabletSingleFile(c, offline, setTime, () -> {
      return null;
    });
  }

  private void testSingleTabletSingleFile(AccumuloClient c, boolean offline, boolean setTime,
      Callable<Void> preLoadAction) throws Exception {
    addSplits(c, tableName, "0333");

    if (offline) {
      c.tableOperations().offline(tableName);
    }

    String dir = getDir("/testSingleTabletSingleFileNoSplits-");

    String h1 = writeData(dir + "/f1.", aconf, 0, 332);

    preLoadAction.call();
    c.tableOperations().importDirectory(dir).to(tableName).tableTime(setTime).load();
    // running again with ignoreEmptyDir set to true will not throw an exception
    c.tableOperations().importDirectory(dir).to(tableName).tableTime(setTime).ignoreEmptyDir(true)
        .load();
    // but if run with with ignoreEmptyDir value set to false, an IllegalArgument exception will
    // be thrown
    try {
      c.tableOperations().importDirectory(dir).to(tableName).tableTime(setTime)
          .ignoreEmptyDir(false).load();
    } catch (IllegalArgumentException ex) {
      // expected the exception
    }
    // or if not supplied at all, the IllegalArgument exception will be thrown as well
    try {
      c.tableOperations().importDirectory(dir).to(tableName).tableTime(setTime).load();
    } catch (IllegalArgumentException ex) {
      // expected the exception
    }

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

      var ctx = (ClientContext) client;

      var tablet = ctx.getAmple().readTablet(new KeyExtent(ctx.getTableId(tableName), null, null));
      assertEquals(new MetadataTime(0, TimeType.LOGICAL), tablet.getTime());

      var extent = new KeyExtent(ctx.getTableId(tableName), new Text("0333"), null);

      testSingleTabletSingleFile(client, false, true, () -> {
        // Want to test with and without a location, assuming the tablet does not have a location
        // now. Need to validate that assumption.
        assertNull(ctx.getAmple().readTablet(extent).getLocation());
        return null;
      });

      assertEquals(new MetadataTime(1, TimeType.LOGICAL),
          ctx.getAmple().readTablet(extent).getTime());

      int added = 0;
      try (var writer = client.createBatchWriter(tableName);
          var scanner = client.createScanner(tableName)) {
        for (var entry : scanner) {
          Mutation m = new Mutation(entry.getKey().getRow());
          m.at().family(entry.getKey().getColumnFamily())
              .qualifier(entry.getKey().getColumnFamily())
              .visibility(entry.getKey().getColumnVisibility())
              .put(Integer.parseInt(entry.getValue().toString()) * 10 + "");
          writer.addMutation(m);
          added++;
        }
      }

      // verify data written by batch writer overwrote bulk imported data
      try (var scanner = client.createScanner(tableName)) {
        assertEquals(2,
            scanner.stream().mapToLong(e -> e.getKey().getTimestamp()).min().orElse(-1));
        assertEquals(2 + added - 1,
            scanner.stream().mapToLong(e -> e.getKey().getTimestamp()).max().orElse(-1));
        scanner.forEach((k, v) -> {
          assertEquals(Integer.parseInt(k.getRow().toString()) * 10,
              Integer.parseInt(v.toString()));
        });
      }

      String dir = getDir("/testSetTime-");
      writeData(dir + "/f1.", aconf, 0, 332);

      // For this import tablet should be hosted so the bulk import operation will have to
      // coordinate getting time with the hosted tablet. The time should refect the batch writes
      // just done.
      client.tableOperations().importDirectory(dir).to(tableName).tableTime(true).load();

      // verify bulk imported data overwrote batch written data
      try (var scanner = client.createScanner(tableName)) {
        assertEquals(2 + added,
            scanner.stream().mapToLong(e -> e.getKey().getTimestamp()).min().orElse(-1));
        assertEquals(2 + added,
            scanner.stream().mapToLong(e -> e.getKey().getTimestamp()).max().orElse(-1));
        scanner.forEach((k, v) -> {
          assertEquals(Integer.parseInt(k.getRow().toString()), Integer.parseInt(v.toString()));
        });
      }

      client.tableOperations().flush(tableName, null, null, true);
      assertEquals(new MetadataTime(2 + added, TimeType.LOGICAL),
          ctx.getAmple().readTablet(extent).getTime());

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
      assertTrue(c instanceof ExecutionException, "Wrong exception: " + c);
      assertTrue(c.getCause() instanceof IllegalArgumentException,
          "Wrong exception: " + c.getCause());
      var msg = c.getCause().getMessage();
      assertTrue(msg.contains("bad-file.rf"), "Bad File not in exception: " + msg);

      // test max tablets hit using load plan on the server side
      c = assertThrows(AccumuloException.class, () -> testBulkFileMax(true));
      msg = c.getMessage();
      assertTrue(msg.contains("bad-file.rf"), "Bad File not in exception: " + msg);
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
        final var importMappingOptions = c.tableOperations().importDirectory(dir).to(tableName);
        var e = assertThrows(Exception.class, importMappingOptions::load);
        Throwable cause = e.getCause();
        assertTrue(cause instanceof FileNotFoundException
            || cause.getCause() instanceof FileNotFoundException);
      } finally {
        fs.setPermission(rFilePath, originalPerms);
      }

      originalPerms = fs.getFileStatus(new Path(dir)).getPermission();
      fs.setPermission(new Path(dir), FsPermission.valueOf("dr--r--r--"));
      try {
        final var importMappingOptions = c.tableOperations().importDirectory(dir).to(tableName);
        var ae = assertThrows(AccumuloException.class, importMappingOptions::load);
        assertTrue(ae.getCause() instanceof FileNotFoundException);
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

      final var importMappingOptions = c.tableOperations().importDirectory(dir).to(tableName);

      // Create a plan with more files than exists in dir
      LoadPlan loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, null, row(333))
          .loadFileTo("f2.rf", RangeType.TABLE, null, row(666))
          .loadFileTo("f3.rf", RangeType.TABLE, null, row(666)).build();
      final var tooManyFiles = importMappingOptions.plan(loadPlan);
      assertThrows(IllegalArgumentException.class, tooManyFiles::load);

      // Create a plan with fewer files than exists in dir
      loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, null, row(333)).build();
      final var tooFewFiles = importMappingOptions.plan(loadPlan);
      assertThrows(IllegalArgumentException.class, tooFewFiles::load);

      // Create a plan with tablet boundary that does not exist
      loadPlan = LoadPlan.builder().loadFileTo("f1.rf", RangeType.TABLE, null, row(555))
          .loadFileTo("f2.rf", RangeType.TABLE, null, row(555)).build();
      final var nonExistentBoundary = importMappingOptions.plan(loadPlan);
      assertThrows(AccumuloException.class, nonExistentBoundary::load);
    }
  }

  @Test
  public void testEmptyDir() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String dir = getDir("/testBulkFile-");
      FileSystem fs = getCluster().getFileSystem();
      fs.mkdirs(new Path(dir));
      assertThrows(IllegalArgumentException.class,
          () -> c.tableOperations().importDirectory(dir).to(tableName).load());
    }
  }

  // Test that the ignore option does not throw an exception if the import directory contains
  // no files.
  @Test
  public void testEmptyDirWithIgnoreOption() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String dir = getDir("/testBulkFile-");
      FileSystem fs = getCluster().getFileSystem();
      fs.mkdirs(new Path(dir));
      c.tableOperations().importDirectory(dir).to(tableName).ignoreEmptyDir(true).load();
    }
  }

  /*
   * This test imports a file where the first row of the file is equal to the last row of the first
   * tablet. There was a bug where this scenario would cause bulk import to hang forever.
   */
  @Test
  public void testEndOfFirstTablet() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String dir = getDir("/testBulkFile-");
      FileSystem fs = getCluster().getFileSystem();
      fs.mkdirs(new Path(dir));

      addSplits(c, tableName, "0333");

      var h1 = writeData(dir + "/f1.", aconf, 333, 333);

      c.tableOperations().importDirectory(dir).to(tableName).load();

      verifyData(c, tableName, 333, 333, false);

      Map<String,Set<String>> hashes = new HashMap<>();
      hashes.put("0333", Set.of(h1));
      hashes.put("null", Set.of());
      verifyMetadata(c, tableName, hashes);
    }
  }

  @Test
  public void testManyFiles() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      String dir = getDir("/testBulkFile-");
      FileSystem fs = getCluster().getFileSystem();
      fs.mkdirs(new Path(dir));

      addSplits(c, tableName, "5000");

      for (int i = 0; i < 100; i++) {
        writeData(dir + "/f" + i + ".", aconf, i * 100, (i + 1) * 100 - 1);
      }

      c.tableOperations().importDirectory(dir).to(tableName).load();

      verifyData(c, tableName, 0, 100 * 100 - 1, false);

      c.tableOperations().compact(tableName, new CompactionConfig().setWait(true));

      verifyData(c, tableName, 0, 100 * 100 - 1, false);
    }
  }

  @Test
  public void testConcurrentCompactions() throws Exception {
    // run test with bulk imports happening in parallel
    testConcurrentCompactions(true);
    // run the test with bulk imports happening serially
    testConcurrentCompactions(false);
  }

  private void testConcurrentCompactions(boolean parallelBulkImports) throws Exception {
    // Tests compactions running concurrently with bulk import to ensure that data is not bulk
    // imported twice. Doing a large number of bulk imports should naturally cause compactions to
    // happen. This test ensures that compactions running concurrently with bulk import does not
    // cause duplicate imports of a files. For example if a files is imported into a tablet and then
    // compacted away then the file should not be imported again by the FATE operation doing the
    // bulk import. The test is structured in such a way that duplicate imports would be detected.

    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {
      c.tableOperations().delete(tableName);
      // Create table without versioning iterator. This done to detect the same file being imported
      // more than once.
      c.tableOperations().create(tableName, new NewTableConfiguration().withoutDefaultIterators());

      addSplits(c, tableName, "0999 1999 2999 3999 4999 5999 6999 7999 8999");

      String dir = getDir("/testBulkFile-");

      final int N = 100;

      ExecutorService executor;
      if (parallelBulkImports) {
        executor = Executors.newFixedThreadPool(16);
      } else {
        // execute the bulk imports in the current thread which will cause them to run serially
        executor = MoreExecutors.newDirectExecutorService();
      }

      // Do N bulk imports of the exact same data.
      var futures = IntStream.range(0, N).mapToObj(i -> executor.submit(() -> {
        try {
          String iterationDir = dir + "/iteration" + i;
          // Create 10 files for the bulk import.
          for (int f = 0; f < 10; f++) {
            writeData(iterationDir + "/f" + f + ".", aconf, f * 1000, (f + 1) * 1000 - 1);
          }
          c.tableOperations().importDirectory(iterationDir).to(tableName).tableTime(true).load();
          getCluster().getFileSystem().delete(new Path(iterationDir), true);
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      })).collect(Collectors.toList());

      // wait for all bulk imports and check for errors in background threads
      for (var future : futures) {
        future.get();
      }

      executor.shutdown();

      try (var scanner = c.createScanner(tableName)) {
        // Count the number of times each row is seen.
        Map<String,Long> rowCounts = scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        var expectedRows = IntStream.range(0, 10000).mapToObj(i -> String.format("%04d", i))
            .collect(Collectors.toSet());
        assertEquals(expectedRows, rowCounts.keySet());
        // Each row should be duplicated once for each bulk import. If a file were imported twice,
        // then would see a higher count.
        assertTrue(rowCounts.values().stream().allMatch(l -> l == N));
      }

      // Its expected that compactions ran while the bulk imports were running. If no compactions
      // ran, then each tablet would have N files. Verify each tablet has less than N files.
      try (var scanner = c.createScanner("accumulo.metadata")) {
        scanner.setRange(MetadataSchema.TabletsSection
            .getRange(getCluster().getServerContext().getTableId(tableName)));
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
        // Get the count of files for each tablet.
        Map<String,Long> rowCounts = scanner.stream().map(e -> e.getKey().getRowData().toString())
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        assertTrue(rowCounts.values().stream().allMatch(l -> l < N));
        // expect to see 10 tablets
        assertEquals(10, rowCounts.size());
      }
    }
  }

  @Test
  public void testExceptionInMetadataUpdate() throws Exception {
    try (AccumuloClient c = Accumulo.newClient().from(getClientProps()).build()) {

      // after setting this up, bulk imports should fail
      setupBulkConstraint(getPrincipal(), c);

      String dir = getDir("/testExceptionInMetadataUpdate-");

      writeData(dir + "/f1.", aconf, 0, 333);

      // operation should fail with the constraint on the table
      assertThrows(AccumuloException.class,
          () -> c.tableOperations().importDirectory(dir).to(tableName).load());

      removeBulkConstraint(getPrincipal(), c);

      // should succeed after removing the constraint
      String h1 = writeData(dir + "/f1.", aconf, 0, 333);
      c.tableOperations().importDirectory(dir).to(tableName).load();

      // verifty the data was bulk imported
      verifyData(c, tableName, 0, 333, false);
      verifyMetadata(c, tableName, Map.of("null", Set.of(h1)));
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
    try (TabletsMetadata tablets = TabletsMetadata.builder(client).forTable(TableId.of(id))
        .fetch(FILES, LOADED, PREV_ROW).build()) {
      for (TabletMetadata tablet : tablets) {
        assertTrue(tablet.getLoaded().isEmpty());

        Set<String> fileHashes = tablet.getFiles().stream().map(f -> hash(f.getMetadataPath()))
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
        .forFile(UnreferencedTabletFile.of(fs, new Path(filename)), fs, fs.getConf(),
            NoCryptoServiceFactory.NONE)
        .withTableConfiguration(aconf).build()) {
      writer.startDefaultLocalityGroup();
      for (int i = s; i <= e; i++) {
        writer.append(new Key(new Text(row(i))), new Value(Integer.toString(i)));
      }
    }

    return hash(filename);
  }

  /**
   * This constraint is used to simulate an error in the metadata write for a bulk import.
   */
  public static class NoBulkConstratint implements Constraint {

    public static final String CANARY_VALUE = "a!p@a#c$h%e^&*()";
    public static final short CANARY_CODE = 31234;

    @Override
    public String getViolationDescription(short violationCode) {
      if (violationCode == 1) {
        return "Bulk import files are not allowed in this test";
      } else if (violationCode == CANARY_CODE) {
        return "Check used to see if constraint is active";
      }

      return null;
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {
      for (var colUpdate : mutation.getUpdates()) {
        var fam = new Text(colUpdate.getColumnFamily());
        if (fam.equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
          var stf = new StoredTabletFile(new String(colUpdate.getColumnQualifier(), UTF_8));
          if (stf.getFileName().startsWith("I")) {
            return List.of((short) 1);
          }
        }

        if (new String(colUpdate.getValue(), UTF_8).equals(CANARY_VALUE)) {
          return List.of(CANARY_CODE);
        }

      }

      return null;
    }
  }

  static void setupBulkConstraint(String principal, AccumuloClient c)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    // add a constraint to the metadata table that disallows bulk import files to be added
    c.securityOperations().grantTablePermission(principal, AccumuloTable.METADATA.tableName(),
        TablePermission.WRITE);
    c.securityOperations().grantTablePermission(principal, AccumuloTable.METADATA.tableName(),
        TablePermission.ALTER_TABLE);

    c.tableOperations().addConstraint(AccumuloTable.METADATA.tableName(),
        NoBulkConstratint.class.getName());

    var metaConstraints = new MetadataConstraints();
    SystemEnvironment env = EasyMock.createMock(SystemEnvironment.class);
    ServerContext context = EasyMock.createMock(ServerContext.class);
    EasyMock.expect(env.getServerContext()).andReturn(context);
    EasyMock.replay(env);

    // wait for the constraint to be active on the metadata table
    Wait.waitFor(() -> {
      try (var bw = c.createBatchWriter(AccumuloTable.METADATA.tableName())) {
        Mutation m = new Mutation("~garbage");
        m.put("", "", NoBulkConstratint.CANARY_VALUE);
        // This test assume the metadata constraint check will not flag this mutation, the following
        // validates this assumption.
        assertNull(metaConstraints.check(env, m));
        bw.addMutation(m);
        return false;
      } catch (MutationsRejectedException e) {
        return e.getConstraintViolationSummaries().stream()
            .anyMatch(cvs -> cvs.violationCode == NoBulkConstratint.CANARY_CODE);
      }
    });

    // delete the junk added to the metadata table
    try (var bw = c.createBatchWriter(AccumuloTable.METADATA.tableName())) {
      Mutation m = new Mutation("~garbage");
      m.putDelete("", "");
      bw.addMutation(m);
    }
  }

  static void removeBulkConstraint(String principal, AccumuloClient c)
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    int constraintNum = c.tableOperations().listConstraints(AccumuloTable.METADATA.tableName())
        .get(NoBulkConstratint.class.getName());
    c.tableOperations().removeConstraint(AccumuloTable.METADATA.tableName(), constraintNum);
    c.securityOperations().revokeTablePermission(principal, AccumuloTable.METADATA.tableName(),
        TablePermission.WRITE);
    c.securityOperations().revokeTablePermission(principal, AccumuloTable.METADATA.tableName(),
        TablePermission.ALTER_TABLE);
  }
}

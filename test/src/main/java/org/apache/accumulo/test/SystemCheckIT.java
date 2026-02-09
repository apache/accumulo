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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.fate.zookeeper.ZooUtil;
import org.apache.accumulo.core.lock.ServiceLockPaths;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.TServerInstance;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.metadata.schema.RootTabletMetadata;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.log.WalStateManager;
import org.apache.accumulo.server.util.adminCommand.SystemCheck;
import org.apache.accumulo.server.util.adminCommand.SystemCheck.Check;
import org.apache.accumulo.server.util.adminCommand.SystemCheck.CheckCommandOpts;
import org.apache.accumulo.server.util.adminCommand.SystemCheck.CheckStatus;
import org.apache.accumulo.server.util.checkCommand.CheckRunner;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

public class SystemCheckIT extends ConfigurableMacBase {
  private static final PrintStream ORIGINAL_OUT = System.out;

  @AfterEach
  public void assertCorrectPostTestState() {
    // ensure that the output after the test is System.out
    assertEquals(ORIGINAL_OUT, System.out);
  }

  /*
   * The following tests test the expected outputs and run order of the admin check command (e.g.,
   * are the correct checks run, dependencies run before the actual check, run in the correct order,
   * etc.) without actually testing the correct functionality of the checks
   */

  @Test
  public void testAdminCheckList() throws Exception {
    // verifies output of list command

    var p = getCluster().exec(SystemCheck.class, "list");
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();

    // Checks that the header is correct and that all checks are in the output
    assertTrue(
        out.contains("Check Name") && out.contains("Description") && out.contains("Depends on"));
    List<Check> checksSeen = new ArrayList<>();
    Arrays.stream(out.split("\\s+")).forEach(word -> {
      try {
        checksSeen.add(Check.valueOf(word));
      } catch (IllegalArgumentException e) {
        // skip
      }
    });
    assertTrue(checksSeen.containsAll(List.of(Check.values())));
  }

  @Test
  public void testAdminCheckListAndRunTogether() throws Exception {
    // Tries to execute list and run together; should not work

    var p = getCluster().exec(SystemCheck.class, "list", "run");
    assertNotEquals(0, p.getProcess().waitFor());
    p = getCluster().exec(SystemCheck.class, "run", "list");
    assertNotEquals(0, p.getProcess().waitFor());
    p = getCluster().exec(SystemCheck.class, "run", Check.SYSTEM_CONFIG.name(), "list");
    assertNotEquals(0, p.getProcess().waitFor());
    p = getCluster().exec(SystemCheck.class, "list", Check.SYSTEM_CONFIG.name(), "run");
    assertNotEquals(0, p.getProcess().waitFor());
  }

  @Test
  public void testAdminCheckListAndRunInvalidArgs() throws Exception {
    // tests providing invalid args to check

    // extra args to list
    var p = getCluster().exec(SystemCheck.class, "list", "abc");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("'list' does not expect any further arguments"));
    p = getCluster().exec(SystemCheck.class, "list", Check.SYSTEM_CONFIG.name());
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("'list' does not expect any further arguments"));
    p = getCluster().exec(SystemCheck.class, "list", "--name_pattern", "abc");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("'list' does not expect any further arguments"));
    // invalid check to run
    p = getCluster().exec(SystemCheck.class, "run", "123");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("IllegalArgumentException"));
    // no provided pattern
    p = getCluster().exec(SystemCheck.class, "run", "--name_pattern");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("ParameterException"));
    // no checks match pattern
    p = getCluster().exec(SystemCheck.class, "run", "--name_pattern", "abc");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("No checks matched the given pattern"));
    // invalid pattern
    p = getCluster().exec(SystemCheck.class, "run", "--name_pattern", "[abc");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("PatternSyntaxException"));
    // more than one arg provided to pattern
    p = getCluster().exec(SystemCheck.class, "run", "--name_pattern", ".*files", ".*files");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("Expected one argument (the regex pattern)"));
    // no list or run provided
    p = getCluster().exec(SystemCheck.class);
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("Must use either 'list' or 'run'"));
  }

  @Test
  public void testAdminCheckRunNoCheckFailures() throws Exception {
    // tests running the checks with none failing on run
    Check rootTableCheck = Check.ROOT_TABLE;
    Check systemFilesCheck = Check.SYSTEM_FILES;
    Check userFilesCheck = Check.USER_FILES;

    boolean[] allChecksPass = new boolean[Check.values().length];
    Arrays.fill(allChecksPass, true);

    // no checks specified: should run all
    String out1 = executeCheckCommand(new String[] {"run"}, allChecksPass);
    // all checks specified: should run all
    String[] allChecksArgs = new String[Check.values().length + 1];
    allChecksArgs[0] = "run";
    for (int i = 1; i < allChecksArgs.length; i++) {
      allChecksArgs[i] = Check.values()[i - 1].name();
    }
    String out2 = executeCheckCommand(allChecksArgs, allChecksPass);
    // this pattern: should run all
    String out3 =
        executeCheckCommand(new String[] {"run", "--name_pattern", "[A-Z]+_[A-Z]+"}, allChecksPass);
    // run subset of checks
    String out4 = executeCheckCommand(
        new String[] {"run", rootTableCheck.name(), systemFilesCheck.name(), userFilesCheck.name()},
        allChecksPass);
    // run same subset of checks but using a pattern to specify the checks (case shouldn't matter)
    String out5 = executeCheckCommand(new String[] {"run", "--name_pattern", "ROOT_TABLE|.*files"},
        allChecksPass);

    String expRunAllRunOrder =
        "Running dummy check SYSTEM_CONFIG\nDummy check SYSTEM_CONFIG completed with status OK\n"
            + "Running dummy check SERVER_CONFIG\nDummy check SERVER_CONFIG completed with status OK\n"
            + "Running dummy check TABLE_LOCKS\nDummy check TABLE_LOCKS completed with status OK\n"
            + "Running dummy check ROOT_METADATA\nDummy check ROOT_METADATA completed with status OK\n"
            + "Running dummy check ROOT_TABLE\nDummy check ROOT_TABLE completed with status OK\n"
            + "Running dummy check METADATA_TABLE\nDummy check METADATA_TABLE completed with status OK\n"
            + "Running dummy check SYSTEM_FILES\nDummy check SYSTEM_FILES completed with status OK\n"
            + "Running dummy check USER_FILES\nDummy check USER_FILES completed with status OK";
    String expRunSubRunOrder =
        "Running dummy check ROOT_TABLE\nDummy check ROOT_TABLE completed with status OK\n"
            + "Running dummy check SYSTEM_FILES\nDummy check SYSTEM_FILES completed with status OK\n"
            + "Running dummy check USER_FILES\nDummy check USER_FILES completed with status OK\n";
    // The dashes at the beginning and end of the string marks the begging and end of the
    // printed table allowing us to ensure the table only includes what is expected
    String expRunAllStatusInfo =
        "-SYSTEM_CONFIG|OKSERVER_CONFIG|OKTABLE_LOCKS|OKROOT_METADATA|OKROOT_TABLE|OK"
            + "METADATA_TABLE|OKSYSTEM_FILES|OKUSER_FILES|OK-";
    String expRunSubStatusInfo =
        "-SYSTEM_CONFIG|FILTERED_OUTSERVER_CONFIG|FILTERED_OUTTABLE_LOCKS|FILTERED_OUT"
            + "ROOT_METADATA|FILTERED_OUTROOT_TABLE|OKMETADATA_TABLE|FILTERED_OUT"
            + "SYSTEM_FILES|OKUSER_FILES|OK-";

    assertTrue(out1.contains(expRunAllRunOrder));
    assertTrue(out2.contains(expRunAllRunOrder));
    assertTrue(out3.contains(expRunAllRunOrder));
    assertTrue(out4.contains(expRunSubRunOrder));
    assertTrue(out5.contains(expRunSubRunOrder));

    assertNoOtherChecksRan(out4, true, rootTableCheck, systemFilesCheck, userFilesCheck);
    assertNoOtherChecksRan(out5, true, rootTableCheck, systemFilesCheck, userFilesCheck);
    // no need to check out1-3 above, those should run all

    out1 = out1.replaceAll("\\s+", "");
    out2 = out2.replaceAll("\\s+", "");
    out3 = out3.replaceAll("\\s+", "");
    out4 = out4.replaceAll("\\s+", "");
    out5 = out5.replaceAll("\\s+", "");

    assertTrue(out1.contains(expRunAllStatusInfo));
    assertTrue(out2.contains(expRunAllStatusInfo));
    assertTrue(out3.contains(expRunAllStatusInfo));
    assertTrue(out4.contains(expRunSubStatusInfo));
    assertTrue(out5.contains(expRunSubStatusInfo));
  }

  @Test
  public void testAdminCheckRunWithCheckFailures() throws Exception {
    // tests running checks with some failing

    boolean[] rootTableFails = new boolean[] {true, true, true, true, false, true, true, true};
    boolean[] systemConfigFails = new boolean[] {false, true, true, true, true, true, true, true};
    boolean[] userFilesAndMetadataTableFails =
        new boolean[] {true, true, true, true, true, false, true, false};

    // run all checks with ROOT_TABLE failing: only SYSTEM_CONFIG and ROOT_METADATA should pass
    // the rest should be filtered out as skipped due to dependency failure
    String out1 = executeCheckCommand(new String[] {"run"}, rootTableFails);
    // run all checks with SYSTEM_CONFIG failing: only SYSTEM_CONFIG should run and fail
    // the rest should be filtered out as skipped due to dependency failure
    String out2 = executeCheckCommand(new String[] {"run"}, systemConfigFails);
    // run subset of checks: SYSTEM_CONFIG, ROOT_TABLE, USER_FILES with USER_FILES and
    // METADATA_TABLE failing
    // should successfully run SYSTEM_CONFIG, ROOT_TABLE, fail to run USER_FILES and
    // filter out the rest
    String out3 =
        executeCheckCommand(new String[] {"run", "SYSTEM_CONFIG", "ROOT_TABLE", "USER_FILES"},
            userFilesAndMetadataTableFails);
    // run same subset but specified using pattern
    String out4 = executeCheckCommand(
        new String[] {"run", "--name_pattern", "SYSTEM_CONFIG|ROOT_TABLE|USER_FILES"},
        userFilesAndMetadataTableFails);

    String expRunOrder1 =
        "Running dummy check SYSTEM_CONFIG\nDummy check SYSTEM_CONFIG completed with status OK\n"
            + "Running dummy check SERVER_CONFIG\nDummy check SERVER_CONFIG completed with status OK\n"
            + "Running dummy check TABLE_LOCKS\nDummy check TABLE_LOCKS completed with status OK\n"
            + "Running dummy check ROOT_METADATA\nDummy check ROOT_METADATA completed with status OK\n"
            + "Running dummy check ROOT_TABLE\nDummy check ROOT_TABLE completed with status FAILED";
    String expRunOrder2 =
        "Running dummy check SYSTEM_CONFIG\nDummy check SYSTEM_CONFIG completed with status FAILED\n";
    String expRunOrder3And4 =
        "Running dummy check SYSTEM_CONFIG\nDummy check SYSTEM_CONFIG completed with status OK\n"
            + "Running dummy check ROOT_TABLE\nDummy check ROOT_TABLE completed with status OK\n"
            + "Running dummy check USER_FILES\nDummy check USER_FILES completed with status FAILED";

    assertTrue(out1.contains(expRunOrder1));
    assertTrue(out2.contains(expRunOrder2));
    assertTrue(out3.contains(expRunOrder3And4));
    assertTrue(out4.contains(expRunOrder3And4));

    assertNoOtherChecksRan(out1, true, Check.SYSTEM_CONFIG, Check.SERVER_CONFIG, Check.TABLE_LOCKS,
        Check.ROOT_TABLE, Check.ROOT_METADATA);
    assertNoOtherChecksRan(out2, true, Check.SYSTEM_CONFIG);
    assertNoOtherChecksRan(out3, true, Check.SYSTEM_CONFIG, Check.ROOT_TABLE, Check.USER_FILES);
    assertNoOtherChecksRan(out4, true, Check.SYSTEM_CONFIG, Check.ROOT_TABLE, Check.USER_FILES);

    out1 = out1.replaceAll("\\s+", "");
    out2 = out2.replaceAll("\\s+", "");
    out3 = out3.replaceAll("\\s+", "");
    out4 = out4.replaceAll("\\s+", "");

    String expStatusInfo1 = "-SYSTEM_CONFIG|OKSERVER_CONFIG|OKTABLE_LOCKS|OKROOT_METADATA|OK"
        + "ROOT_TABLE|FAILEDMETADATA_TABLE|SKIPPED_DEPENDENCY_FAILED"
        + "SYSTEM_FILES|SKIPPED_DEPENDENCY_FAILEDUSER_FILES|SKIPPED_DEPENDENCY_FAILED-";
    String expStatusInfo2 = "-SYSTEM_CONFIG|FAILEDSERVER_CONFIG|SKIPPED_DEPENDENCY_FAILED"
        + "TABLE_LOCKS|SKIPPED_DEPENDENCY_FAILEDROOT_METADATA|SKIPPED_DEPENDENCY_FAILED"
        + "ROOT_TABLE|SKIPPED_DEPENDENCY_FAILEDMETADATA_TABLE|SKIPPED_DEPENDENCY_FAILED"
        + "SYSTEM_FILES|SKIPPED_DEPENDENCY_FAILEDUSER_FILES|SKIPPED_DEPENDENCY_FAILED-";
    String expStatusInfo3And4 =
        "-SYSTEM_CONFIG|OKSERVER_CONFIG|FILTERED_OUTTABLE_LOCKS|FILTERED_OUT"
            + "ROOT_METADATA|FILTERED_OUTROOT_TABLE|OKMETADATA_TABLE|FILTERED_OUT"
            + "SYSTEM_FILES|FILTERED_OUTUSER_FILES|FAILED";

    assertTrue(out1.contains(expStatusInfo1));
    assertTrue(out2.contains(expStatusInfo2));
    assertTrue(out3.contains(expStatusInfo3And4));
    assertTrue(out4.contains(expStatusInfo3And4));
  }

  /*
   * The following tests test the expected functionality of the admin check command (e.g., are the
   * checks correctly identifying problems)
   */

  @Test
  public void testTableLocksCheck() throws Exception {
    String table = getUniqueNames(1)[0];
    Check tableLocksCheck = Check.TABLE_LOCKS;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.tableOperations().create(table);

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);
      client.tableOperations().flush(table, null, null, true);

      // the slow compaction is to ensure we hold a table lock when the check runs, so we have
      // locks to check
      IteratorSetting is = new IteratorSetting(1, SlowIterator.class);
      is.addOption("sleepTime", "10000");
      CompactionConfig slowCompaction = new CompactionConfig();
      slowCompaction.setWait(false);
      slowCompaction.setIterators(List.of(is));
      client.tableOperations().compact(table, slowCompaction);

      // test passing case
      var p = getCluster().exec(SystemCheck.class, "run", tableLocksCheck.name());
      assertEquals(0, p.getProcess().waitFor());
      String out = p.readStdOut();
      assertTrue(out.contains("locks are valid"));
      assertTrue(out.contains("Check TABLE_LOCKS completed with status OK"));
      assertNoOtherChecksRan(out, false, tableLocksCheck);

      // test a failing case
      // write an invalid table lock
      final var context = getCluster().getServerContext();
      final var zrw = context.getZooSession().asReaderWriter();
      final var path = new ServiceLockPaths(context.getZooCache()).createTableLocksPath();
      zrw.putPersistentData(path.toString() + "/foo", new byte[0], ZooUtil.NodeExistsPolicy.FAIL);
      p = getCluster().exec(SystemCheck.class, "run", tableLocksCheck.name());
      assertEquals(1, p.getProcess().waitFor());
      out = p.readStdOut();
      assertTrue(
          out.contains("Some table and namespace locks are INVALID (the table/namespace DNE)"));
      assertTrue(out.contains("Check TABLE_LOCKS completed with status FAILED"));
      assertNoOtherChecksRan(out, false, tableLocksCheck);
    }
  }

  @Test
  public void testMetadataTableCheck() throws Exception {
    Check metaTableCheck = Check.METADATA_TABLE;
    String table = getUniqueNames(1)[0];

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      client.tableOperations().create(table);

      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);
      client.tableOperations().flush(table, null, null, true);
    }

    // test passing case
    var p = getCluster().exec(SystemCheck.class, "run", metaTableCheck.name());
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(out.contains("Looking for offline tablets"));
    assertTrue(out.contains("Checking some references"));
    assertTrue(out.contains("Looking for missing columns"));
    assertTrue(out.contains("Looking for invalid columns"));
    assertTrue(out.contains("Check METADATA_TABLE completed with status OK"));
    assertNoOtherChecksRan(out, false, metaTableCheck);

    // test a failing case
    // delete a required column for the metadata of the table we created
    final var context = getCluster().getServerContext();
    final String tableId = context.tableOperations().tableIdMap().get(table);
    final String tablet = tableId + "<";
    try (var writer = context.createBatchWriter(SystemTables.METADATA.tableName())) {
      var mut = new Mutation(tablet);
      mut.putDelete(MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.getColumnFamily(),
          MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.getColumnQualifier());
      writer.addMutation(mut);
    }
    p = getCluster().exec(SystemCheck.class, "run", metaTableCheck.name());
    assertEquals(1, p.getProcess().waitFor());
    out = p.readStdOut();
    assertTrue(out.contains("Tablet " + tablet + " is missing required columns"));
    assertTrue(out.contains("Check METADATA_TABLE completed with status FAILED"));
    assertNoOtherChecksRan(out, false, metaTableCheck);
  }

  @Test
  public void testRootTableCheck() throws Exception {
    Check rootTableCheck = Check.ROOT_TABLE;

    // test passing case
    // no extra setup needed, just check the root table
    var p = getCluster().exec(SystemCheck.class, "run", rootTableCheck.name());
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(out.contains("Looking for offline tablets"));
    assertTrue(out.contains("Checking some references"));
    assertTrue(out.contains("Looking for missing columns"));
    assertTrue(out.contains("Looking for invalid columns"));
    assertTrue(out.contains("Check ROOT_TABLE completed with status OK"));
    assertNoOtherChecksRan(out, false, rootTableCheck);

    // test a failing case
    // delete a required column for the metadata of the metadata table
    final var context = getCluster().getServerContext();
    final String tableId = SystemTables.METADATA.tableId().canonical();
    final String tablet = tableId + "<";
    try (var writer = context.createBatchWriter(SystemTables.ROOT.tableName())) {
      var mut = new Mutation(tablet);
      mut.putDelete(MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.getColumnFamily(),
          MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.getColumnQualifier());
      writer.addMutation(mut);
    }
    p = getCluster().exec(SystemCheck.class, "run", rootTableCheck.name());
    assertEquals(1, p.getProcess().waitFor());
    out = p.readStdOut();
    assertTrue(out.contains("Tablet " + tablet + " is missing required columns"));
    assertTrue(out.contains("Check ROOT_TABLE completed with status FAILED"));
    assertNoOtherChecksRan(out, false, rootTableCheck);
  }

  @Test
  public void testRootMetadataCheck() throws Exception {
    Check rootMetaCheck = Check.ROOT_METADATA;

    // test passing case
    // no extra setup needed, just check the root table metadata
    var p = getCluster().exec(SystemCheck.class, "run", rootMetaCheck.name());
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(out.contains("Looking for offline tablets"));
    assertTrue(out.contains("Looking for missing columns"));
    assertTrue(out.contains("Looking for invalid columns"));
    assertTrue(out.contains("Check ROOT_METADATA completed with status OK"));
    assertNoOtherChecksRan(out, false, rootMetaCheck);

    // test a failing case
    // delete a required column for the metadata of the root tablet
    final var context = getCluster().getServerContext();
    final var zrw = context.getZooSession().asReaderWriter();
    var json = new String(zrw.getData(RootTable.ZROOT_TABLET), UTF_8);
    var rtm = new RootTabletMetadata(json);
    var tablet = rtm.toKeyValues().firstKey().getRow();
    var mut = new Mutation(tablet);
    mut.putDelete(MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.getColumnFamily(),
        MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN.getColumnQualifier());
    rtm.update(mut);
    zrw.putPersistentData(RootTable.ZROOT_TABLET, rtm.toJson().getBytes(UTF_8),
        ZooUtil.NodeExistsPolicy.OVERWRITE);

    p = getCluster().exec(SystemCheck.class, "run", rootMetaCheck.name());
    assertEquals(1, p.getProcess().waitFor());
    out = p.readStdOut();
    assertTrue(out.contains("Tablet " + tablet + " is missing required columns"));
    assertTrue(out.contains("Check ROOT_METADATA completed with status FAILED"));
    assertNoOtherChecksRan(out, false, rootMetaCheck);
  }

  @Test
  public void testSystemFilesCheck() throws Exception {
    Check sysFilesCheck = Check.SYSTEM_FILES;

    // test passing case
    // no extra setup needed, just run the check
    var p = getCluster().exec(SystemCheck.class, "run", sysFilesCheck.name());
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(Pattern.compile("missing files: 0, total files: [1-9]+").matcher(out).find());
    assertTrue(out.contains("Check SYSTEM_FILES completed with status OK"));
    assertNoOtherChecksRan(out, false, sysFilesCheck);

    // test a failing case
    // read the root table to find where the metadata table rfile is located in HDFS then delete it
    Path path;
    ServerContext context = getCluster().getServerContext();
    try (var scanner = context.createScanner(SystemTables.ROOT.tableName(), Authorizations.EMPTY)) {
      scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
      var pathJsonData = scanner.iterator().next().getKey().getColumnQualifier().toString();
      path = new Path(StoredTabletFile.of(pathJsonData).getMetadataPath());
      getCluster().getServerContext().getVolumeManager().delete(path);
    }
    p = getCluster().exec(SystemCheck.class, "run", sysFilesCheck.name());
    assertEquals(1, p.getProcess().waitFor());
    out = p.readStdOut();
    assertTrue(out.contains("File " + path + " is missing"));
    assertTrue(Pattern.compile("missing files: 1, total files: [1-9]+").matcher(out).find());
    assertTrue(out.contains("Check SYSTEM_FILES completed with status FAILED"));
    assertNoOtherChecksRan(out, false, sysFilesCheck);
  }

  @Test
  public void testUserFilesCheck() throws Exception {
    Check userFilesCheck = Check.USER_FILES;

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // test passing case
      // create a table, insert some data, and flush so there's a file to check
      String table = getUniqueNames(1)[0];
      client.tableOperations().create(table);
      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);
      client.tableOperations().flush(table, null, null, true);

      var p = getCluster().exec(SystemCheck.class, "run", userFilesCheck.name());
      assertEquals(0, p.getProcess().waitFor());
      String out = p.readStdOut();
      assertTrue(Pattern.compile("missing files: 0, total files: [1-9]+").matcher(out).find());
      assertTrue(out.contains("Check USER_FILES completed with status OK"));
      assertNoOtherChecksRan(out, false, userFilesCheck);

      // test a failing case
      // read the metadata for the table to find where the rfile is located in HDFS then delete it
      Path path;
      try (var scanner =
          client.createScanner(SystemTables.METADATA.tableName(), Authorizations.EMPTY)) {
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
        var pathJsonData = scanner.iterator().next().getKey().getColumnQualifier().toString();
        path = new Path(StoredTabletFile.of(pathJsonData).getMetadataPath());
        getCluster().getServerContext().getVolumeManager().delete(path);
      }
      p = getCluster().exec(SystemCheck.class, "run", userFilesCheck.name());
      assertEquals(1, p.getProcess().waitFor());
      out = p.readStdOut();
      assertTrue(out.contains("File " + path + " is missing"));
      assertTrue(Pattern.compile("missing files: 1, total files: [1-9]+").matcher(out).find());
      assertTrue(out.contains("Check USER_FILES completed with status FAILED"));
      assertNoOtherChecksRan(out, false, userFilesCheck);
    }
  }

  @Test
  public void testSystemConfigCheck() throws Exception {
    Check sysConfCheck = Check.SYSTEM_CONFIG;

    // test passing case
    var p = getCluster().exec(SystemCheck.class, "run", sysConfCheck.name());
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(out.contains("Checking ZooKeeper locks for Accumulo server processes"));
    assertTrue(out.contains("Checking ZooKeeper table nodes"));
    assertTrue(out.contains("Checking that WAL metadata in ZooKeeper is valid"));
    assertTrue(out.contains("Check SYSTEM_CONFIG completed with status OK"));
    assertNoOtherChecksRan(out, false, sysConfCheck);

    // test a failing case
    // delete the ZK data for the metadata table
    var context = getCluster().getServerContext();
    var zrw = context.getZooSession().asReaderWriter();
    zrw.recursiveDelete(Constants.ZTABLES + "/" + SystemTables.METADATA.tableId(),
        ZooUtil.NodeMissingPolicy.FAIL);

    p = getCluster().exec(SystemCheck.class, "run", sysConfCheck.name());
    assertEquals(1, p.getProcess().waitFor());
    out = p.readStdOut();
    assertTrue(out.contains("Failed to find table ("
        + (Map.entry(SystemTables.METADATA.tableName(), SystemTables.METADATA.tableId())) + ")"));
    assertTrue(out.contains("Check SYSTEM_CONFIG completed with status FAILED"));
    assertNoOtherChecksRan(out, false, sysConfCheck);
  }

  @Test
  public void testSystemConfigCheck2() throws Exception {
    // test a failing case
    // delete a WAL in HDFS that is referenced in ZK

    Check sysConfCheck = Check.SYSTEM_CONFIG;
    var context = getCluster().getServerContext();
    var zrw = context.getZooSession().asReaderWriter();
    var rootWalsDir = WalStateManager.ZWALS;

    // Need to ensure some form of mutation happens to a table so at least one WAL exists for us
    // to delete. This call creates some data in the metadata table
    getCluster().getServerContext().tableOperations().create(getUniqueNames(1)[0]);

    // need to find a TServer with a WAL, so we can delete the WAL from DFS
    String fullWalPathZk = null;
    TServerInstance tServerInstance = null;
    var tserversIter = zrw.getChildren(rootWalsDir).stream().iterator();
    outer: while (tserversIter.hasNext()) {
      var tserverInstanceStr = tserversIter.next();
      var walPaths = zrw.getChildren(rootWalsDir + "/" + tserverInstanceStr);
      if (!walPaths.isEmpty()) {
        var walPathsIter = walPaths.iterator();
        while (walPathsIter.hasNext()) {
          var walPath = walPathsIter.next();
          if (zrw.getData(rootWalsDir + "/" + tserverInstanceStr + "/" + walPath) != null) {
            fullWalPathZk = rootWalsDir + "/" + tserverInstanceStr + "/" + walPath;
            tServerInstance = new TServerInstance(tserverInstanceStr);
            break outer;
          }
        }
      }
    }
    assertNotNull(fullWalPathZk, "Could not find a WAL in ZK");
    var wal = WalStateManager.parse(zrw.getData(fullWalPathZk));

    // delete from HDFS
    context.getVolumeManager().delete(wal.getSecond());

    var p = getCluster().exec(SystemCheck.class, "run", sysConfCheck.name());
    assertEquals(1, p.getProcess().waitFor());
    var out = p.readStdOut();
    assertTrue(out.contains(
        "WAL metadata for tserver " + tServerInstance + " references a WAL that does not exist"));
    assertTrue(out.contains("Check SYSTEM_CONFIG completed with status FAILED"));
    assertNoOtherChecksRan(out, false, sysConfCheck);
  }

  @Test
  public void testServerConfigCheck() throws Exception {
    Check servConfCheck = Check.SERVER_CONFIG;

    // test passing case
    var p = getCluster().exec(SystemCheck.class, "run", servConfCheck.name());
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(out.contains("Checking server configuration"));
    assertTrue(out.contains("Checking that all configured properties are valid"));
    assertTrue(out.contains("Checking that all required config properties are present"));
    assertTrue(out.contains("Check SERVER_CONFIG completed with status OK"));
    assertNoOtherChecksRan(out, false, servConfCheck);

    // no simple way to test for a failure case
  }

  private String executeCheckCommand(String[] checkCmdArgs, boolean[] checksPass) throws Exception {
    String output;
    SystemCheck check = createDummyCheckCommand(checksPass);

    try (ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outStream)) {
      System.setOut(printStream);
      try {
        check.execute(checkCmdArgs);
      } catch (IllegalStateException e) {
        // This will happen if one of the commands fails.
        // The output is checked by the calling command,
        // so eat this exception.
      }
      output = outStream.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      System.setOut(ORIGINAL_OUT);
    }

    return output;
  }

  private SystemCheck createDummyCheckCommand(boolean[] checksPass) throws Exception {
    DummyCheckCommand opts = new DummyCheckCommand(checksPass);
    SystemCheck check = new SystemCheck(opts);
    return check;
  }

  /**
   * Asserts that no checks (other than those provided) ran.
   */
  private void assertNoOtherChecksRan(String out, boolean isDummyCheck, Check... checks) {
    Set<Check> otherChecks = Sets.difference(Set.of(Check.values()), Set.of(checks));
    for (var check : otherChecks) {
      assertFalse(
          out.contains("Running " + (isDummyCheck ? "dummy " : "") + "check " + check.name()));
    }
  }

  static abstract class DummyCheckRunner implements CheckRunner {
    private final boolean passes;

    public DummyCheckRunner(boolean passes) {
      this.passes = passes;
    }

    @Override
    public CheckStatus runCheck(ServerContext context, ServerUtilOpts opts, boolean fixFiles)
        throws Exception {
      CheckStatus status = passes ? CheckStatus.OK : CheckStatus.FAILED;

      System.out.println("Running dummy check " + getCheck());
      // no work to perform in the dummy check runner
      System.out.println("Dummy check " + getCheck() + " completed with status " + status);
      return status;
    }
  }

  static class DummySystemConfigCheckRunner extends DummyCheckRunner {
    public DummySystemConfigCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Check getCheck() {
      return Check.SYSTEM_CONFIG;
    }
  }

  static class DummyServerConfigCheckRunner extends DummyCheckRunner {
    public DummyServerConfigCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Check getCheck() {
      return Check.SERVER_CONFIG;
    }
  }

  static class DummyTableLocksCheckRunner extends DummyCheckRunner {
    public DummyTableLocksCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Check getCheck() {
      return Check.TABLE_LOCKS;
    }
  }

  static class DummyRootMetadataCheckRunner extends DummyCheckRunner {
    public DummyRootMetadataCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Check getCheck() {
      return Check.ROOT_METADATA;
    }
  }

  static class DummyRootTableCheckRunner extends DummyCheckRunner {
    public DummyRootTableCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Check getCheck() {
      return Check.ROOT_TABLE;
    }
  }

  static class DummyMetadataTableCheckRunner extends DummyCheckRunner {
    public DummyMetadataTableCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Check getCheck() {
      return Check.METADATA_TABLE;
    }
  }

  static class DummySystemFilesCheckRunner extends DummyCheckRunner {
    public DummySystemFilesCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Check getCheck() {
      return Check.SYSTEM_FILES;
    }
  }

  static class DummyUserFilesCheckRunner extends DummyCheckRunner {
    public DummyUserFilesCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Check getCheck() {
      return Check.USER_FILES;
    }
  }

  class DummyCheckCommand extends CheckCommandOpts {
    final Map<Check,Supplier<CheckRunner>> checkRunners;

    public DummyCheckCommand(boolean[] checksPass) {
      this.checkRunners = new TreeMap<>();
      this.checkRunners.put(Check.SYSTEM_CONFIG,
          () -> new DummySystemConfigCheckRunner(checksPass[0]));
      this.checkRunners.put(Check.SERVER_CONFIG,
          () -> new DummyServerConfigCheckRunner(checksPass[1]));
      this.checkRunners.put(Check.TABLE_LOCKS, () -> new DummyTableLocksCheckRunner(checksPass[2]));
      this.checkRunners.put(Check.ROOT_METADATA,
          () -> new DummyRootMetadataCheckRunner(checksPass[3]));
      this.checkRunners.put(Check.ROOT_TABLE, () -> new DummyRootTableCheckRunner(checksPass[4]));
      this.checkRunners.put(Check.METADATA_TABLE,
          () -> new DummyMetadataTableCheckRunner(checksPass[5]));
      this.checkRunners.put(Check.SYSTEM_FILES,
          () -> new DummySystemFilesCheckRunner(checksPass[6]));
      this.checkRunners.put(Check.USER_FILES, () -> new DummyUserFilesCheckRunner(checksPass[7]));
    }

    @Override
    public synchronized ServerContext getServerContext() {
      // Don't use the MiniAccumuloCluster's ServerContext
      // because ServerKeywordExecutable will close it
      // and cause errors during subsequent IT operations.
      return new ServerContext(getCluster().getServerContext().getSiteConfiguration());
    }

    @Override
    public CheckRunner getCheckRunner(Check check) {
      return checkRunners.get(check).get();
    }
  }
}

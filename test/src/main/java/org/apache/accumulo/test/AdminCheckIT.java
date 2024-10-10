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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.CompactionConfig;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.checkCommand.CheckRunner;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.apache.accumulo.test.functional.ReadWriteIT;
import org.apache.accumulo.test.functional.SlowIterator;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.beust.jcommander.JCommander;

public class AdminCheckIT extends ConfigurableMacBase {
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

    var p = getCluster().exec(Admin.class, "check", "list");
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();

    // Checks that the header is correct and that all checks are in the output
    assertTrue(
        out.contains("Check Name") && out.contains("Description") && out.contains("Depends on"));
    List<Admin.CheckCommand.Check> checksSeen = new ArrayList<>();
    Arrays.stream(out.split("\\s+")).forEach(word -> {
      try {
        checksSeen.add(Admin.CheckCommand.Check.valueOf(word));
      } catch (IllegalArgumentException e) {
        // skip
      }
    });
    assertTrue(checksSeen.containsAll(List.of(Admin.CheckCommand.Check.values())));
  }

  @Test
  public void testAdminCheckListAndRunTogether() throws Exception {
    // Tries to execute list and run together; should not work

    var p = getCluster().exec(Admin.class, "check", "list", "run");
    assertNotEquals(0, p.getProcess().waitFor());
    p = getCluster().exec(Admin.class, "check", "run", "list");
    assertNotEquals(0, p.getProcess().waitFor());
    p = getCluster().exec(Admin.class, "check", "run",
        Admin.CheckCommand.Check.SYSTEM_CONFIG.name(), "list");
    assertNotEquals(0, p.getProcess().waitFor());
    p = getCluster().exec(Admin.class, "check", "list",
        Admin.CheckCommand.Check.SYSTEM_CONFIG.name(), "run");
    assertNotEquals(0, p.getProcess().waitFor());
  }

  @Test
  public void testAdminCheckListAndRunInvalidArgs() throws Exception {
    // tests providing invalid args to check

    // extra args to list
    var p = getCluster().exec(Admin.class, "check", "list", "abc");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("'list' does not expect any further arguments"));
    p = getCluster().exec(Admin.class, "check", "list",
        Admin.CheckCommand.Check.SYSTEM_CONFIG.name());
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("'list' does not expect any further arguments"));
    p = getCluster().exec(Admin.class, "check", "list", "-p", "abc");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("'list' does not expect any further arguments"));
    // invalid check to run
    p = getCluster().exec(Admin.class, "check", "run", "123");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("IllegalArgumentException"));
    // no provided pattern
    p = getCluster().exec(Admin.class, "check", "run", "-p");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("ParameterException"));
    // no checks match pattern
    p = getCluster().exec(Admin.class, "check", "run", "-p", "abc");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("No checks matched the given pattern"));
    // invalid pattern
    p = getCluster().exec(Admin.class, "check", "run", "-p", "[abc");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("PatternSyntaxException"));
    // more than one arg provided to pattern
    p = getCluster().exec(Admin.class, "check", "run", "-p", ".*files", ".*files");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("Expected one argument (the regex pattern)"));
    // no list or run provided
    p = getCluster().exec(Admin.class, "check");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("Must use either 'list' or 'run'"));
  }

  @Test
  public void testAdminCheckRunNoCheckFailures() {
    // tests running the checks with none failing on run

    boolean[] allChecksPass = new boolean[Admin.CheckCommand.Check.values().length];
    Arrays.fill(allChecksPass, true);

    // no checks specified: should run all
    String out1 = executeCheckCommand(new String[] {"check", "run"}, allChecksPass);
    // all checks specified: should run all
    String[] allChecksArgs = new String[Admin.CheckCommand.Check.values().length + 2];
    allChecksArgs[0] = "check";
    allChecksArgs[1] = "run";
    for (int i = 2; i < allChecksArgs.length; i++) {
      allChecksArgs[i] = Admin.CheckCommand.Check.values()[i - 2].name();
    }
    String out2 = executeCheckCommand(allChecksArgs, allChecksPass);
    // this pattern: should run all
    String out3 =
        executeCheckCommand(new String[] {"check", "run", "-p", "[A-Z]+_[A-Z]+"}, allChecksPass);
    // run subset of checks
    String out4 = executeCheckCommand(
        new String[] {"check", "run", "ROOT_TABLE", "SYSTEM_FILES", "USER_FILES"}, allChecksPass);
    // run same subset of checks but using a pattern to specify the checks (case shouldn't matter)
    String out5 = executeCheckCommand(new String[] {"check", "run", "-p", "ROOT_TABLE|.*files"},
        allChecksPass);

    String expRunAllRunOrder =
        "Running dummy check SYSTEM_CONFIG\nDummy check SYSTEM_CONFIG completed with status OK\n"
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
    String expRunAllStatusInfo = "-SYSTEM_CONFIG|OKROOT_METADATA|OKROOT_TABLE|OK"
        + "METADATA_TABLE|OKSYSTEM_FILES|OKUSER_FILES|OK-";
    String expRunSubStatusInfo = "-SYSTEM_CONFIG|FILTERED_OUTROOT_METADATA|FILTERED_OUT"
        + "ROOT_TABLE|OKMETADATA_TABLE|FILTERED_OUTSYSTEM_FILES|OKUSER_FILES|OK-";

    assertTrue(out1.contains(expRunAllRunOrder));
    assertTrue(out2.contains(expRunAllRunOrder));
    assertTrue(out3.contains(expRunAllRunOrder));
    assertTrue(out4.contains(expRunSubRunOrder));
    assertTrue(out5.contains(expRunSubRunOrder));

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
  public void testAdminCheckRunWithCheckFailures() {
    // tests running checks with some failing

    boolean[] rootTableFails = new boolean[] {true, true, false, true, true, true};
    boolean[] systemConfigFails = new boolean[] {false, true, true, true, true, true};
    boolean[] userFilesAndMetadataTableFails = new boolean[] {true, true, true, false, true, false};

    // run all checks with ROOT_TABLE failing: only SYSTEM_CONFIG and ROOT_METADATA should pass
    // the rest should be filtered out as skipped due to dependency failure
    String out1 = executeCheckCommand(new String[] {"check", "run"}, rootTableFails);
    // run all checks with SYSTEM_CONFIG failing: only SYSTEM_CONFIG should run and fail
    // the rest should be filtered out as skipped due to dependency failure
    String out2 = executeCheckCommand(new String[] {"check", "run"}, systemConfigFails);
    // run subset of checks: SYSTEM_CONFIG, ROOT_TABLE, USER_FILES with USER_FILES and
    // METADATA_TABLE failing
    // should successfully run SYSTEM_CONFIG, ROOT_TABLE, fail to run USER_FILES and
    // filter out the rest
    String out3 = executeCheckCommand(
        new String[] {"check", "run", "SYSTEM_CONFIG", "ROOT_TABLE", "USER_FILES"},
        userFilesAndMetadataTableFails);
    // run same subset but specified using pattern
    String out4 = executeCheckCommand(
        new String[] {"check", "run", "-p", "SYSTEM_CONFIG|ROOT_TABLE|USER_FILES"},
        userFilesAndMetadataTableFails);

    String expRunOrder1 =
        "Running dummy check SYSTEM_CONFIG\nDummy check SYSTEM_CONFIG completed with status OK\n"
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

    out1 = out1.replaceAll("\\s+", "");
    out2 = out2.replaceAll("\\s+", "");
    out3 = out3.replaceAll("\\s+", "");
    out4 = out4.replaceAll("\\s+", "");

    String expStatusInfo1 = "-SYSTEM_CONFIG|OKROOT_METADATA|OKROOT_TABLE|FAILED"
        + "METADATA_TABLE|SKIPPED_DEPENDENCY_FAILEDSYSTEM_FILES|SKIPPED_DEPENDENCY_FAILED"
        + "USER_FILES|SKIPPED_DEPENDENCY_FAILED-";
    String expStatusInfo2 = "-SYSTEM_CONFIG|FAILEDROOT_METADATA|SKIPPED_DEPENDENCY_FAILED"
        + "ROOT_TABLE|SKIPPED_DEPENDENCY_FAILEDMETADATA_TABLE|SKIPPED_DEPENDENCY_FAILED"
        + "SYSTEM_FILES|SKIPPED_DEPENDENCY_FAILEDUSER_FILES|SKIPPED_DEPENDENCY_FAILED-";
    String expStatusInfo3And4 = "-SYSTEM_CONFIG|OKROOT_METADATA|FILTERED_OUTROOT_TABLE|OK"
        + "METADATA_TABLE|FILTERED_OUTSYSTEM_FILES|FILTERED_OUTUSER_FILES|FAILED";

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
  public void testSystemConfigCheck() throws Exception {
    String table = getUniqueNames(1)[0];

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

      var p = getCluster().exec(Admin.class, "check", "run", "system_config");
      assertEquals(0, p.getProcess().waitFor());
      String out = p.readStdOut();
      assertTrue(out.contains("locks are valid"));
      assertTrue(out.contains("Check SYSTEM_CONFIG completed with status OK"));
    }
  }

  @Test
  public void testPassingMetadataTableCheck() throws Exception {
    // Tests the METADATA_TABLE check in the case where all checks pass

    // no extra setup needed, just check the metadata table
    var p = getCluster().exec(Admin.class, "check", "run", "metadata_table");
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(out.contains("Looking for offline tablets"));
    assertTrue(out.contains("Checking some references"));
    assertTrue(out.contains("Looking for missing columns"));
    assertTrue(out.contains("Looking for invalid columns"));
    assertTrue(out.contains("Check METADATA_TABLE completed with status OK"));
  }

  @Test
  public void testPassingRootTableCheck() throws Exception {
    // Tests the ROOT_TABLE check in the case where all checks pass

    // no extra setup needed, just check the root table
    var p = getCluster().exec(Admin.class, "check", "run", "root_table");
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(out.contains("Looking for offline tablets"));
    assertTrue(out.contains("Checking some references"));
    assertTrue(out.contains("Looking for missing columns"));
    assertTrue(out.contains("Looking for invalid columns"));
    assertTrue(out.contains("Check ROOT_TABLE completed with status OK"));
  }

  @Test
  public void testPassingRootMetadataCheck() throws Exception {
    // Tests the ROOT_TABLE check in the case where all checks pass

    // no extra setup needed, just check the root table metadata
    var p = getCluster().exec(Admin.class, "check", "run", "root_metadata");
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(out.contains("Looking for offline tablets"));
    assertTrue(out.contains("Looking for missing columns"));
    assertTrue(out.contains("Looking for invalid columns"));
    assertTrue(out.contains("Check ROOT_METADATA completed with status OK"));
  }

  @Test
  public void testPassingSystemFilesCheck() throws Exception {
    // Tests the SYSTEM_FILES check in the case where it should pass

    // no extra setup needed, just run the check
    var p = getCluster().exec(Admin.class, "check", "run", "system_files");
    assertEquals(0, p.getProcess().waitFor());
    String out = p.readStdOut();
    assertTrue(Pattern.compile("missing files: 0, total files: [1-9]+").matcher(out).find());
  }

  @Test
  public void testPassingUserFilesCheck() throws Exception {
    // Tests the USER_FILES check in the case where it should pass

    try (AccumuloClient client = Accumulo.newClient().from(getClientProperties()).build()) {
      // create a table, insert some data, and flush so there's a file to check
      String table = getUniqueNames(1)[0];
      client.tableOperations().create(table);
      ReadWriteIT.ingest(client, 10, 10, 10, 0, table);
      client.tableOperations().flush(table, null, null, true);

      var p = getCluster().exec(Admin.class, "check", "run", "user_files");
      assertEquals(0, p.getProcess().waitFor());
      String out = p.readStdOut();
      assertTrue(Pattern.compile("missing files: 0, total files: [1-9]+").matcher(out).find());
    }
  }

  // TODO 4892 need failing tests...

  private String executeCheckCommand(String[] checkCmdArgs, boolean[] checksPass) {
    String output;
    Admin admin = createMockAdmin(checksPass);

    try (ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outStream)) {
      System.setOut(printStream);
      admin.execute(checkCmdArgs);
      output = outStream.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      EasyMock.verify(admin);
      System.setOut(ORIGINAL_OUT);
    }

    return output;
  }

  private Admin createMockAdmin(boolean[] checksPass) {
    // mocking admin.execute() to just execute "check" with our dummy check command
    Admin admin = EasyMock.createMock(Admin.class);
    admin.execute(EasyMock.anyObject(String[].class));
    EasyMock.expectLastCall().andAnswer((IAnswer<Void>) () -> {
      String[] args = EasyMock.getCurrentArgument(0);
      ServerUtilOpts opts = new ServerUtilOpts();
      JCommander cl = new JCommander(opts);
      Admin.CheckCommand dummyCheckCommand = new DummyCheckCommand(checksPass);
      cl.addCommand("check", dummyCheckCommand);
      cl.parse(args);
      Admin.executeCheckCommand(getServerContext(), dummyCheckCommand, opts);
      return null;
    });
    EasyMock.replay(admin);
    return admin;
  }

  static abstract class DummyCheckRunner implements CheckRunner {
    private final boolean passes;

    public DummyCheckRunner(boolean passes) {
      this.passes = passes;
    }

    @Override
    public Admin.CheckCommand.CheckStatus runCheck(ServerContext context, ServerUtilOpts opts,
        boolean fixFiles) throws Exception {
      Admin.CheckCommand.CheckStatus status =
          passes ? Admin.CheckCommand.CheckStatus.OK : Admin.CheckCommand.CheckStatus.FAILED;

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
    public Admin.CheckCommand.Check getCheck() {
      return Admin.CheckCommand.Check.SYSTEM_CONFIG;
    }
  }

  static class DummyRootMetadataCheckRunner extends DummyCheckRunner {
    public DummyRootMetadataCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Admin.CheckCommand.Check getCheck() {
      return Admin.CheckCommand.Check.ROOT_METADATA;
    }
  }

  static class DummyRootTableCheckRunner extends DummyCheckRunner {
    public DummyRootTableCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Admin.CheckCommand.Check getCheck() {
      return Admin.CheckCommand.Check.ROOT_TABLE;
    }
  }

  static class DummyMetadataTableCheckRunner extends DummyCheckRunner {
    public DummyMetadataTableCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Admin.CheckCommand.Check getCheck() {
      return Admin.CheckCommand.Check.METADATA_TABLE;
    }
  }

  static class DummySystemFilesCheckRunner extends DummyCheckRunner {
    public DummySystemFilesCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Admin.CheckCommand.Check getCheck() {
      return Admin.CheckCommand.Check.SYSTEM_FILES;
    }
  }

  static class DummyUserFilesCheckRunner extends DummyCheckRunner {
    public DummyUserFilesCheckRunner(boolean passes) {
      super(passes);
    }

    @Override
    public Admin.CheckCommand.Check getCheck() {
      return Admin.CheckCommand.Check.USER_FILES;
    }
  }

  static class DummyCheckCommand extends Admin.CheckCommand {
    final Map<Check,Supplier<CheckRunner>> checkRunners;

    public DummyCheckCommand(boolean[] checksPass) {
      this.checkRunners = new TreeMap<>();
      this.checkRunners.put(Check.SYSTEM_CONFIG,
          () -> new DummySystemConfigCheckRunner(checksPass[0]));
      this.checkRunners.put(Check.ROOT_METADATA,
          () -> new DummyRootMetadataCheckRunner(checksPass[1]));
      this.checkRunners.put(Check.ROOT_TABLE, () -> new DummyRootTableCheckRunner(checksPass[2]));
      this.checkRunners.put(Check.METADATA_TABLE,
          () -> new DummyMetadataTableCheckRunner(checksPass[3]));
      this.checkRunners.put(Check.SYSTEM_FILES,
          () -> new DummySystemFilesCheckRunner(checksPass[4]));
      this.checkRunners.put(Check.USER_FILES, () -> new DummyUserFilesCheckRunner(checksPass[5]));
    }

    @Override
    public CheckRunner getCheckRunner(Check check) {
      return checkRunners.get(check).get();
    }
  }
}

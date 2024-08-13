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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.Admin;
import org.apache.accumulo.server.util.checkCommand.CheckRunner;
import org.apache.accumulo.test.functional.ConfigurableMacBase;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.beust.jcommander.JCommander;

public class AdminCheckIT extends ConfigurableMacBase {

  private static final Map<Admin.CheckCommand.Check,Supplier<CheckRunner>> DUMMY_CHECK_RUNNERS =
      new EnumMap<>(Admin.CheckCommand.Check.class);
  private static final Map<Admin.CheckCommand.Check,Supplier<CheckRunner>> ORIGINAL_CHECK_RUNNERS =
      new EnumMap<>(getRealCheckRunners());
  private static final PrintStream ORIGINAL_OUT = System.out;

  @AfterEach
  public void assertCorrectPostTestState() {
    // ensure that changes to the real check runners are not kept after the test completes
    assertEquals(ORIGINAL_CHECK_RUNNERS, getRealCheckRunners());
    // ensure that the output after the test is System.out
    assertEquals(ORIGINAL_OUT, System.out);
  }

  /*
   * The following tests test the expected outputs and functionality of the admin check command
   * (e.g., are the correct checks run, dependencies run before the actual check, run in the correct
   * order, etc.) without actually testing the correct functionality of the checks
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
    // invalid arg to run
    p = getCluster().exec(Admin.class, "check", "run", "123");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("IllegalArgumentException"));
    // no provided pattern
    p = getCluster().exec(Admin.class, "check", "run", "-p");
    assertNotEquals(0, p.getProcess().waitFor());
    assertTrue(p.readStdOut().contains("Expected a regex pattern to be provided"));
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
  public void testAdminCheckRunAll() {
    // tests running all the checks

    boolean[] checksPass = new boolean[Admin.CheckCommand.Check.values().length];
    Arrays.fill(checksPass, true);

    // no checks specified: should run all
    String out1 = executeCheckCommand(new String[] {"check", "run"}, checksPass);

    // all checks specified: should run all
    String[] allChecksArgs = new String[Admin.CheckCommand.Check.values().length + 2];
    allChecksArgs[0] = "check";
    allChecksArgs[1] = "run";
    for (int i = 2; i < allChecksArgs.length; i++) {
      allChecksArgs[i] = Admin.CheckCommand.Check.values()[i - 2].name();
    }
    String out2 = executeCheckCommand(allChecksArgs, checksPass);

    // these two checks specified: should run all
    String out3 = executeCheckCommand(new String[] {"check", "run",
        Admin.CheckCommand.Check.SYSTEM_FILES.name(), Admin.CheckCommand.Check.USER_FILES.name()},
        checksPass);

    // the two checks ending in 'files': should run all
    String out4 = executeCheckCommand(new String[] {"check", "run", "-p", ".*files"}, checksPass);

    String expRunOrder =
        "Running dummy check SYSTEM_CONFIG\nDummy check SYSTEM_CONFIG completed with status OK\n"
            + "Running dummy check ROOT_METADATA\nDummy check ROOT_METADATA completed with status OK\n"
            + "Running dummy check ROOT_TABLE\nDummy check ROOT_TABLE completed with status OK\n"
            + "Running dummy check METADATA_TABLE\nDummy check METADATA_TABLE completed with status OK\n"
            + "Running dummy check SYSTEM_FILES\nDummy check SYSTEM_FILES completed with status OK\n"
            + "Running dummy check USER_FILES\nDummy check USER_FILES completed with status OK";
    // The dashes at the beginning and end of the string marks the begging and end of the
    // printed table allowing us to ensure the table only includes what is expected
    String expStatusInfo = "-SYSTEM_CONFIG|OKROOT_METADATA|OKROOT_TABLE|OK"
        + "METADATA_TABLE|OKSYSTEM_FILES|OKUSER_FILES|OK-";

    assertTrue(out1.contains(expRunOrder));
    assertTrue(out2.contains(expRunOrder));
    assertTrue(out3.contains(expRunOrder));
    assertTrue(out4.contains(expRunOrder));

    out1 = out1.replaceAll("\\s+", "");
    out2 = out2.replaceAll("\\s+", "");
    out3 = out3.replaceAll("\\s+", "");
    out4 = out4.replaceAll("\\s+", "");

    assertTrue(out1.contains(expStatusInfo));
    assertTrue(out2.contains(expStatusInfo));
    assertTrue(out3.contains(expStatusInfo));
    assertTrue(out4.contains(expStatusInfo));
  }

  @Test
  public void testAdminCheckRunWithFailingChecks() {
    // tests running checks with some failing

    boolean[] rootTableFails = new boolean[] {true, true, false, true, true, true};
    boolean[] systemConfigFails = new boolean[] {false, true, true, true, true, true};

    // run all checks with ROOT_TABLE failing: only SYSTEM_CONFIG and ROOT_METADATA should pass
    String out1 = executeCheckCommand(new String[] {"check", "run"}, rootTableFails);

    // run all checks with SYSTEM_CONFIG failing: only SYSTEM_CONFIG should run and fail
    String out2 = executeCheckCommand(new String[] {"check", "run"}, systemConfigFails);

    // run SYSTEM_FILES with ROOT_TABLE failing: only SYSTEM_CONFIG and ROOT_METADATA should pass
    String out3 = executeCheckCommand(
        new String[] {"check", "run", Admin.CheckCommand.Check.SYSTEM_FILES.name()},
        rootTableFails);

    String expRunOrder1 =
        "Running dummy check SYSTEM_CONFIG\nDummy check SYSTEM_CONFIG completed with status OK\n"
            + "Running dummy check ROOT_METADATA\nDummy check ROOT_METADATA completed with status OK\n"
            + "Running dummy check ROOT_TABLE\nDummy check ROOT_TABLE completed with status FAILED";
    String expRunOrder2 =
        "Running dummy check SYSTEM_CONFIG\nDummy check SYSTEM_CONFIG completed with status FAILED\n";
    String expRunOrder3 = expRunOrder1;

    assertTrue(out1.contains(expRunOrder1));
    assertTrue(out2.contains(expRunOrder2));
    assertTrue(out3.contains(expRunOrder3));

    out1 = out1.replaceAll("\\s+", "");
    out2 = out2.replaceAll("\\s+", "");
    out3 = out3.replaceAll("\\s+", "");

    String expStatusInfo1 = "-SYSTEM_CONFIG|OKROOT_METADATA|OKROOT_TABLE|FAILED"
        + "METADATA_TABLE|SKIPPED_DEPENDENCY_FAILEDSYSTEM_FILES|SKIPPED_DEPENDENCY_FAILED"
        + "USER_FILES|SKIPPED_DEPENDENCY_FAILED-";
    String expStatusInfo2 = "-SYSTEM_CONFIG|FAILEDROOT_METADATA|SKIPPED_DEPENDENCY_FAILED"
        + "ROOT_TABLE|SKIPPED_DEPENDENCY_FAILEDMETADATA_TABLE|SKIPPED_DEPENDENCY_FAILED"
        + "SYSTEM_FILES|SKIPPED_DEPENDENCY_FAILEDUSER_FILES|SKIPPED_DEPENDENCY_FAILED-";
    String expStatusInfo3 = "-SYSTEM_CONFIG|OKROOT_METADATA|OKROOT_TABLE|FAILED"
        + "SYSTEM_FILES|SKIPPED_DEPENDENCY_FAILED-";

    assertTrue(out1.contains(expStatusInfo1));
    assertTrue(out2.contains(expStatusInfo2));
    assertTrue(out3.contains(expStatusInfo3));
  }

  private String executeCheckCommand(String[] checkCmdArgs, boolean[] checksPass) {
    String output;

    try (ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(outStream)) {
      Admin admin = createMockAdmin();
      setCheckRunnersAsDummies(checksPass);
      System.setOut(printStream);
      admin.execute(checkCmdArgs);
      output = outStream.toString();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      resetCheckRunners();
      System.setOut(ORIGINAL_OUT);
    }

    return output;
  }

  private Admin createMockAdmin() {
    // mocking admin.execute() to just execute the check command; avoids issues with some
    // unneeded calls done in the real execute()
    Admin admin = EasyMock.createMock(Admin.class);
    admin.execute(EasyMock.anyObject(String[].class));
    EasyMock.expectLastCall().andAnswer((IAnswer<Void>) () -> {
      String[] args = EasyMock.getCurrentArgument(0);
      ServerUtilOpts opts = new ServerUtilOpts();
      JCommander cl = new JCommander(opts);
      Admin.CheckCommand checkCommand = new Admin.CheckCommand();
      cl.addCommand("check", checkCommand);
      cl.parse(args);
      Admin.executeCheckCommand(getServerContext(), checkCommand);
      return null;
    });
    EasyMock.replay(admin);
    return admin;
  }

  /**
   *
   * @return the real check runner map used in Admin
   */
  @SuppressWarnings("unchecked")
  private static Map<Admin.CheckCommand.Check,Supplier<CheckRunner>> getRealCheckRunners() {
    Class<?> clazz = Admin.CheckCommand.Check.class;
    Field checkRunners;
    Map<Admin.CheckCommand.Check,Supplier<CheckRunner>> realCheckRunners;
    try {
      checkRunners = clazz.getDeclaredField("CHECK_RUNNERS");
      checkRunners.setAccessible(true);
      realCheckRunners =
          (Map<Admin.CheckCommand.Check,Supplier<CheckRunner>>) checkRunners.get(null);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
    return realCheckRunners;
  }

  /**
   * Sets the admin check runners to the dummy check runners for testing. Must always later call
   * {@link AdminCheckIT#resetCheckRunners()}
   */
  private void setCheckRunnersAsDummies(boolean[] checksPass) {
    assertEquals(Admin.CheckCommand.Check.values().length, checksPass.length);

    DUMMY_CHECK_RUNNERS.put(Admin.CheckCommand.Check.SYSTEM_CONFIG,
        () -> new DummySystemConfigCheckRunner(checksPass[0]));
    DUMMY_CHECK_RUNNERS.put(Admin.CheckCommand.Check.ROOT_METADATA,
        () -> new DummyRootMetadataCheckRunner(checksPass[1]));
    DUMMY_CHECK_RUNNERS.put(Admin.CheckCommand.Check.ROOT_TABLE,
        () -> new DummyRootTableCheckRunner(checksPass[2]));
    DUMMY_CHECK_RUNNERS.put(Admin.CheckCommand.Check.METADATA_TABLE,
        () -> new DummyMetadataTableCheckRunner(checksPass[3]));
    DUMMY_CHECK_RUNNERS.put(Admin.CheckCommand.Check.SYSTEM_FILES,
        () -> new DummySystemFilesCheckRunner(checksPass[4]));
    DUMMY_CHECK_RUNNERS.put(Admin.CheckCommand.Check.USER_FILES,
        () -> new DummyUserFilesCheckRunner(checksPass[5]));

    getRealCheckRunners().clear();
    getRealCheckRunners().putAll(DUMMY_CHECK_RUNNERS);
  }

  /**
   * Resets the admin check runners to their original value. Must always call this after
   * {@link AdminCheckIT#setCheckRunnersAsDummies(boolean[])}
   */
  private void resetCheckRunners() {
    getRealCheckRunners().clear();
    getRealCheckRunners().putAll(ORIGINAL_CHECK_RUNNERS);
  }

  static abstract class DummyCheckRunner implements CheckRunner {
    private final boolean passes;

    public DummyCheckRunner(boolean passes) {
      this.passes = passes;
    }

    @Override
    public Admin.CheckCommand.CheckStatus runCheck() {
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
}

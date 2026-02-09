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
package org.apache.accumulo.server.util.adminCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.server.cli.ServerUtilOpts;
import org.apache.accumulo.server.util.ServerKeywordExecutable;
import org.apache.accumulo.server.util.adminCommand.CheckServer.CheckCommandOpts;
import org.apache.accumulo.server.util.checkCommand.CheckRunner;
import org.apache.accumulo.server.util.checkCommand.MetadataTableCheckRunner;
import org.apache.accumulo.server.util.checkCommand.RootMetadataCheckRunner;
import org.apache.accumulo.server.util.checkCommand.RootTableCheckRunner;
import org.apache.accumulo.server.util.checkCommand.ServerConfigCheckRunner;
import org.apache.accumulo.server.util.checkCommand.SystemConfigCheckRunner;
import org.apache.accumulo.server.util.checkCommand.SystemFilesCheckRunner;
import org.apache.accumulo.server.util.checkCommand.TableLocksCheckRunner;
import org.apache.accumulo.server.util.checkCommand.UserFilesCheckRunner;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;

@AutoService(KeywordExecutable.class)
public class CheckServer extends ServerKeywordExecutable<CheckCommandOpts> {

  // This only exists because it is called from ITs
  public static void main(String[] args) throws Exception {
    new CheckServer().execute(args);
  }

  public static class CheckCommandOpts extends ServerUtilOpts {
    @Parameter(names = "list",
        description = "Lists the different checks that can be run, the description of each check, and the other check(s) each check depends on.")
    boolean list;

    @Parameter(names = "run",
        description = "Runs the provided check(s) (explicit list or regex pattern specified following '-p'), beginning with their dependencies, or all checks if none are provided.")
    boolean run;

    @Parameter(names = {"-p", "--name_pattern"},
        description = "Runs all checks that match the provided regex pattern.")
    String pattern;

    @Parameter(description = "[<Check>...]")
    List<String> checks;

    @Parameter(names = "--fixFiles", description = "Removes dangling file pointers. Used by the "
        + "USER_FILES and SYSTEM_FILES checks.")
    boolean fixFiles = false;

    /**
     * This should be used to get the check runner instead of {@link Check#getCheckRunner()}. This
     * exists so that its functionality can be changed for testing.
     *
     * @return the interface for running a check
     */
    public CheckRunner getCheckRunner(Check check) {
      return check.getCheckRunner();
    }
  }

  public enum Check {
    // Caution should be taken when changing or adding any new checks: order is important
    SYSTEM_CONFIG(SystemConfigCheckRunner::new, "Validate the system config stored in ZooKeeper",
        Collections.emptyList()),
    SERVER_CONFIG(ServerConfigCheckRunner::new, "Validate the server configuration",
        Collections.singletonList(SYSTEM_CONFIG)),
    TABLE_LOCKS(TableLocksCheckRunner::new,
        "Ensures that table and namespace locks are valid and are associated with a FATE op",
        Collections.singletonList(SYSTEM_CONFIG)),
    ROOT_METADATA(RootMetadataCheckRunner::new,
        "Checks integrity of the root tablet metadata stored in ZooKeeper",
        Collections.singletonList(SYSTEM_CONFIG)),
    ROOT_TABLE(RootTableCheckRunner::new,
        "Scans all the tablet metadata stored in the root table and checks integrity",
        Collections.singletonList(ROOT_METADATA)),
    METADATA_TABLE(MetadataTableCheckRunner::new,
        "Scans all the tablet metadata stored in the metadata table and checks integrity",
        Collections.singletonList(ROOT_TABLE)),
    SYSTEM_FILES(SystemFilesCheckRunner::new,
        "Checks that files in system tablet metadata exist in DFS",
        Collections.singletonList(ROOT_TABLE)),
    USER_FILES(UserFilesCheckRunner::new, "Checks that files in user tablet metadata exist in DFS",
        Collections.singletonList(METADATA_TABLE));

    private final Supplier<CheckRunner> checkRunner;
    private final String description;
    private final List<Check> dependencies;

    Check(Supplier<CheckRunner> checkRunner, String description, List<Check> dependencies) {
      this.checkRunner = Objects.requireNonNull(checkRunner);
      this.description = Objects.requireNonNull(description);
      this.dependencies = Objects.requireNonNull(dependencies);
    }

    /**
     * This should not be called directly; use {@link CheckCommandOpts#getCheckRunner(Check)}
     * instead
     *
     *
     * @return the interface for running a check
     */
    public CheckRunner getCheckRunner() {
      return checkRunner.get();
    }

    /**
     * @return the description of the check
     */
    public String getDescription() {
      return description;
    }

    /**
     * @return the list of other checks the check depends on
     */
    public List<Check> getDependencies() {
      return dependencies;
    }
  }

  public enum CheckStatus {
    OK, FAILED, SKIPPED_DEPENDENCY_FAILED, FILTERED_OUT;
  }

  public CheckServer() {
    super(new CheckCommandOpts());
  }

  // Visible for testing
  public <T extends CheckCommandOpts> CheckServer(T testOpts) {
    super(testOpts);
  }

  @Override
  public String keyword() {
    return "check";
  }

  @Override
  public UsageGroup usageGroup() {
    return UsageGroup.ADMIN;
  }

  @Override
  public String description() {
    return "Performs checks for problems in Accumulo.";
  }

  @Override
  public void execute(JCommander cl, CheckCommandOpts options) throws Exception {

    validateAndTransformCheckCommand(options);

    if (options.list) {
      listChecks();
    } else if (options.run) {
      ServerContext context = options.getServerContext();
      var givenChecks = options.checks.stream().map(name -> Check.valueOf(name.toUpperCase()))
          .collect(Collectors.toList());
      executeRunCheckCommand(options, givenChecks, context);
    }
  }

  private void validateAndTransformCheckCommand(CheckCommandOpts cmd) {
    Preconditions.checkArgument(cmd.list != cmd.run, "Must use either 'list' or 'run'");
    if (cmd.list) {
      Preconditions.checkArgument(cmd.checks == null && cmd.pattern == null,
          "'list' does not expect any further arguments");
    } else if (cmd.pattern != null) {
      Preconditions.checkArgument(cmd.checks == null, "Expected one argument (the regex pattern)");
      List<String> matchingChecks = new ArrayList<>();
      var pattern = Pattern.compile(cmd.pattern.toUpperCase());
      for (Check check : Check.values()) {
        if (pattern.matcher(check.name()).matches()) {
          matchingChecks.add(check.name());
        }
      }
      Preconditions.checkArgument(!matchingChecks.isEmpty(),
          "No checks matched the given pattern: " + pattern.pattern());
      cmd.checks = matchingChecks;
    } else {
      if (cmd.checks == null) {
        cmd.checks =
            EnumSet.allOf(Check.class).stream().map(Enum::name).collect(Collectors.toList());
      }
    }
  }

  private void listChecks() {
    System.out.println();
    System.out.printf("%-20s | %-90s | %-20s%n", "Check Name", "Description", "Depends on");
    System.out.println("-".repeat(130));
    for (Check check : Check.values()) {
      System.out.printf("%-20s | %-90s | %-20s%n", check.name(), check.getDescription(),
          check.getDependencies().stream().map(Check::name).collect(Collectors.joining(", ")));
    }
    System.out.println("-".repeat(130));
    System.out.println();
  }

  private void executeRunCheckCommand(CheckCommandOpts cmd, List<Check> givenChecks,
      ServerContext context) throws Exception {
    // Get all the checks in the order they are declared in the enum
    final var allChecks = Check.values();
    final TreeMap<Check,CheckStatus> checkStatus = new TreeMap<>();

    for (Check check : allChecks) {
      if (depsFailed(check, checkStatus)) {
        checkStatus.put(check, CheckStatus.SKIPPED_DEPENDENCY_FAILED);
      } else {
        if (givenChecks.contains(check)) {
          checkStatus.put(check, cmd.getCheckRunner(check).runCheck(context, cmd, cmd.fixFiles));
        } else {
          checkStatus.put(check, CheckStatus.FILTERED_OUT);
        }
      }
    }

    printChecksResults(checkStatus);

    if (checkStatus.values().stream().anyMatch(status -> status == CheckStatus.FAILED)) {
      throw new IllegalStateException("One or more checks failed.");
    }
  }

  private boolean depsFailed(Check check, TreeMap<Check,CheckStatus> checkStatus) {
    return check.getDependencies().stream()
        .anyMatch(dep -> checkStatus.get(dep) == CheckStatus.FAILED
            || checkStatus.get(dep) == CheckStatus.SKIPPED_DEPENDENCY_FAILED);
  }

  private void printChecksResults(TreeMap<Check,CheckStatus> checkStatus) {
    System.out.println();
    System.out.printf("%-20s | %-20s%n", "Check Name", "Status");
    System.out.println("-".repeat(50));
    for (Map.Entry<Check,CheckStatus> entry : checkStatus.entrySet()) {
      System.out.printf("%-20s | %-20s%n", entry.getKey().name(), entry.getValue().name());
    }
    System.out.println("-".repeat(50));
    System.out.println();
  }

}

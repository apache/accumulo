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
package org.apache.accumulo.shell.commands;

import static org.apache.accumulo.core.util.Validators.NEW_TABLE_NAME;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.constraints.VisibilityConstraint;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iteratorsImpl.IteratorConfigUtil;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellUtil;
import org.apache.accumulo.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class CreateTableCommand extends Command {
  private Option createTableOptCopySplits;
  private Option createTableOptCopyConfig;
  private Option createTableOptSplit;
  private Option createTableOptTimeLogical;
  private Option createTableOptTimeMillis;
  private Option createTableNoDefaultIters;
  private Option createTableOptEVC;
  private Option base64Opt;
  private Option createTableOptFormatter;
  private Option createTableOptInitProp;
  private Option createTableOptLocalityProps;
  private Option createTableOptIteratorProps;
  private Option createTableOptOffline;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, TableExistsException,
      TableNotFoundException, IOException {

    final String tableName = cl.getArgs()[0];
    NewTableConfiguration ntc = new NewTableConfiguration();

    NEW_TABLE_NAME.validate(tableName);

    if (shellState.getAccumuloClient().tableOperations().exists(tableName)) {
      throw new TableExistsException(null, tableName, null);
    }

    final boolean decode = cl.hasOption(base64Opt.getOpt());

    // Prior to 2.0, if splits were provided at table creation the table was created separately
    // and then the addSplits method was called. Starting with 2.0, the splits will be
    // stored on the file system and created before the table is brought online.
    if (cl.hasOption(createTableOptSplit.getOpt())) {
      ntc = ntc.withSplits(new TreeSet<>(
          ShellUtil.scanFile(cl.getOptionValue(createTableOptSplit.getOpt()), decode)));
    } else if (cl.hasOption(createTableOptCopySplits.getOpt())) {
      final String oldTable = cl.getOptionValue(createTableOptCopySplits.getOpt());
      if (!shellState.getAccumuloClient().tableOperations().exists(oldTable)) {
        throw new TableNotFoundException(null, oldTable, null);
      }
      ntc = ntc.withSplits(
          new TreeSet<>(shellState.getAccumuloClient().tableOperations().listSplits(oldTable)));
    }

    if (cl.hasOption(createTableOptCopyConfig.getOpt())) {
      final String oldTable = cl.getOptionValue(createTableOptCopyConfig.getOpt());
      if (!shellState.getAccumuloClient().tableOperations().exists(oldTable)) {
        throw new TableNotFoundException(null, oldTable, null);
      }
    }

    TimeType timeType = TimeType.MILLIS;
    if (cl.hasOption(createTableOptTimeLogical.getOpt())) {
      timeType = TimeType.LOGICAL;
    }

    Map<String,String> props = ShellUtil.parseMapOpt(cl, createTableOptInitProp);

    // Set iterator if supplied
    if (cl.hasOption(createTableOptIteratorProps.getOpt())) {
      ntc = attachIteratorToNewTable(cl, shellState, ntc);
    }

    // Set up locality groups, if supplied
    if (cl.hasOption(createTableOptLocalityProps.getOpt())) {
      ntc = setLocalityForNewTable(cl, ntc);
    }

    // set offline table creation property
    if (cl.hasOption(createTableOptOffline.getOpt())) {
      ntc = ntc.createOffline();
    }

    // create table.
    shellState.getAccumuloClient().tableOperations().create(tableName,
        ntc.setTimeType(timeType).setProperties(props));

    shellState.setTableName(tableName); // switch shell to new table context

    if (cl.hasOption(createTableNoDefaultIters.getOpt())) {
      Set<String> initialProps = IteratorConfigUtil.generateInitialTableProperties(true).keySet();
      shellState.getAccumuloClient().tableOperations().modifyProperties(tableName,
          properties -> initialProps.forEach(properties::remove));
    }

    // Copy options if flag was set
    if (cl.hasOption(createTableOptCopyConfig.getOpt())) {
      if (shellState.getAccumuloClient().tableOperations().exists(tableName)) {
        final Map<String,String> configuration = shellState.getAccumuloClient().tableOperations()
            .getConfiguration(cl.getOptionValue(createTableOptCopyConfig.getOpt()));

        shellState.getAccumuloClient().tableOperations().modifyProperties(tableName,
            properties -> configuration.entrySet().stream()
                .filter(entry -> Property.isValidTablePropertyKey(entry.getKey()))
                .forEach(entry -> properties.put(entry.getKey(), entry.getValue())));
      }
    }

    if (cl.hasOption(createTableOptEVC.getOpt())) {
      try {
        shellState.getAccumuloClient().tableOperations().addConstraint(tableName,
            VisibilityConstraint.class.getName());
      } catch (AccumuloException e) {
        Shell.log.warn("{} while setting visibility constraint, but table was created",
            e.getMessage(), e);
      }
    }

    // Load custom formatter if set
    if (cl.hasOption(createTableOptFormatter.getOpt())) {
      final String formatterClass = cl.getOptionValue(createTableOptFormatter.getOpt());

      shellState.getAccumuloClient().tableOperations().setProperty(tableName,
          Property.TABLE_FORMATTER_CLASS.toString(), formatterClass);
    }
    return 0;
  }

  /**
   * Add supplied locality groups information to a NewTableConfiguration object.
   *
   * Used in conjunction with createtable shell command to allow locality groups to be configured
   * upon table creation.
   */
  private NewTableConfiguration setLocalityForNewTable(CommandLine cl, NewTableConfiguration ntc) {
    HashMap<String,Set<Text>> localityGroupMap = new HashMap<>();
    String[] options = cl.getOptionValues(createTableOptLocalityProps.getOpt());
    for (String localityInfo : options) {
      final String[] parts = localityInfo.split("=", 2);
      if (parts.length < 2) {
        throw new IllegalArgumentException("Missing '=' or there are spaces between entries");
      }
      final String groupName = parts[0];
      final HashSet<Text> colFams = new HashSet<>();
      for (String family : parts[1].split(",")) {
        colFams.add(new Text(family.getBytes(Shell.CHARSET)));
      }
      // check that group names are not duplicated on usage line
      if (localityGroupMap.put(groupName, colFams) != null) {
        throw new IllegalArgumentException(
            "Duplicate locality group name found. Group names must be unique");
      }
    }
    ntc.setLocalityGroups(localityGroupMap);
    return ntc;
  }

  /**
   * Add supplied iterator information to NewTableConfiguration object.
   *
   * Used in conjunction with createtable shell command to allow an iterator to be configured upon
   * table creation.
   */
  private NewTableConfiguration attachIteratorToNewTable(final CommandLine cl,
      final Shell shellState, NewTableConfiguration ntc) {
    EnumSet<IteratorScope> scopeEnumSet;
    IteratorSetting iteratorSetting;
    if (shellState.iteratorProfiles.isEmpty()) {
      throw new IllegalArgumentException("No shell iterator profiles have been created.");
    }
    String[] options = cl.getOptionValues(createTableOptIteratorProps.getOpt());
    for (String profileInfo : options) {
      String[] parts = profileInfo.split(":", 2);
      String profileName = parts[0];
      // The iteratorProfiles.get calls below will throw an NPE if the profile does not exist
      // This can occur when the profile actually does not exist or if there is
      // extraneous spacing in the iterator profile argument list causing the parser to read a scope
      // as a profile.
      try {
        iteratorSetting = shellState.iteratorProfiles.get(profileName).get(0);
      } catch (NullPointerException ex) {
        throw new IllegalArgumentException("invalid iterator argument. Either"
            + " profile does not exist or unexpected spaces in argument list.", ex);
      }
      // handle case where only the profile is supplied. Use all scopes by default if no scope args
      // are provided.
      if (parts.length == 1) {
        // add all scopes to enum set
        scopeEnumSet = EnumSet.allOf(IteratorScope.class);
      } else {
        // user provided scope arguments exist, parse them
        List<String> scopeArgs = Arrays.asList(parts[1].split(","));
        // there are only three allowable scope values
        if (scopeArgs.size() > 3) {
          throw new IllegalArgumentException("Too many scope arguments supplied");
        }
        // handle the 'all' argument separately since it is not an allowable enum value for
        // IteratorScope
        // if 'all' is used, it should be the only scope provided
        if (scopeArgs.contains("all")) {
          if (scopeArgs.size() > 1) {
            throw new IllegalArgumentException("Cannot use 'all' in conjunction with other scopes");
          }
          scopeEnumSet = EnumSet.allOf(IteratorScope.class);
        } else {
          // 'all' is not involved, examine the scope arguments and populate iterator scope EnumSet
          scopeEnumSet = validateScopes(scopeArgs);
        }
      }
      ntc.attachIterator(iteratorSetting, scopeEnumSet);
    }
    return ntc;
  }

  /**
   * Validate that the provided scope arguments are valid iterator scope settings. Checking for
   * duplicate entries and invalid scope values.
   *
   * Returns an EnumSet of scopes to be set.
   */
  private EnumSet<IteratorScope> validateScopes(final List<String> scopeList) {
    EnumSet<IteratorScope> scopes = EnumSet.noneOf(IteratorScope.class);
    for (String scopeStr : scopeList) {
      try {
        IteratorScope scope = IteratorScope.valueOf(scopeStr);
        if (!scopes.add(scope)) {
          throw new IllegalArgumentException("duplicate scope arguments found");
        }
      } catch (IllegalArgumentException ex) {
        throw new IllegalArgumentException("illegal scope arguments: " + ex.getMessage(), ex);
      }
    }
    return scopes;
  }

  @Override
  public String description() {
    return "creates a new table, with optional aggregators, iterators, locality"
        + " groups and optionally pre-split";
  }

  @Override
  public String usage() {
    return getName() + " <tableName>";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    createTableOptCopyConfig =
        new Option("cc", "copy-config", true, "table to copy configuration from");
    createTableOptCopySplits =
        new Option("cs", "copy-splits", true, "table to copy current splits from");
    createTableOptSplit = new Option("sf", "splits-file", true,
        "file with a newline-separated list of rows to split the table with");
    createTableOptTimeLogical = new Option("tl", "time-logical", false, "use logical time");
    createTableOptTimeMillis = new Option("tm", "time-millis", false, "use time in milliseconds");
    createTableNoDefaultIters = new Option("ndi", "no-default-iterators", false,
        "prevent creation of the normal default iterator set");
    createTableOptEVC = new Option("evc", "enable-visibility-constraint", false,
        "prevent users from writing data they cannot read. When enabling this,"
            + " consider disabling bulk import and alter table.");
    createTableOptFormatter = new Option("f", "formatter", true, "default formatter to set");
    createTableOptInitProp =
        new Option("prop", "init-properties", true, "user defined initial properties");
    createTableOptCopyConfig.setArgName("table");
    createTableOptCopySplits.setArgName("table");
    createTableOptSplit.setArgName("filename");
    createTableOptFormatter.setArgName("className");
    createTableOptInitProp.setArgName("properties");

    createTableOptLocalityProps =
        new Option("l", "locality", true, "create locality groups at table creation");
    createTableOptLocalityProps.setArgName("group=col_fam[,col_fam]");
    createTableOptLocalityProps.setArgs(Option.UNLIMITED_VALUES);

    createTableOptIteratorProps = new Option("i", "iter", true,
        "initialize iterator at table creation using profile. If no scope supplied, all"
            + " scopes are activated.");
    createTableOptIteratorProps.setArgName("profile[:[all]|[scan[,]][minc[,]][majc]]");
    createTableOptIteratorProps.setArgs(Option.UNLIMITED_VALUES);

    createTableOptOffline = new Option("o", "offline", false, "create table in offline mode");

    // Splits and CopySplits are put in an optionsgroup to make them
    // mutually exclusive
    final OptionGroup splitOrCopySplit = new OptionGroup();
    splitOrCopySplit.addOption(createTableOptSplit);
    splitOrCopySplit.addOption(createTableOptCopySplits);

    final OptionGroup timeGroup = new OptionGroup();
    timeGroup.addOption(createTableOptTimeLogical);
    timeGroup.addOption(createTableOptTimeMillis);

    base64Opt = new Option("b64", "base64encoded", false, "decode encoded split points");
    o.addOption(base64Opt);

    o.addOptionGroup(splitOrCopySplit);
    o.addOptionGroup(timeGroup);
    o.addOption(createTableOptSplit);
    o.addOption(createTableOptCopyConfig);
    o.addOption(createTableNoDefaultIters);
    o.addOption(createTableOptEVC);
    o.addOption(createTableOptFormatter);
    o.addOption(createTableOptInitProp);
    o.addOption(createTableOptLocalityProps);
    o.addOption(createTableOptIteratorProps);
    o.addOption(createTableOptOffline);

    return o;
  }

  @Override
  public int numArgs() {
    return 1;
  }

  @Override
  public void registerCompletion(final Token root,
      final Map<Command.CompletionSet,Set<String>> special) {
    registerCompletionForNamespaces(root, special);
  }
}

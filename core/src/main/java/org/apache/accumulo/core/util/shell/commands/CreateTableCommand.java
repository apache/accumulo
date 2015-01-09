/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.util.shell.commands;

import static com.google.common.base.Charsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.security.VisibilityConstraint;
import org.apache.accumulo.core.util.Base64;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Token;
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

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException,
      TableExistsException, TableNotFoundException, IOException, ClassNotFoundException {

    final String testTableName = cl.getArgs()[0];

    if (!testTableName.matches(Tables.VALID_NAME_REGEX)) {
      shellState.getReader().println("Only letters, numbers and underscores are allowed for use in table names.");
      throw new IllegalArgumentException();
    }

    final String tableName = cl.getArgs()[0];
    if (shellState.getConnector().tableOperations().exists(tableName)) {
      throw new TableExistsException(null, tableName, null);
    }
    final SortedSet<Text> partitions = new TreeSet<Text>();
    final boolean decode = cl.hasOption(base64Opt.getOpt());

    if (cl.hasOption(createTableOptSplit.getOpt())) {
      final String f = cl.getOptionValue(createTableOptSplit.getOpt());

      String line;
      Scanner file = new Scanner(new File(f), UTF_8.name());
      try {
        while (file.hasNextLine()) {
          line = file.nextLine();
          if (!line.isEmpty())
            partitions.add(decode ? new Text(Base64.decodeBase64(line.getBytes(UTF_8))) : new Text(line));
        }
      } finally {
        file.close();
      }
    } else if (cl.hasOption(createTableOptCopySplits.getOpt())) {
      final String oldTable = cl.getOptionValue(createTableOptCopySplits.getOpt());
      if (!shellState.getConnector().tableOperations().exists(oldTable)) {
        throw new TableNotFoundException(null, oldTable, null);
      }
      partitions.addAll(shellState.getConnector().tableOperations().listSplits(oldTable));
    }

    if (cl.hasOption(createTableOptCopyConfig.getOpt())) {
      final String oldTable = cl.getOptionValue(createTableOptCopyConfig.getOpt());
      if (!shellState.getConnector().tableOperations().exists(oldTable)) {
        throw new TableNotFoundException(null, oldTable, null);
      }
    }

    TimeType timeType = TimeType.MILLIS;
    if (cl.hasOption(createTableOptTimeLogical.getOpt())) {
      timeType = TimeType.LOGICAL;
    }

    // create table
    shellState.getConnector().tableOperations().create(tableName, true, timeType);
    if (partitions.size() > 0) {
      shellState.getConnector().tableOperations().addSplits(tableName, partitions);
    }

    shellState.setTableName(tableName); // switch shell to new table context

    if (cl.hasOption(createTableNoDefaultIters.getOpt())) {
      for (String key : IteratorUtil.generateInitialTableProperties(true).keySet()) {
        shellState.getConnector().tableOperations().removeProperty(tableName, key);
      }
    }

    // Copy options if flag was set
    if (cl.hasOption(createTableOptCopyConfig.getOpt())) {
      if (shellState.getConnector().tableOperations().exists(tableName)) {
        final Iterable<Entry<String,String>> configuration = shellState.getConnector().tableOperations()
            .getProperties(cl.getOptionValue(createTableOptCopyConfig.getOpt()));
        for (Entry<String,String> entry : configuration) {
          if (Property.isValidTablePropertyKey(entry.getKey())) {
            shellState.getConnector().tableOperations().setProperty(tableName, entry.getKey(), entry.getValue());
          }
        }
      }
    }

    if (cl.hasOption(createTableOptEVC.getOpt())) {
      try {
        shellState.getConnector().tableOperations().addConstraint(tableName, VisibilityConstraint.class.getName());
      } catch (AccumuloException e) {
        Shell.log.warn(e.getMessage() + " while setting visibility constraint, but table was created");
      }
    }

    // Load custom formatter if set
    if (cl.hasOption(createTableOptFormatter.getOpt())) {
      final String formatterClass = cl.getOptionValue(createTableOptFormatter.getOpt());

      shellState.getConnector().tableOperations().setProperty(tableName, Property.TABLE_FORMATTER_CLASS.toString(), formatterClass);
    }

    return 0;
  }

  @Override
  public String description() {
    return "creates a new table, with optional aggregators and optionally pre-split";
  }

  @Override
  public String usage() {
    return getName() + " <tableName>";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    createTableOptCopyConfig = new Option("cc", "copy-config", true, "table to copy configuration from");
    createTableOptCopySplits = new Option("cs", "copy-splits", true, "table to copy current splits from");
    createTableOptSplit = new Option("sf", "splits-file", true, "file with a newline-separated list of rows to split the table with");
    createTableOptTimeLogical = new Option("tl", "time-logical", false, "use logical time");
    createTableOptTimeMillis = new Option("tm", "time-millis", false, "use time in milliseconds");
    createTableNoDefaultIters = new Option("ndi", "no-default-iterators", false, "prevent creation of the normal default iterator set");
    createTableOptEVC = new Option("evc", "enable-visibility-constraint", false,
        "prevent users from writing data they cannot read.  When enabling this, consider disabling bulk import and alter table.");
    createTableOptFormatter = new Option("f", "formatter", true, "default formatter to set");

    createTableOptCopyConfig.setArgName("table");
    createTableOptCopySplits.setArgName("table");
    createTableOptSplit.setArgName("filename");
    createTableOptFormatter.setArgName("className");

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

    return o;
  }

  @Override
  public int numArgs() {
    return 1;
  }

  @Override
  public void registerCompletion(final Token root, final Map<Command.CompletionSet,Set<String>> special) {
    registerCompletionForNamespaces(root, special);
  }
}

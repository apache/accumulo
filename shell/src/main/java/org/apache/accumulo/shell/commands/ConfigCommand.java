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

import static org.apache.accumulo.core.client.security.SecurityErrorCode.PERMISSION_DENIED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.Shell.PrintFile;
import org.apache.accumulo.shell.ShellOptions;
import org.apache.accumulo.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.LineReader;

import com.google.common.collect.ImmutableSortedMap;

public class ConfigCommand extends Command {
  private Option tableOpt, deleteOpt, setOpt, filterOpt, filterWithValuesOpt, disablePaginationOpt,
      outputFileOpt, namespaceOpt;

  private int COL1 = 10, COL2 = 7;
  private LineReader reader;

  @Override
  public void registerCompletion(final Token root,
      final Map<Command.CompletionSet,Set<String>> completionSet) {
    final Token cmd = new Token(getName());
    final Token sub = new Token("-" + setOpt.getOpt());
    for (Property p : Property.values()) {
      if (!p.getKey().endsWith(".") && !p.isExperimental()) {
        sub.addSubcommand(new Token(p.toString()));
      }
    }
    cmd.addSubcommand(sub);
    root.addSubcommand(cmd);
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException,
      NamespaceNotFoundException {
    reader = shellState.getReader();

    final String tableName = cl.getOptionValue(tableOpt.getOpt());
    if (tableName != null && !shellState.getAccumuloClient().tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, null);
    }
    final String namespace = cl.getOptionValue(namespaceOpt.getOpt());
    if (namespace != null
        && !shellState.getAccumuloClient().namespaceOperations().exists(namespace)) {
      throw new NamespaceNotFoundException(null, namespace, null);
    }
    if (cl.hasOption(deleteOpt.getOpt())) {
      // delete property from table
      String property = cl.getOptionValue(deleteOpt.getOpt());
      if (property.contains("=")) {
        throw new BadArgumentException("Invalid '=' operator in delete operation.", fullCommand,
            fullCommand.indexOf('='));
      }
      String invalidTablePropFormatString =
          "Invalid per-table property : {}, still removing from zookeeper if it's there.";
      if (tableName != null) {
        if (!Property.isValidTablePropertyKey(property)) {
          Shell.log.warn(invalidTablePropFormatString, property);
        }
        shellState.getAccumuloClient().tableOperations().removeProperty(tableName, property);
        Shell.log.debug("Successfully deleted table configuration option.");
      } else if (namespace != null) {
        if (!Property.isValidTablePropertyKey(property)) {
          Shell.log.warn(invalidTablePropFormatString, property);
        }
        shellState.getAccumuloClient().namespaceOperations().removeProperty(namespace, property);
        Shell.log.debug("Successfully deleted namespace configuration option.");
      } else {
        if (!Property.isValidZooPropertyKey(property)) {
          Shell.log.warn(invalidTablePropFormatString, property);
        }
        shellState.getAccumuloClient().instanceOperations().removeProperty(property);
        Shell.log.debug("Successfully deleted system configuration option.");
      }
    } else if (cl.hasOption(setOpt.getOpt())) {
      // set property on table
      String property = cl.getOptionValue(setOpt.getOpt()), value = null;
      if (!property.contains("=")) {
        throw new BadArgumentException("Missing '=' operator in set operation.", fullCommand,
            fullCommand.indexOf(property));
      }
      final String[] pair = property.split("=", 2);
      property = pair[0];
      value = pair[1];

      if (tableName != null) {
        if (!Property.isValidTablePropertyKey(property)) {
          throw new BadArgumentException("Invalid per-table property.", fullCommand,
              fullCommand.indexOf(property));
        }
        if (property.equals(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey())) {
          new ColumnVisibility(value); // validate that it is a valid expression
        }
        shellState.getAccumuloClient().tableOperations().setProperty(tableName, property, value);
        Shell.log.debug("Successfully set table configuration option.");
      } else if (namespace != null) {
        if (!Property.isValidTablePropertyKey(property)) {
          throw new BadArgumentException("Invalid per-table property.", fullCommand,
              fullCommand.indexOf(property));
        }
        if (property.equals(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey())) {
          new ColumnVisibility(value); // validate that it is a valid expression
        }
        shellState.getAccumuloClient().namespaceOperations().setProperty(namespace, property,
            value);
        Shell.log.debug("Successfully set table configuration option.");
      } else {
        if (!Property.isValidZooPropertyKey(property)) {
          throw new BadArgumentException("Property cannot be modified in zookeeper", fullCommand,
              fullCommand.indexOf(property));
        }
        shellState.getAccumuloClient().instanceOperations().setProperty(property, value);
        Shell.log.debug("Successfully set system configuration option.");
      }
    } else {
      boolean warned = false;
      // display properties
      final TreeMap<String,String> systemConfig = new TreeMap<>();
      try {
        systemConfig
            .putAll(shellState.getAccumuloClient().instanceOperations().getSystemConfiguration());
      } catch (AccumuloSecurityException e) {
        if (e.getSecurityErrorCode() == PERMISSION_DENIED) {
          Shell.log.warn(
              "User unable to retrieve system configuration (requires System.SYSTEM permission)");
          warned = true;
        } else {
          throw e;
        }
      }

      final String outputFile = cl.getOptionValue(outputFileOpt.getOpt());
      final PrintFile printFile = outputFile == null ? null : new PrintFile(outputFile);

      final TreeMap<String,String> siteConfig = new TreeMap<>();
      try {
        siteConfig
            .putAll(shellState.getAccumuloClient().instanceOperations().getSiteConfiguration());
      } catch (AccumuloSecurityException e) {
        if (e.getSecurityErrorCode() == PERMISSION_DENIED) {
          Shell.log.warn(
              "User unable to retrieve site configuration (requires System.SYSTEM permission)");
          warned = true;
        } else {
          throw e;
        }
      }

      final TreeMap<String,String> defaults = new TreeMap<>();
      for (Entry<String,String> defaultEntry : DefaultConfiguration.getInstance()) {
        defaults.put(defaultEntry.getKey(), defaultEntry.getValue());
      }

      final TreeMap<String,String> namespaceConfig = new TreeMap<>();
      if (tableName != null) {
        String n = Namespaces.getNamespaceName(shellState.getContext(),
            shellState.getContext().getNamespaceId(shellState.getContext().getTableId(tableName)));
        try {
          shellState.getAccumuloClient().namespaceOperations().getConfiguration(n)
              .forEach(namespaceConfig::put);
        } catch (AccumuloSecurityException e) {
          if (e.getSecurityErrorCode() == PERMISSION_DENIED) {
            Shell.log.warn(
                "User unable to retrieve {} namespace configuration (requires Namespace.ALTER_NAMESPACE permission)",
                StringUtils.isEmpty(n) ? "default" : n);
            warned = true;
          } else {
            throw e;
          }
        }
      }

      Map<String,String> acuconf = systemConfig;
      if (acuconf.isEmpty()) {
        acuconf = defaults;
      }

      if (tableName != null) {
        if (warned) {
          Shell.log.warn(
              "User does not have permission to see entire configuration heirarchy. Property values shown below may be set above the table level.");
        }
        try {
          acuconf = shellState.getAccumuloClient().tableOperations().getConfiguration(tableName);
        } catch (AccumuloException e) {
          if (e.getCause() != null && e.getCause() instanceof AccumuloSecurityException) {
            AccumuloSecurityException ase = (AccumuloSecurityException) e.getCause();
            if (ase.getSecurityErrorCode() == PERMISSION_DENIED) {
              Shell.log.error(
                  "User unable to retrieve {} table configuration (requires Table.ALTER_TABLE permission)",
                  tableName);
            }
          }
          throw e;
        }
      } else if (namespace != null) {
        if (warned) {
          Shell.log.warn(
              "User does not have permission to see entire configuration heirarchy. Property values shown below may be set above the namespace level.");
        }
        try {
          acuconf =
              shellState.getAccumuloClient().namespaceOperations().getConfiguration(namespace);
        } catch (AccumuloSecurityException e) {
          Shell.log.error(
              "User unable to retrieve {} namespace configuration (requires Namespace.ALTER_NAMESPACE permission)",
              StringUtils.isEmpty(namespace) ? "default" : namespace);
          throw e;
        }
      }
      final Map<String,String> sortedConf = ImmutableSortedMap.copyOf(acuconf);

      for (Entry<String,String> propEntry : acuconf.entrySet()) {
        final String key = propEntry.getKey();
        final String value = propEntry.getValue();
        // only show properties which names or values
        // match the filter text

        if (matchTheFilterText(cl, key, value)) {
          continue;
        }
        if ((tableName != null || namespace != null) && !Property.isValidTablePropertyKey(key)) {
          continue;
        }
        COL2 = Math.max(COL2, propEntry.getKey().length() + 3);
      }

      final ArrayList<String> output = new ArrayList<>();
      printConfHeader(output);

      for (Entry<String,String> propEntry : sortedConf.entrySet()) {
        final String key = propEntry.getKey();
        final String value = propEntry.getValue();
        // only show properties which names or values
        // match the filter text

        if (matchTheFilterText(cl, key, value)) {
          continue;
        }
        if ((tableName != null || namespace != null) && !Property.isValidTablePropertyKey(key)) {
          continue;
        }
        String siteVal = siteConfig.get(key);
        String sysVal = systemConfig.get(key);
        String curVal = propEntry.getValue();
        String dfault = defaults.get(key);
        String nspVal = namespaceConfig.get(key);
        boolean printed = false;

        if (sysVal != null) {
          if (dfault != null && key.toLowerCase().contains("password")) {
            siteVal = sysVal = dfault = curVal = curVal.replaceAll(".", "*");
          }
          if (defaults.containsKey(key) && !Property.getPropertyByKey(key).isExperimental()) {
            printConfLine(output, "default", key, dfault);
            printed = true;
          }
          if (!defaults.containsKey(key) || !defaults.get(key).equals(siteVal)) {
            printConfLine(output, "site", printed ? "   @override" : key,
                siteVal == null ? "" : siteVal);
            printed = true;
          }
          if (!siteConfig.containsKey(key) || !siteVal.equals(sysVal)) {
            printConfLine(output, "system", printed ? "   @override" : key, sysVal);
            printed = true;
          }
        }
        if (nspVal != null) {
          // If the user can't see the system configuration, then print the default
          // configuration value if the current namespace value is different from it.
          if (sysVal == null && dfault != null && !dfault.equals(nspVal)
              && !Property.getPropertyByKey(key).isExperimental()) {
            printConfLine(output, "default", key, dfault);
            printed = true;
          }
          if (!systemConfig.containsKey(key) || !sysVal.equals(nspVal)) {
            printConfLine(output, "namespace", printed ? "   @override" : key, nspVal);
            printed = true;
          }
        }

        // show per-table value only if it is different (overridden)
        if (tableName != null && !curVal.equals(nspVal)) {
          // If the user can't see the system configuration, then print the default
          // configuration value if the current table value is different from it.
          if (nspVal == null && dfault != null && !dfault.equals(curVal)
              && !Property.getPropertyByKey(key).isExperimental()) {
            printConfLine(output, "default", key, dfault);
            printed = true;
          }
          printConfLine(output, "table", printed ? "   @override" : key, curVal);
        } else if (namespace != null && !curVal.equals(sysVal)) {
          // If the user can't see the system configuration, then print the default
          // configuration value if the current namespace value is different from it.
          if (sysVal == null && dfault != null && !dfault.equals(curVal)
              && !Property.getPropertyByKey(key).isExperimental()) {
            printConfLine(output, "default", key, dfault);
            printed = true;
          }
          printConfLine(output, "namespace", printed ? "   @override" : key, curVal);
        }
      }
      printConfFooter(output);
      shellState.printLines(output.iterator(), !cl.hasOption(disablePaginationOpt.getOpt()),
          printFile);
      if (printFile != null) {
        printFile.close();
      }
    }
    return 0;
  }

  private boolean matchTheFilterText(CommandLine cl, String key, String value) {
    if (cl.hasOption(filterOpt.getOpt()) && !key.contains(cl.getOptionValue(filterOpt.getOpt()))) {
      return true;
    }
    return cl.hasOption(filterWithValuesOpt.getOpt())
        && !(key.contains(cl.getOptionValue(filterWithValuesOpt.getOpt()))
            || value.contains(cl.getOptionValue(filterWithValuesOpt.getOpt())));
  }

  private void printConfHeader(List<String> output) {
    printConfFooter(output);
    output.add(String.format("%-" + COL1 + "s | %-" + COL2 + "s | %s", "SCOPE", "NAME", "VALUE"));
    printConfFooter(output);
  }

  private void printConfLine(List<String> output, String s1, String s2, String s3) {
    if (s2.length() < COL2) {
      s2 += " " + Shell.repeat(".", COL2 - s2.length() - 1);
    }
    output.add(String.format("%-" + COL1 + "s | %-" + COL2 + "s | %s", s1, s2, s3.replace("\n",
        "\n" + Shell.repeat(" ", COL1 + 1) + "|" + Shell.repeat(" ", COL2 + 2) + "| ")));
  }

  private void printConfFooter(List<String> output) {
    int col3 =
        Math.max(1, Math.min(Integer.MAX_VALUE, reader.getTerminal().getWidth() - COL1 - COL2 - 6));
    output.add(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%-" + col3 + "s",
        Shell.repeat("-", COL1), Shell.repeat("-", COL2), Shell.repeat("-", col3)));
  }

  @Override
  public String description() {
    return "prints system properties and table specific properties";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    final OptionGroup og = new OptionGroup();
    final OptionGroup tgroup = new OptionGroup();

    tableOpt = new Option(ShellOptions.tableOption, "table", true,
        "table to display/set/delete properties for");
    deleteOpt = new Option("d", "delete", true, "delete a per-table property");
    setOpt = new Option("s", "set", true, "set a per-table property");
    filterOpt = new Option("f", "filter", true,
        "show only properties that contain this string in their name.");
    filterWithValuesOpt = new Option("fv", "filter-with-values", true,
        "show only properties that contain this string in their name or value");
    disablePaginationOpt =
        new Option("np", "no-pagination", false, "disables pagination of output");
    outputFileOpt = new Option("o", "output", true, "local file to write the scan output to");
    namespaceOpt = new Option(ShellOptions.namespaceOption, "namespace", true,
        "namespace to display/set/delete properties for");

    tableOpt.setArgName("table");
    deleteOpt.setArgName("property");
    setOpt.setArgName("property=value");
    filterOpt.setArgName("string");
    filterWithValuesOpt.setArgName("string");
    outputFileOpt.setArgName("file");
    namespaceOpt.setArgName("namespace");

    og.addOption(deleteOpt);
    og.addOption(setOpt);
    og.addOption(filterOpt);
    og.addOption(filterWithValuesOpt);

    tgroup.addOption(tableOpt);
    tgroup.addOption(namespaceOpt);

    o.addOptionGroup(tgroup);
    o.addOptionGroup(og);
    o.addOption(disablePaginationOpt);
    o.addOption(outputFileOpt);

    return o;
  }

  @Override
  public int numArgs() {
    return 0;
  }
}

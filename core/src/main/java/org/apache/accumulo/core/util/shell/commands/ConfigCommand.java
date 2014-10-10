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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import jline.console.ConsoleReader;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Shell.PrintFile;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class ConfigCommand extends Command {
  private Option tableOpt, deleteOpt, setOpt, filterOpt, disablePaginationOpt, outputFileOpt, namespaceOpt;

  private int COL1 = 10, COL2 = 7;
  private ConsoleReader reader;

  @Override
  public void registerCompletion(final Token root, final Map<Command.CompletionSet,Set<String>> completionSet) {
    final Token cmd = new Token(getName());
    final Token sub = new Token("-" + setOpt.getOpt());
    for (Property p : Property.values()) {
      if (!(p.getKey().endsWith(".")) && !p.isExperimental()) {
        sub.addSubcommand(new Token(p.toString()));
      }
    }
    cmd.addSubcommand(sub);
    root.addSubcommand(cmd);
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, IOException, ClassNotFoundException, NamespaceNotFoundException {
    reader = shellState.getReader();

    final String tableName = cl.getOptionValue(tableOpt.getOpt());
    if (tableName != null && !shellState.getConnector().tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, null);
    }
    final String namespace = cl.getOptionValue(namespaceOpt.getOpt());
    if (namespace != null && !shellState.getConnector().namespaceOperations().exists(namespace)) {
      throw new NamespaceNotFoundException(null, namespace, null);
    }
    if (cl.hasOption(deleteOpt.getOpt())) {
      // delete property from table
      String property = cl.getOptionValue(deleteOpt.getOpt());
      if (property.contains("=")) {
        throw new BadArgumentException("Invalid '=' operator in delete operation.", fullCommand, fullCommand.indexOf('='));
      }
      if (tableName != null) {
        if (!Property.isValidTablePropertyKey(property)) {
          Shell.log.warn("Invalid per-table property : " + property + ", still removing from zookeeper if it's there.");
        }
        shellState.getConnector().tableOperations().removeProperty(tableName, property);
        Shell.log.debug("Successfully deleted table configuration option.");
      } else if (namespace != null) {
        if (!Property.isValidTablePropertyKey(property)) {
          Shell.log.warn("Invalid per-table property : " + property + ", still removing from zookeeper if it's there.");
        }
        shellState.getConnector().namespaceOperations().removeProperty(namespace, property);
        Shell.log.debug("Successfully deleted namespace configuration option.");
      } else {
        if (!Property.isValidZooPropertyKey(property)) {
          Shell.log.warn("Invalid per-table property : " + property + ", still removing from zookeeper if it's there.");
        }
        shellState.getConnector().instanceOperations().removeProperty(property);
        Shell.log.debug("Successfully deleted system configuration option");
      }
    } else if (cl.hasOption(setOpt.getOpt())) {
      // set property on table
      String property = cl.getOptionValue(setOpt.getOpt()), value = null;
      if (!property.contains("=")) {
        throw new BadArgumentException("Missing '=' operator in set operation.", fullCommand, fullCommand.indexOf(property));
      }
      final String pair[] = property.split("=", 2);
      property = pair[0];
      value = pair[1];

      if (tableName != null) {
        if (!Property.isValidTablePropertyKey(property)) {
          throw new BadArgumentException("Invalid per-table property.", fullCommand, fullCommand.indexOf(property));
        }
        if (property.equals(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey())) {
          new ColumnVisibility(value); // validate that it is a valid expression
        }
        shellState.getConnector().tableOperations().setProperty(tableName, property, value);
        Shell.log.debug("Successfully set table configuration option.");
      } else if (namespace != null) {
        if (!Property.isValidTablePropertyKey(property)) {
          throw new BadArgumentException("Invalid per-table property.", fullCommand, fullCommand.indexOf(property));
        }
        if (property.equals(Property.TABLE_DEFAULT_SCANTIME_VISIBILITY.getKey())) {
          new ColumnVisibility(value); // validate that it is a valid expression
        }
        shellState.getConnector().namespaceOperations().setProperty(namespace, property, value);
        Shell.log.debug("Successfully set table configuration option.");
      } else {
        if (!Property.isValidZooPropertyKey(property)) {
          throw new BadArgumentException("Property cannot be modified in zookeeper", fullCommand, fullCommand.indexOf(property));
        }
        shellState.getConnector().instanceOperations().setProperty(property, value);
        Shell.log.debug("Successfully set system configuration option");
      }
    } else {
      // display properties
      final TreeMap<String,String> systemConfig = new TreeMap<String,String>();
      systemConfig.putAll(shellState.getConnector().instanceOperations().getSystemConfiguration());

      final String outputFile = cl.getOptionValue(outputFileOpt.getOpt());
      final PrintFile printFile = outputFile == null ? null : new PrintFile(outputFile);

      final TreeMap<String,String> siteConfig = new TreeMap<String,String>();
      siteConfig.putAll(shellState.getConnector().instanceOperations().getSiteConfiguration());

      final TreeMap<String,String> defaults = new TreeMap<String,String>();
      for (Entry<String,String> defaultEntry : AccumuloConfiguration.getDefaultConfiguration()) {
        defaults.put(defaultEntry.getKey(), defaultEntry.getValue());
      }

      final TreeMap<String,String> namespaceConfig = new TreeMap<String,String>();
      if (tableName != null) {
        String n = Namespaces.getNamespaceName(shellState.getInstance(),
            Tables.getNamespaceId(shellState.getInstance(), Tables.getTableId(shellState.getInstance(), tableName)));
        for (Entry<String,String> e : shellState.getConnector().namespaceOperations().getProperties(n)) {
          namespaceConfig.put(e.getKey(), e.getValue());
        }
      }

      Iterable<Entry<String,String>> acuconf = shellState.getConnector().instanceOperations().getSystemConfiguration().entrySet();
      if (tableName != null) {
        acuconf = shellState.getConnector().tableOperations().getProperties(tableName);
      } else if (namespace != null) {
        acuconf = shellState.getConnector().namespaceOperations().getProperties(namespace);
      }
      final TreeMap<String,String> sortedConf = new TreeMap<String,String>();
      for (Entry<String,String> propEntry : acuconf) {
        sortedConf.put(propEntry.getKey(), propEntry.getValue());
      }

      for (Entry<String,String> propEntry : acuconf) {
        final String key = propEntry.getKey();
        // only show properties with similar names to that
        // specified, or all of them if none specified
        if (cl.hasOption(filterOpt.getOpt()) && !key.contains(cl.getOptionValue(filterOpt.getOpt()))) {
          continue;
        }
        if ((tableName != null || namespace != null) && !Property.isValidTablePropertyKey(key)) {
          continue;
        }
        COL2 = Math.max(COL2, propEntry.getKey().length() + 3);
      }

      final ArrayList<String> output = new ArrayList<String>();
      printConfHeader(output);

      for (Entry<String,String> propEntry : sortedConf.entrySet()) {
        final String key = propEntry.getKey();

        // only show properties with similar names to that
        // specified, or all of them if none specified
        if (cl.hasOption(filterOpt.getOpt()) && !key.contains(cl.getOptionValue(filterOpt.getOpt()))) {
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

        if (dfault != null && key.toLowerCase().contains("password")) {
          siteVal = sysVal = dfault = curVal = curVal.replaceAll(".", "*");
        }
        if (sysVal != null) {
          if (defaults.containsKey(key) && !Property.getPropertyByKey(key).isExperimental()) {
            printConfLine(output, "default", key, dfault);
            printed = true;
          }
          if (!defaults.containsKey(key) || !defaults.get(key).equals(siteVal)) {
            printConfLine(output, "site", printed ? "   @override" : key, siteVal == null ? "" : siteVal);
            printed = true;
          }
          if (!siteConfig.containsKey(key) || !siteVal.equals(sysVal)) {
            printConfLine(output, "system", printed ? "   @override" : key, sysVal);
            printed = true;
          }

        }
        if (nspVal != null) {
          if (!systemConfig.containsKey(key) || !sysVal.equals(nspVal)) {
            printConfLine(output, "namespace", printed ? "   @override" : key, nspVal);
            printed = true;
          }
        }

        // show per-table value only if it is different (overridden)
        if (tableName != null && !curVal.equals(nspVal)) {
          printConfLine(output, "table", printed ? "   @override" : key, curVal);
        } else if (namespace != null && !curVal.equals(sysVal)) {
          printConfLine(output, "namespace", printed ? "   @override" : key, curVal);
        }
      }
      printConfFooter(output);
      shellState.printLines(output.iterator(), !cl.hasOption(disablePaginationOpt.getOpt()), printFile);
      if (printFile != null) {
        printFile.close();
      }
    }
    return 0;
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
    output.add(String.format("%-" + COL1 + "s | %-" + COL2 + "s | %s", s1, s2,
        s3.replace("\n", "\n" + Shell.repeat(" ", COL1 + 1) + "|" + Shell.repeat(" ", COL2 + 2) + "|" + " ")));
  }

  private void printConfFooter(List<String> output) {
    int col3 = Math.max(1, Math.min(Integer.MAX_VALUE, reader.getTerminal().getWidth() - COL1 - COL2 - 6));
    output.add(String.format("%" + COL1 + "s-+-%" + COL2 + "s-+-%-" + col3 + "s", Shell.repeat("-", COL1), Shell.repeat("-", COL2), Shell.repeat("-", col3)));
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

    tableOpt = new Option(Shell.tableOption, "table", true, "table to display/set/delete properties for");
    deleteOpt = new Option("d", "delete", true, "delete a per-table property");
    setOpt = new Option("s", "set", true, "set a per-table property");
    filterOpt = new Option("f", "filter", true, "show only properties that contain this string");
    disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
    outputFileOpt = new Option("o", "output", true, "local file to write the scan output to");
    namespaceOpt = new Option(Shell.namespaceOption, "namespace", true, "namespace to display/set/delete properties for");

    tableOpt.setArgName("table");
    deleteOpt.setArgName("property");
    setOpt.setArgName("property=value");
    filterOpt.setArgName("string");
    outputFileOpt.setArgName("file");
    namespaceOpt.setArgName("namespace");

    og.addOption(deleteOpt);
    og.addOption(setOpt);
    og.addOption(filterOpt);

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

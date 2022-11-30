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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.OptionDescriber.IteratorOptions;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.AgeOffFilter;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.ReqVisFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.ShellCommandException;
import org.apache.accumulo.shell.ShellCommandException.ErrorCode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.jline.reader.LineReader;

public class SetIterCommand extends Command {

  private Option allScopeOpt, mincScopeOpt, majcScopeOpt, scanScopeOpt;
  Option profileOpt, priorityOpt, nameOpt;
  Option ageoffTypeOpt, regexTypeOpt, versionTypeOpt, reqvisTypeOpt, classnameTypeOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException,
      ShellCommandException {

    boolean tables =
        cl.hasOption(OptUtil.tableOpt().getOpt()) || !shellState.getTableName().isEmpty();
    boolean namespaces = cl.hasOption(OptUtil.namespaceOpt().getOpt());

    final int priority = Integer.parseInt(cl.getOptionValue(priorityOpt.getOpt()));

    final Map<String,String> options = new HashMap<>();
    String classname = cl.getOptionValue(classnameTypeOpt.getOpt());
    if (cl.hasOption(regexTypeOpt.getOpt())) {
      classname = RegExFilter.class.getName();
    } else if (cl.hasOption(ageoffTypeOpt.getOpt())) {
      classname = AgeOffFilter.class.getName();
    } else if (cl.hasOption(versionTypeOpt.getOpt())) {
      classname = VersioningIterator.class.getName();
    } else if (cl.hasOption(reqvisTypeOpt.getOpt())) {
      classname = ReqVisFilter.class.getName();
    }

    // ACCUMULO-4791: The SetIterCommand class as well as methods within the Shell.java class all
    // require that a table or namespace be provided or otherwise they will not execute. But the
    // setShellIter command does not require either of these values. In order to get around
    // this requirement we will check to see if a profile name has been provided (indicating that
    // we are setting a shell iterator). If so, temporarily set the table state to an
    // existing table such as accumulo.metadata. This allows the command to complete successfully.
    // After completion reassign the table to its original value and continue.
    String currentTableName = shellState.getTableName();
    String tmpTable = null;
    String configuredName;
    try {
      if (profileOpt != null && (currentTableName == null || currentTableName.isBlank())) {
        tmpTable = MetadataTable.NAME;
        shellState.setTableName(tmpTable);
        tables = cl.hasOption(OptUtil.tableOpt().getOpt()) || !currentTableName.isEmpty();
      }
      ClassLoader classloader = shellState.getClassLoader(cl, shellState);
      // Get the iterator options, with potentially a name provided by the OptionDescriber impl or
      // through user input
      configuredName = setUpOptions(classloader, shellState.getReader(), shellState.getWriter(),
          classname, options);
    } finally {
      // ACCUMULO-4792: reset table name and continue
      if (tmpTable != null) {
        shellState.setTableName(currentTableName);
      }
    }

    // Try to get the name provided by the setiter command
    String name = cl.getOptionValue(nameOpt.getOpt(), null);

    // Cannot continue if no name is provided
    if (name == null && configuredName == null) {
      throw new IllegalArgumentException("No provided or default name for iterator");
    } else if (name == null) {
      // Fall back to the name from OptionDescriber or user input if none is provided on setiter
      // option
      name = configuredName;
    }

    if (namespaces) {
      try {
        setNamespaceProperties(cl, shellState, priority, options, classname, name);
      } catch (NamespaceNotFoundException e) {
        throw new IllegalArgumentException(e);
      }
    } else if (tables) {
      setTableProperties(cl, shellState, priority, options, classname, name);
    } else {
      throw new IllegalArgumentException("No table or namespace specified");
    }
    return 0;
  }

  protected void setTableProperties(final CommandLine cl, final Shell shellState,
      final int priority, final Map<String,String> options, final String classname,
      final String name) throws AccumuloException, AccumuloSecurityException, ShellCommandException,
      TableNotFoundException {
    // remove empty values

    final String tableName = OptUtil.getTableOpt(cl, shellState);

    ScanCommand.ensureTserversCanLoadIterator(shellState, tableName, classname);

    options.values().removeIf(v -> v == null || v.isEmpty());

    final EnumSet<IteratorScope> scopes = EnumSet.noneOf(IteratorScope.class);
    if (cl.hasOption(allScopeOpt.getOpt()) || cl.hasOption(mincScopeOpt.getOpt())) {
      scopes.add(IteratorScope.minc);
    }
    if (cl.hasOption(allScopeOpt.getOpt()) || cl.hasOption(majcScopeOpt.getOpt())) {
      scopes.add(IteratorScope.majc);
    }
    if (cl.hasOption(allScopeOpt.getOpt()) || cl.hasOption(scanScopeOpt.getOpt())) {
      scopes.add(IteratorScope.scan);
    }
    if (scopes.isEmpty()) {
      throw new IllegalArgumentException("You must select at least one scope to configure");
    }
    final IteratorSetting setting = new IteratorSetting(priority, name, classname, options);
    shellState.getAccumuloClient().tableOperations().attachIterator(tableName, setting, scopes);
  }

  protected void setNamespaceProperties(final CommandLine cl, final Shell shellState,
      final int priority, final Map<String,String> options, final String classname,
      final String name) throws AccumuloException, AccumuloSecurityException, ShellCommandException,
      NamespaceNotFoundException {
    // remove empty values

    final String namespace = OptUtil.getNamespaceOpt(cl, shellState);

    if (!shellState.getAccumuloClient().namespaceOperations().testClassLoad(namespace, classname,
        SortedKeyValueIterator.class.getName())) {
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE,
          "Servers are unable to load " + classname + " as type "
              + SortedKeyValueIterator.class.getName());
    }

    options.values().removeIf(v -> v == null || v.isEmpty());

    final EnumSet<IteratorScope> scopes = EnumSet.noneOf(IteratorScope.class);
    if (cl.hasOption(allScopeOpt.getOpt()) || cl.hasOption(mincScopeOpt.getOpt())) {
      scopes.add(IteratorScope.minc);
    }
    if (cl.hasOption(allScopeOpt.getOpt()) || cl.hasOption(majcScopeOpt.getOpt())) {
      scopes.add(IteratorScope.majc);
    }
    if (cl.hasOption(allScopeOpt.getOpt()) || cl.hasOption(scanScopeOpt.getOpt())) {
      scopes.add(IteratorScope.scan);
    }
    if (scopes.isEmpty()) {
      throw new IllegalArgumentException("You must select at least one scope to configure");
    }
    final IteratorSetting setting = new IteratorSetting(priority, name, classname, options);
    shellState.getAccumuloClient().namespaceOperations().attachIterator(namespace, setting, scopes);
  }

  private static String setUpOptions(ClassLoader classloader, final LineReader reader,
      final PrintWriter writer, final String className, final Map<String,String> options)
      throws IOException, ShellCommandException {
    String input;
    SortedKeyValueIterator<Key,Value> skvi;
    String clazzName;
    try {
      @SuppressWarnings("unchecked")
      var clazz = (Class<? extends SortedKeyValueIterator<Key,Value>>) classloader
          .loadClass(className).asSubclass(SortedKeyValueIterator.class);
      clazzName = clazz.getName();
      skvi = clazz.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      StringBuilder msg = new StringBuilder("Unable to load ").append(className);
      if (className.indexOf('.') < 0) {
        msg.append("; did you use a fully qualified package name?");
      } else {
        msg.append("; class not found.");
      }
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, msg.toString());
    } catch (ClassCastException e) {
      String msg = className + " loaded successfully but does not implement SortedKeyValueIterator."
          + " This class cannot be used with this command.";
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, msg);
    }

    OptionDescriber iterOptions = null;
    if (OptionDescriber.class.isAssignableFrom(skvi.getClass())) {
      iterOptions = (OptionDescriber) skvi;
    }

    String iteratorName;
    if (iterOptions != null) {
      final IteratorOptions itopts = iterOptions.describeOptions();
      iteratorName = itopts.getName();

      if (iteratorName == null) {
        throw new IllegalArgumentException(
            className + " described its default distinguishing name as null");
      }
      String shortClassName = className;
      if (className.contains(".")) {
        shortClassName = className.substring(className.lastIndexOf('.') + 1);
      }
      final Map<String,String> localOptions = new HashMap<>();
      do {
        // clean up the overall options that caused things to fail
        for (String key : localOptions.keySet()) {
          options.remove(key);
        }
        localOptions.clear();

        writer.println(itopts.getDescription());

        String prompt;
        if (itopts.getNamedOptions() != null) {
          for (Entry<String,String> e : itopts.getNamedOptions().entrySet()) {
            prompt = Shell.repeat("-", 10) + "> set " + shortClassName + " parameter " + e.getKey()
                + ", " + e.getValue() + ": ";
            writer.flush();
            input = reader.readLine(prompt);

            if (input == null) {
              writer.println();
              throw new IOException("Input stream closed");
            }
            // Places all Parameters and Values into the LocalOptions, even if the value is "".
            // This allows us to check for "" values when setting the iterators and allows us to
            // remove
            // the parameter and value from the table property.
            localOptions.put(e.getKey(), input);
          }
        }

        if (itopts.getUnnamedOptionDescriptions() != null) {
          for (String desc : itopts.getUnnamedOptionDescriptions()) {
            writer.println(Shell.repeat("-", 10) + "> entering options: " + desc);
            input = "start";
            prompt = Shell.repeat("-", 10) + "> set " + shortClassName
                + " option (<name> <value>, hit enter to skip): ";
            while (true) {
              writer.flush();
              input = reader.readLine(prompt);
              if (input == null) {
                writer.println();
                throw new IOException("Input stream closed");
              } else {
                input = new String(input);
              }

              if (input.isEmpty()) {
                break;
              }

              String[] sa = input.split(" ", 2);
              localOptions.put(sa[0], sa[1]);
            }
          }
        }

        options.putAll(localOptions);
        if (!iterOptions.validateOptions(options)) {
          writer.println("invalid options for " + clazzName);
        }

      } while (!iterOptions.validateOptions(options));
    } else {
      writer.flush();
      writer.println("The iterator class does not implement OptionDescriber."
          + " Consider this for better iterator configuration using this setiter command.");
      iteratorName = reader.readLine("Name for iterator (enter to skip): ");
      if (iteratorName == null) {
        writer.println();
        throw new IOException("Input stream closed");
      } else if (iteratorName.isBlank()) {
        // Treat whitespace or empty string as no name provided
        iteratorName = null;
      }

      writer.flush();
      writer.println("Optional, configure name-value options for iterator:");
      String prompt = Shell.repeat("-", 10) + "> set option (<name> <value>, hit enter to skip): ";
      final HashMap<String,String> localOptions = new HashMap<>();

      while (true) {
        writer.flush();
        input = reader.readLine(prompt);
        if (input == null) {
          writer.println();
          throw new IOException("Input stream closed");
        } else if (input.isBlank()) {
          break;
        }

        String[] sa = input.split(" ", 2);
        localOptions.put(sa[0], sa[1]);
      }

      options.putAll(localOptions);
    }

    return iteratorName;
  }

  @Override
  public String description() {
    return "sets a table-specific or namespace-specific iterator";
  }

  // Set all options common to both iterators and shell iterators
  protected void setBaseOptions(Options options) {
    setPriorityOptions(options);
    setNameOptions(options);
    setIteratorTypeOptions(options);
  }

  private void setNameOptions(Options options) {
    nameOpt = new Option("n", "name", true, "iterator to set");
    nameOpt.setArgName("itername");
    options.addOption(nameOpt);
  }

  private void setPriorityOptions(Options options) {
    priorityOpt = new Option("p", "priority", true, "the order in which the iterator is applied");
    priorityOpt.setArgName("pri");
    priorityOpt.setRequired(true);
    options.addOption(priorityOpt);
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    setBaseOptions(o);
    setScopeOptions(o);
    setTableOptions(o);
    return o;
  }

  private void setScopeOptions(Options o) {
    allScopeOpt =
        new Option("all", "all-scopes", false, "applied at scan time, minor and major compactions");
    mincScopeOpt = new Option(IteratorScope.minc.name(), "minor-compaction", false,
        "applied at minor compaction");
    majcScopeOpt = new Option(IteratorScope.majc.name(), "major-compaction", false,
        "applied at major compaction");
    scanScopeOpt =
        new Option(IteratorScope.scan.name(), "scan-time", false, "applied at scan time");
    o.addOption(allScopeOpt);
    o.addOption(mincScopeOpt);
    o.addOption(majcScopeOpt);
    o.addOption(scanScopeOpt);
  }

  private void setTableOptions(Options o) {
    final OptionGroup tableGroup = new OptionGroup();
    tableGroup.addOption(OptUtil.tableOpt("table to configure iterators on"));
    tableGroup.addOption(OptUtil.namespaceOpt("namespace to configure iterators on"));
    o.addOptionGroup(tableGroup);
  }

  private void setIteratorTypeOptions(Options o) {
    final OptionGroup typeGroup = new OptionGroup();
    classnameTypeOpt = new Option("class", "class-name", true,
        "a java class that implements SortedKeyValueIterator");
    classnameTypeOpt.setArgName("name");
    regexTypeOpt = new Option("regex", "regular-expression", false, "a regex matching iterator");
    versionTypeOpt = new Option("vers", "version", false, "a versioning iterator");
    reqvisTypeOpt = new Option("reqvis", "require-visibility", false,
        "an iterator that omits entries with empty visibilities");
    ageoffTypeOpt = new Option("ageoff", "ageoff", false, "an aging off iterator");

    typeGroup.addOption(classnameTypeOpt);
    typeGroup.addOption(regexTypeOpt);
    typeGroup.addOption(versionTypeOpt);
    typeGroup.addOption(reqvisTypeOpt);
    typeGroup.addOption(ageoffTypeOpt);
    typeGroup.setRequired(true);
    o.addOptionGroup(typeGroup);
  }

  @Override
  public int numArgs() {
    return 0;
  }
}

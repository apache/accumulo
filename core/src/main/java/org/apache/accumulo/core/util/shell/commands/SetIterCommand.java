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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import jline.console.ConsoleReader;

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
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.ShellCommandException;
import org.apache.accumulo.core.util.shell.ShellCommandException.ErrorCode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

public class SetIterCommand extends Command {

  private Option allScopeOpt, mincScopeOpt, majcScopeOpt, scanScopeOpt, nameOpt, priorityOpt;
  private Option aggTypeOpt, ageoffTypeOpt, regexTypeOpt, versionTypeOpt, reqvisTypeOpt, classnameTypeOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, IOException, ShellCommandException {

    boolean tables = cl.hasOption(OptUtil.tableOpt().getOpt()) || !shellState.getTableName().isEmpty();
    boolean namespaces = cl.hasOption(OptUtil.namespaceOpt().getOpt());

    final int priority = Integer.parseInt(cl.getOptionValue(priorityOpt.getOpt()));

    final Map<String,String> options = new HashMap<String,String>();
    String classname = cl.getOptionValue(classnameTypeOpt.getOpt());
    if (cl.hasOption(aggTypeOpt.getOpt())) {
      Shell.log.warn("aggregators are deprecated");
      @SuppressWarnings("deprecation")
      String deprecatedClassName = org.apache.accumulo.core.iterators.AggregatingIterator.class.getName();
      classname = deprecatedClassName;
    } else if (cl.hasOption(regexTypeOpt.getOpt())) {
      classname = RegExFilter.class.getName();
    } else if (cl.hasOption(ageoffTypeOpt.getOpt())) {
      classname = AgeOffFilter.class.getName();
    } else if (cl.hasOption(versionTypeOpt.getOpt())) {
      classname = VersioningIterator.class.getName();
    } else if (cl.hasOption(reqvisTypeOpt.getOpt())) {
      classname = ReqVisFilter.class.getName();
    }

    ClassLoader classloader = shellState.getClassLoader(cl, shellState);

    // Get the iterator options, with potentially a name provided by the OptionDescriber impl or through user input
    String configuredName = setUpOptions(classloader, shellState.getReader(), classname, options);

    // Try to get the name provided by the setiter command
    String name = cl.getOptionValue(nameOpt.getOpt(), null);

    // Cannot continue if no name is provided
    if (null == name && null == configuredName) {
      throw new IllegalArgumentException("No provided or default name for iterator");
    } else if (null == name) {
      // Fall back to the name from OptionDescriber or user input if none is provided on setiter option
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

  protected void setTableProperties(final CommandLine cl, final Shell shellState, final int priority, final Map<String,String> options, final String classname,
      final String name) throws AccumuloException, AccumuloSecurityException, ShellCommandException, TableNotFoundException {
    // remove empty values

    final String tableName = OptUtil.getTableOpt(cl, shellState);

    if (!shellState.getConnector().tableOperations().testClassLoad(tableName, classname, SortedKeyValueIterator.class.getName())) {
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, "Servers are unable to load " + classname + " as type "
          + SortedKeyValueIterator.class.getName());
    }

    final String aggregatorClass = options.get("aggregatorClass");
    @SuppressWarnings("deprecation")
    String deprecatedAggregatorClassName = org.apache.accumulo.core.iterators.aggregation.Aggregator.class.getName();
    if (aggregatorClass != null && !shellState.getConnector().tableOperations().testClassLoad(tableName, aggregatorClass, deprecatedAggregatorClassName)) {
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, "Servers are unable to load " + aggregatorClass + " as type "
          + deprecatedAggregatorClassName);
    }

    for (Iterator<Entry<String,String>> i = options.entrySet().iterator(); i.hasNext();) {
      final Entry<String,String> entry = i.next();
      if (entry.getValue() == null || entry.getValue().isEmpty()) {
        i.remove();
      }
    }
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
    shellState.getConnector().tableOperations().attachIterator(tableName, setting, scopes);
  }

  protected void setNamespaceProperties(final CommandLine cl, final Shell shellState, final int priority, final Map<String,String> options,
      final String classname, final String name) throws AccumuloException, AccumuloSecurityException, ShellCommandException, NamespaceNotFoundException {
    // remove empty values

    final String namespace = OptUtil.getNamespaceOpt(cl, shellState);

    if (!shellState.getConnector().namespaceOperations().testClassLoad(namespace, classname, SortedKeyValueIterator.class.getName())) {
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, "Servers are unable to load " + classname + " as type "
          + SortedKeyValueIterator.class.getName());
    }

    final String aggregatorClass = options.get("aggregatorClass");
    @SuppressWarnings("deprecation")
    String deprecatedAggregatorClassName = org.apache.accumulo.core.iterators.aggregation.Aggregator.class.getName();
    if (aggregatorClass != null && !shellState.getConnector().namespaceOperations().testClassLoad(namespace, aggregatorClass, deprecatedAggregatorClassName)) {
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, "Servers are unable to load " + aggregatorClass + " as type "
          + deprecatedAggregatorClassName);
    }

    for (Iterator<Entry<String,String>> i = options.entrySet().iterator(); i.hasNext();) {
      final Entry<String,String> entry = i.next();
      if (entry.getValue() == null || entry.getValue().isEmpty()) {
        i.remove();
      }
    }
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
    shellState.getConnector().namespaceOperations().attachIterator(namespace, setting, scopes);
  }

  private static String setUpOptions(ClassLoader classloader, final ConsoleReader reader, final String className, final Map<String,String> options)
      throws IOException, ShellCommandException {
    String input;
    @SuppressWarnings("rawtypes")
    SortedKeyValueIterator untypedInstance;
    @SuppressWarnings("rawtypes")
    Class<? extends SortedKeyValueIterator> clazz;
    try {
      clazz = classloader.loadClass(className).asSubclass(SortedKeyValueIterator.class);
      untypedInstance = clazz.newInstance();
    } catch (ClassNotFoundException e) {
      StringBuilder msg = new StringBuilder("Unable to load ").append(className);
      if (className.indexOf('.') < 0) {
        msg.append("; did you use a fully qualified package name?");
      } else {
        msg.append("; class not found.");
      }
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, msg.toString());
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(e.getMessage());
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(e.getMessage());
    } catch (ClassCastException e) {
      StringBuilder msg = new StringBuilder(50);
      msg.append(className).append(" loaded successfully but does not implement SortedKeyValueIterator.");
      msg.append(" This class cannot be used with this command.");
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, msg.toString());
    }

    @SuppressWarnings("unchecked")
    SortedKeyValueIterator<Key,Value> skvi = (SortedKeyValueIterator<Key,Value>) untypedInstance;
    OptionDescriber iterOptions = null;
    if (OptionDescriber.class.isAssignableFrom(skvi.getClass())) {
      iterOptions = (OptionDescriber) skvi;
    }

    String iteratorName;
    if (null != iterOptions) {
      final IteratorOptions itopts = iterOptions.describeOptions();
      iteratorName = itopts.getName();

      if (iteratorName == null) {
        throw new IllegalArgumentException(className + " described its default distinguishing name as null");
      }
      String shortClassName = className;
      if (className.contains(".")) {
        shortClassName = className.substring(className.lastIndexOf('.') + 1);
      }
      final Map<String,String> localOptions = new HashMap<String,String>();
      do {
        // clean up the overall options that caused things to fail
        for (String key : localOptions.keySet()) {
          options.remove(key);
        }
        localOptions.clear();

        reader.println(itopts.getDescription());

        String prompt;
        if (itopts.getNamedOptions() != null) {
          for (Entry<String,String> e : itopts.getNamedOptions().entrySet()) {
            prompt = Shell.repeat("-", 10) + "> set " + shortClassName + " parameter " + e.getKey() + ", " + e.getValue() + ": ";
            reader.flush();
            input = reader.readLine(prompt);
            if (input == null) {
              reader.println();
              throw new IOException("Input stream closed");
            }
            // Places all Parameters and Values into the LocalOptions, even if the value is "".
            // This allows us to check for "" values when setting the iterators and allows us to remove
            // the parameter and value from the table property.
            localOptions.put(e.getKey(), input);
          }
        }

        if (itopts.getUnnamedOptionDescriptions() != null) {
          for (String desc : itopts.getUnnamedOptionDescriptions()) {
            reader.println(Shell.repeat("-", 10) + "> entering options: " + desc);
            input = "start";
            prompt = Shell.repeat("-", 10) + "> set " + shortClassName + " option (<name> <value>, hit enter to skip): ";
            while (true) {
              reader.flush();
              input = reader.readLine(prompt);
              if (input == null) {
                reader.println();
                throw new IOException("Input stream closed");
              } else {
                input = new String(input);
              }

              if (input.length() == 0)
                break;

              String[] sa = input.split(" ", 2);
              localOptions.put(sa[0], sa[1]);
            }
          }
        }

        options.putAll(localOptions);
        if (!iterOptions.validateOptions(options))
          reader.println("invalid options for " + clazz.getName());

      } while (!iterOptions.validateOptions(options));
    } else {
      reader.flush();
      reader.println("The iterator class does not implement OptionDescriber. Consider this for better iterator configuration using this setiter command.");
      iteratorName = reader.readLine("Name for iterator (enter to skip): ");
      if (null == iteratorName) {
        reader.println();
        throw new IOException("Input stream closed");
      } else if (StringUtils.isWhitespace(iteratorName)) {
        // Treat whitespace or empty string as no name provided
        iteratorName = null;
      }

      reader.flush();
      reader.println("Optional, configure name-value options for iterator:");
      String prompt = Shell.repeat("-", 10) + "> set option (<name> <value>, hit enter to skip): ";
      final HashMap<String,String> localOptions = new HashMap<String,String>();

      while (true) {
        reader.flush();
        input = reader.readLine(prompt);
        if (input == null) {
          reader.println();
          throw new IOException("Input stream closed");
        } else if (StringUtils.isWhitespace(input)) {
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

  @Override
  public Options getOptions() {
    final Options o = new Options();

    priorityOpt = new Option("p", "priority", true, "the order in which the iterator is applied");
    priorityOpt.setArgName("pri");
    priorityOpt.setRequired(true);

    nameOpt = new Option("n", "name", true, "iterator to set");
    nameOpt.setArgName("itername");

    allScopeOpt = new Option("all", "all-scopes", false, "applied at scan time, minor and major compactions");
    mincScopeOpt = new Option(IteratorScope.minc.name(), "minor-compaction", false, "applied at minor compaction");
    majcScopeOpt = new Option(IteratorScope.majc.name(), "major-compaction", false, "applied at major compaction");
    scanScopeOpt = new Option(IteratorScope.scan.name(), "scan-time", false, "applied at scan time");

    final OptionGroup typeGroup = new OptionGroup();
    classnameTypeOpt = new Option("class", "class-name", true, "a java class that implements SortedKeyValueIterator");
    classnameTypeOpt.setArgName("name");
    aggTypeOpt = new Option("agg", "aggregator", false, "an aggregating type");
    regexTypeOpt = new Option("regex", "regular-expression", false, "a regex matching iterator");
    versionTypeOpt = new Option("vers", "version", false, "a versioning iterator");
    reqvisTypeOpt = new Option("reqvis", "require-visibility", false, "an iterator that omits entries with empty visibilities");
    ageoffTypeOpt = new Option("ageoff", "ageoff", false, "an aging off iterator");

    typeGroup.addOption(classnameTypeOpt);
    typeGroup.addOption(aggTypeOpt);
    typeGroup.addOption(regexTypeOpt);
    typeGroup.addOption(versionTypeOpt);
    typeGroup.addOption(reqvisTypeOpt);
    typeGroup.addOption(ageoffTypeOpt);
    typeGroup.setRequired(true);

    final OptionGroup tableGroup = new OptionGroup();
    tableGroup.addOption(OptUtil.tableOpt("table to configure iterators on"));
    tableGroup.addOption(OptUtil.namespaceOpt("namespace to configure iterators on"));

    o.addOption(priorityOpt);
    o.addOption(nameOpt);
    o.addOption(allScopeOpt);
    o.addOption(mincScopeOpt);
    o.addOption(majcScopeOpt);
    o.addOption(scanScopeOpt);
    o.addOptionGroup(typeGroup);
    o.addOptionGroup(tableGroup);
    return o;
  }

  @Override
  public int numArgs() {
    return 0;
  }
}

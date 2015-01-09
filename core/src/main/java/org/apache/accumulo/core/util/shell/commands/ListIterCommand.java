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

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class ListIterCommand extends Command {
  private Option nameOpt, allScopesOpt;
  private Map<IteratorScope,Option> scopeOpts;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {

    boolean tables = cl.hasOption(OptUtil.tableOpt().getOpt()) || !shellState.getTableName().isEmpty();
    boolean namespaces = cl.hasOption(OptUtil.namespaceOpt().getOpt());

    final Map<String,EnumSet<IteratorScope>> iterators;
    if (namespaces) {
      iterators = shellState.getConnector().namespaceOperations().listIterators(OptUtil.getNamespaceOpt(cl, shellState));
    } else if (tables) {
      iterators = shellState.getConnector().tableOperations().listIterators(OptUtil.getTableOpt(cl, shellState));
    } else {
      throw new IllegalArgumentException("No table or namespace specified");
    }

    if (cl.hasOption(nameOpt.getOpt())) {
      final String name = cl.getOptionValue(nameOpt.getOpt());
      if (!iterators.containsKey(name)) {
        Shell.log.warn("no iterators found that match your criteria");
        return 0;
      }
      final EnumSet<IteratorScope> scopes = iterators.get(name);
      iterators.clear();
      iterators.put(name, scopes);
    }

    final boolean allScopes = cl.hasOption(allScopesOpt.getOpt());
    Set<IteratorScope> desiredScopes = new HashSet<IteratorScope>();
    for (IteratorScope scope : IteratorScope.values()) {
      if (allScopes || cl.hasOption(scopeOpts.get(scope).getOpt()))
        desiredScopes.add(scope);
    }
    if (desiredScopes.isEmpty()) {
      throw new IllegalArgumentException("You must select at least one scope to configure");
    }
    final StringBuilder sb = new StringBuilder("-\n");
    for (Entry<String,EnumSet<IteratorScope>> entry : iterators.entrySet()) {
      final String name = entry.getKey();
      final EnumSet<IteratorScope> scopes = entry.getValue();
      for (IteratorScope scope : scopes) {
        if (desiredScopes.contains(scope)) {
          IteratorSetting setting;
          if (namespaces) {
            setting = shellState.getConnector().namespaceOperations().getIteratorSetting(OptUtil.getNamespaceOpt(cl, shellState), name, scope);
          } else if (tables) {
            setting = shellState.getConnector().tableOperations().getIteratorSetting(OptUtil.getTableOpt(cl, shellState), name, scope);
          } else {
            throw new IllegalArgumentException("No table or namespace specified");
          }
          sb.append("-    Iterator ").append(setting.getName()).append(", ").append(scope).append(" scope options:\n");
          sb.append("-        ").append("iteratorPriority").append(" = ").append(setting.getPriority()).append("\n");
          sb.append("-        ").append("iteratorClassName").append(" = ").append(setting.getIteratorClass()).append("\n");
          for (Entry<String,String> optEntry : setting.getOptions().entrySet()) {
            sb.append("-        ").append(optEntry.getKey()).append(" = ").append(optEntry.getValue()).append("\n");
          }
        }
      }
    }
    sb.append("-");
    shellState.getReader().println(sb.toString());

    return 0;
  }

  @Override
  public String description() {
    return "lists table-specific or namespace-specific iterators configured in this shell session";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    nameOpt = new Option("n", "name", true, "iterator to list");
    nameOpt.setArgName("itername");

    allScopesOpt = new Option("all", "all-scopes", false, "list from all scopes");
    o.addOption(allScopesOpt);

    scopeOpts = new EnumMap<IteratorScope,Option>(IteratorScope.class);
    scopeOpts.put(IteratorScope.minc, new Option(IteratorScope.minc.name(), "minor-compaction", false, "list iterator for minor compaction scope"));
    scopeOpts.put(IteratorScope.majc, new Option(IteratorScope.majc.name(), "major-compaction", false, "list iterator for major compaction scope"));
    scopeOpts.put(IteratorScope.scan, new Option(IteratorScope.scan.name(), "scan-time", false, "list iterator for scan scope"));

    OptionGroup grp = new OptionGroup();
    grp.addOption(OptUtil.tableOpt("table to list the configured iterators on"));
    grp.addOption(OptUtil.namespaceOpt("namespace to list the configured iterators on"));
    o.addOptionGroup(grp);
    o.addOption(nameOpt);

    for (Option opt : scopeOpts.values()) {
      o.addOption(opt);
    }

    return o;
  }
}

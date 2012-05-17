/**
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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ListIterCommand extends Command {
  private Option nameOpt;
  private Map<IteratorScope,Option> scopeOpts;
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    String tableName = OptUtil.getTableOpt(cl, shellState);
    
    Map<String,EnumSet<IteratorScope>> iterators = shellState.getConnector().tableOperations().listIterators(tableName);
    
    if (cl.hasOption(nameOpt.getOpt())) {
      String name = cl.getOptionValue(nameOpt.getOpt());
      if (!iterators.containsKey(name)) {
        Shell.log.warn("no iterators found that match your criteria");
        return 0;
      }
      EnumSet<IteratorScope> scopes = iterators.get(name);
      iterators.clear();
      iterators.put(name, scopes);
    }
    
    boolean hasScope = false;
    for (IteratorScope scope : IteratorScope.values()) {
      if (cl.hasOption(scopeOpts.get(scope).getOpt()))
        hasScope = true;
    }
    if (!hasScope)
      throw new IllegalArgumentException("You must select at least one scope to configure");
    
    StringBuilder sb = new StringBuilder("-\n");
    for (String name : iterators.keySet()) {
      for (IteratorScope scope : iterators.get(name)) {
        if (cl.hasOption(scopeOpts.get(scope).getOpt())) {
          IteratorSetting setting = shellState.getConnector().tableOperations().getIteratorSetting(tableName, name, scope);
          sb.append("-    Iterator ").append(setting.getName()).append(", ").append(scope).append(" scope options:\n");
          sb.append("-        ").append("iteratorPriority").append(" = ").append(setting.getPriority()).append("\n");
          sb.append("-        ").append("iteratorClassName").append(" = ").append(setting.getIteratorClass()).append("\n");
          for (Entry<String,String> optEntry : setting.getOptions().entrySet()) {
            sb.append("-        ").append(optEntry.getKey()).append(" = ").append(optEntry.getValue()).append("\n");
          }
        }
      }
    }
    sb.append("-\n");
    shellState.getReader().printString(sb.toString());
    
    return 0;
  }
  
  public String description() {
    return "lists table-specific iterators";
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    
    nameOpt = new Option("n", "name", true, "iterator to list");
    nameOpt.setArgName("itername");
    
    scopeOpts = new EnumMap<IteratorScope,Option>(IteratorScope.class);
    scopeOpts.put(IteratorScope.minc, new Option(IteratorScope.minc.name(), "minor-compaction", false, "list iterator for minor compaction scope"));
    scopeOpts.put(IteratorScope.majc, new Option(IteratorScope.majc.name(), "major-compaction", false, "list iterator for major compaction scope"));
    scopeOpts.put(IteratorScope.scan, new Option(IteratorScope.scan.name(), "scan-time", false, "list iterator for scan scope"));
    
    o.addOption(OptUtil.tableOpt("table to list the configured iterators on"));
    o.addOption(nameOpt);
    
    for (Option opt : scopeOpts.values()) {
      o.addOption(opt);
    }
    
    return o;
  }
}

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

import java.util.EnumSet;

import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class DeleteIterCommand extends Command {
  private Option mincScopeOpt, majcScopeOpt, scanScopeOpt, nameOpt;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    String tableName = OptUtil.getTableOpt(cl, shellState);
    
    String name = cl.getOptionValue(nameOpt.getOpt());
    if (!shellState.getConnector().tableOperations().listIterators(tableName).containsKey(name)) {
      Shell.log.warn("no iterators found that match your criteria");
      return 0;
    }
    
    EnumSet<IteratorScope> scopes = EnumSet.noneOf(IteratorScope.class);
    if (cl.hasOption(mincScopeOpt.getOpt()))
      scopes.add(IteratorScope.minc);
    if (cl.hasOption(majcScopeOpt.getOpt()))
      scopes.add(IteratorScope.majc);
    if (cl.hasOption(scanScopeOpt.getOpt()))
      scopes.add(IteratorScope.scan);
    if (scopes.isEmpty())
      throw new IllegalArgumentException("You must select at least one scope to configure");
    shellState.getConnector().tableOperations().removeIterator(tableName, name, scopes);
    return 0;
  }
  
  @Override
  public String description() {
    return "deletes a table-specific iterator";
  }
  
  public Options getOptions() {
    Options o = new Options();
    
    nameOpt = new Option("n", "name", true, "iterator to delete");
    nameOpt.setArgName("itername");
    nameOpt.setRequired(true);
    
    mincScopeOpt = new Option(IteratorScope.minc.name(), "minor-compaction", false, "remove from minor compaction scope");
    majcScopeOpt = new Option(IteratorScope.majc.name(), "major-compaction", false, "remove from major compaction scope");
    scanScopeOpt = new Option(IteratorScope.scan.name(), "scan-time", false, "remove from scan scope");
    
    o.addOption(OptUtil.tableOpt("table to delete the iterator from"));
    o.addOption(nameOpt);
    
    o.addOption(mincScopeOpt);
    o.addOption(majcScopeOpt);
    o.addOption(scanScopeOpt);
    
    return o;
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
}

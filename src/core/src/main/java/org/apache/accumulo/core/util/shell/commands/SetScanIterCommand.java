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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.ShellCommandException;
import org.apache.accumulo.core.util.shell.ShellCommandException.ErrorCode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

public class SetScanIterCommand extends SetIterCommand {
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      IOException, ShellCommandException {
    return super.execute(fullCommand, cl, shellState);
  }
  
  @Override
  protected void setTableProperties(CommandLine cl, Shell shellState, String tableName, int priority, Map<String,String> options, String classname, String name)
      throws AccumuloException, AccumuloSecurityException, ShellCommandException {
    // instead of setting table properties, just put the options
    // in a map to use at scan time
    if (!shellState.getConnector().instanceOperations().testClassLoad(classname, SortedKeyValueIterator.class.getName()))
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, "Servers are unable to load " + classname + " as type "
          + SortedKeyValueIterator.class.getName());
    options.put("iteratorClassName", classname);
    options.put("iteratorPriority", Integer.toString(priority));
    Map<String,Map<String,String>> tableIterators = shellState.scanIteratorOptions.get(tableName);
    if (tableIterators == null) {
      tableIterators = new HashMap<String,Map<String,String>>();
      shellState.scanIteratorOptions.put(tableName, tableIterators);
    }
    tableIterators.put(name, options);
  }
  
  @Override
  public String description() {
    return "sets a table-specific scan iterator for this shell session";
  }
  
  @Override
  public Options getOptions() {
    // Remove the options that specify which type of iterator this is, since
    // they are all scan iterators with this command.
    HashSet<OptionGroup> groups = new HashSet<OptionGroup>();
    Options parentOptions = super.getOptions();
    Options modifiedOptions = new Options();
    for (Iterator<?> it = parentOptions.getOptions().iterator(); it.hasNext();) {
      Option o = (Option) it.next();
      if (!IteratorScope.majc.name().equals(o.getOpt()) && !IteratorScope.minc.name().equals(o.getOpt()) && !IteratorScope.scan.name().equals(o.getOpt())) {
        modifiedOptions.addOption(o);
        OptionGroup group = parentOptions.getOptionGroup(o);
        if (group != null)
          groups.add(group);
      }
    }
    for (OptionGroup group : groups) {
      modifiedOptions.addOptionGroup(group);
    }
    return modifiedOptions;
  }
  
}

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
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.util.TableDiskUsage;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class DUCommand extends Command {
  
  private Option optTablePattern;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws IOException, TableNotFoundException {
    
    SortedSet<String> tablesToFlush = new TreeSet<String>(Arrays.asList(cl.getArgs()));
    if (cl.hasOption(optTablePattern.getOpt())) {
      for (String table : shellState.getConnector().tableOperations().list())
        if (table.matches(cl.getOptionValue(optTablePattern.getOpt())))
          tablesToFlush.add(table);
    }
    try {
      AccumuloConfiguration acuConf = new ConfigurationCopy(shellState.getConnector().instanceOperations().getSystemConfiguration());
      TableDiskUsage.printDiskUsage(acuConf, tablesToFlush, FileSystem.get(new Configuration()), shellState.getConnector());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return 0;
  }
  
  @Override
  public String description() {
    return "prints how much space is used by files referenced by a table.  When multiple tables are specified it prints how much space is used by files shared between tables, if any.";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    
    optTablePattern = new Option("p", "pattern", true, "regex pattern of table names");
    optTablePattern.setArgName("pattern");
    
    o.addOption(optTablePattern);
    
    return o;
  }
  
  @Override
  public String usage() {
    return getName() + " <table>{ <table>}";
  }
  
  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }
}

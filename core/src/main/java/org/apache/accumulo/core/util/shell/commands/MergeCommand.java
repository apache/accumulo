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

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.util.Merge;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class MergeCommand extends Command {
  private Option verboseOpt, forceOpt, sizeOpt, allOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    boolean verbose = shellState.isVerbose();
    boolean force = false;
    boolean all = false;
    long size = -1;
    final String tableName = OptUtil.getTableOpt(cl, shellState);
    final Text startRow = OptUtil.getStartRow(cl);
    final Text endRow = OptUtil.getEndRow(cl);
    if (cl.hasOption(verboseOpt.getOpt())) {
      verbose = true;
    }
    if (cl.hasOption(forceOpt.getOpt())) {
      force = true;
    }
    if (cl.hasOption(allOpt.getOpt())) {
      all = true;
    }
    if (cl.hasOption(sizeOpt.getOpt())) {
      size = AccumuloConfiguration.getMemoryInBytes(cl.getOptionValue(sizeOpt.getOpt()));
    }
    if (startRow == null && endRow == null && size < 0 && !all) {
      shellState.getReader().flush();
      String line = shellState.getReader().readLine("Merge the entire table { " + tableName + " } into one tablet (yes|no)? ");
      if (line == null)
        return 0;
      if (!line.equalsIgnoreCase("y") && !line.equalsIgnoreCase("yes"))
        return 0;
    }
    if (size < 0) {
      shellState.getConnector().tableOperations().merge(tableName, startRow, endRow);
    } else {
      final boolean finalVerbose = verbose;
      final Merge merge = new Merge() {
        protected void message(String fmt, Object... args) {
          if (finalVerbose) {
            try {
              shellState.getReader().println(String.format(fmt, args));
            } catch (IOException ex) {
              throw new RuntimeException(ex);
            }
          }
        }
      };
      merge.mergomatic(shellState.getConnector(), tableName, startRow, endRow, size, force);
    }
    return 0;
  }

  @Override
  public String description() {
    return "merges tablets in a table";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    verboseOpt = new Option("v", "verbose", false, "verbose output during merge");
    sizeOpt = new Option("s", "size", true, "merge tablets to the given size over the entire table");
    forceOpt = new Option("f", "force", false, "merge small tablets to large tablets, even if it goes over the given size");
    allOpt = new Option("", "all", false, "allow an entire table to be merged into one tablet without prompting the user for confirmation");
    Option startRowOpt = OptUtil.startRowOpt();
    startRowOpt.setDescription("begin row (NOT inclusive)");
    o.addOption(startRowOpt);
    o.addOption(OptUtil.endRowOpt());
    o.addOption(OptUtil.tableOpt("table to be merged"));
    o.addOption(verboseOpt);
    o.addOption(sizeOpt);
    o.addOption(forceOpt);
    o.addOption(allOpt);
    return o;
  }

}

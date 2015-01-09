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

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class DeleteRowsCommand extends Command {
  private Option forceOpt;
  private Option startRowOptExclusive;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    final String tableName = OptUtil.getTableOpt(cl, shellState);
    final Text startRow = OptUtil.getStartRow(cl);
    final Text endRow = OptUtil.getEndRow(cl);
    if (!cl.hasOption(forceOpt.getOpt()) && (startRow == null || endRow == null)) {
      shellState.getReader().println("Not deleting unbounded range. Specify both ends, or use --force");
      return 1;
    }
    shellState.getConnector().tableOperations().deleteRows(tableName, startRow, endRow);
    return 0;
  }

  @Override
  public String description() {
    return "deletes a range of rows in a table.  Note that rows matching the start row ARE NOT deleted, but rows matching the end row ARE deleted.";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    forceOpt = new Option("f", "force", false, "delete data even if start or end are not specified");
    startRowOptExclusive = new Option(OptUtil.START_ROW_OPT, "begin-row", true, "begin row (exclusive)");
    startRowOptExclusive.setArgName("begin-row");
    o.addOption(startRowOptExclusive);
    o.addOption(OptUtil.endRowOpt());
    o.addOption(OptUtil.tableOpt("table to delete a row range from"));
    o.addOption(forceOpt);
    return o;
  }
}

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

import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class GetAvailabilityCommand extends TableOperation {

  private Option optRow;
  private Option optStartRowExclusive;
  private Option optEndRowExclusive;
  private Range range;

  @Override
  public String getName() {
    return "getavailability";
  }

  @Override
  public String description() {
    return "Retrieves the availability (HOSTED, ONDEMAND, UNHOSTED) for a range of tablets";
  }

  @Override
  protected void doTableOp(Shell shellState, String tableName) throws Exception {
    shellState.getWriter().println("TABLE: " + tableName);
    shellState.getWriter().println("TABLET ID    AVAILABILITY");
    shellState.getAccumuloClient().tableOperations().getTabletInformation(tableName, range)
        .forEach(p -> shellState.getWriter()
            .println(String.format("%-10s   %s", p.getTabletId(), p.getTabletAvailability())));
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    if ((cl.hasOption(OptUtil.START_ROW_OPT) || cl.hasOption(OptUtil.END_ROW_OPT))
        && cl.hasOption(optRow.getOpt())) {
      // did not see a way to make commons cli do this check... it has mutually exclusive options
      // but does not support the or
      throw new IllegalArgumentException("Options -" + optRow.getOpt() + " AND (-"
          + OptUtil.START_ROW_OPT + " OR -" + OptUtil.END_ROW_OPT + ") are mutually exclusive ");
    }

    if (cl.hasOption(optRow.getOpt())) {
      this.range = new Range(new Text(cl.getOptionValue(optRow.getOpt()).getBytes(Shell.CHARSET)));
    } else {
      Text startRow = OptUtil.getStartRow(cl);
      Text endRow = OptUtil.getEndRow(cl);
      final boolean startInclusive = !cl.hasOption(optStartRowExclusive.getOpt());
      final boolean endInclusive = !cl.hasOption(optEndRowExclusive.getOpt());
      this.range = new Range(startRow, startInclusive, endRow, endInclusive);
    }
    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  public Options getOptions() {
    Option optStartRowInclusive =
        new Option(OptUtil.START_ROW_OPT, "begin-row", true, "begin row (inclusive)");
    optStartRowExclusive = new Option("be", "begin-exclusive", false,
        "make start row exclusive (by default it's inclusive");
    optStartRowExclusive.setArgName("begin-exclusive");
    optEndRowExclusive = new Option("ee", "end-exclusive", false,
        "make end row exclusive (by default it's inclusive)");
    optEndRowExclusive.setArgName("end-exclusive");
    optRow = new Option("r", "row", true, "tablet row to read");
    optRow.setArgName("row");

    final Options opts = super.getOptions();
    opts.addOption(optStartRowInclusive);
    opts.addOption(optStartRowExclusive);
    opts.addOption(OptUtil.endRowOpt());
    opts.addOption(optEndRowExclusive);
    opts.addOption(optRow);

    return opts;
  }

}

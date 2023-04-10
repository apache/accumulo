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

import java.io.UnsupportedEncodingException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class OndemandCommand extends TableOperation {

  private Option scanOptRow;
  private Option optStartRowExclusive;
  private Option optEndRowExclusive;


  @Override
  public String description() {
    return "starts the process of converting the table to ondemand";
  }

  @Override
  protected void doTableOp(final Shell shellState, final String tableName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    shellState.getAccumuloClient().tableOperations().setTabletHostingGoal(tableName, null, tableName);
    shellState.getAccumuloClient().tableOperations().onDemand(tableName, wait);
    Shell.log.info("Ondemand of table {} {}", tableName, wait ? " completed." : " initiated...");
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    if ((cl.hasOption(OptUtil.START_ROW_OPT) || cl.hasOption(OptUtil.END_ROW_OPT))
        && cl.hasOption(scanOptRow.getOpt())) {
      // did not see a way to make commons cli do this check... it has mutually exclusive options
      // but does not support the or
      throw new IllegalArgumentException("Options -" + scanOptRow.getOpt() + " AND (-"
          + OptUtil.START_ROW_OPT + " OR -" + OptUtil.END_ROW_OPT + ") are mutually exclusive ");
    }

    if (cl.hasOption(scanOptRow.getOpt())) {
      new Range(new Text(cl.getOptionValue(scanOptRow.getOpt()).getBytes(Shell.CHARSET)));
    } else {
      Text startRow = OptUtil.getStartRow(cl);
      Text endRow = OptUtil.getEndRow(cl);
      final boolean startInclusive = !cl.hasOption(optStartRowExclusive.getOpt());
      final boolean endInclusive = !cl.hasOption(optEndRowExclusive.getOpt());
      new Range(startRow, startInclusive, endRow, endInclusive);
    }
  }

  @Override
  public Options getOptions() {
    optStartRowExclusive = new Option("be", "begin-exclusive", false,
        "make start row exclusive (by default it's inclusive)");
    optStartRowExclusive.setArgName("begin-exclusive");
    optEndRowExclusive = new Option("ee", "end-exclusive", false,
        "make end row exclusive (by default it's inclusive)");
    optEndRowExclusive.setArgName("end-exclusive");
    scanOptRow = new Option("r", "row", true, "row to scan");
    scanOptRow.setArgName("row");

    
    final Options opts = super.getOptions();
    
    return opts;
  }
}

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.SummaryRetriever;
import org.apache.accumulo.core.client.summary.Summary;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class SummariesCommand extends TableOperation {

  private Text startRow;
  private Text endRow;
  private boolean paginate;
  private String selectionRegex = ".*";

  private Option disablePaginationOpt;
  private Option summarySelectionOpt;

  @Override
  public String description() {
    return "retrieves summary statistics";
  }

  @Override
  protected void doTableOp(final Shell shellState, final String tableName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
    AccumuloClient client = shellState.getAccumuloClient();
    SummaryRetriever retriever =
        client.tableOperations().summaries(tableName).withMatchingConfiguration(selectionRegex);
    if (startRow != null) {
      retriever.startRow(startRow);
    }

    if (endRow != null) {
      retriever.endRow(endRow);
    }

    Collection<Summary> summaries = retriever.retrieve();

    ArrayList<String> lines = new ArrayList<>();

    boolean addEmpty = false;
    for (Summary summary : summaries) {
      if (addEmpty) {
        lines.add("");
      }
      addEmpty = true;
      lines.add(String.format(" Summarizer         : %s", summary.getSummarizerConfiguration()));
      lines.add(String.format(" File Statistics    : %s", summary.getFileStatistics()));
      lines.add(" Summary Statistics : ");

      Map<String,Long> stats = summary.getStatistics();
      ArrayList<String> keys = new ArrayList<>(stats.keySet());
      Collections.sort(keys);
      for (String key : keys) {
        lines.add(String.format("    %-60s = %,d", key, stats.get(key)));
      }
    }

    shellState.printLines(lines.iterator(), paginate);

  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    startRow = OptUtil.getStartRow(cl);
    endRow = OptUtil.getEndRow(cl);
    paginate = !cl.hasOption(disablePaginationOpt.getOpt());
    if (cl.hasOption(summarySelectionOpt.getOpt())) {
      selectionRegex = cl.getOptionValue(summarySelectionOpt.getOpt());
    } else {
      selectionRegex = ".*";
    }
    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  public Options getOptions() {
    final Options opts = super.getOptions();
    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    summarySelectionOpt = new Option("sr", "select-regex", true,
        "regex to select summaries. Matches against class name and options used to"
            + " generate summaries.");
    opts.addOption(disablePaginationOpt);
    opts.addOption(summarySelectionOpt);
    opts.addOption(OptUtil.startRowOpt());
    opts.addOption(OptUtil.endRowOpt());
    return opts;
  }
}

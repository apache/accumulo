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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class FlushCommand extends TableOperation {
  private Text startRow;
  private Text endRow;

  private boolean wait;
  private Option waitOpt;

  @Override
  public String description() {
    return "flushes a tables data that is currently in memory to disk";
  }

  @Override
  protected void doTableOp(final Shell shellState, final String tableName)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    shellState.getAccumuloClient().tableOperations().flush(tableName, startRow, endRow, wait);
    Shell.log.info("Flush of table {} {}", tableName, wait ? " completed." : " initiated...");
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    wait = cl.hasOption(waitOpt.getLongOpt());
    startRow = OptUtil.getStartRow(cl);
    endRow = OptUtil.getEndRow(cl);
    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  public Options getOptions() {
    final Options opts = super.getOptions();
    waitOpt = new Option("w", "wait", false, "wait for flush to finish");
    opts.addOption(waitOpt);
    opts.addOption(OptUtil.startRowOpt());
    opts.addOption(OptUtil.endRowOpt());

    return opts;
  }
}

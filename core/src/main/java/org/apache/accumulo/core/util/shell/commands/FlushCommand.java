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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
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
  
  protected void doTableOp(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    shellState.getConnector().tableOperations().flush(tableName, startRow, endRow, wait);
    Shell.log.info("Flush of table " + tableName + (wait ? " completed." : " initiated..."));
    if (tableName.equals(Constants.METADATA_TABLE_NAME)) {
      Shell.log.info("  May need to flush " + Constants.METADATA_TABLE_NAME + " table multiple times.");
      Shell.log.info("  Flushing " + Constants.METADATA_TABLE_NAME + " causes writes to itself and");
      Shell.log.info("  minor compactions, which also cause writes to itself.");
      Shell.log.info("  Check the monitor web page and give it time to settle.");
    }
  }
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    wait = cl.hasOption(waitOpt.getLongOpt());
    startRow = OptUtil.getStartRow(cl);
    endRow = OptUtil.getEndRow(cl);
    return super.execute(fullCommand, cl, shellState);
  }
  
  @Override
  public Options getOptions() {
    Options opts = super.getOptions();
    waitOpt = new Option("w", "wait", false, "wait for flush to finish");
    opts.addOption(waitOpt);
    opts.addOption(OptUtil.startRowOpt());
    opts.addOption(OptUtil.endRowOpt());
    
    return opts;
  }
}

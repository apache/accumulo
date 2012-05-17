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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class CompactCommand extends TableOperation {
  private Option noFlushOption, waitOpt;
  private boolean flush;
  private Text startRow;
  private Text endRow;
  
  boolean override = false;
  private boolean wait;
  
  @Override
  public String description() {
    return "sets all tablets for a table to major compact as soon as possible (based on current time)";
  }
  
  protected void doTableOp(Shell shellState, String tableName) throws AccumuloException, AccumuloSecurityException {
    // compact the tables
    try {
      if (wait)
        Shell.log.info("Compacting table ...");
      
      shellState.getConnector().tableOperations().compact(tableName, startRow, endRow, flush, wait);
      
      Shell.log.info("Compaction of table " + tableName + " " + (wait ? "completed" : "started") + " for given range");
    } catch (Exception ex) {
      throw new AccumuloException(ex);
    }
  }
  
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    flush = !cl.hasOption(noFlushOption.getOpt());
    startRow = OptUtil.getStartRow(cl);
    endRow = OptUtil.getEndRow(cl);
    wait = cl.hasOption(waitOpt.getOpt());
    
    return super.execute(fullCommand, cl, shellState);
  }
  
  @Override
  public Options getOptions() {
    Options opts = super.getOptions();
    
    opts.addOption(OptUtil.startRowOpt());
    opts.addOption(OptUtil.endRowOpt());
    noFlushOption = new Option("nf", "noFlush", false, "do not flush table data in memory before compacting.");
    opts.addOption(noFlushOption);
    waitOpt = new Option("w", "wait", false, "wait for compact to finish");
    opts.addOption(waitOpt);
    
    return opts;
  }
}

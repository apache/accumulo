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

import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class UnlockCommand extends TableOperation {

  private boolean wait;
  private Option waitOpt;

  @Override
  public String description() {
    return "unlocks a table, allowing operations to be performed on the table";
  }

  @Override
  protected void doTableOp(Shell shellState, String tableName) throws Exception {
    if (tableName.equals(SystemTables.ROOT.tableName())) {
      Shell.log.info("  The {} is always online.", SystemTables.ROOT.tableName());
    } else {
      shellState.getAccumuloClient().tableOperations().unlock(tableName, wait);
      Shell.log.info("Unlock of table {} {}", tableName, wait ? " completed." : " initiated...");
    }
  }

  @Override
  public int execute(String fullCommand, org.apache.commons.cli.CommandLine cl, Shell shellState)
      throws Exception {
    wait = cl.hasOption(waitOpt.getLongOpt());
    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  public Options getOptions() {
    final Options opts = super.getOptions();
    waitOpt = new Option("w", "wait", false, "wait for unlock to finish");
    opts.addOption(waitOpt);
    return opts;
  }
}

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

import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class GetTimeTypeCommand extends Command {

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    final String tableName = OptUtil.getTableOpt(cl, shellState);
    if (!shellState.getAccumuloClient().tableOperations().exists(tableName)) {
      throw new TableNotFoundException(null, tableName, null);
    }
    List<String> lines = new LinkedList<>();
    lines.add(shellState.getAccumuloClient().tableOperations().getTimeType(tableName).toString());
    if (!lines.iterator().hasNext()) {
      lines.add("Failed to retrieve TimeType");
    }
    shellState.printLines(lines.iterator(), false);
    return 0;
  }

  @Override
  public String description() {
    return "retrieves the TimeType for a table";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options opts = new Options();
    opts.addOption(OptUtil.tableOpt("table from which to retrieve TimeType"));
    return opts;
  }
}

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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class GetGroupsCommand extends Command {

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    final String tableName = OptUtil.getTableOpt(cl, shellState);

    final Map<String,Set<Text>> groups =
        shellState.getAccumuloClient().tableOperations().getLocalityGroups(tableName);

    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      shellState.getWriter()
          .println(entry.getKey() + "=" + LocalityGroupUtil.encodeColumnFamilies(entry.getValue()));
    }
    return 0;
  }

  @Override
  public String description() {
    return "gets the locality groups for a given table";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options opts = new Options();
    opts.addOption(OptUtil.tableOpt("table to fetch locality groups from"));
    return opts;
  }
}

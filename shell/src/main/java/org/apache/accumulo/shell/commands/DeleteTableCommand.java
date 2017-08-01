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
package org.apache.accumulo.shell.commands;

import java.util.Iterator;
import java.util.Set;

import org.apache.accumulo.core.client.impl.Namespace;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.util.Pair;
import org.apache.accumulo.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteTableCommand extends TableOperation {
  private static final Logger log = LoggerFactory.getLogger(DeleteTableCommand.class);

  private Option forceOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    if (cl.hasOption(forceOpt.getOpt())) {
      super.force();
    } else {
      super.noForce();
    }
    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  public String description() {
    return "deletes a table";
  }

  @Override
  protected void doTableOp(final Shell shellState, final String tableName) throws Exception {
    shellState.getConnector().tableOperations().delete(tableName);
    shellState.getReader().println("Table: [" + tableName + "] has been deleted.");

    if (shellState.getTableName().equals(tableName)) {
      shellState.setTableName("");
    }
  }

  @Override
  public Options getOptions() {
    forceOpt = new Option("f", "force", false, "force deletion without prompting");
    final Options opts = super.getOptions();

    opts.addOption(forceOpt);
    return opts;
  }

  @Override
  protected void pruneTables(String pattern, Set<String> tables) {
    Iterator<String> tableNames = tables.iterator();
    while (tableNames.hasNext()) {
      String table = tableNames.next();
      Pair<String,String> qualifiedName = Tables.qualify(table);
      if (Namespace.ACCUMULO.equals(qualifiedName.getFirst())) {
        log.trace("Removing table from deletion set: {}", table);
        tableNames.remove();
      }
    }
  }
}

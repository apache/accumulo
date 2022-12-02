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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class DeleteNamespaceCommand extends Command {
  private Option forceOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    boolean force = false;
    boolean operate = true;
    if (cl.hasOption(forceOpt.getOpt())) {
      force = true;
    }
    String namespace = cl.getArgs()[0];

    if (!force) {
      shellState.getWriter().flush();
      String line =
          shellState.getReader().readLine(getName() + " { " + namespace + " } (yes|no)? ");
      operate = line != null && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"));
    }
    if (operate) {
      doTableOp(shellState, namespace, force);
    }
    return 0;
  }

  @Override
  public String description() {
    return "deletes a namespace";
  }

  protected void doTableOp(final Shell shellState, final String namespace, boolean force)
      throws Exception {
    boolean resetContext = false;
    String currentTable = shellState.getTableName();

    NamespaceId namespaceId = Namespaces.getNamespaceId(shellState.getContext(), namespace);
    List<String> tables = Namespaces.getTableNames(shellState.getContext(), namespaceId);
    resetContext = tables.contains(currentTable);

    if (force) {
      for (String table : shellState.getAccumuloClient().tableOperations().list()) {
        if (table.startsWith(namespace + ".")) {
          shellState.getAccumuloClient().tableOperations().delete(table);
        }
      }
    }

    shellState.getAccumuloClient().namespaceOperations().delete(namespace);
    if (resetContext) {
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
  public int numArgs() {
    return 1;
  }

  @Override
  public void registerCompletion(final Token root,
      final Map<Command.CompletionSet,Set<String>> special) {
    registerCompletionForNamespaces(root, special);
  }
}

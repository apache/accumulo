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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.TableNamespaceNotFoundException;
import org.apache.accumulo.core.client.impl.TableNamespaces;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class DeleteNamespaceCommand extends Command {
  private Option forceOpt;
  
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    boolean force = false;
    boolean operate = true;
    if (cl.hasOption(forceOpt.getOpt())) {
      force = true;
    }
    String namespace = cl.getArgs()[0];
    
    if (!force) {
      shellState.getReader().flush();
      String line = shellState.getReader().readLine(getName() + " { " + namespace + " } (yes|no)? ");
      operate = line != null && (line.equalsIgnoreCase("y") || line.equalsIgnoreCase("yes"));
    }
    if (operate) {
      doTableOp(shellState, namespace, force);
    }
    return 0;
  }
  
  @Override
  public String description() {
    return "deletes a table namespace";
  }
  
  protected void doTableOp(final Shell shellState, final String namespace, boolean force) throws Exception {
    boolean resetContext = false;
    String currentTable = shellState.getTableName();
    if (!TableNamespaces.getNameToIdMap(shellState.getInstance()).containsKey(namespace)) {
      throw new TableNamespaceNotFoundException(null, namespace, null);
    }
    
    String namespaceId = TableNamespaces.getNamespaceId(shellState.getInstance(), namespace);
    List<String> tables = TableNamespaces.getTableNames(shellState.getInstance(), namespaceId);
    resetContext = tables.contains(currentTable);

    if (force) {
      shellState.getConnector().tableNamespaceOperations().delete(namespace, true);
    } else {
      shellState.getConnector().tableNamespaceOperations().delete(namespace);
    }
    if (namespace.equals(Constants.SYSTEM_TABLE_NAMESPACE)) {
      shellState.getReader().println("Table namespace: [" + namespace + "], can't delete system or default namespace.");
    } else {
      shellState.getReader().println("Table namespace: [" + namespace + "] has been deleted.");
    }
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
  public void registerCompletion(final Token root, final Map<Command.CompletionSet,Set<String>> special) {
    registerCompletionForTableNamespaces(root, special);
  }
}

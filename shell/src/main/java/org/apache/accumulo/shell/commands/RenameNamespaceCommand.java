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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.NamespaceNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.clientImpl.Namespaces;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.Token;
import org.apache.commons.cli.CommandLine;

public class RenameNamespaceCommand extends Command {
  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      TableExistsException, NamespaceNotFoundException, NamespaceExistsException {
    String old = cl.getArgs()[0];
    String newer = cl.getArgs()[1];
    boolean resetContext = false;
    TableId currentTableId = null;
    if (shellState.getTableName() != null && !shellState.getTableName().isEmpty()) {
      NamespaceId namespaceId = Namespaces.getNamespaceId(shellState.getContext(), old);
      List<TableId> tableIds = Namespaces.getTableIds(shellState.getContext(), namespaceId);
      currentTableId = shellState.getContext().getTableId(shellState.getTableName());
      resetContext = tableIds.contains(currentTableId);
    }

    shellState.getAccumuloClient().namespaceOperations().rename(old, newer);

    if (resetContext) {
      shellState.setTableName(shellState.getContext().getTableName(currentTableId));
    }

    return 0;
  }

  @Override
  public String usage() {
    return getName() + " <current namespace> <new namespace>";
  }

  @Override
  public String description() {
    return "renames a namespace";
  }

  @Override
  public void registerCompletion(final Token root,
      final Map<Command.CompletionSet,Set<String>> special) {
    registerCompletionForNamespaces(root, special);
  }

  @Override
  public int numArgs() {
    return 2;
  }
}

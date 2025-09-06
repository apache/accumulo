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
import org.apache.accumulo.core.client.ResourceGroupNotFoundException;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;

public class DeleteResourceGroupCommand extends Command {

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    String resourceGroup = cl.getArgs()[0];
    try {
      shellState.getAccumuloClient().resourceGroupOperations()
          .remove(ResourceGroupId.of(resourceGroup));
    } catch (AccumuloException | AccumuloSecurityException | ResourceGroupNotFoundException e) {
      Shell.log.error("Error deleting resource group {}", resourceGroup, e);
      return 1;
    }
    return 0;
  }

  @Override
  public String description() {
    return "deletes a resource group";
  }

  @Override
  public String usage() {
    return getName() + " <ResourceGroup name>";
  }

  @Override
  public int numArgs() {
    return 1;
  }
}

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

import static org.apache.accumulo.shell.ShellUtil.readPropertiesFromFile;

import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ResourceGroupNotFoundException;
import org.apache.accumulo.core.client.admin.ResourceGroupOperations;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CreateResourceGroupCommand extends Command {

  private Option createRGPropFileOpt;

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    final String resourceGroup = cl.getArgs()[0];
    final ResourceGroupId rgid = ResourceGroupId.of(resourceGroup);
    final ResourceGroupOperations ops = shellState.getAccumuloClient().resourceGroupOperations();
    try {
      ops.create(rgid);
    } catch (AccumuloException | AccumuloSecurityException e) {
      Shell.log.error("Error creating resource group {}", resourceGroup, e);
      return 1;
    }

    String filename = cl.getOptionValue(createRGPropFileOpt.getOpt());
    if (filename != null) {
      final Map<String,String> initProperties = readPropertiesFromFile(filename);
      try {
        ops.modifyProperties(rgid, c -> c.putAll(initProperties));
      } catch (IllegalArgumentException | AccumuloException | AccumuloSecurityException
          | ResourceGroupNotFoundException e) {
        Shell.log.error("Error setting initial resource group properties for {}", rgid, e);
        return 1;
      }
    }
    return 0;
  }

  @Override
  public Options getOptions() {

    createRGPropFileOpt = new Option("f", "file", true, "user-defined initial properties file");
    createRGPropFileOpt.setArgName("properties-file");

    final Options o = new Options();
    o.addOption(createRGPropFileOpt);
    return o;
  }

  @Override
  public String description() {
    return "creates a resource group";
  }

  @Override
  public String usage() {
    return getName() + "[-f <initial-properties-file>] <ResourceGroup name>";
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }

}

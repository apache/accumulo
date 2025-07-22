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

import org.apache.accumulo.core.client.admin.ResourceGroupOperations;
import org.apache.accumulo.core.data.ResourceGroupId;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ResourceGroupCommand extends Command {

  private Option createOpt;
  private Option listOpt;
  private Option deleteOpt;

  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {

    final ResourceGroupOperations ops = shellState.getAccumuloClient().resourceGroupOperations();

    if (cl.hasOption(createOpt.getOpt())) {
      ops.createConfiguration(ResourceGroupId.of(cl.getOptionValue(createOpt)));
    } else if (cl.hasOption(deleteOpt.getOpt())) {
      ops.removeConfiguration(ResourceGroupId.of(cl.getOptionValue(deleteOpt)));
    } else if (cl.hasOption(listOpt.getOpt())) {
      shellState.printLines(ops.list().iterator(), false);
    }
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    createOpt = new Option("c", "create", true, "create a resource group");
    createOpt.setArgName("group");
    o.addOption(createOpt);

    listOpt = new Option("l", "list", false, "display resource group names");
    o.addOption(listOpt);

    deleteOpt = new Option("d", "delete", true, "delete a resource group");
    deleteOpt.setArgName("group");
    o.addOption(deleteOpt);

    return o;
  }

  @Override
  public String description() {
    return "create, list, or remove resource groups";
  }

  @Override
  public int numArgs() {
    // TODO Auto-generated method stub
    return 0;
  }

}

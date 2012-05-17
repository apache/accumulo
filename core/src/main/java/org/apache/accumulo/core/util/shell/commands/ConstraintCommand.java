/**
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

import java.util.Map.Entry;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.ShellCommandException;
import org.apache.accumulo.core.util.shell.ShellCommandException.ErrorCode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class ConstraintCommand extends Command {
  @Override
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception {
    String tableName = OptUtil.getTableOpt(cl, shellState);
    int i;
    
    switch (OptUtil.getAldOpt(cl)) {
      case ADD:
        for (String constraint : cl.getArgs()) {
          if (!shellState.getConnector().instanceOperations().testClassLoad(constraint, Constraint.class.getName()))
            throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE, "Servers are unable to load " + constraint + " as type "
                + Constraint.class.getName());
          i = shellState.getConnector().tableOperations().addConstraint(tableName, constraint);
          shellState.getReader().printString("Added constraint " + constraint + " to table " + tableName + " with number " + i + "\n");
        }
        break;
      case DELETE:
        for (String constraint : cl.getArgs()) {
          i = Integer.parseInt(constraint);
          shellState.getConnector().tableOperations().removeConstraint(tableName, i);
          shellState.getReader().printString("Removed constraint " + i + " from table " + tableName + "\n");
        }
        break;
      case LIST:
        for (Entry<String,Integer> property : shellState.getConnector().tableOperations().listConstraints(tableName).entrySet())
          shellState.getReader().printString(property.toString() + "\n");
    }
    
    return 0;
  }
  
  @Override
  public String description() {
    return "adds, deletes, or lists constraints for a table";
  }
  
  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }
  
  @Override
  public String usage() {
    return getName() + " <constraint>{ <constraint>}";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    o.addOptionGroup(OptUtil.addListDeleteGroup("constraint"));
    o.addOption(OptUtil.tableOpt("table to add, delete, or list constraints for"));
    return o;
  }
}

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

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;

public abstract class OptUtil {
  public static String configureTableOpt(CommandLine cl, Shell shellState) throws TableNotFoundException {
    String tableName;
    
    if (cl.hasOption(Shell.tableOption)) {
      tableName = cl.getOptionValue(Shell.tableOption);
      if (!shellState.getConnector().tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
    } else {
      shellState.checkTableState();
      tableName = shellState.getTableName();
    }
    
    return tableName;
  }
  
  public static Option tableOpt() {
    return tableOpt("tableName");
  }
  
  public static Option tableOpt(String description) {
    Option tableOpt = new Option(Shell.tableOption, "table", true, description);
    tableOpt.setArgName("table");
    tableOpt.setRequired(false);
    return tableOpt;
  }
  
  public static enum AdlOpt {
    ADD("a"), DELETE("d"), LIST("l");
    
    public final String opt;
    
    private AdlOpt(String opt) {
      this.opt = opt;
    }
  }
  
  public static AdlOpt configureAldOpt(CommandLine cl) {
    if (cl.hasOption(AdlOpt.ADD.opt)) {
      return AdlOpt.ADD;
    } else if (cl.hasOption(AdlOpt.DELETE.opt)) {
      return AdlOpt.DELETE;
    } else {
      return AdlOpt.LIST;
    }
  }
  
  public static OptionGroup addListDeleteGroup(String name) {
    Option addOpt = new Option(AdlOpt.ADD.opt, "add", false, "add " + name);
    Option deleteOpt = new Option(AdlOpt.DELETE.opt, "delete", false, "delete " + name);
    Option listOpt = new Option(AdlOpt.LIST.opt, "list", false, "list " + name + "(s)");
    OptionGroup og = new OptionGroup();
    og.addOption(addOpt);
    og.addOption(deleteOpt);
    og.addOption(listOpt);
    og.setRequired(true);
    return og;
  }
}

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

import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;

public class MaxRowCommand extends ScanCommand {
  private Option tableOpt, optAuths, optStartRow, optEndRow, optStartRowExclusice, optEndRowExclusice;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      IOException, ParseException {
    
    String tableName;
    
    if (cl.hasOption(tableOpt.getOpt())) {
      tableName = cl.getOptionValue(tableOpt.getOpt());
      if (!shellState.getConnector().tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
    }
    
    else {
      shellState.checkTableState();
      tableName = shellState.getTableName();
    }
    
    Text startRow = cl.hasOption(optStartRow.getOpt()) ? new Text(cl.getOptionValue(optStartRow.getOpt())) : null;
    Text endRow = cl.hasOption(optEndRow.getOpt()) ? new Text(cl.getOptionValue(optEndRow.getOpt())) : null;
    
    boolean startInclusive = !cl.hasOption(optStartRowExclusice.getOpt());
    boolean endInclusive = !cl.hasOption(optEndRowExclusice.getOpt());
    try {
      Text max = shellState.getConnector().tableOperations().getMaxRow(tableName, getAuths(cl, shellState), startRow, startInclusive, endRow, endInclusive);
      if (max != null)
        shellState.getReader().printString(max.toString() + "\n");
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    return 0;
  }
  
  protected Authorizations getAuths(CommandLine cl, Shell shellState) throws AccumuloSecurityException, AccumuloException {
    String user = shellState.getConnector().whoami();
    Authorizations auths = shellState.getConnector().securityOperations().getUserAuthorizations(user);
    if (cl.hasOption(optAuths.getOpt())) {
      auths = CreateUserCommand.parseAuthorizations(cl.getOptionValue(optAuths.getOpt()));
    }
    return auths;
  }
  
  @Override
  public String description() {
    return "find the max row in a table within a given range";
  }
  
  @Override
  public Options getOptions() {
    
    Options opts = new Options();
    
    tableOpt = new Option(Shell.tableOption, "table", true, "table to be created");
    tableOpt.setArgName("table");
    
    optAuths = new Option("s", "scan-authorizations", true, "scan authorizations (all user auths are used if this argument is not specified)");
    optAuths.setArgName("comma-separated-authorizations");
    
    optStartRow = new Option("b", "begin-row", true, "begin row");
    optStartRow.setArgName("begin-row");
    
    optEndRow = new Option("e", "end-row", true, "end row");
    optEndRow.setArgName("end-row");
    
    optStartRowExclusice = new Option("be", "begin-exclusive", false, "make start row exclusive, by defaults it inclusive");
    optStartRowExclusice.setArgName("begin-exclusive");
    
    optEndRowExclusice = new Option("ee", "end-exclusive", false, "make end row exclusive, by defaults it inclusive");
    optEndRowExclusice.setArgName("end-exclusive");
    
    opts.addOption(tableOpt);
    opts.addOption(optAuths);
    opts.addOption(optStartRow);
    opts.addOption(optEndRow);
    opts.addOption(optStartRowExclusice);
    opts.addOption(optEndRowExclusice);
    
    return opts;
  }
}
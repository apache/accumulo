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

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

@Deprecated
// deprecated since 1.4
public class SelectCommand extends Command {
  
  private Option selectOptAuths, timestampOpt, disablePaginationOpt, tableOpt;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      IOException {
    String tableName;
    
    shellState.log.warn("select is deprecated, use 'scan -r <row> -c <columnfamily>[:<columnqualifier>]'");

    if (cl.hasOption(tableOpt.getOpt())) {
      tableName = cl.getOptionValue(tableOpt.getOpt());
      if (!shellState.getConnector().tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
    }
    
    else {
      shellState.checkTableState();
      tableName = shellState.getTableName();
    }
    
    Authorizations authorizations = cl.hasOption(selectOptAuths.getOpt()) ? CreateUserCommand.parseAuthorizations(cl.getOptionValue(selectOptAuths.getOpt()))
        : Constants.NO_AUTHS;
    Scanner scanner = shellState.getConnector().createScanner(tableName.toString(), authorizations);
    
    Key key = new Key(new Text(cl.getArgs()[0]), new Text(cl.getArgs()[1]), new Text(cl.getArgs()[2]));
    scanner.setRange(new Range(key, key.followingKey(PartialKey.ROW_COLFAM_COLQUAL)));
    
    // output the records
    shellState.printRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()));
    
    return 0;
  }
  
  @Override
  public String description() {
    return "scans for and displays a single record";
  }
  
  @Override
  public String usage() {
    return getName() + " <row> <columnfamily> <columnqualifier>";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    selectOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations");
    selectOptAuths.setArgName("comma-separated-authorizations");
    timestampOpt = new Option("st", "show-timestamps", false, "enables displaying timestamps");
    disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
    tableOpt = new Option(Shell.tableOption, "tableName", true, "table");
    tableOpt.setArgName("table");
    tableOpt.setRequired(false);
    
    o.addOption(selectOptAuths);
    o.addOption(timestampOpt);
    o.addOption(disablePaginationOpt);
    o.addOption(tableOpt);
    return o;
  }
  
  @Override
  public int numArgs() {
    return 3;
  }
}
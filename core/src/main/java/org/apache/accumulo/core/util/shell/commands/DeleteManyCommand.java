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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.DeleterFormatter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DeleteManyCommand extends ScanCommand {
  private Option forceOpt;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      IOException, ParseException {
    String tableName = OptUtil.getTableOpt(cl, shellState);
    
    // handle first argument, if present, the authorizations list to
    // scan with
    Authorizations auths = getAuths(cl, shellState);
    final Scanner scanner = shellState.getConnector().createScanner(tableName, auths);
    
    scanner.addScanIterator(new IteratorSetting(Integer.MAX_VALUE, "NOVALUE", SortedKeyIterator.class));
    
    // handle remaining optional arguments
    scanner.setRange(getRange(cl));
    
    // handle columns
    fetchColumns(cl, scanner);
    
    // output / delete the records
    BatchWriter writer = shellState.getConnector().createBatchWriter(tableName, 1024 * 1024, 1000L, 4);
    shellState.printLines(new DeleterFormatter(writer, scanner, cl.hasOption(timestampOpt.getOpt()), shellState, cl.hasOption(forceOpt.getOpt())), false);
    
    return 0;
  }
  
  @Override
  public String description() {
    return "scans a table and deletes the resulting records";
  }
  
  @Override
  public Options getOptions() {
    forceOpt = new Option("f", "force", false, "force deletion without prompting");
    Options opts = super.getOptions();
    opts.addOption(forceOpt);
    opts.addOption(OptUtil.tableOpt("table to delete entries from"));
    return opts;
  }
  
}

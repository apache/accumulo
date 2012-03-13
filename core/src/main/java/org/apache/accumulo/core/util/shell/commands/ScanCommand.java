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
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.BinaryFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.start.classloader.AccumuloClassLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;

public class ScanCommand extends Command {
  
  private Option scanOptAuths, scanOptStartRow, scanOptEndRow, scanOptRow, scanOptColumns, disablePaginationOpt, tableOpt, showFewOpt, formatterOpt;
  protected Option timestampOpt;
  
  public int execute(String fullCommand, CommandLine cl, Shell shellState) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      IOException, ParseException {
    
    String tableName;
    Class<? extends Formatter> formatter = null;
    
    if (cl.hasOption(tableOpt.getOpt())) {
      tableName = cl.getOptionValue(tableOpt.getOpt());
      if (!shellState.getConnector().tableOperations().exists(tableName))
        throw new TableNotFoundException(null, tableName, null);
      
      // Use the configured formatter unless one was provided
      if (!cl.hasOption(formatterOpt.getOpt())) {
        formatter = FormatterCommand.getCurrentFormatter(tableName, shellState);
      }
    } else {
      shellState.checkTableState();
      tableName = shellState.getTableName();
    }

    // handle first argument, if present, the authorizations list to
    // scan with
    Authorizations auths = getAuths(cl, shellState);
    Scanner scanner = shellState.getConnector().createScanner(tableName, auths);
    
    // handle session-specific scan iterators
    addScanIterators(shellState, scanner, tableName);
    
    // handle remaining optional arguments
    scanner.setRange(getRange(cl));
    
    // handle columns
    fetchColumns(cl, scanner);
    
    // output the records
    if (cl.hasOption(showFewOpt.getOpt())) {
      String showLength = cl.getOptionValue(showFewOpt.getOpt());
      try {
        int length = Integer.parseInt(showLength);
        if (length < 1) {
          throw new IllegalArgumentException();
        }
        BinaryFormatter.getlength(length);
        printBinaryRecords(cl, shellState, scanner);
      } catch (NumberFormatException nfe) {
        shellState.getReader().printString("Arg must be an integer. \n");
      } catch (IllegalArgumentException iae) {
        shellState.getReader().printString("Arg must be greater than one. \n");
      }
      
    } else {
      if (null == formatter) {
        printRecords(cl, shellState, scanner);
      } else {
        printRecords(cl, shellState, scanner, formatter);
      }
    }
    
    return 0;
  }
  
  protected void addScanIterators(Shell shellState, Scanner scanner, String tableName) {
    List<IteratorSetting> tableScanIterators = shellState.scanIteratorOptions.get(shellState.getTableName());
    if (tableScanIterators == null) {
      Shell.log.debug("Found no scan iterators to set");
      return;
    }
    Shell.log.debug("Found " + tableScanIterators.size() + " scan iterators to set");
    
    for (IteratorSetting setting : tableScanIterators) {
      Shell.log.debug("Setting scan iterator " + setting.getName() + " at priority " + setting.getPriority() + " using class name "
          + setting.getIteratorClass());
      for (Entry<String,String> option : setting.getProperties().entrySet()) {
        Shell.log.debug("Setting option for " + setting.getName() + ": " + option.getKey() + "=" + option.getValue());
      }
      scanner.addScanIterator(setting);
    }
  }
  
  protected void printRecords(CommandLine cl, Shell shellState, Iterable<Entry<Key,Value>> scanner) throws IOException {
    if (cl.hasOption(formatterOpt.getOpt())) {
      try {
        String className = cl.getOptionValue(formatterOpt.getOpt());
        Class<? extends Formatter> formatterClass = AccumuloClassLoader.loadClass(className, Formatter.class);
        
        printRecords(cl, shellState, scanner, formatterClass);
      } catch (ClassNotFoundException e) {
        shellState.getReader().printString("Formatter class could not be loaded.\n" + e.getMessage() + "\n");
      }
    } else {
      shellState.printRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()));
    }
  }
  
  protected void printRecords(CommandLine cl, Shell shellState, Iterable<Entry<Key,Value>> scanner, Class<? extends Formatter> formatter) throws IOException {
    shellState.printRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()), formatter);
  }
  
  protected void printBinaryRecords(CommandLine cl, Shell shellState, Iterable<Entry<Key,Value>> scanner) throws IOException {
    shellState.printBinaryRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()));
  }
  
  protected void fetchColumns(CommandLine cl, ScannerBase scanner) {
    if (cl.hasOption(scanOptColumns.getOpt())) {
      for (String a : cl.getOptionValue(scanOptColumns.getOpt()).split(",")) {
        String sa[] = a.split(":", 2);
        if (sa.length == 1)
          scanner.fetchColumnFamily(new Text(a));
        else
          scanner.fetchColumn(new Text(sa[0]), new Text(sa[1]));
      }
    }
  }
  
  protected Range getRange(CommandLine cl) {
    if ((cl.hasOption(scanOptStartRow.getOpt()) || cl.hasOption(scanOptEndRow.getOpt())) && cl.hasOption(scanOptRow.getOpt())) {
      // did not see a way to make commons cli do this check... it has mutually exclusive options but does not support the or
      throw new IllegalArgumentException("Options -" + scanOptRow.getOpt() + " AND (-" + scanOptStartRow.getOpt() + " OR -" + scanOptEndRow.getOpt()
          + ") are mutally exclusive ");
    }

    if (cl.hasOption(scanOptRow.getOpt())) {
      return new Range(new Text(cl.getOptionValue(scanOptRow.getOpt())));
    } else {
      Text startRow = cl.hasOption(scanOptStartRow.getOpt()) ? new Text(cl.getOptionValue(scanOptStartRow.getOpt())) : null;
      Text endRow = cl.hasOption(scanOptEndRow.getOpt()) ? new Text(cl.getOptionValue(scanOptEndRow.getOpt())) : null;
      return new Range(startRow, endRow);
    }
  }
  
  protected Authorizations getAuths(CommandLine cl, Shell shellState) throws AccumuloSecurityException, AccumuloException {
    String user = shellState.getConnector().whoami();
    Authorizations auths = shellState.getConnector().securityOperations().getUserAuthorizations(user);
    if (cl.hasOption(scanOptAuths.getOpt())) {
      auths = CreateUserCommand.parseAuthorizations(cl.getOptionValue(scanOptAuths.getOpt()));
    }
    return auths;
  }
  
  @Override
  public String description() {
    return "scans the table, and displays the resulting records";
  }
  
  @Override
  public Options getOptions() {
    Options o = new Options();
    
    scanOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations (all user auths are used if this argument is not specified)");
    scanOptStartRow = new Option("b", "begin-row", true, "begin row (inclusive)");
    scanOptEndRow = new Option("e", "end-row", true, "end row (inclusive)");
    scanOptRow = new Option("r", "row", true, "row to scan");
    scanOptColumns = new Option("c", "columns", true, "comma-separated columns");
    timestampOpt = new Option("st", "show-timestamps", false, "enables displaying timestamps");
    disablePaginationOpt = new Option("np", "no-pagination", false, "disables pagination of output");
    tableOpt = new Option(Shell.tableOption, "tableName", true, "table to be scanned");
    showFewOpt = new Option("f", "show few", true, "Only shows certain amount of characters");
    formatterOpt = new Option("fm", "formatter", true, "fully qualified name of the formatter class to use");
    
    scanOptAuths.setArgName("comma-separated-authorizations");
    scanOptRow.setArgName("row");
    scanOptStartRow.setArgName("start-row");
    scanOptEndRow.setArgName("end-row");
    scanOptColumns.setArgName("<columnfamily>[:<columnqualifier>]{,<columnfamily>[:<columnqualifier>]}");
    tableOpt.setArgName("table");
    tableOpt.setRequired(false);
    showFewOpt.setRequired(false);
    showFewOpt.setArgName("int");
    formatterOpt.setArgName("className");
    
    o.addOption(scanOptAuths);
    o.addOption(scanOptRow);
    o.addOption(scanOptStartRow);
    o.addOption(scanOptEndRow);
    o.addOption(scanOptColumns);
    o.addOption(timestampOpt);
    o.addOption(disablePaginationOpt);
    o.addOption(tableOpt);
    o.addOption(showFewOpt);
    o.addOption(formatterOpt);
    
    return o;
  }
  
  @Override
  public int numArgs() {
    return 0;
  }
}

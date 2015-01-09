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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.BinaryFormatter;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.interpret.DefaultScanInterpreter;
import org.apache.accumulo.core.util.interpret.ScanInterpreter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Shell.PrintFile;
import org.apache.accumulo.start.classloader.vfs.AccumuloVFSClassLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class ScanCommand extends Command {

  private Option scanOptAuths, scanOptRow, scanOptColumns, disablePaginationOpt, showFewOpt, formatterOpt, interpreterOpt, formatterInterpeterOpt,
      outputFileOpt;

  protected Option timestampOpt;
  private Option optStartRowExclusive;
  private Option optEndRowExclusive;
  private Option timeoutOption;
  private Option profileOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    final PrintFile printFile = getOutputFile(cl);
    final String tableName = OptUtil.getTableOpt(cl, shellState);

    final Class<? extends Formatter> formatter = getFormatter(cl, tableName, shellState);
    final ScanInterpreter interpeter = getInterpreter(cl, tableName, shellState);

    // handle first argument, if present, the authorizations list to
    // scan with
    final Authorizations auths = getAuths(cl, shellState);
    final Scanner scanner = shellState.getConnector().createScanner(tableName, auths);

    // handle session-specific scan iterators
    addScanIterators(shellState, cl, scanner, tableName);

    // handle remaining optional arguments
    scanner.setRange(getRange(cl, interpeter));

    // handle columns
    fetchColumns(cl, scanner, interpeter);

    // set timeout
    scanner.setTimeout(getTimeout(cl), TimeUnit.MILLISECONDS);

    // output the records
    if (cl.hasOption(showFewOpt.getOpt())) {
      final String showLength = cl.getOptionValue(showFewOpt.getOpt());
      try {
        final int length = Integer.parseInt(showLength);
        if (length < 1) {
          throw new IllegalArgumentException();
        }
        BinaryFormatter.getlength(length);
        printBinaryRecords(cl, shellState, scanner, printFile);
      } catch (NumberFormatException nfe) {
        shellState.getReader().println("Arg must be an integer.");
      } catch (IllegalArgumentException iae) {
        shellState.getReader().println("Arg must be greater than one.");
      }

    } else {
      printRecords(cl, shellState, scanner, formatter, printFile);
    }
    if (printFile != null) {
      printFile.close();
    }

    return 0;
  }

  protected long getTimeout(final CommandLine cl) {
    if (cl.hasOption(timeoutOption.getLongOpt())) {
      return AccumuloConfiguration.getTimeInMillis(cl.getOptionValue(timeoutOption.getLongOpt()));
    }

    return Long.MAX_VALUE;
  }

  protected void addScanIterators(final Shell shellState, CommandLine cl, final Scanner scanner, final String tableName) {

    List<IteratorSetting> tableScanIterators;
    if (cl.hasOption(profileOpt.getOpt())) {
      String profile = cl.getOptionValue(profileOpt.getOpt());
      tableScanIterators = shellState.iteratorProfiles.get(profile);

      if (tableScanIterators == null) {
        throw new IllegalArgumentException("Profile " + profile + " does not exist");
      }
    } else {
      tableScanIterators = shellState.scanIteratorOptions.get(tableName);
      if (tableScanIterators == null) {
        Shell.log.debug("Found no scan iterators to set");
        return;
      }
    }

    Shell.log.debug("Found " + tableScanIterators.size() + " scan iterators to set");

    for (IteratorSetting setting : tableScanIterators) {
      Shell.log.debug("Setting scan iterator " + setting.getName() + " at priority " + setting.getPriority() + " using class name "
          + setting.getIteratorClass());
      for (Entry<String,String> option : setting.getOptions().entrySet()) {
        Shell.log.debug("Setting option for " + setting.getName() + ": " + option.getKey() + "=" + option.getValue());
      }
      scanner.addScanIterator(setting);
    }
  }

  protected void printRecords(final CommandLine cl, final Shell shellState, final Iterable<Entry<Key,Value>> scanner, final Class<? extends Formatter> formatter)
      throws IOException {
    printRecords(cl, shellState, scanner, formatter, null);
  }

  protected void printRecords(final CommandLine cl, final Shell shellState, final Iterable<Entry<Key,Value>> scanner,
      final Class<? extends Formatter> formatter, PrintFile outFile) throws IOException {
    if (outFile == null) {
      shellState.printRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()), formatter);
    } else {
      shellState.printRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()), formatter, outFile);
    }
  }

  protected void printBinaryRecords(final CommandLine cl, final Shell shellState, final Iterable<Entry<Key,Value>> scanner) throws IOException {
    printBinaryRecords(cl, shellState, scanner, null);
  }

  protected void printBinaryRecords(final CommandLine cl, final Shell shellState, final Iterable<Entry<Key,Value>> scanner, PrintFile outFile)
      throws IOException {
    if (outFile == null) {
      shellState.printBinaryRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()));
    } else {
      shellState.printBinaryRecords(scanner, cl.hasOption(timestampOpt.getOpt()), !cl.hasOption(disablePaginationOpt.getOpt()), outFile);
    }
  }

  protected ScanInterpreter getInterpreter(final CommandLine cl, final String tableName, final Shell shellState) throws Exception {

    Class<? extends ScanInterpreter> clazz = null;
    try {
      if (cl.hasOption(interpreterOpt.getOpt())) {
        clazz = AccumuloVFSClassLoader.loadClass(cl.getOptionValue(interpreterOpt.getOpt()), ScanInterpreter.class);
      } else if (cl.hasOption(formatterInterpeterOpt.getOpt())) {
        clazz = AccumuloVFSClassLoader.loadClass(cl.getOptionValue(formatterInterpeterOpt.getOpt()), ScanInterpreter.class);
      }
    } catch (ClassNotFoundException e) {
      shellState.getReader().println("Interpreter class could not be loaded.\n" + e.getMessage());
    }

    if (clazz == null)
      clazz = InterpreterCommand.getCurrentInterpreter(tableName, shellState);

    if (clazz == null)
      clazz = DefaultScanInterpreter.class;

    return clazz.newInstance();
  }

  protected Class<? extends Formatter> getFormatter(final CommandLine cl, final String tableName, final Shell shellState) throws IOException {

    try {
      if (cl.hasOption(formatterOpt.getOpt())) {
        return shellState.getClassLoader(cl, shellState).loadClass(cl.getOptionValue(formatterOpt.getOpt())).asSubclass(Formatter.class);
      } else if (cl.hasOption(formatterInterpeterOpt.getOpt())) {
        return shellState.getClassLoader(cl, shellState).loadClass(cl.getOptionValue(formatterInterpeterOpt.getOpt())).asSubclass(Formatter.class);
      }
    } catch (Exception e) {
      shellState.getReader().println("Formatter class could not be loaded.\n" + e.getMessage());
    }

    return shellState.getFormatter(tableName);
  }

  protected void fetchColumns(final CommandLine cl, final ScannerBase scanner, final ScanInterpreter formatter) throws UnsupportedEncodingException {
    if (cl.hasOption(scanOptColumns.getOpt())) {
      for (String a : cl.getOptionValue(scanOptColumns.getOpt()).split(",")) {
        final String sa[] = a.split(":", 2);
        if (sa.length == 1) {
          scanner.fetchColumnFamily(formatter.interpretColumnFamily(new Text(a.getBytes(Shell.CHARSET))));
        } else {
          scanner.fetchColumn(formatter.interpretColumnFamily(new Text(sa[0].getBytes(Shell.CHARSET))),
              formatter.interpretColumnQualifier(new Text(sa[1].getBytes(Shell.CHARSET))));
        }
      }
    }
  }

  protected Range getRange(final CommandLine cl, final ScanInterpreter formatter) throws UnsupportedEncodingException {
    if ((cl.hasOption(OptUtil.START_ROW_OPT) || cl.hasOption(OptUtil.END_ROW_OPT)) && cl.hasOption(scanOptRow.getOpt())) {
      // did not see a way to make commons cli do this check... it has mutually exclusive options but does not support the or
      throw new IllegalArgumentException("Options -" + scanOptRow.getOpt() + " AND (-" + OptUtil.START_ROW_OPT + " OR -" + OptUtil.END_ROW_OPT
          + ") are mutally exclusive ");
    }

    if (cl.hasOption(scanOptRow.getOpt())) {
      return new Range(formatter.interpretRow(new Text(cl.getOptionValue(scanOptRow.getOpt()).getBytes(Shell.CHARSET))));
    } else {
      Text startRow = OptUtil.getStartRow(cl);
      if (startRow != null)
        startRow = formatter.interpretBeginRow(startRow);
      Text endRow = OptUtil.getEndRow(cl);
      if (endRow != null)
        endRow = formatter.interpretEndRow(endRow);
      final boolean startInclusive = !cl.hasOption(optStartRowExclusive.getOpt());
      final boolean endInclusive = !cl.hasOption(optEndRowExclusive.getOpt());
      return new Range(startRow, startInclusive, endRow, endInclusive);
    }
  }

  protected Authorizations getAuths(final CommandLine cl, final Shell shellState) throws AccumuloSecurityException, AccumuloException {
    final String user = shellState.getConnector().whoami();
    Authorizations auths = shellState.getConnector().securityOperations().getUserAuthorizations(user);
    if (cl.hasOption(scanOptAuths.getOpt())) {
      auths = ScanCommand.parseAuthorizations(cl.getOptionValue(scanOptAuths.getOpt()));
    }
    return auths;
  }

  static Authorizations parseAuthorizations(final String field) {
    if (field == null || field.isEmpty()) {
      return Authorizations.EMPTY;
    }
    return new Authorizations(field.split(","));
  }

  @Override
  public String description() {
    return "scans the table, and displays the resulting records";
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    scanOptAuths = new Option("s", "scan-authorizations", true, "scan authorizations (all user auths are used if this argument is not specified)");
    optStartRowExclusive = new Option("be", "begin-exclusive", false, "make start row exclusive (by default it's inclusive)");
    optStartRowExclusive.setArgName("begin-exclusive");
    optEndRowExclusive = new Option("ee", "end-exclusive", false, "make end row exclusive (by default it's inclusive)");
    optEndRowExclusive.setArgName("end-exclusive");
    scanOptRow = new Option("r", "row", true, "row to scan");
    scanOptColumns = new Option("c", "columns", true, "comma-separated columns");
    timestampOpt = new Option("st", "show-timestamps", false, "display timestamps");
    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    showFewOpt = new Option("f", "show-few", true, "show only a specified number of characters");
    formatterOpt = new Option("fm", "formatter", true, "fully qualified name of the formatter class to use");
    interpreterOpt = new Option("i", "interpreter", true, "fully qualified name of the interpreter class to use");
    formatterInterpeterOpt = new Option("fi", "fmt-interpreter", true, "fully qualified name of a class that is a formatter and interpreter");
    timeoutOption = new Option(null, "timeout", true,
        "time before scan should fail if no data is returned. If no unit is given assumes seconds.  Units d,h,m,s,and ms are supported.  e.g. 30s or 100ms");
    outputFileOpt = new Option("o", "output", true, "local file to write the scan output to");

    scanOptAuths.setArgName("comma-separated-authorizations");
    scanOptRow.setArgName("row");
    scanOptColumns.setArgName("<columnfamily>[:<columnqualifier>]{,<columnfamily>[:<columnqualifier>]}");
    showFewOpt.setRequired(false);
    showFewOpt.setArgName("int");
    formatterOpt.setArgName("className");
    timeoutOption.setArgName("timeout");
    outputFileOpt.setArgName("file");

    profileOpt = new Option("pn", "profile", true, "iterator profile name");
    profileOpt.setArgName("profile");

    o.addOption(scanOptAuths);
    o.addOption(scanOptRow);
    o.addOption(OptUtil.startRowOpt());
    o.addOption(OptUtil.endRowOpt());
    o.addOption(optStartRowExclusive);
    o.addOption(optEndRowExclusive);
    o.addOption(scanOptColumns);
    o.addOption(timestampOpt);
    o.addOption(disablePaginationOpt);
    o.addOption(OptUtil.tableOpt("table to be scanned"));
    o.addOption(showFewOpt);
    o.addOption(formatterOpt);
    o.addOption(interpreterOpt);
    o.addOption(formatterInterpeterOpt);
    o.addOption(timeoutOption);
    o.addOption(outputFileOpt);
    o.addOption(profileOpt);

    return o;
  }

  @Override
  public int numArgs() {
    return 0;
  }

  protected PrintFile getOutputFile(final CommandLine cl) throws FileNotFoundException {
    final String outputFile = cl.getOptionValue(outputFileOpt.getOpt());
    return (outputFile == null ? null : new PrintFile(outputFile));
  }
}

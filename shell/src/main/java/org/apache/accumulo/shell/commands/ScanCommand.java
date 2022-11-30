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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.ConfigurationTypeHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.Shell.PrintFile;
import org.apache.accumulo.shell.ShellCommandException;
import org.apache.accumulo.shell.ShellCommandException.ErrorCode;
import org.apache.accumulo.shell.ShellUtil;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class ScanCommand extends Command {

  private Option scanOptAuths, scanOptRow, scanOptColumns, disablePaginationOpt, showFewOpt,
      formatterOpt, interpreterOpt, formatterInterpeterOpt, outputFileOpt, scanOptCf, scanOptCq;

  protected Option timestampOpt;
  protected Option profileOpt;
  private Option optStartRowExclusive;
  private Option optStartRowInclusive;
  private Option optEndRowExclusive;
  private Option timeoutOption;
  private Option sampleOpt;
  private Option contextOpt;
  private Option executionHintsOpt;
  private Option scanServerOpt;

  protected void setupSampling(final String tableName, final CommandLine cl, final Shell shellState,
      ScannerBase scanner)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException {
    if (getUseSample(cl)) {
      SamplerConfiguration samplerConfig =
          shellState.getAccumuloClient().tableOperations().getSamplerConfiguration(tableName);
      if (samplerConfig == null) {
        throw new SampleNotPresentException(
            "Table " + tableName + " does not have sampling configured");
      }
      Shell.log.debug("Using sampling configuration : {}", samplerConfig);
      scanner.setSamplerConfiguration(samplerConfig);
    }
  }

  protected ConsistencyLevel getConsistency(CommandLine cl) {
    if (cl.hasOption(scanServerOpt.getOpt())) {
      String arg = cl.getOptionValue(scanServerOpt.getOpt());
      return ConsistencyLevel.valueOf(arg.toUpperCase());
    } else {
      return ConsistencyLevel.IMMEDIATE;
    }
  }

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    try (final PrintFile printFile = getOutputFile(cl)) {
      final String tableName = OptUtil.getTableOpt(cl, shellState);

      final Class<? extends Formatter> formatter = getFormatter(cl, tableName, shellState);
      @SuppressWarnings("deprecation")
      final org.apache.accumulo.core.util.interpret.ScanInterpreter interpeter =
          getInterpreter(cl, tableName, shellState);

      String classLoaderContext = null;
      if (cl.hasOption(contextOpt.getOpt())) {
        classLoaderContext = cl.getOptionValue(contextOpt.getOpt());
      }
      // handle first argument, if present, the authorizations list to
      // scan with
      final Authorizations auths = getAuths(cl, shellState);
      final Scanner scanner = shellState.getAccumuloClient().createScanner(tableName, auths);
      if (classLoaderContext != null) {
        scanner.setClassLoaderContext(classLoaderContext);
      }
      // handle session-specific scan iterators
      addScanIterators(shellState, cl, scanner, tableName);

      // handle remaining optional arguments
      scanner.setRange(getRange(cl, interpeter));

      // handle columns
      fetchColumns(cl, scanner, interpeter);
      fetchColumsWithCFAndCQ(cl, scanner, interpeter);

      // set timeout
      scanner.setTimeout(getTimeout(cl), TimeUnit.MILLISECONDS);

      setupSampling(tableName, cl, shellState, scanner);

      scanner.setExecutionHints(ShellUtil.parseMapOpt(cl, executionHintsOpt));

      try {
        scanner.setConsistencyLevel(getConsistency(cl));
      } catch (IllegalArgumentException e) {
        Shell.log.error("Consistency Level argument must be immediate or eventual", e);
      }

      // output the records

      final FormatterConfig config = new FormatterConfig();
      config.setPrintTimestamps(cl.hasOption(timestampOpt.getOpt()));
      if (cl.hasOption(showFewOpt.getOpt())) {
        final String showLength = cl.getOptionValue(showFewOpt.getOpt());
        try {
          final int length = Integer.parseInt(showLength);
          config.setShownLength(length);
        } catch (NumberFormatException nfe) {
          Shell.log.error("Arg must be an integer.", nfe);
        } catch (IllegalArgumentException iae) {
          Shell.log.error("Arg must be greater than one.", iae);
        }
      }
      printRecords(cl, shellState, config, scanner, formatter, printFile);
    }

    return 0;
  }

  protected boolean getUseSample(CommandLine cl) {
    return cl.hasOption(sampleOpt.getLongOpt());
  }

  protected long getTimeout(final CommandLine cl) {
    if (cl.hasOption(timeoutOption.getLongOpt())) {
      return ConfigurationTypeHelper.getTimeInMillis(cl.getOptionValue(timeoutOption.getLongOpt()));
    }

    return Long.MAX_VALUE;
  }

  static void ensureTserversCanLoadIterator(final Shell shellState, String tableName,
      String classname) throws AccumuloException, AccumuloSecurityException, TableNotFoundException,
      ShellCommandException {
    if (!shellState.getAccumuloClient().tableOperations().testClassLoad(tableName, classname,
        SortedKeyValueIterator.class.getName())) {
      throw new ShellCommandException(ErrorCode.INITIALIZATION_FAILURE,
          "Servers are unable to load " + classname + " as type "
              + SortedKeyValueIterator.class.getName());
    }
  }

  protected void addScanIterators(final Shell shellState, CommandLine cl, final ScannerBase scanner,
      final String tableName) throws Exception {

    List<IteratorSetting> tableScanIterators;
    if (cl.hasOption(profileOpt.getOpt())) {
      String profile = cl.getOptionValue(profileOpt.getOpt());
      tableScanIterators = shellState.iteratorProfiles.get(profile);

      if (tableScanIterators == null) {
        throw new IllegalArgumentException("Profile " + profile + " does not exist");
      }

      for (IteratorSetting iteratorSetting : tableScanIterators) {
        ensureTserversCanLoadIterator(shellState, tableName, iteratorSetting.getIteratorClass());
      }
    } else {
      tableScanIterators = shellState.scanIteratorOptions.get(tableName);
      if (tableScanIterators == null) {
        Shell.log.debug("Found no scan iterators to set");
        return;
      }
    }

    Shell.log.debug("Found {} scan iterators to set", tableScanIterators.size());

    for (IteratorSetting setting : tableScanIterators) {
      Shell.log.debug("Setting scan iterator {} at priority {} using class name {}",
          setting.getName(), setting.getPriority(), setting.getIteratorClass());
      for (Entry<String,String> option : setting.getOptions().entrySet()) {
        Shell.log.debug("Setting option for {}: {}={}", setting.getName(), option.getKey(),
            option.getValue());
      }
      scanner.addScanIterator(setting);
    }
  }

  protected void printRecords(final CommandLine cl, final Shell shellState, FormatterConfig config,
      final Iterable<Entry<Key,Value>> scanner, final Class<? extends Formatter> formatter,
      PrintFile outFile) throws IOException {
    if (outFile == null) {
      shellState.printRecords(scanner, config, !cl.hasOption(disablePaginationOpt.getOpt()),
          formatter);
    } else {
      shellState.printRecords(scanner, config, !cl.hasOption(disablePaginationOpt.getOpt()),
          formatter, outFile);
    }
  }

  @Deprecated(since = "2.1.0")
  protected org.apache.accumulo.core.util.interpret.ScanInterpreter getInterpreter(
      final CommandLine cl, final String tableName, final Shell shellState) throws Exception {

    Class<? extends org.apache.accumulo.core.util.interpret.ScanInterpreter> clazz = null;
    try {
      if (cl.hasOption(interpreterOpt.getOpt())) {
        Shell.log
            .warn("Scan Interpreter option is deprecated and will be removed in a future version.");

        clazz = ClassLoaderUtil.loadClass(cl.getOptionValue(interpreterOpt.getOpt()),
            org.apache.accumulo.core.util.interpret.ScanInterpreter.class);
      } else if (cl.hasOption(formatterInterpeterOpt.getOpt())) {
        Shell.log
            .warn("Scan Interpreter option is deprecated and will be removed in a future version.");

        clazz = ClassLoaderUtil.loadClass(cl.getOptionValue(formatterInterpeterOpt.getOpt()),
            org.apache.accumulo.core.util.interpret.ScanInterpreter.class);
      }
    } catch (ClassNotFoundException e) {
      Shell.log.error("Interpreter class could not be loaded.", e);
    }

    if (clazz == null) {
      clazz = InterpreterCommand.getCurrentInterpreter(tableName, shellState);
    }

    if (clazz == null) {
      clazz = org.apache.accumulo.core.util.interpret.DefaultScanInterpreter.class;
    }

    return clazz.getDeclaredConstructor().newInstance();
  }

  protected Class<? extends Formatter> getFormatter(final CommandLine cl, final String tableName,
      final Shell shellState) throws IOException {

    try {
      if (cl.hasOption(formatterOpt.getOpt())) {
        Shell.log.warn("Formatter option is deprecated and will be removed in a future version.");

        return shellState.getClassLoader(cl, shellState)
            .loadClass(cl.getOptionValue(formatterOpt.getOpt())).asSubclass(Formatter.class);
      } else if (cl.hasOption(formatterInterpeterOpt.getOpt())) {
        Shell.log.warn("Formatter option is deprecated and will be removed in a future version.");

        return shellState.getClassLoader(cl, shellState)
            .loadClass(cl.getOptionValue(formatterInterpeterOpt.getOpt()))
            .asSubclass(Formatter.class);
      }
    } catch (Exception e) {
      Shell.log.error("Formatter class could not be loaded.", e);
    }

    return shellState.getFormatter(tableName);
  }

  protected void fetchColumns(final CommandLine cl, final ScannerBase scanner,
      @SuppressWarnings("deprecation") final org.apache.accumulo.core.util.interpret.ScanInterpreter formatter)
      throws UnsupportedEncodingException {

    if ((cl.hasOption(scanOptCf.getOpt()) || cl.hasOption(scanOptCq.getOpt()))
        && cl.hasOption(scanOptColumns.getOpt())) {

      String formattedString =
          String.format("Option -%s is mutually exclusive with options -%s and -%s.",
              scanOptColumns.getOpt(), scanOptCf.getOpt(), scanOptCq.getOpt());
      throw new IllegalArgumentException(formattedString);
    }

    if (cl.hasOption(scanOptColumns.getOpt())) {
      for (String a : cl.getOptionValue(scanOptColumns.getOpt()).split(",")) {
        final String[] sa = a.split(":", 2);
        if (sa.length == 1) {
          @SuppressWarnings("deprecation")
          var interprettedCF = formatter.interpretColumnFamily(new Text(a.getBytes(Shell.CHARSET)));
          scanner.fetchColumnFamily(interprettedCF);
        } else {
          @SuppressWarnings("deprecation")
          var interprettedCF =
              formatter.interpretColumnFamily(new Text(sa[0].getBytes(Shell.CHARSET)));
          @SuppressWarnings("deprecation")
          var interprettedCQ =
              formatter.interpretColumnQualifier(new Text(sa[1].getBytes(Shell.CHARSET)));
          scanner.fetchColumn(interprettedCF, interprettedCQ);
        }
      }
    }
  }

  private void fetchColumsWithCFAndCQ(CommandLine cl, Scanner scanner,
      @SuppressWarnings("deprecation") org.apache.accumulo.core.util.interpret.ScanInterpreter interpeter) {
    String cf = "";
    String cq = "";
    if (cl.hasOption(scanOptCf.getOpt())) {
      cf = cl.getOptionValue(scanOptCf.getOpt());
    }
    if (cl.hasOption(scanOptCq.getOpt())) {
      cq = cl.getOptionValue(scanOptCq.getOpt());
    }

    if (cf.isEmpty() && !cq.isEmpty()) {
      String formattedString = String.format("Option -%s is required when using -%s.",
          scanOptCf.getOpt(), scanOptCq.getOpt());
      throw new IllegalArgumentException(formattedString);
    } else if (!cf.isEmpty() && cq.isEmpty()) {
      @SuppressWarnings("deprecation")
      var interprettedCF = interpeter.interpretColumnFamily(new Text(cf.getBytes(Shell.CHARSET)));
      scanner.fetchColumnFamily(interprettedCF);
    } else if (!cf.isEmpty() && !cq.isEmpty()) {
      @SuppressWarnings("deprecation")
      var interprettedCF = interpeter.interpretColumnFamily(new Text(cf.getBytes(Shell.CHARSET)));
      @SuppressWarnings("deprecation")
      var interprettedCQ =
          interpeter.interpretColumnQualifier(new Text(cq.getBytes(Shell.CHARSET)));
      scanner.fetchColumn(interprettedCF, interprettedCQ);

    }

  }

  protected Range getRange(final CommandLine cl,
      @SuppressWarnings("deprecation") final org.apache.accumulo.core.util.interpret.ScanInterpreter formatter)
      throws UnsupportedEncodingException {
    if ((cl.hasOption(OptUtil.START_ROW_OPT) || cl.hasOption(OptUtil.END_ROW_OPT))
        && cl.hasOption(scanOptRow.getOpt())) {
      // did not see a way to make commons cli do this check... it has mutually exclusive options
      // but does not support the or
      throw new IllegalArgumentException("Options -" + scanOptRow.getOpt() + " AND (-"
          + OptUtil.START_ROW_OPT + " OR -" + OptUtil.END_ROW_OPT + ") are mutually exclusive ");
    }

    if (cl.hasOption(scanOptRow.getOpt())) {
      @SuppressWarnings("deprecation")
      var interprettedRow = formatter
          .interpretRow(new Text(cl.getOptionValue(scanOptRow.getOpt()).getBytes(Shell.CHARSET)));
      return new Range(interprettedRow);
    } else {
      Text startRow = OptUtil.getStartRow(cl);
      if (startRow != null) {
        @SuppressWarnings("deprecation")
        var interprettedBeginRow = formatter.interpretBeginRow(startRow);
        startRow = interprettedBeginRow;
      }
      Text endRow = OptUtil.getEndRow(cl);
      if (endRow != null) {
        @SuppressWarnings("deprecation")
        var interprettedEndRow = formatter.interpretEndRow(endRow);
        endRow = interprettedEndRow;
      }
      final boolean startInclusive = !cl.hasOption(optStartRowExclusive.getOpt());
      final boolean endInclusive = !cl.hasOption(optEndRowExclusive.getOpt());
      return new Range(startRow, startInclusive, endRow, endInclusive);
    }
  }

  protected Authorizations getAuths(final CommandLine cl, final Shell shellState)
      throws AccumuloSecurityException, AccumuloException {
    final String user = shellState.getAccumuloClient().whoami();
    Authorizations auths =
        shellState.getAccumuloClient().securityOperations().getUserAuthorizations(user);
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

    scanOptAuths = new Option("s", "scan-authorizations", true,
        "scan authorizations (all user auths are used if this argument is not specified)");
    optStartRowExclusive = new Option("be", "begin-exclusive", false,
        "make start row exclusive (by default it's inclusive)");
    optStartRowExclusive.setArgName("begin-exclusive");
    optEndRowExclusive = new Option("ee", "end-exclusive", false,
        "make end row exclusive (by default it's inclusive)");
    optEndRowExclusive.setArgName("end-exclusive");
    scanOptRow = new Option("r", "row", true, "row to scan");
    scanOptColumns = new Option("c", "columns", true,
        "comma-separated columns. This option is mutually exclusive with cf and cq");
    scanOptCf = new Option("cf", "column-family", true, "column family to scan.");
    scanOptCq = new Option("cq", "column-qualifier", true, "column qualifier to scan");

    timestampOpt = new Option("st", "show-timestamps", false, "display timestamps");
    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    showFewOpt = new Option("f", "show-few", true, "show only a specified number of characters");
    formatterOpt =
        new Option("fm", "formatter", true, "fully qualified name of the formatter class to use");
    interpreterOpt = new Option("i", "interpreter", true,
        "fully qualified name of the interpreter class to use");
    formatterInterpeterOpt = new Option("fi", "fmt-interpreter", true,
        "fully qualified name of a class that is a formatter and interpreter");
    timeoutOption = new Option(null, "timeout", true,
        "time before scan should fail if no data is returned. If no unit is"
            + " given assumes seconds. Units d,h,m,s,and ms are supported. e.g. 30s or 100ms");
    outputFileOpt = new Option("o", "output", true, "local file to write the scan output to");
    sampleOpt = new Option(null, "sample", false, "Show sample");
    contextOpt = new Option("cc", "context", true, "name of the classloader context");
    executionHintsOpt = new Option(null, "execution-hints", true, "Execution hints map");
    scanServerOpt =
        new Option("cl", "consistency-level", true, "set consistency level (experimental)");

    scanOptAuths.setArgName("comma-separated-authorizations");
    scanOptRow.setArgName("row");
    scanOptColumns
        .setArgName("<columnfamily>[:<columnqualifier>]{,<columnfamily>[:<columnqualifier>]}");
    scanOptCf.setArgName("column-family");
    scanOptCq.setArgName("column-qualifier");
    showFewOpt.setRequired(false);
    showFewOpt.setArgName("int");
    formatterOpt.setArgName("className");
    timeoutOption.setArgName("timeout");
    outputFileOpt.setArgName("file");
    contextOpt.setArgName("context");
    executionHintsOpt.setArgName("<key>=<value>{,<key>=<value>}");
    scanServerOpt.setArgName("immediate|eventual");

    profileOpt = new Option("pn", "profile", true, "iterator profile name");
    profileOpt.setArgName("profile");

    o.addOption(scanOptAuths);
    o.addOption(scanOptRow);
    optStartRowInclusive =
        new Option(OptUtil.START_ROW_OPT, "begin-row", true, "begin row (inclusive)");
    optStartRowInclusive.setArgName("begin-row");
    o.addOption(optStartRowInclusive);
    o.addOption(OptUtil.endRowOpt());
    o.addOption(optStartRowExclusive);
    o.addOption(optEndRowExclusive);
    o.addOption(scanOptColumns);
    o.addOption(scanOptCf);
    o.addOption(scanOptCq);
    o.addOption(timestampOpt);
    o.addOption(disablePaginationOpt);
    o.addOption(OptUtil.tableOpt("table to be scanned"));
    o.addOption(showFewOpt);
    o.addOption(formatterOpt);
    o.addOption(interpreterOpt);
    o.addOption(formatterInterpeterOpt);
    o.addOption(timeoutOption);
    if (Arrays.asList(ScanCommand.class.getName(), GrepCommand.class.getName(),
        EGrepCommand.class.getName()).contains(this.getClass().getName())) {
      // supported subclasses must handle the output file option properly
      // only add this option to commands which handle it correctly
      o.addOption(outputFileOpt);
    }
    o.addOption(profileOpt);
    o.addOption(sampleOpt);
    o.addOption(contextOpt);
    o.addOption(executionHintsOpt);
    o.addOption(scanServerOpt);

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

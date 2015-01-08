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
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.GrepIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.Formatter;
import org.apache.accumulo.core.util.interpret.ScanInterpreter;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.PrintFile;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class GrepCommand extends ScanCommand {

  private Option numThreadsOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    final PrintFile printFile = getOutputFile(cl);

    final String tableName = OptUtil.getTableOpt(cl, shellState);

    if (cl.getArgList().isEmpty()) {
      throw new MissingArgumentException("No terms specified");
    }
    final Class<? extends Formatter> formatter = getFormatter(cl, tableName, shellState);
    final ScanInterpreter interpeter = getInterpreter(cl, tableName, shellState);

    // handle first argument, if present, the authorizations list to
    // scan with
    int numThreads = 20;
    if (cl.hasOption(numThreadsOpt.getOpt())) {
      numThreads = Integer.parseInt(cl.getOptionValue(numThreadsOpt.getOpt()));
    }
    final Authorizations auths = getAuths(cl, shellState);
    final BatchScanner scanner = shellState.getConnector().createBatchScanner(tableName, auths, numThreads);
    scanner.setRanges(Collections.singletonList(getRange(cl, interpeter)));

    scanner.setTimeout(getTimeout(cl), TimeUnit.MILLISECONDS);

    for (int i = 0; i < cl.getArgs().length; i++) {
      setUpIterator(Integer.MAX_VALUE - cl.getArgs().length + i, "grep" + i, cl.getArgs()[i], scanner, cl);
    }
    try {
      // handle columns
      fetchColumns(cl, scanner, interpeter);

      // output the records
      printRecords(cl, shellState, scanner, formatter, printFile);
    } finally {
      scanner.close();
    }

    return 0;
  }

  protected void setUpIterator(final int prio, final String name, final String term, final BatchScanner scanner, CommandLine cl) throws IOException {
    if (prio < 0) {
      throw new IllegalArgumentException("Priority < 0 " + prio);
    }
    final IteratorSetting grep = new IteratorSetting(prio, name, GrepIterator.class);
    GrepIterator.setTerm(grep, term);
    scanner.addScanIterator(grep);
  }

  @Override
  public String description() {
    return "searches each row, column family, column qualifier and value in a table for a substring (not a regular expression), in parallel, on the server side";
  }

  @Override
  public Options getOptions() {
    final Options opts = super.getOptions();
    numThreadsOpt = new Option("nt", "num-threads", true, "number of threads to use");
    opts.addOption(numThreadsOpt);
    return opts;
  }

  @Override
  public String usage() {
    return getName() + " <term>{ <term>}";
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }
}

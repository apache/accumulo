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

import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.format.DeleterFormatter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class DeleteManyCommand extends ScanCommand {
  private Option forceOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws Exception {
    final String tableName = OptUtil.getTableOpt(cl, shellState);

    @SuppressWarnings("deprecation")
    final org.apache.accumulo.core.util.interpret.ScanInterpreter interpeter =
        getInterpreter(cl, tableName, shellState);

    // handle first argument, if present, the authorizations list to
    // scan with
    final Authorizations auths = getAuths(cl, shellState);
    final Scanner scanner = shellState.getAccumuloClient().createScanner(tableName, auths);

    scanner.addScanIterator(
        new IteratorSetting(Integer.MAX_VALUE, "NOVALUE", SortedKeyIterator.class));

    // handle session-specific scan iterators
    addScanIterators(shellState, cl, scanner, tableName);

    // handle remaining optional arguments
    scanner.setRange(getRange(cl, interpeter));

    scanner.setTimeout(getTimeout(cl), TimeUnit.MILLISECONDS);

    // handle columns
    fetchColumns(cl, scanner, interpeter);

    // output / delete the records
    final BatchWriter writer = shellState.getAccumuloClient().createBatchWriter(tableName,
        new BatchWriterConfig().setTimeout(getTimeout(cl), TimeUnit.MILLISECONDS));
    FormatterConfig config = new FormatterConfig();
    config.setPrintTimestamps(cl.hasOption(timestampOpt.getOpt()));
    shellState.printLines(
        new DeleterFormatter(writer, scanner, config, shellState, cl.hasOption(forceOpt.getOpt())),
        false);

    return 0;
  }

  @Override
  public String description() {
    return "scans a table and deletes the resulting records";
  }

  @Override
  public Options getOptions() {
    forceOpt = new Option("f", "force", false, "force deletion without prompting");
    final Options opts = super.getOptions();
    opts.addOption(forceOpt);
    opts.addOption(OptUtil.tableOpt("table to delete entries from"));
    return opts;
  }

}

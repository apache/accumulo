/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.shell.commands;

import java.io.IOException;
import java.util.Base64;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.core.util.format.DefaultFormatter;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.Shell.Command;
import org.apache.accumulo.shell.Shell.PrintFile;
import org.apache.accumulo.shell.Shell.PrintLine;
import org.apache.accumulo.shell.Shell.PrintShell;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.io.Text;

public class GetSplitsCommand extends Command {

  private Option outputFileOpt, maxSplitsOpt, base64Opt, verboseOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState)
      throws IOException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
    final String tableName = OptUtil.getTableOpt(cl, shellState);

    final String outputFile = cl.getOptionValue(outputFileOpt.getOpt());
    final String m = cl.getOptionValue(maxSplitsOpt.getOpt());
    final int maxSplits = m == null ? 0 : Integer.parseInt(m);
    final boolean encode = cl.hasOption(base64Opt.getOpt());
    final boolean verbose = cl.hasOption(verboseOpt.getOpt());

    try (PrintLine p =
        outputFile == null ? new PrintShell(shellState.getReader()) : new PrintFile(outputFile)) {
      if (verbose) {
        String systemTableToCheck =
            MetadataTable.NAME.equals(tableName) ? RootTable.NAME : MetadataTable.NAME;
        final Scanner scanner =
            shellState.getAccumuloClient().createScanner(systemTableToCheck, Authorizations.EMPTY);
        TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
        final Text start =
            new Text(shellState.getAccumuloClient().tableOperations().tableIdMap().get(tableName));
        final Text end = new Text(start);
        end.append(new byte[] {'<'}, 0, 1);
        scanner.setRange(new Range(start, end));
        for (final Entry<Key,Value> next : scanner) {
          if (TabletColumnFamily.PREV_ROW_COLUMN.hasColumns(next.getKey())) {
            KeyExtent extent = KeyExtent.fromMetaPrevRow(next);
            final String pr = encode(encode, extent.prevEndRow());
            final String er = encode(encode, extent.endRow());
            final String line =
                String.format("%-26s (%s, %s%s", extent.obscured(), pr == null ? "-inf" : pr,
                    er == null ? "+inf" : er, er == null ? ") Default Tablet " : "]");
            p.print(line);
          }
        }
      } else {
        for (Text row : maxSplits > 0
            ? shellState.getAccumuloClient().tableOperations().listSplits(tableName, maxSplits)
            : shellState.getAccumuloClient().tableOperations().listSplits(tableName)) {
          p.print(encode(encode, row));
        }
      }

    }

    return 0;
  }

  private static String encode(final boolean encode, final Text text) {
    if (text == null) {
      return null;
    }
    final int length = text.getLength();
    return encode ? Base64.getEncoder().encodeToString(TextUtil.getBytes(text))
        : DefaultFormatter.appendText(new StringBuilder(), text, length).toString();
  }

  @Override
  public String description() {
    return "retrieves the current split points for tablets in the current table";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options opts = new Options();

    outputFileOpt = new Option("o", "output", true, "local file to write the splits to");
    outputFileOpt.setArgName("file");

    maxSplitsOpt =
        new Option("m", "max", true, "maximum number of splits to return (evenly spaced)");
    maxSplitsOpt.setArgName("num");

    base64Opt = new Option("b64", "base64encoded", false, "encode the split points");

    verboseOpt =
        new Option("v", "verbose", false, "print out the tablet information with start/end rows");

    opts.addOption(outputFileOpt);
    opts.addOption(maxSplitsOpt);
    opts.addOption(base64Opt);
    opts.addOption(verboseOpt);
    opts.addOption(OptUtil.tableOpt("table to get splits for"));

    return opts;
  }
}

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
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.ShellCommandException;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class HelpCommand extends Command {
  private Option disablePaginationOpt;
  private Option noWrapOpt;

  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws ShellCommandException, IOException {
    int numColumns = shellState.getReader().getTerminal().getWidth();
    if (cl.hasOption(noWrapOpt.getOpt())) {
      numColumns = Integer.MAX_VALUE;
    }
    // print help summary
    if (cl.getArgs().length == 0) {
      int i = 0;
      for (String cmd : shellState.commandFactory.keySet()) {
        i = Math.max(i, cmd.length());
      }
      if (numColumns < 40) {
        throw new IllegalArgumentException("numColumns must be at least 40 (was " + numColumns + ")");
      }
      final ArrayList<String> output = new ArrayList<String>();
      for (Entry<String,Command[]> cmdGroup : shellState.commandGrouping.entrySet()) {
        output.add(cmdGroup.getKey());
        for (Command c : cmdGroup.getValue()) {
          String n = c.getName();
          String s = c.description();
          if (s == null) {
            s = "";
          }
          int beginIndex = 0;
          int endIndex = s.length();
          while (beginIndex < endIndex && s.charAt(beginIndex) == ' ')
            beginIndex++;
          String dash = "-";
          while (endIndex > beginIndex && endIndex - beginIndex + i + 5 > numColumns) {
            endIndex = s.lastIndexOf(" ", numColumns + beginIndex - i - 5);
            if (endIndex == -1 || endIndex < beginIndex) {
              endIndex = numColumns + beginIndex - i - 5 - 1;
              output.add(String.format("%-" + i + "s  %s  %s-", n, dash, s.substring(beginIndex, endIndex)));
              dash = " ";
              beginIndex = endIndex;
            } else {
              output.add(String.format("%-" + i + "s  %s  %s", n, dash, s.substring(beginIndex, endIndex)));
              dash = " ";
              beginIndex = endIndex + 1;
            }
            n = "";
            endIndex = s.length();
            while (beginIndex < endIndex && s.charAt(beginIndex) == ' ') {
              beginIndex++;
            }
          }
          output.add(String.format("%-" + i + "s  %s  %s", n, dash, s.substring(beginIndex, endIndex)));
        }
        output.add("");
      }
      shellState.printLines(output.iterator(), !cl.hasOption(disablePaginationOpt.getOpt()));
    }

    // print help for every command on command line
    for (String cmd : cl.getArgs()) {
      final Command c = shellState.commandFactory.get(cmd);
      if (c == null) {
        shellState.getReader().println(String.format("Unknown command \"%s\".  Enter \"help\" for a list possible commands.", cmd));
      } else {
        c.printHelp(shellState, numColumns);
      }
    }
    return 0;
  }

  public String description() {
    return "provides information about the available commands";
  }

  public void registerCompletion(final Token root, final Map<Command.CompletionSet,Set<String>> special) {
    registerCompletionForCommands(root, special);
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    disablePaginationOpt = new Option("np", "no-pagination", false, "disable pagination of output");
    o.addOption(disablePaginationOpt);
    noWrapOpt = new Option("nw", "no-wrap", false, "disable wrapping of output");
    o.addOption(noWrapOpt);
    return o;
  }

  @Override
  public String usage() {
    return getName() + " [ <command>{ <command>} ]";
  }

  @Override
  public int numArgs() {
    return Shell.NO_FIXED_ARG_LENGTH_CHECK;
  }
}

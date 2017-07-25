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
package org.apache.accumulo.shell;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.shell.Shell.Command.CompletionSet;
import org.apache.accumulo.shell.commands.QuotedStringTokenizer;

import jline.console.completer.Completer;

public class ShellCompletor implements Completer {

  // private static final Logger log = Logger.getLogger(ShellCompletor.class);

  Map<CompletionSet,Set<String>> options;
  Token root = null;

  public ShellCompletor() {}

  public ShellCompletor(Token rootToken, Map<CompletionSet,Set<String>> options) {
    this.root = rootToken;
    this.options = options;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public int complete(String buffer, int cursor, List candidates) {
    try {
      return _complete(buffer, cursor, candidates);
    } catch (Exception e) {
      candidates.add("");
      candidates.add(e.getMessage());
      return cursor;
    }
  }

  private int _complete(String fullBuffer, int cursor, List<String> candidates) {
    boolean inTableFlag = false, inUserFlag = false, inNamespaceFlag = false;
    // Only want to grab the buffer up to the cursor because
    // the user could be trying to tab complete in the middle
    // of the line
    String buffer = fullBuffer.substring(0, cursor);

    Token current_command_token = root;
    String current_string_token = null;
    boolean end_space = buffer.endsWith(" ");

    // tabbing with no text
    if (buffer.length() == 0) {
      candidates.addAll(root.getSubcommandNames());
      return 0;
    }

    String prefix = "";

    QuotedStringTokenizer qst = new QuotedStringTokenizer(buffer);

    Iterator<String> iter = qst.iterator();
    while (iter.hasNext()) {
      current_string_token = iter.next();
      current_string_token = current_string_token.replaceAll("([\\s'\"])", "\\\\$1");

      if (!iter.hasNext()) {
        // if we end in a space and that space isn't part of the last token
        // (which would be the case at the start of a quote) OR the buffer
        // ends with a " indicating that we need to start on the next command
        // and not complete the current command.
        if (end_space && !current_string_token.endsWith(" ") || buffer.endsWith("\"")) {
          // match subcommands

          // we're in a subcommand so try to match the universal
          // option flags if we're there
          if (current_string_token.trim().equals("-" + Shell.tableOption)) {
            candidates.addAll(options.get(Shell.Command.CompletionSet.TABLENAMES));
            prefix += "-" + Shell.tableOption + " ";
          } else if (current_string_token.trim().equals("-" + Shell.userOption)) {
            candidates.addAll(options.get(Shell.Command.CompletionSet.USERNAMES));
            prefix += "-" + Shell.userOption + " ";
          } else if (current_string_token.trim().equals("-" + Shell.namespaceOption)) {
            candidates.addAll(options.get(Shell.Command.CompletionSet.NAMESPACES));
            prefix += "-" + Shell.namespaceOption + " ";
          } else if (current_command_token != null) {
            Token next = current_command_token.getSubcommand(current_string_token);
            if (next != null) {
              current_command_token = next;

              if (current_command_token.getCaseSensitive())
                prefix += current_string_token + " ";
              else
                prefix += current_string_token.toUpperCase() + " ";

              candidates.addAll(current_command_token.getSubcommandNames());
            }
          }
          Collections.sort(candidates);
          return (prefix.length());
        }
        // need to match current command
        // if we're in -t <table>, -u <user>, or -tn <namespace> complete those
        if (inTableFlag) {
          for (String a : options.get(Shell.Command.CompletionSet.TABLENAMES))
            if (a.startsWith(current_string_token))
              candidates.add(a);
        } else if (inUserFlag) {
          for (String a : options.get(Shell.Command.CompletionSet.USERNAMES))
            if (a.startsWith(current_string_token))
              candidates.add(a);
        } else if (inNamespaceFlag) {
          for (String a : options.get(Shell.Command.CompletionSet.NAMESPACES))
            if (a.startsWith(current_string_token))
              candidates.add(a);
        } else if (current_command_token != null)
          candidates.addAll(current_command_token.getSubcommandNames(current_string_token));

        Collections.sort(candidates);
        return (prefix.length());
      }

      if (current_string_token.trim().equals("-" + Shell.tableOption))
        inTableFlag = true;
      else if (current_string_token.trim().equals("-" + Shell.userOption))
        inUserFlag = true;
      else if (current_string_token.trim().equals("-" + Shell.namespaceOption))
        inNamespaceFlag = true;
      else
        inUserFlag = inTableFlag = inNamespaceFlag = false;

      if (current_command_token != null && current_command_token.getCaseSensitive())
        prefix += current_string_token + " ";
      else
        prefix += current_string_token.toUpperCase() + " ";

      if (current_command_token != null && current_command_token.getSubcommandNames().contains(current_string_token))
        current_command_token = current_command_token.getSubcommand(current_string_token);

    }
    return 0;
  }
}

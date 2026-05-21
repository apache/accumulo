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
package org.apache.accumulo.core.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameterized;
import com.google.auto.service.AutoService;

@AutoService(KeywordExecutable.class)
public class ShellCompletionCommand implements KeywordExecutable {

  private static final String HEADER = """
      #! /usr/bin/env bash
      #
      # Licensed to the Apache Software Foundation (ASF) under one
      # or more contributor license agreements.  See the NOTICE file
      # distributed with this work for additional information
      # regarding copyright ownership.  The ASF licenses this file
      # to you under the Apache License, Version 2.0 (the
      # "License"); you may not use this file except in compliance
      # with the License.  You may obtain a copy of the License at
      #
      #   https://www.apache.org/licenses/LICENSE-2.0
      #
      # Unless required by applicable law or agreed to in writing,
      # software distributed under the License is distributed on an
      # "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
      # KIND, either express or implied.  See the License for the
      # specific language governing permissions and limitations
      # under the License.
      #

      CMD=""

      #
      # Find the last word on the command line that does
      # not start with a hyphen.
      #
      function get_command() {
        length=${#COMP_WORDS[@]}
        for ((idx = length - 1; idx >= 0; idx--)); do
          word="${COMP_WORDS[idx]}"
          case $word in""";

  private static final String MIDDLE = """
              CMD=$word
              break
              ;;
          esac
        done
      }

      _complete_accumulo() {
        current="$2"
        get_command
        case \"$CMD\" in
      """;

  private static final String FOOTER = """
        esac
      }

      complete -F _complete_accumulo accumulo""";

  @Override
  public String keyword() {
    return "create-autocomplete-script";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.CONFIG;
  }

  @Override
  public String description() {
    return "Generates a bash completion script";
  }

  @Override
  public void execute(String[] args) throws Exception {
    Map<CommandGroup,Map<String,KeywordExecutable>> executables =
        Main.getExecutables(ClassLoader.getSystemClassLoader());
    System.out.println(HEADER);
    printKeywords(executables);
    System.out.println(MIDDLE);
    printCommands(executables);
    System.out.println(FOOTER);
  }

  private void printKeywords(Map<CommandGroup,Map<String,KeywordExecutable>> executables) {
    String caseLine = "      \"s\") ;&";
    String caseLastLine = "      \"s\")";

    List<String> knownCommands = new ArrayList<>();
    knownCommands.add("accumulo");
    executables.keySet().stream().filter(cg -> cg.key().length() > 0).map(CommandGroup::key)
        .forEach(knownCommands::add);
    executables.entrySet().forEach(e -> {
      e.getValue().values().forEach(ke -> {
        knownCommands.add(ke.keyword());
      });
    });
    Collections.sort(knownCommands);
    int length = knownCommands.size();
    for (int i = 0; i < length; i++) {
      String cmd = knownCommands.get(i);
      if (i < length - 1) {
        System.out.println(caseLine.replace("s", cmd));
      } else {
        System.out.println(caseLastLine.replace("s", cmd));
      }
    }
  }

  private void printCommands(Map<CommandGroup,Map<String,KeywordExecutable>> executables) {
    for (CommandGroup group : executables.keySet()) {
      if (group == CommandGroups.CLIENT) {
        continue;
      }
      List<String> subcommands = new ArrayList<>(executables.get(group).keySet());
      printCommand(group.key(), subcommands);
    }
    List<String> subcommands = new ArrayList<>(executables.get(CommandGroups.CLIENT).keySet());
    subcommands.addAll(executables.keySet().stream().filter(cg -> cg.key().length() > 0)
        .map(CommandGroup::key).toList());
    printCommand("accumulo", subcommands);

    executables.entrySet().forEach(e -> {
      e.getValue().values().forEach(ke -> {
        printParameters(ke);
      });
    });
  }

  private void printCommand(String name, List<String> subcommands) {
    String commandText = """
            "%s")
              commands="%s"
              mapfile -t COMPREPLY < <(compgen -W "${commands}" -- "${current}")
              return 0
              ;;
        """.formatted(name, String.join(" ", subcommands));
    System.out.println(commandText);
  }

  private void printParameters(KeywordExecutable ke) {
    List<String> params = getParameterNames(ke);
    if (params == null || params.isEmpty()) {
      return;
    }
    String parameterText = """
            "%s")
              parameters="%s"
              mapfile -t COMPREPLY < <(compgen -W "${parameters}" -- "${current}")
              return 0
              ;;
        """.formatted(ke.keyword(), String.join(" ", params));
    System.out.println(parameterText);
  }

  private List<String> getParameterNames(KeywordExecutable ke) {
    Object options = ke.getOptions();
    if (options == null) {
      return null;
    }
    List<String> names = new ArrayList<>();
    JCommander cl = new JCommander(options);
    Map<Parameterized,ParameterDescription> parameters = cl.getFields();
    parameters.values().forEach(pd -> names.addAll(Arrays.asList(pd.getParameter().names())));
    return names;
  }
}

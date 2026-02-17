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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.accumulo.start.Main;
import org.apache.accumulo.start.spi.CommandGroup;
import org.apache.accumulo.start.spi.CommandGroups;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.zookeeper.common.StringUtils;

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
      """;

  private static final String FOOTER = """
      complete -F _complete_accumulo accumulo
      """;

  @Override
  public String keyword() {
    return "create-autocomplete-script";
  }

  @Override
  public CommandGroup commandGroup() {
    return CommandGroups.OTHER;
  }

  @Override
  public String description() {
    return "Generates a bash completion script";
  }

  @Override
  public void execute(String[] args) throws Exception {
    System.out.println(HEADER);
    printCompletionMethod();
    System.out.println(FOOTER);
  }

  private void printCompletionMethod() {
    Map<CommandGroup,Map<String,KeywordExecutable>> executables =
        Main.getExecutables(ClassLoader.getSystemClassLoader());

    System.out.println("_complete_accumulo() {");
    System.out.println("  prior=\"$3\"");
    System.out.println("  current=\"$2\"");

    System.out.println("  case \"$prior\" in");
    for (CommandGroup group : executables.keySet()) {
      if (group == CommandGroups.CLIENT) {
        continue;
      }
      List<String> cmds = new ArrayList<>(executables.get(group).keySet());
      System.out.println("    " + group.key() + ")");
      System.out.println("      commands=\"" + StringUtils.joinStrings(cmds, " ") + "\"");
      System.out
          .println("      mapfile -t COMPREPLY < <(compgen -W \"${commands}\" -- \"${current}\")");
      System.out.println("      return 0");
      System.out.println("      ;;");
    }
    List<String> cmds = new ArrayList<>(executables.get(CommandGroups.CLIENT).keySet());
    cmds.addAll(executables.keySet().stream().map(cg -> cg.key()).collect(Collectors.toList()));
    System.out.println("    *)");
    System.out.println("      commands=\"" + StringUtils.joinStrings(cmds, " ") + "\"");
    System.out
        .println("      mapfile -t COMPREPLY < <(compgen -W \"${commands}\" -- \"${current}\")");
    System.out.println("      return 0");
    System.out.println("      ;;");
    System.out.println("  esac");
    System.out.println("}\n");
  }
}

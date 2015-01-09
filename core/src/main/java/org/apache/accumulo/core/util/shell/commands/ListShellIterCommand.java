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

import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 *
 */
public class ListShellIterCommand extends Command {

  private Option nameOpt, profileOpt;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws Exception {
    if (shellState.iteratorProfiles.size() == 0)
      return 0;

    final StringBuilder sb = new StringBuilder();

    String profile = null;
    if (cl.hasOption(profileOpt.getOpt()))
      profile = cl.getOptionValue(profileOpt.getOpt());

    String name = null;
    if (cl.hasOption(nameOpt.getOpt()))
      name = cl.getOptionValue(nameOpt.getOpt());

    Set<Entry<String,List<IteratorSetting>>> es = shellState.iteratorProfiles.entrySet();
    for (Entry<String,List<IteratorSetting>> entry : es) {
      if (profile != null && !profile.equals(entry.getKey()))
        continue;

      sb.append("-\n");
      sb.append("- Profile : " + entry.getKey() + "\n");
      for (IteratorSetting setting : entry.getValue()) {
        if (name != null && !name.equals(setting.getName()))
          continue;

        sb.append("-    Iterator ").append(setting.getName()).append(", ").append(" options:\n");
        sb.append("-        ").append("iteratorPriority").append(" = ").append(setting.getPriority()).append("\n");
        sb.append("-        ").append("iteratorClassName").append(" = ").append(setting.getIteratorClass()).append("\n");
        for (Entry<String,String> optEntry : setting.getOptions().entrySet()) {
          sb.append("-        ").append(optEntry.getKey()).append(" = ").append(optEntry.getValue()).append("\n");
        }
      }
    }

    if (sb.length() > 0) {
      sb.append("-\n");
    }

    shellState.getReader().print(sb.toString());

    return 0;
  }

  public String description() {
    return "lists iterators profiles configured in shell";
  }

  @Override
  public int numArgs() {
    return 0;
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();

    profileOpt = new Option("pn", "profile", true, "iterator profile name");
    profileOpt.setArgName("profile");

    nameOpt = new Option("n", "name", true, "iterator to list");
    nameOpt.setArgName("itername");

    o.addOption(profileOpt);
    o.addOption(nameOpt);

    return o;
  }
}

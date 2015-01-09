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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.util.shell.Shell;
import org.apache.accumulo.core.util.shell.Shell.Command;
import org.apache.accumulo.core.util.shell.Token;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CloneTableCommand extends Command {

  private Option setPropsOption;
  private Option excludePropsOption;
  private Option noFlushOption;

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, TableExistsException {

    final HashMap<String,String> props = new HashMap<String,String>();
    final HashSet<String> exclude = new HashSet<String>();
    boolean flush = true;

    if (cl.hasOption(setPropsOption.getOpt())) {
      String[] keyVals = cl.getOptionValue(setPropsOption.getOpt()).split(",");
      for (String keyVal : keyVals) {
        String[] sa = keyVal.split("=");
        props.put(sa[0], sa[1]);
      }
    }

    if (cl.hasOption(excludePropsOption.getOpt())) {
      String[] keys = cl.getOptionValue(excludePropsOption.getOpt()).split(",");
      for (String key : keys) {
        exclude.add(key);
      }
    }

    if (cl.hasOption(noFlushOption.getOpt())) {
      flush = false;
    }

    shellState.getConnector().tableOperations().clone(cl.getArgs()[0], cl.getArgs()[1], flush, props, exclude);
    return 0;
  }

  @Override
  public String usage() {
    return getName() + " <current table name> <new table name>";
  }

  @Override
  public String description() {
    return "clones a table";
  }

  public void registerCompletion(final Token root, final Map<Command.CompletionSet,Set<String>> completionSet) {
    registerCompletionForTables(root, completionSet);
  }

  @Override
  public Options getOptions() {
    final Options o = new Options();
    setPropsOption = new Option("s", "set", true, "set initial properties before the table comes online. Expects <prop>=<value>{,<prop>=<value>}");
    o.addOption(setPropsOption);
    excludePropsOption = new Option("e", "exclude", true, "exclude properties that should not be copied from source table. Expects <prop>{,<prop>}");
    o.addOption(excludePropsOption);
    noFlushOption = new Option("nf", "noFlush", false, "do not flush table data in memory before cloning.");
    o.addOption(noFlushOption);
    return o;
  }

  @Override
  public int numArgs() {
    return 2;
  }
}

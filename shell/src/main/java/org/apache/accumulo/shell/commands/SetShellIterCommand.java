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
package org.apache.accumulo.shell.commands;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.shell.Shell;
import org.apache.accumulo.shell.ShellCommandException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetShellIterCommand extends SetIterCommand {

  private static final Logger log = LoggerFactory.getLogger(SetShellIterCommand.class);

  @Override
  public int execute(final String fullCommand, final CommandLine cl, final Shell shellState) throws AccumuloException, AccumuloSecurityException,
      TableNotFoundException, IOException, ShellCommandException {
    return super.execute(fullCommand, cl, shellState);
  }

  @Override
  protected void setTableProperties(final CommandLine cl, final Shell shellState, final int priority, final Map<String,String> options, final String classname,
      final String name) throws AccumuloException, AccumuloSecurityException, ShellCommandException, TableNotFoundException {
    // instead of setting table properties, just put the options in a list to use at scan time

    String profile = cl.getOptionValue(profileOpt.getOpt());

    // instead of setting table properties, just put the options in a list to use at scan time
    for (Iterator<Entry<String,String>> i = options.entrySet().iterator(); i.hasNext();) {
      final Entry<String,String> entry = i.next();
      if (entry.getValue() == null || entry.getValue().isEmpty()) {
        i.remove();
      }
    }

    List<IteratorSetting> tableScanIterators = shellState.iteratorProfiles.get(profile);
    if (tableScanIterators == null) {
      tableScanIterators = new ArrayList<>();
      shellState.iteratorProfiles.put(profile, tableScanIterators);
    }
    final IteratorSetting setting = new IteratorSetting(priority, name, classname);
    setting.addOptions(options);

    Iterator<IteratorSetting> iter = tableScanIterators.iterator();
    while (iter.hasNext()) {
      if (iter.next().getName().equals(name)) {
        iter.remove();
      }
    }

    tableScanIterators.add(setting);
  }

  @Override
  public String description() {
    return "adds an iterator to a profile for this shell session";
  }

  @Override
  public Options getOptions() {

    final Options o = new Options();

    profileOpt = new Option("pn", "profile", true, "iterator profile name");
    profileOpt.setRequired(true);
    profileOpt.setArgName("profile");

    priorityOpt = new Option("p", "priority", true, "the order in which the iterator is applied");
    priorityOpt.setArgName("pri");
    priorityOpt.setRequired(true);

    final OptionGroup typeGroup = new OptionGroup();
    classnameTypeOpt = new Option("class", "class-name", true, "a java class that implements SortedKeyValueIterator");
    classnameTypeOpt.setArgName("name");
    aggTypeOpt = new Option("agg", "aggregator", false, "an aggregating type");
    regexTypeOpt = new Option("regex", "regular-expression", false, "a regex matching iterator");
    versionTypeOpt = new Option("vers", "version", false, "a versioning iterator");
    reqvisTypeOpt = new Option("reqvis", "require-visibility", false, "an iterator that omits entries with empty visibilities");
    ageoffTypeOpt = new Option("ageoff", "ageoff", false, "an aging off iterator");

    typeGroup.addOption(classnameTypeOpt);
    typeGroup.addOption(aggTypeOpt);
    typeGroup.addOption(regexTypeOpt);
    typeGroup.addOption(versionTypeOpt);
    typeGroup.addOption(reqvisTypeOpt);
    typeGroup.addOption(ageoffTypeOpt);
    typeGroup.setRequired(true);

    nameOpt = new Option("n", "name", true, "iterator to set");
    nameOpt.setArgName("itername");

    o.addOption(profileOpt);
    o.addOptionGroup(typeGroup);
    o.addOption(priorityOpt);
    o.addOption(nameOpt);

    return o;
  }

}
